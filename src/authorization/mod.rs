//! Autorización explícita por uso (SPEC-AUTH-0001, DEC-0099).
//!
//! Todo contenido del usuario que salga del Navigator hacia otro nodo lleva un
//! [`Grant`]: un permiso de un solo uso ligado a los bytes exactos (digest), al
//! nodo exacto (DID) y a un vencimiento. Solo el [`Authorizer`] los emite, y
//! solo el [`dispatcher::Dispatcher`] los consume; el resto del runtime no
//! puede enviar nada por su cuenta.
//!
//! - Explícitos: el usuario decide cada ítem de una solicitud
//!   (`authorization.requested` → `authorization.decision`).
//! - Implícito (P5): el mensaje literal del usuario al Star que eligió, dentro
//!   de su ámbito de privacidad.
//! - Sin cabeza: política del operador solo para sondas y pruebas sintéticas.

pub mod dispatcher;

pub use galaxia_fhs::authorization::DOMAIN_TOOL_ARGS;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::p2p::peer_cache::{now_ms, PeerEntry};
use crate::protocol::fhs::{
    self, AuthorizationDataClass as DataClass, AuthorizationDestination as Destination,
    AuthorizationOutcome as Outcome, AuthorizationRetention as Retention,
};
use crate::runtime::events::{AgentEvent, EventSink};
use crate::runtime::providers::{self, Scope};
use galaxia_fhs::authorization as digest;

/// Digest de los argumentos de una herramienta (clase `tool_args`).
pub fn tool_args_digest(
    value: &crate::protocol::fhs::DynamicValue,
) -> Result<[u8; 32], digest::DigestError> {
    digest::value_digest(DOMAIN_TOOL_ARGS, value)
}

/// Vigencia de una solicitud sin respuesta.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);
/// Vigencia de un `Grant` desde que se emite.
const GRANT_LIFETIME: Duration = Duration::from_secs(60);
const MAX_RECORDS: usize = 256;
pub const POLICY_VERSION: &str = "auth-1/lista-del-operador";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("solicitud de autorización inválida: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrantError {
    #[error("el permiso venció")]
    Expired,
    #[error("el permiso ya se usó")]
    AlreadyConsumed,
    #[error("el contenido que sale no coincide con lo autorizado")]
    DigestMismatch,
    #[error("el nodo destino no es el autorizado")]
    ProviderMismatch,
    #[error("el permiso no cubre esta capacidad")]
    CapabilityMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecideError {
    #[error("autorización desconocida, repetida o vencida")]
    Unknown,
    #[error("la decisión es de otra sesión")]
    WrongSession,
    #[error("el digest del lote no coincide")]
    BatchMismatch,
    #[error("la decisión nombra un ítem que no existe")]
    UnknownItem,
}

/// Un envío concreto que se pide autorizar (los bytes ya existen).
#[derive(Clone, Debug)]
pub struct ItemSpec {
    pub item_id: String,
    pub capability: String,
    pub provider_did: String,
    pub provider_name: String,
    pub data_class: DataClass,
    /// Tipo, tamaño y caracteres; nunca el contenido.
    pub summary: String,
    pub digest: [u8; 32],
    pub destination: Destination,
    pub retention: Retention,
    pub depends_on: Vec<String>,
    pub public_network: bool,
    pub failover: bool,
    pub side_effects: bool,
    pub retry: bool,
}

impl ItemSpec {
    pub fn new(
        item_id: impl Into<String>,
        capability: impl Into<String>,
        provider_did: impl Into<String>,
        provider_name: impl Into<String>,
        data_class: DataClass,
        summary: impl Into<String>,
        digest: [u8; 32],
    ) -> Self {
        Self {
            item_id: item_id.into(),
            capability: capability.into(),
            provider_did: provider_did.into(),
            provider_name: provider_name.into(),
            data_class,
            summary: summary.into(),
            digest,
            destination: Destination::Network,
            retention: Retention::Ephemeral,
            depends_on: vec![],
            public_network: false,
            failover: false,
            side_effects: false,
            retry: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Basis {
    Explicit,
    Implicit,
    Headless,
}

impl Basis {
    fn as_str(self) -> &'static str {
        match self {
            Basis::Explicit => "explicit",
            Basis::Implicit => "implicit",
            Basis::Headless => "headless",
        }
    }
}

struct GrantInner {
    authorization_id: String,
    item_id: String,
    capability: String,
    provider_did: String,
    digest: [u8; 32],
    data_class: DataClass,
    basis: Basis,
    expires: Instant,
    consumed: AtomicBool,
    shared: Arc<Shared>,
}

/// Permiso de un solo uso. Sus campos son privados y solo el [`Authorizer`]
/// lo construye.
#[derive(Clone)]
pub struct Grant {
    inner: Arc<GrantInner>,
}

impl std::fmt::Debug for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Grant")
            .field("authorization_id", &self.inner.authorization_id)
            .field("item_id", &self.inner.item_id)
            .field("capability", &self.inner.capability)
            .field("provider_did", &self.inner.provider_did)
            .finish_non_exhaustive()
    }
}

impl Grant {
    pub fn provider_did(&self) -> &str {
        &self.inner.provider_did
    }
    pub fn capability(&self) -> &str {
        &self.inner.capability
    }
    pub fn digest(&self) -> [u8; 32] {
        self.inner.digest
    }
    pub fn basis(&self) -> Basis {
        self.inner.basis
    }
    pub fn item_id(&self) -> &str {
        &self.inner.item_id
    }
    pub fn authorization_id(&self) -> &str {
        &self.inner.authorization_id
    }

    /// Como [`Grant::consume`] pero sin consumir: valida un lote completo antes
    /// de gastar ninguno de sus permisos.
    pub(crate) fn check(
        &self,
        capability: &str,
        provider_did: &str,
        digest_now: [u8; 32],
    ) -> Result<(), GrantError> {
        let inner = &self.inner;
        if Instant::now() >= inner.expires {
            return Err(GrantError::Expired);
        }
        if inner.consumed.load(Ordering::SeqCst) {
            return Err(GrantError::AlreadyConsumed);
        }
        if inner.capability != capability {
            return Err(GrantError::CapabilityMismatch);
        }
        if inner.provider_did != provider_did {
            return Err(GrantError::ProviderMismatch);
        }
        if inner.digest != digest_now {
            return Err(GrantError::DigestMismatch);
        }
        Ok(())
    }

    /// Valida lo que va a salir contra lo autorizado y consume el permiso de
    /// forma atómica **antes** de escribir en el transporte. Cualquier fallo
    /// posterior (acuse perdido, caída) no devuelve el permiso: reintentar
    /// exige una autorización nueva.
    pub(crate) fn consume(
        &self,
        capability: &str,
        provider_did: &str,
        digest_now: [u8; 32],
    ) -> Result<(), GrantError> {
        let inner = &self.inner;
        if Instant::now() >= inner.expires {
            inner.shared.mark(
                &inner.authorization_id,
                &inner.item_id,
                Outcome::Expired,
                "vencido",
            );
            return Err(GrantError::Expired);
        }
        if inner.capability != capability {
            return Err(GrantError::CapabilityMismatch);
        }
        if inner.provider_did != provider_did {
            return Err(GrantError::ProviderMismatch);
        }
        if inner.digest != digest_now {
            return Err(GrantError::DigestMismatch);
        }
        if inner.consumed.swap(true, Ordering::SeqCst) {
            return Err(GrantError::AlreadyConsumed);
        }
        inner.shared.mark(
            &inner.authorization_id,
            &inner.item_id,
            Outcome::Consumed,
            "en envío",
        );
        inner.shared.audit(json!({
            "event": "consumed",
            "authorization_id": inner.authorization_id,
            "item_id": inner.item_id,
            "capability": inner.capability,
            "provider_did": inner.provider_did,
            "data_class": inner.data_class.as_str_name(),
            "digest": hex(&inner.digest),
            "basis": inner.basis.as_str(),
        }));
        Ok(())
    }

    /// Resultado del envío (`SENT` o `FAILED` con el motivo).
    pub(crate) fn finish(&self, outcome: Outcome, reason: &str) {
        let inner = &self.inner;
        inner
            .shared
            .mark(&inner.authorization_id, &inner.item_id, outcome, reason);
        inner.shared.audit(json!({
            "event": "finished",
            "authorization_id": inner.authorization_id,
            "item_id": inner.item_id,
            "outcome": outcome.as_str_name(),
            "reason": reason,
        }));
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Resultado de un ítem tras la decisión (o su falta).
#[derive(Debug, Clone)]
pub enum ItemOutcome {
    Granted(Grant),
    Denied(String),
    Expired,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct ItemResult {
    pub item_id: String,
    pub outcome: ItemOutcome,
}

#[derive(Debug, Clone)]
pub struct Resolution {
    pub authorization_id: String,
    pub items: Vec<ItemResult>,
}

impl Resolution {
    pub fn grant(&self, item_id: &str) -> Option<&Grant> {
        self.items.iter().find_map(|r| match &r.outcome {
            ItemOutcome::Granted(grant) if r.item_id == item_id => Some(grant),
            _ => None,
        })
    }

    pub fn all_expired(&self) -> bool {
        !self.items.is_empty()
            && self
                .items
                .iter()
                .all(|r| matches!(r.outcome, ItemOutcome::Expired))
    }

    pub fn all_cancelled(&self) -> bool {
        !self.items.is_empty()
            && self
                .items
                .iter()
                .all(|r| matches!(r.outcome, ItemOutcome::Cancelled))
    }
}

/// Dónde y para quién se pide la autorización.
pub struct Ctx<'a> {
    /// Sesión del Portal (la decisión solo vale para ella).
    pub session: &'a str,
    pub conversation: &'a str,
    pub turn: &'a str,
    pub sink: &'a dyn EventSink,
}

/// Política sin interfaz (sondas y pruebas sintéticas): por defecto deniega.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum HeadlessPolicy {
    #[default]
    Deny,
    AllowSynthetic,
}

impl HeadlessPolicy {
    pub fn from_env() -> Self {
        match std::env::var("FHS_AUTH_POLICY").ok().as_deref() {
            Some("allow-synthetic") => Self::AllowSynthetic,
            _ => Self::Deny,
        }
    }
}

enum Decision {
    Decided(HashMap<String, bool>),
    Cancelled,
}

struct Pending {
    session: String,
    conversation: String,
    batch_digest: [u8; 32],
    item_ids: HashSet<String>,
    tx: oneshot::Sender<Decision>,
}

struct Record {
    session: String,
    batch: Outcome,
    items: Vec<(String, Outcome, String)>,
}

#[derive(Default)]
struct State {
    pending: HashMap<String, Pending>,
    records: HashMap<String, Record>,
    order: VecDeque<String>,
}

pub(crate) struct Shared {
    state: Mutex<State>,
    audit_file: Option<Mutex<File>>,
    trusted: Arc<HashSet<String>>,
    seen: Mutex<HashSet<String>>,
}

impl Shared {
    fn audit(&self, mut value: serde_json::Value) {
        value["ts_ms"] = json!(now_ms());
        tracing::info!(target: "authorization-audit", "{value}");
        if let Some(file) = &self.audit_file {
            if let Ok(mut file) = file.lock() {
                let _ = writeln!(file, "{value}");
            }
        }
    }

    fn mark(&self, authorization_id: &str, item_id: &str, outcome: Outcome, reason: &str) {
        let mut state = self.state.lock().expect("authorizer");
        if let Some(record) = state.records.get_mut(authorization_id) {
            if let Some(entry) = record.items.iter_mut().find(|(id, _, _)| id == item_id) {
                entry.1 = outcome;
                entry.2 = reason.to_string();
            }
        }
    }

    fn remember(&self, authorization_id: &str, record: Record) {
        let mut state = self.state.lock().expect("authorizer");
        state.records.insert(authorization_id.to_string(), record);
        state.order.push_back(authorization_id.to_string());
        while state.order.len() > MAX_RECORDS {
            if let Some(old) = state.order.pop_front() {
                state.records.remove(&old);
            }
        }
    }
}

/// Emisor único de permisos y tabla de decisiones pendientes.
#[derive(Clone)]
pub struct Authorizer {
    shared: Arc<Shared>,
    headless: Option<HeadlessPolicy>,
}

impl Authorizer {
    /// `trusted`: DIDs que el operador verificó (`FHS_TRUSTED_NODES`).
    /// `headless`: sin interfaz, solo `Some` en sondas y pruebas sintéticas.
    pub fn new(
        trusted: Arc<HashSet<String>>,
        audit_path: Option<&std::path::Path>,
        headless: Option<HeadlessPolicy>,
    ) -> Self {
        let audit_file = audit_path.and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| tracing::warn!("no se pudo abrir la bitácora de autorización: {e}"))
                .ok()
                .map(Mutex::new)
        });
        Self {
            shared: Arc::new(Shared {
                state: Mutex::default(),
                audit_file,
                trusted,
                seen: Mutex::default(),
            }),
            headless,
        }
    }

    /// Para pruebas: interactivo, sin bitácora en disco.
    pub fn for_tests() -> Self {
        Self::new(Arc::default(), None, None)
    }

    /// Para pruebas con política sin cabeza.
    pub fn headless_for_tests(policy: HeadlessPolicy) -> Self {
        Self::new(Arc::default(), None, Some(policy))
    }

    fn trust_level(&self, did: &str) -> &'static str {
        if self.shared.trusted.contains(did) {
            "operator"
        } else {
            "community"
        }
    }

    fn to_proto(&self, spec: &ItemSpec) -> fhs::AuthorizationItem {
        let first_time = !self
            .shared
            .seen
            .lock()
            .expect("seen")
            .contains(&spec.provider_did);
        fhs::AuthorizationItem {
            item_id: spec.item_id.clone(),
            capability_id: spec.capability.clone(),
            provider_did: spec.provider_did.clone(),
            provider_name: spec.provider_name.clone(),
            trust_level: self.trust_level(&spec.provider_did).into(),
            policy_version: POLICY_VERSION.into(),
            data_class: spec.data_class as i32,
            data_summary: spec.summary.clone(),
            payload_digest: spec.digest.to_vec(),
            destination: spec.destination as i32,
            retention: spec.retention as i32,
            depends_on: spec.depends_on.clone(),
            first_time_node: first_time,
            public_network: spec.public_network,
            failover: spec.failover,
            side_effects: spec.side_effects,
            retry: spec.retry,
            implicit: false,
        }
    }

    fn validate(items: &[ItemSpec]) -> Result<(), AuthError> {
        if items.is_empty() {
            return Err(AuthError::Invalid("sin ítems".into()));
        }
        let ids: HashSet<&str> = items.iter().map(|i| i.item_id.as_str()).collect();
        if ids.len() != items.len() {
            return Err(AuthError::Invalid("ítems repetidos".into()));
        }
        for item in items {
            if item.item_id.is_empty() || item.provider_did.is_empty() || item.capability.is_empty()
            {
                return Err(AuthError::Invalid(format!(
                    "ítem incompleto: {}",
                    item.item_id
                )));
            }
            if item.digest == [0; 32] {
                return Err(AuthError::Invalid(format!("sin digest: {}", item.item_id)));
            }
            for dep in &item.depends_on {
                if dep == &item.item_id || !ids.contains(dep.as_str()) {
                    return Err(AuthError::Invalid(format!(
                        "dependencia inválida en {}: {dep}",
                        item.item_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Pide autorización para un lote de ítems y espera la decisión. Sin
    /// decisión en `ttl` (reloj monotónico del Navigator), todo vence y no se
    /// envía nada.
    pub async fn request(
        &self,
        ctx: &Ctx<'_>,
        items: Vec<ItemSpec>,
        ttl: Duration,
    ) -> Result<Resolution, AuthError> {
        Self::validate(&items)?;
        let authorization_id = Uuid::new_v4().to_string();
        let expires_at = now_ms() + i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        let proto_items: Vec<fhs::AuthorizationItem> =
            items.iter().map(|i| self.to_proto(i)).collect();
        let batch_digest = digest::batch_digest(
            &authorization_id,
            ctx.conversation,
            ctx.turn,
            expires_at,
            &proto_items,
        );

        let allow: HashMap<String, bool> = match self.headless {
            Some(policy) => {
                let allowed = policy == HeadlessPolicy::AllowSynthetic;
                items.iter().map(|i| (i.item_id.clone(), allowed)).collect()
            }
            None => {
                let (tx, rx) = oneshot::channel();
                self.shared
                    .state
                    .lock()
                    .expect("authorizer")
                    .pending
                    .insert(
                        authorization_id.clone(),
                        Pending {
                            session: ctx.session.into(),
                            conversation: ctx.conversation.into(),
                            batch_digest,
                            item_ids: items.iter().map(|i| i.item_id.clone()).collect(),
                            tx,
                        },
                    );
                ctx.sink.emit(AgentEvent::AuthorizationRequested {
                    authorization_id: authorization_id.clone(),
                    conversation_id: ctx.conversation.into(),
                    turn_id: ctx.turn.into(),
                    expires_at,
                    batch_digest: batch_digest.to_vec(),
                    items: proto_items.clone(),
                });
                self.shared.audit(json!({
                    "event": "requested",
                    "authorization_id": authorization_id,
                    "items": items.iter().map(|i| json!({
                        "item_id": i.item_id, "capability": i.capability,
                        "provider_did": i.provider_did,
                        "data_class": i.data_class.as_str_name(),
                        "digest": hex(&i.digest),
                    })).collect::<Vec<_>>(),
                }));
                match tokio::time::timeout(ttl, rx).await {
                    Ok(Ok(Decision::Decided(map))) => map,
                    Ok(Ok(Decision::Cancelled)) | Ok(Err(_)) => {
                        return Ok(self.finish_without_decision(
                            ctx,
                            authorization_id,
                            &items,
                            Outcome::Cancelled,
                        ));
                    }
                    Err(_) => {
                        // Vence: quita el pendiente (si nadie lo consumió).
                        self.shared
                            .state
                            .lock()
                            .expect("authorizer")
                            .pending
                            .remove(&authorization_id);
                        return Ok(self.finish_without_decision(
                            ctx,
                            authorization_id,
                            &items,
                            Outcome::Expired,
                        ));
                    }
                }
            }
        };
        Ok(self.resolve(ctx, authorization_id, items, allow))
    }

    fn finish_without_decision(
        &self,
        ctx: &Ctx<'_>,
        authorization_id: String,
        items: &[ItemSpec],
        outcome: Outcome,
    ) -> Resolution {
        let reason = if outcome == Outcome::Expired {
            "sin respuesta a tiempo"
        } else {
            "cancelado"
        };
        let statuses: Vec<(String, Outcome, String)> = items
            .iter()
            .map(|i| (i.item_id.clone(), outcome, reason.to_string()))
            .collect();
        self.shared.remember(
            &authorization_id,
            Record {
                session: ctx.session.into(),
                batch: outcome,
                items: statuses.clone(),
            },
        );
        self.emit_resolved(ctx, &authorization_id, outcome, &statuses);
        Resolution {
            authorization_id,
            items: items
                .iter()
                .map(|i| ItemResult {
                    item_id: i.item_id.clone(),
                    outcome: if outcome == Outcome::Expired {
                        ItemOutcome::Expired
                    } else {
                        ItemOutcome::Cancelled
                    },
                })
                .collect(),
        }
    }

    fn emit_resolved(
        &self,
        ctx: &Ctx<'_>,
        authorization_id: &str,
        batch: Outcome,
        statuses: &[(String, Outcome, String)],
    ) {
        self.shared.audit(json!({
            "event": "resolved",
            "authorization_id": authorization_id,
            "outcome": batch.as_str_name(),
            "items": statuses.iter().map(|(id, o, r)| json!({
                "item_id": id, "outcome": o.as_str_name(), "reason": r
            })).collect::<Vec<_>>(),
        }));
        ctx.sink.emit(AgentEvent::AuthorizationResolved {
            authorization_id: authorization_id.into(),
            outcome: batch as i32,
            items: statuses
                .iter()
                .map(|(id, outcome, reason)| fhs::AuthorizationItemStatus {
                    item_id: id.clone(),
                    outcome: *outcome as i32,
                    reason: reason.clone(),
                })
                .collect(),
        });
    }

    /// Aplica la decisión: un ítem se permite solo si el usuario lo permitió y
    /// todas sus dependencias se permitieron (cascada).
    fn resolve(
        &self,
        ctx: &Ctx<'_>,
        authorization_id: String,
        items: Vec<ItemSpec>,
        allow: HashMap<String, bool>,
    ) -> Resolution {
        let granted = cascade(&items, &allow);
        let basis = if self.headless.is_some() {
            Basis::Headless
        } else {
            Basis::Explicit
        };
        let expires = Instant::now() + GRANT_LIFETIME;
        let mut results = Vec::new();
        let mut statuses = Vec::new();
        for item in &items {
            let outcome = match granted.get(&item.item_id) {
                Some(Ok(())) => {
                    self.shared
                        .seen
                        .lock()
                        .expect("seen")
                        .insert(item.provider_did.clone());
                    statuses.push((item.item_id.clone(), Outcome::Allowed, String::new()));
                    ItemOutcome::Granted(Grant {
                        inner: Arc::new(GrantInner {
                            authorization_id: authorization_id.clone(),
                            item_id: item.item_id.clone(),
                            capability: item.capability.clone(),
                            provider_did: item.provider_did.clone(),
                            digest: item.digest,
                            data_class: item.data_class,
                            basis,
                            expires,
                            consumed: AtomicBool::new(false),
                            shared: self.shared.clone(),
                        }),
                    })
                }
                Some(Err(reason)) => {
                    statuses.push((item.item_id.clone(), Outcome::Denied, reason.clone()));
                    ItemOutcome::Denied(reason.clone())
                }
                None => {
                    statuses.push((item.item_id.clone(), Outcome::Denied, "sin decisión".into()));
                    ItemOutcome::Denied("sin decisión".into())
                }
            };
            results.push(ItemResult {
                item_id: item.item_id.clone(),
                outcome,
            });
        }
        let allowed = statuses
            .iter()
            .filter(|(_, o, _)| *o == Outcome::Allowed)
            .count();
        let batch = match (allowed, statuses.len()) {
            (0, _) => Outcome::Denied,
            (a, n) if a == n => Outcome::Allowed,
            _ => Outcome::Partial,
        };
        self.shared.remember(
            &authorization_id,
            Record {
                session: ctx.session.into(),
                batch,
                items: statuses.clone(),
            },
        );
        self.emit_resolved(ctx, &authorization_id, batch, &statuses);
        Resolution {
            authorization_id,
            items: results,
        }
    }

    /// Decisión del cliente. Se ignora si es desconocida, repetida, vencida,
    /// de otra sesión o de otro lote.
    pub fn decide(
        &self,
        session: &str,
        message: &fhs::AuthorizationDecisionMessage,
    ) -> Result<(), DecideError> {
        let mut state = self.shared.state.lock().expect("authorizer");
        let Some(pending) = state.pending.get(&message.authorization_id) else {
            return Err(DecideError::Unknown);
        };
        if pending.session != session {
            return Err(DecideError::WrongSession);
        }
        if message.batch_digest != pending.batch_digest {
            return Err(DecideError::BatchMismatch);
        }
        if message
            .decisions
            .iter()
            .any(|d| !pending.item_ids.contains(&d.item_id))
        {
            return Err(DecideError::UnknownItem);
        }
        // Consumo atómico: la decisión se usa una sola vez.
        let pending = state
            .pending
            .remove(&message.authorization_id)
            .expect("pendiente presente");
        let map: HashMap<String, bool> = message
            .decisions
            .iter()
            .map(|d| (d.item_id.clone(), d.allow))
            .collect();
        let _ = pending.tx.send(Decision::Decided(map));
        Ok(())
    }

    /// Cancela lo pendiente de una conversación (`chat.cancel`).
    pub fn cancel_conversation(&self, session: &str, conversation: &str) {
        let mut state = self.shared.state.lock().expect("authorizer");
        let ids: Vec<String> = state
            .pending
            .iter()
            .filter(|(_, p)| p.session == session && p.conversation == conversation)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(pending) = state.pending.remove(&id) {
                let _ = pending.tx.send(Decision::Cancelled);
            }
        }
    }

    /// Cancela todo lo pendiente de una sesión que se cerró.
    pub fn cancel_session(&self, session: &str) {
        let mut state = self.shared.state.lock().expect("authorizer");
        let ids: Vec<String> = state
            .pending
            .iter()
            .filter(|(_, p)| p.session == session)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(pending) = state.pending.remove(&id) {
                let _ = pending.tx.send(Decision::Cancelled);
            }
        }
    }

    /// Estado de una autorización para reconectar sin ambigüedad.
    pub fn status(
        &self,
        session: &str,
        authorization_id: &str,
    ) -> fhs::AuthorizationResolvedMessage {
        let state = self.shared.state.lock().expect("authorizer");
        if state
            .pending
            .get(authorization_id)
            .is_some_and(|p| p.session == session)
        {
            return fhs::AuthorizationResolvedMessage {
                authorization_id: authorization_id.into(),
                outcome: Outcome::Pending as i32,
                items: vec![],
            };
        }
        match state.records.get(authorization_id) {
            Some(record) if record.session == session => fhs::AuthorizationResolvedMessage {
                authorization_id: authorization_id.into(),
                outcome: record.batch as i32,
                items: record
                    .items
                    .iter()
                    .map(|(id, outcome, reason)| fhs::AuthorizationItemStatus {
                        item_id: id.clone(),
                        outcome: *outcome as i32,
                        reason: reason.clone(),
                    })
                    .collect(),
            },
            _ => fhs::AuthorizationResolvedMessage {
                authorization_id: authorization_id.into(),
                outcome: Outcome::Unspecified as i32,
                items: vec![],
            },
        }
    }

    /// P5: el mensaje literal del usuario al Star que eligió, dentro de su
    /// ámbito de privacidad y no vetado. Es un `Grant` como cualquier otro:
    /// misma estructura, mismo `Dispatcher`, misma bitácora.
    pub fn implicit_user_message(
        &self,
        star: &PeerEntry,
        scope: Option<Scope>,
        vetoed: &HashSet<String>,
        message: &str,
    ) -> Result<Grant, AuthError> {
        if !providers::allowed(star, scope) || vetoed.contains(&star.did) {
            return Err(AuthError::Invalid(
                "el Star no está dentro del ámbito de privacidad elegido".into(),
            ));
        }
        let authorization_id = Uuid::new_v4().to_string();
        let item_id = "implicit-user-message".to_string();
        let grant = Grant {
            inner: Arc::new(GrantInner {
                authorization_id: authorization_id.clone(),
                item_id: item_id.clone(),
                capability: "chat".into(),
                provider_did: star.did.clone(),
                digest: digest::user_message_digest(message),
                data_class: DataClass::UserMessage,
                basis: Basis::Implicit,
                expires: Instant::now() + GRANT_LIFETIME,
                consumed: AtomicBool::new(false),
                shared: self.shared.clone(),
            }),
        };
        self.shared.remember(
            &authorization_id,
            Record {
                session: String::new(),
                batch: Outcome::Allowed,
                items: vec![(item_id.clone(), Outcome::Allowed, "implícito".into())],
            },
        );
        self.shared.audit(json!({
            "event": "implicit_granted",
            "authorization_id": authorization_id,
            "item_id": item_id,
            "provider_did": star.did,
            "digest": hex(&grant.inner.digest),
        }));
        Ok(grant)
    }
}

/// Un ítem pasa solo si se permitió y todas sus dependencias pasaron.
fn cascade(
    items: &[ItemSpec],
    allow: &HashMap<String, bool>,
) -> HashMap<String, Result<(), String>> {
    let mut result: HashMap<String, Result<(), String>> = HashMap::new();
    let mut remaining: Vec<&ItemSpec> = items.iter().collect();
    // Orden topológico por pasadas; los ciclos no pasan la validación.
    while !remaining.is_empty() {
        let before = remaining.len();
        remaining.retain(|item| {
            if !item.depends_on.iter().all(|d| result.contains_key(d)) {
                return true;
            }
            let outcome = if !allow.get(&item.item_id).copied().unwrap_or(false) {
                Err("denegado".to_string())
            } else if let Some(dep) = item
                .depends_on
                .iter()
                .find(|d| result.get(*d).is_some_and(Result::is_err))
            {
                Err(format!("depende de «{dep}», que no se autorizó"))
            } else {
                Ok(())
            };
            result.insert(item.item_id.clone(), outcome);
            false
        });
        if remaining.len() == before {
            for item in remaining.drain(..) {
                result.insert(item.item_id.clone(), Err("dependencias circulares".into()));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests;
