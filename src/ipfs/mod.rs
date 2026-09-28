//! Adjuntos por IPFS desde el Navigator (DEC-0095): sube al Kubo local,
//! lleva el libro de pines ([`ledger`]) y los libera cuando ningún turno los
//! usa. El OCR lee el CID por su propio Kubo; aquí nunca hay gateway.
//!
//! El estado (libro, reservas, salud) vive tras un único `Mutex`: toda
//! admisión y transición es atómica. Las llamadas a Kubo que fijan o quitan
//! pines se serializan con `mutation`, para que un unpin del barrido no pise
//! un `add` concurrente del mismo CID.

pub mod ledger;

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use galaxia_fhs::ipfs::{KuboClient, KuboError};
use galaxia_fhs::p2p::peer_cache::now_ms;
use serde_json::{json, Value};
use tokio::task::AbortHandle;

use ledger::{Ledger, ReleaseError, ReleaseOutcome, Store, FAILURE_GRACE_MS, SUCCESS_GRACE_MS};

pub const MAX_UPLOADS_PER_SESSION: usize = 1;
pub const MAX_UPLOADS: usize = 4;
pub const MAX_UNIQUE_BYTES: u64 = 1_000_000_000;
const MAX_REPO_FRACTION: f64 = 0.8;
const MIN_FREE_BYTES: u64 = 2_000_000_000;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// La auditoría de pines corre al arrancar y cada 10 barridos.
const AUDIT_EVERY: u32 = 10;

/// Pista de lectura para lectores externos (DEC-0045). Nuestro OCR la ignora.
pub fn gateway_hint(network: &str) -> &'static str {
    if network == "public" {
        "https://ipfs.io/ipfs"
    } else {
        ""
    }
}

#[derive(Clone, Debug)]
pub struct IpfsConfig {
    pub api_url: String,
    pub token_file: PathBuf,
    /// Red de este Navigator (`IPFS_NETWORK`): `public` o `private`.
    pub network: String,
    pub ledger_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct IpfsError {
    pub code: &'static str,
    pub message: String,
}

impl IpfsError {
    fn overloaded(message: impl Into<String>) -> Self {
        Self {
            code: "OVERLOADED",
            message: message.into(),
        }
    }
    fn upstream(error: &KuboError) -> Self {
        Self {
            code: "UPSTREAM_UNAVAILABLE",
            message: format!("IPFS no disponible: {error}"),
        }
    }
}

/// Turnos vivos: `turn_id → AbortHandle` de su tarea. Se escribe de forma
/// síncrona al lanzar y al terminar cada turno; el barrido no depende de
/// ningún evento para saber si un turno murió.
#[derive(Clone, Default)]
pub struct TurnRegistry {
    turns: Arc<Mutex<HashMap<String, AbortHandle>>>,
}

impl TurnRegistry {
    pub fn register(&self, turn_id: &str, handle: AbortHandle) {
        self.turns
            .lock()
            .expect("turnos")
            .insert(turn_id.into(), handle);
    }

    pub fn unregister(&self, turn_id: &str) {
        self.turns.lock().expect("turnos").remove(turn_id);
    }

    /// Vivo: registrado y su tarea no ha terminado. Un turno abortado sigue
    /// "vivo" hasta que la tarea termina de verdad.
    fn alive(&self, turn_id: &str) -> bool {
        self.turns
            .lock()
            .expect("turnos")
            .get(turn_id)
            .is_some_and(|h| !h.is_finished())
    }

    fn prune(&self) {
        self.turns
            .lock()
            .expect("turnos")
            .retain(|_, h| !h.is_finished());
    }
}

#[derive(Default)]
struct State {
    ledger: Ledger,
    uploads: usize,
    uploads_by_session: HashMap<String, usize>,
    reserved_bytes: u64,
    /// Kubo no respondió en la última operación: no se admiten subidas.
    degraded: Option<String>,
    /// Libro en cuarentena: ningún unpin automático.
    unpin_blocked: bool,
    ledger_issue: Option<String>,
    foreign_pins: BTreeSet<String>,
    missing_pins: BTreeSet<String>,
    failed_reuse: BTreeSet<String>,
    audited: bool,
}

struct Inner {
    kubo: KuboClient,
    network: String,
    store: Store,
    state: Mutex<State>,
    mutation: tokio::sync::Mutex<()>,
    turns: TurnRegistry,
}

#[derive(Clone)]
pub struct IpfsService {
    inner: Arc<Inner>,
}

/// Plaza de subida reservada; se devuelve al soltarla, en cualquier camino.
struct Reservation {
    inner: Arc<Inner>,
    session: String,
    bytes: u64,
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Reservation({}, {} B)", self.session, self.bytes)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut s = self.inner.state.lock().expect("ipfs");
        s.uploads -= 1;
        s.reserved_bytes -= self.bytes;
        if let Some(n) = s.uploads_by_session.get_mut(&self.session) {
            *n -= 1;
            if *n == 0 {
                s.uploads_by_session.remove(&self.session);
            }
        }
    }
}

/// Marca el fin de un turno que subió algo. Al soltarse (éxito, error,
/// cancelación o pánico) fija la gracia de sus leases y lo da de baja del
/// registro, ambos de forma síncrona.
pub struct ReleaseGuard {
    service: IpfsService,
    turn_id: String,
    success: bool,
}

impl ReleaseGuard {
    /// El provider ya terminó de leer: basta la gracia corta.
    pub fn succeeded(&mut self) {
        self.success = true;
    }
}

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let grace = if self.success {
            SUCCESS_GRACE_MS
        } else {
            FAILURE_GRACE_MS
        };
        self.service.end_turn(&self.turn_id, grace);
        self.service.inner.turns.unregister(&self.turn_id);
    }
}

impl IpfsService {
    /// Carga el libro (recuperando o poniendo en cuarentena), aplica la
    /// transición de arranque y lanza el barrido.
    pub fn start(config: &IpfsConfig) -> Result<Self, String> {
        let kubo =
            KuboClient::new(&config.api_url, &config.token_file).map_err(|e| e.to_string())?;
        let store = Store::new(&config.ledger_path);
        let now = now_ms();
        let loaded = store
            .load(now)
            .map_err(|e| format!("libro de pines {}: {e}", store.path().display()))?;
        let mut state = State {
            ledger: loaded.ledger,
            ..Default::default()
        };
        if let Some(path) = &loaded.recovered_from {
            state.ledger_issue = Some(format!("recuperado de {}", path.display()));
        }
        if let Some(path) = &loaded.quarantined {
            let issue = format!(
                "libro inválido en cuarentena ({}); unpins automáticos detenidos hasta revisarlo",
                path.display()
            );
            tracing::error!("[ipfs] {issue}");
            state.ledger_issue = Some(issue);
        }
        state.unpin_blocked = store.has_quarantine();
        if state.unpin_blocked && loaded.quarantined.is_none() {
            state.ledger_issue = Some(
                "hay un libro en cuarentena (*.corrupt-*); unpins automáticos detenidos".into(),
            );
        }
        for cid in state.ledger.startup(now) {
            tracing::warn!("[ipfs] reuse {cid} quedó a medio subir: se libera como efímero");
            state.failed_reuse.insert(cid);
        }
        store
            .save(&state.ledger)
            .map_err(|e| format!("libro de pines {}: {e}", store.path().display()))?;
        let service = Self {
            inner: Arc::new(Inner {
                kubo,
                network: config.network.clone(),
                store,
                state: Mutex::new(state),
                mutation: tokio::sync::Mutex::new(()),
                turns: TurnRegistry::default(),
            }),
        };
        tokio::spawn(service.clone().sweeper());
        Ok(service)
    }

    pub fn network(&self) -> &str {
        &self.inner.network
    }

    pub fn turns(&self) -> &TurnRegistry {
        &self.inner.turns
    }

    /// Guardia de fin de turno para `turn_id` (ver [`ReleaseGuard`]).
    pub fn release_guard(&self, turn_id: &str) -> ReleaseGuard {
        ReleaseGuard {
            service: self.clone(),
            turn_id: turn_id.into(),
            success: false,
        }
    }

    fn persist(&self, state: &State) {
        if let Err(error) = self.inner.store.save(&state.ledger) {
            tracing::error!("[ipfs] no se pudo guardar el libro de pines: {error}");
        }
    }

    fn mutate<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut state = self.inner.state.lock().expect("ipfs");
        let out = f(&mut state);
        self.persist(&state);
        out
    }

    fn end_turn(&self, turn_id: &str, grace: i64) {
        let mut state = self.inner.state.lock().expect("ipfs");
        if state.ledger.end_turn(turn_id, grace, now_ms()) {
            self.persist(&state);
        }
    }

    fn set_health(&self, result: Result<(), &KuboError>) {
        let mut state = self.inner.state.lock().expect("ipfs");
        match result {
            Ok(()) => {
                if state.degraded.take().is_some() {
                    tracing::info!("[ipfs] Kubo responde de nuevo");
                }
            }
            Err(error) => {
                if state.degraded.is_none() {
                    tracing::error!("[ipfs] Kubo no responde: {error}");
                }
                state.degraded = Some(error.to_string());
            }
        }
    }

    /// Reserva plaza y bytes, evaluando las cuotas del libro atómicamente.
    fn reserve(&self, session: &str, bytes: u64) -> Result<Reservation, IpfsError> {
        let mut s = self.inner.state.lock().expect("ipfs");
        if let Some(reason) = &s.degraded {
            return Err(IpfsError {
                code: "UPSTREAM_UNAVAILABLE",
                message: format!("IPFS no disponible en este Navigator: {reason}"),
            });
        }
        if s.uploads_by_session.get(session).copied().unwrap_or(0) >= MAX_UPLOADS_PER_SESSION {
            return Err(IpfsError::overloaded(
                "Ya hay una subida IPFS en curso en esta sesión",
            ));
        }
        if s.uploads >= MAX_UPLOADS {
            return Err(IpfsError::overloaded(
                "El Navigator ya tiene el máximo de subidas IPFS en curso",
            ));
        }
        if s.ledger.unique_bytes() + s.reserved_bytes + bytes > MAX_UNIQUE_BYTES {
            return Err(IpfsError::overloaded(
                "Se alcanzó la cuota de almacenamiento IPFS del Navigator",
            ));
        }
        s.uploads += 1;
        *s.uploads_by_session.entry(session.into()).or_default() += 1;
        s.reserved_bytes += bytes;
        Ok(Reservation {
            inner: self.inner.clone(),
            session: session.into(),
            bytes,
        })
    }

    /// Espacio en Kubo: repo < 80 % de `StorageMax` y ≥ 2 GB + lo reservado libres.
    async fn check_space(&self, bytes: u64) -> Result<(), IpfsError> {
        let kubo = &self.inner.kubo;
        let stat = kubo.repo_stat().await;
        let free = kubo.free_space().await;
        let (stat, free) = match (stat, free) {
            (Ok(stat), Ok(free)) => (stat, free),
            (Err(e), _) | (_, Err(e)) => {
                self.set_health(Err(&e));
                return Err(IpfsError::upstream(&e));
            }
        };
        if stat.storage_max > 0
            && stat.repo_size as f64 > stat.storage_max as f64 * MAX_REPO_FRACTION
        {
            return Err(IpfsError::overloaded(
                "El repositorio IPFS está casi lleno (más del 80 %)",
            ));
        }
        let reserved = self.inner.state.lock().expect("ipfs").reserved_bytes;
        if free < MIN_FREE_BYTES + reserved.max(bytes) {
            return Err(IpfsError::overloaded(
                "Poco espacio libre en disco para IPFS",
            ));
        }
        Ok(())
    }

    /// Sube el adjunto y lo registra en el libro para `turn_id`. Devuelve el
    /// CID canónico ya fijado.
    pub async fn upload(
        &self,
        session: &str,
        turn_id: &str,
        bytes: Vec<u8>,
        reuse: bool,
    ) -> Result<String, IpfsError> {
        let size = bytes.len() as u64;
        let _reservation = self.reserve(session, size)?;
        self.check_space(size).await?;
        let kubo = &self.inner.kubo;

        // 1. CID sin guardar nada.
        let expected = kubo.add(bytes.clone(), true).await.map_err(|e| {
            self.set_health(Err(&e));
            IpfsError::upstream(&e)
        })?;
        // 2. WAL: el lease existe antes de que Kubo fije nada.
        let lease = self.mutate(|s| {
            s.ledger
                .begin_upload(&expected, size, reuse, turn_id, now_ms())
        });
        // 3. `add` real, serializado con los unpins.
        let added = {
            let _mutation = self.inner.mutation.lock().await;
            kubo.add(bytes, false).await
        };
        match added {
            Ok(cid) if cid == expected => {
                self.set_health(Ok(()));
                self.mutate(|s| s.ledger.activate(&expected, &lease));
                tracing::info!("[ipfs] {expected} fijado ({size} B, turno {turn_id})");
                Ok(expected)
            }
            Ok(other) => {
                tracing::error!(
                    "[ipfs] Kubo devolvió {other} en vez de {expected}: perfil de add desalineado"
                );
                self.mutate(|s| {
                    let now = now_ms();
                    s.ledger.add_cleanup(&other, size, now);
                    s.ledger.release_lease(&expected, &lease, now);
                });
                Err(IpfsError {
                    code: "INTERNAL_ERROR",
                    message: "IPFS devolvió un CID inesperado".into(),
                })
            }
            Err(error) => {
                // Kubo pudo completar el add: el barrido lo quita desde el libro.
                self.set_health(Err(&error));
                self.mutate(|s| {
                    s.ledger
                        .release_lease(&expected, &lease, now_ms() + FAILURE_GRACE_MS)
                });
                Err(IpfsError::upstream(&error))
            }
        }
    }

    /// API de administración: quita `reuse`; el unpin lo hace el barrido.
    pub fn release_reuse(&self, cid: &str) -> Result<ReleaseOutcome, ReleaseError> {
        let cid = galaxia_fhs::ipfs::canonical_cid(cid).map_err(|_| ReleaseError::Unknown)?;
        self.mutate(|s| s.ledger.release_reuse(&cid, now_ms()))
    }

    /// Copia del libro (`GET /admin/ipfs/pins`).
    pub fn ledger(&self) -> Ledger {
        self.inner.state.lock().expect("ipfs").ledger.clone()
    }

    /// Resumen para `/status`.
    pub fn status(&self) -> Value {
        let s = self.inner.state.lock().expect("ipfs");
        let pending: Vec<&String> = s
            .ledger
            .pins
            .iter()
            .filter(|(_, e)| e.unpin.pending)
            .map(|(cid, _)| cid)
            .collect();
        json!({
            "network": self.inner.network,
            "degraded": s.degraded,
            "unpinBlocked": s.unpin_blocked,
            "ledgerIssue": s.ledger_issue,
            "pins": s.ledger.pins.len(),
            "uniqueBytes": s.ledger.unique_bytes(),
            "uploadsInFlight": s.uploads,
            "pendingUnpins": pending,
            "foreignPins": s.foreign_pins,
            "missingPins": s.missing_pins,
            "failedReuse": s.failed_reuse,
        })
    }

    async fn sweeper(self) {
        let mut tick: u32 = 0;
        loop {
            if tick.is_multiple_of(AUDIT_EVERY) {
                self.audit().await;
            }
            self.sweep().await;
            tick = tick.wrapping_add(1);
            tokio::time::sleep(SWEEP_INTERVAL).await;
        }
    }

    /// Transición 4 y 7: turnos muertos a la gracia larga; unpins vencidos.
    pub async fn sweep(&self) {
        let now = now_ms();
        let turns = &self.inner.turns;
        turns.prune();
        let due = self.mutate(|s| {
            for turn in s.ledger.live_turns() {
                if !turns.alive(&turn) {
                    s.ledger.end_turn(&turn, FAILURE_GRACE_MS, now);
                }
            }
            let due = s.ledger.sweep(now);
            if s.unpin_blocked {
                Vec::new()
            } else {
                due
            }
        });
        if due.is_empty() {
            return;
        }
        let _mutation = self.inner.mutation.lock().await;
        for cid in due {
            let still_due = self
                .inner
                .state
                .lock()
                .expect("ipfs")
                .ledger
                .unpin_still_due(&cid);
            if !still_due {
                continue;
            }
            let result = self.inner.kubo.pin_rm(&cid).await;
            self.set_health(result.as_ref().map(|_| ()));
            match result {
                Ok(()) => {
                    tracing::info!("[ipfs] {cid} liberado");
                    self.mutate(|s| {
                        s.ledger.unpinned(&cid);
                        s.missing_pins.remove(&cid);
                    });
                }
                Err(error) => {
                    tracing::warn!("[ipfs] unpin de {cid} falló: {error}");
                    self.mutate(|s| s.ledger.unpin_failed(&cid, now_ms()));
                }
            }
        }
    }

    /// Compara los pines de Kubo con el libro. Un pin ajeno se reporta y no
    /// se toca; un CID que debería estar fijado y no lo está, también.
    pub async fn audit(&self) {
        let kubo = &self.inner.kubo;
        let pins = match (kubo.pin_ls("recursive").await, kubo.pin_ls("direct").await) {
            (Ok(recursive), Ok(direct)) => {
                self.set_health(Ok(()));
                recursive
                    .into_iter()
                    .chain(direct)
                    .collect::<BTreeSet<String>>()
            }
            (Err(e), _) | (_, Err(e)) => {
                // Sin respuesta no se asume "no hay pines".
                self.set_health(Err(&e));
                return;
            }
        };
        self.mutate(|s| {
            let known: BTreeSet<String> = s.ledger.pins.keys().cloned().collect();
            let foreign: BTreeSet<String> = pins.difference(&known).cloned().collect();
            for cid in foreign.difference(&s.foreign_pins) {
                tracing::error!(
                    "[ipfs] pin ajeno al libro en el Kubo del Navigator: {cid} (no se toca)"
                );
            }
            s.foreign_pins = foreign;
            let missing: BTreeSet<String> = s
                .ledger
                .expected_pins()
                .into_iter()
                .filter(|cid| !pins.contains(cid))
                .collect();
            if !s.audited {
                // Arranque: un reuse sin pin no se puede recuperar (el
                // Navigator no guarda los bytes).
                let lost: Vec<String> = missing
                    .iter()
                    .filter(|cid| s.ledger.pins.get(*cid).is_some_and(|e| e.reuse))
                    .cloned()
                    .collect();
                for cid in lost {
                    tracing::error!("[ipfs] reuse {cid} ya no está fijado: se quita del libro");
                    s.ledger.pins.remove(&cid);
                    s.failed_reuse.insert(cid);
                }
                s.audited = true;
            }
            for cid in &missing {
                if !s.missing_pins.contains(cid) {
                    tracing::error!("[ipfs] {cid} debería estar fijado y no lo está");
                }
            }
            s.missing_pins = missing
                .into_iter()
                .filter(|cid| s.ledger.pins.contains_key(cid))
                .collect();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Query, routing::post, Json, Router};
    use std::collections::BTreeMap;

    const A: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const B: &str = "bafkreig6punxegq6ayzlptye5x2qgleoz75j7gqijeqvfojg6gs2pz3f24";

    /// Kubo simulado: `add` devuelve A (o `pinned_cid` al fijar), guarda los
    /// pines y registra los `pin/rm`.
    #[derive(Clone, Default)]
    struct Kubo {
        pinned_cid: Arc<Mutex<Option<String>>>,
        pins: Arc<Mutex<BTreeSet<String>>>,
        removed: Arc<Mutex<Vec<String>>>,
    }

    async fn mock_kubo(kubo: Kubo) -> String {
        let add = {
            let kubo = kubo.clone();
            move |Query(q): Query<BTreeMap<String, String>>| async move {
                let cid = if q.get("only-hash").map(String::as_str) == Some("true") {
                    A.to_string()
                } else {
                    let cid = kubo.pinned_cid.lock().unwrap().clone().unwrap_or(A.into());
                    kubo.pins.lock().unwrap().insert(cid.clone());
                    cid
                };
                Json(json!({"Name": "blob", "Hash": cid, "Size": "1"}))
            }
        };
        let pin_rm = {
            let kubo = kubo.clone();
            move |Query(q): Query<BTreeMap<String, String>>| async move {
                let cid = q["arg"].clone();
                kubo.pins.lock().unwrap().remove(&cid);
                kubo.removed.lock().unwrap().push(cid.clone());
                Json(json!({"Pins": [cid]}))
            }
        };
        let pin_ls = {
            let kubo = kubo.clone();
            move |Query(q): Query<BTreeMap<String, String>>| async move {
                if q.get("type").map(String::as_str) == Some("direct") {
                    return Json(json!({}));
                }
                let keys: serde_json::Map<String, Value> = kubo
                    .pins
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|c| (c.clone(), json!({"Type": "recursive"})))
                    .collect();
                Json(json!({ "Keys": keys }))
            }
        };
        let app = Router::new()
            .route("/api/v0/add", post(add))
            .route("/api/v0/pin/rm", post(pin_rm))
            .route("/api/v0/pin/ls", post(pin_ls))
            .route(
                "/api/v0/repo/stat",
                post(|| async { Json(json!({"RepoSize": 1, "StorageMax": 5_000_000_000u64})) }),
            )
            .route(
                "/api/v0/diag/sys",
                post(|| async { Json(json!({"diskinfo": {"free_space": 100_000_000_000u64}})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    fn config(url: &str) -> IpfsConfig {
        let dir = std::env::temp_dir().join(format!("ipfs-svc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("token"), "t").unwrap();
        IpfsConfig {
            api_url: url.into(),
            token_file: dir.join("token"),
            network: "public".into(),
            ledger_path: dir.join("ipfs-pins.json"),
        }
    }

    fn lease_deadlines(service: &IpfsService, cid: &str) -> Vec<Option<i64>> {
        service.ledger().pins[cid]
            .leases
            .values()
            .map(|l| l.release_after)
            .collect()
    }

    #[tokio::test]
    async fn upload_then_short_grace_after_success() {
        let kubo = Kubo::default();
        let service = IpfsService::start(&config(&mock_kubo(kubo.clone()).await)).unwrap();
        let mut guard = service.release_guard("t1");
        let cid = service.upload("s", "t1", vec![1], false).await.unwrap();
        assert_eq!(cid, A);
        assert!(kubo.pins.lock().unwrap().contains(A));
        assert_eq!(
            service.ledger().pins[A]
                .leases
                .values()
                .next()
                .unwrap()
                .state,
            ledger::LeaseState::Active
        );
        let before = now_ms();
        guard.succeeded();
        drop(guard);
        let deadline = lease_deadlines(&service, A)[0].unwrap();
        assert!(deadline >= before + SUCCESS_GRACE_MS && deadline < before + FAILURE_GRACE_MS);
        // Aún en gracia: el barrido no lo quita.
        service.sweep().await;
        assert!(kubo.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn quotas_per_session_and_global() {
        let service = IpfsService::start(&config(&mock_kubo(Kubo::default()).await)).unwrap();
        let first = service.reserve("s1", 1).unwrap();
        assert_eq!(service.reserve("s1", 1).unwrap_err().code, "OVERLOADED");
        let others: Vec<_> = (2..=4)
            .map(|i| service.reserve(&format!("s{i}"), 1).unwrap())
            .collect();
        assert_eq!(service.reserve("s5", 1).unwrap_err().code, "OVERLOADED");
        drop(first);
        drop(others);
        assert!(service.reserve("s1", MAX_UNIQUE_BYTES + 1).is_err());
        assert!(service.reserve("s1", 1).is_ok(), "las plazas vuelven");
    }

    #[tokio::test]
    async fn unexpected_cid_is_cleaned_up_by_the_next_sweep() {
        let kubo = Kubo::default();
        *kubo.pinned_cid.lock().unwrap() = Some(B.into());
        let service = IpfsService::start(&config(&mock_kubo(kubo.clone()).await)).unwrap();
        let error = service.upload("s", "t", vec![1], false).await.unwrap_err();
        assert_eq!(error.code, "INTERNAL_ERROR");
        service.sweep().await;
        let removed = kubo.removed.lock().unwrap().clone();
        assert!(removed.contains(&B.to_string()), "{removed:?}");
        assert!(service.ledger().pins.is_empty());
    }

    #[tokio::test]
    async fn dead_turns_get_the_long_grace_without_any_event() {
        let kubo = Kubo::default();
        let service = IpfsService::start(&config(&mock_kubo(kubo.clone()).await)).unwrap();
        // Turno registrado y vivo: el barrido no lo toca.
        let task = tokio::spawn(std::future::pending::<()>());
        service.turns().register("vivo", task.abort_handle());
        service.upload("s1", "vivo", vec![1], false).await.unwrap();
        // Turno nunca registrado (p. ej. de otro proceso): muerto.
        *kubo.pinned_cid.lock().unwrap() = None;
        service.sweep().await;
        assert_eq!(lease_deadlines(&service, A), vec![None]);

        task.abort();
        let _ = task.await;
        let before = now_ms();
        service.sweep().await;
        let deadline = lease_deadlines(&service, A)[0].unwrap();
        assert!(deadline >= before + FAILURE_GRACE_MS);
        assert!(kubo.removed.lock().unwrap().is_empty(), "gracia de 5 min");
    }

    #[tokio::test]
    async fn kubo_down_degrades_and_rejects_uploads() {
        let service = IpfsService::start(&config("http://127.0.0.1:9")).unwrap();
        let error = service.upload("s", "t", vec![1], false).await.unwrap_err();
        assert_eq!(error.code, "UPSTREAM_UNAVAILABLE");
        assert!(service.status()["degraded"].is_string());
        assert!(service.ledger().pins.is_empty());
        let again = service.upload("s", "t", vec![1], false).await.unwrap_err();
        assert!(again.message.contains("no disponible"));
    }

    #[tokio::test]
    async fn foreign_pins_are_reported_and_never_touched() {
        let kubo = Kubo::default();
        kubo.pins.lock().unwrap().insert(B.into());
        let service = IpfsService::start(&config(&mock_kubo(kubo.clone()).await)).unwrap();
        service.audit().await;
        service.sweep().await;
        assert_eq!(service.status()["foreignPins"], json!([B]));
        assert!(kubo.pins.lock().unwrap().contains(B));
        assert!(kubo.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn restart_keeps_the_pin_during_the_long_grace() {
        let kubo = Kubo::default();
        let cfg = config(&mock_kubo(kubo.clone()).await);
        let first = IpfsService::start(&cfg).unwrap();
        first.upload("s", "t", vec![1], false).await.unwrap();
        drop(first);
        let before = now_ms();
        let second = IpfsService::start(&cfg).unwrap();
        let deadline = lease_deadlines(&second, A)[0].unwrap();
        assert!(deadline >= before + FAILURE_GRACE_MS);
        second.sweep().await;
        assert!(kubo.pins.lock().unwrap().contains(A));
    }

    #[tokio::test]
    async fn operator_release_of_reuse_is_unpinned_by_the_sweep() {
        let kubo = Kubo::default();
        let service = IpfsService::start(&config(&mock_kubo(kubo.clone()).await)).unwrap();
        {
            let mut guard = service.release_guard("t");
            service.upload("s", "t", vec![1], true).await.unwrap();
            guard.succeeded();
        }
        assert_eq!(service.release_reuse(B), Err(ReleaseError::Unknown));
        // El lease del turno sigue en gracia: aún no se programa la limpieza.
        let outcome = service.release_reuse(A).unwrap();
        assert!(!outcome.cleanup_scheduled && outcome.release_after.is_some());
        assert_eq!(service.release_reuse(A), Err(ReleaseError::NotReuse));
        assert!(
            kubo.pins.lock().unwrap().contains(A),
            "la API nunca despinea"
        );
    }
}
