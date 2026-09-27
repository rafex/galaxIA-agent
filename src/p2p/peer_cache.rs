//! Providers conocidos por sus `NodeAdvertise` firmados (GossipSub
//! `fhs/v1/nodes/advertise`). A diferencia del TS, las entradas expiran:
//! un provider que deja de anunciarse sale de la caché al vencer su TTL.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;

use serde::Serialize;

use crate::protocol::fhs::{self, NodeAdvertiseMessage, ProviderType};

#[derive(Clone, Debug)]
pub struct PeerEntry {
    pub did: String,
    pub beacon: fhs::Beacon,
    pub multiaddrs: Vec<String>,
    pub trust_level: String,
    pub reputation_score: f64,
    pub peer_type: &'static str,
    pub capabilities: Vec<String>,
    pub last_seen_ms: i64,
    pub expires_at_ms: i64,
}

impl PeerEntry {
    pub fn name(&self) -> String {
        self.beacon
            .provider
            .as_ref()
            .map(|p| p.name.clone())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| self.did.clone())
    }
    pub fn description(&self) -> String {
        self.beacon
            .provider
            .as_ref()
            .map(|p| p.description.clone())
            .unwrap_or_default()
    }
    pub fn tags(&self) -> Vec<String> {
        self.beacon
            .provider
            .as_ref()
            .map(|p| p.tags.clone())
            .unwrap_or_default()
    }
    pub fn provider_id(&self) -> String {
        self.beacon
            .provider
            .as_ref()
            .map(|p| p.id.clone())
            .unwrap_or_default()
    }
    pub fn visibility(&self) -> i32 {
        self.beacon
            .provider
            .as_ref()
            .map(|p| p.visibility)
            .unwrap_or_default()
    }
}

/// Vista de `/status` (mismos campos que el Navigator TS; `doctor.sh` los lee).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownPeer {
    pub did: String,
    pub peer_type: &'static str,
    pub capabilities: Vec<String>,
    pub multiaddrs: Vec<String>,
    pub trust_level: String,
    pub last_seen: String,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn peer_type(provider_type: i32) -> &'static str {
    match ProviderType::try_from(provider_type) {
        Ok(ProviderType::Star) => "star",
        Ok(ProviderType::Satellite) => "satellite",
        Ok(ProviderType::Nova) => "nova",
        _ => "unknown",
    }
}

/// Margen sobre el TTL anunciado antes de olvidar a un provider (un anuncio
/// perdido en la malla no debe sacarlo de la caché).
const TTL_GRACE: Duration = Duration::from_secs(30);

/// Tras arrancar, un provider puede tardar hasta un ciclo de anuncios (30 s)
/// en aparecer. Durante esta ventana una búsqueda vacía espera en vez de fallar.
pub const WARM_UP: Duration = Duration::from_secs(35);

#[derive(Clone)]
pub struct PeerCache {
    inner: Arc<RwLock<HashMap<String, PeerEntry>>>,
    started: Instant,
    changed: Arc<Notify>,
}

impl Default for PeerCache {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            started: Instant::now(),
            changed: Arc::new(Notify::new()),
        }
    }
}

impl PeerCache {
    /// Espera a que `ready` se cumpla, solo mientras dure el arranque
    /// ([`WARM_UP`]); después responde de inmediato.
    pub async fn settle(&self, ready: impl Fn(&PeerCache) -> bool) {
        loop {
            let notified = self.changed.notified();
            if ready(self) {
                return;
            }
            let Some(left) = WARM_UP.checked_sub(self.started.elapsed()) else {
                return;
            };
            let _ = tokio::time::timeout(left, notified).await;
        }
    }

    /// Registra un anuncio ya verificado. Devuelve false si no tiene DID.
    pub fn upsert(&self, message: &NodeAdvertiseMessage) -> bool {
        if message.did.is_empty() {
            return false;
        }
        let beacon = message.beacon.clone().unwrap_or_default();
        let mut capabilities: Vec<String> =
            beacon.capabilities.iter().map(|c| c.id.clone()).collect();
        capabilities.extend(beacon.agent_capabilities.iter().map(|c| c.id.clone()));
        let now = now_ms();
        let ttl_ms = i64::from(message.ttl_seconds.max(1)) * 1000 + TTL_GRACE.as_millis() as i64;
        let entry = PeerEntry {
            did: message.did.clone(),
            peer_type: peer_type(
                beacon
                    .provider
                    .as_ref()
                    .map(|p| p.r#type)
                    .unwrap_or_default(),
            ),
            beacon,
            multiaddrs: message.multiaddrs.clone(),
            trust_level: message.trust_level.clone(),
            reputation_score: 0.5,
            capabilities,
            last_seen_ms: now,
            expires_at_ms: now + ttl_ms,
        };
        self.inner
            .write()
            .expect("peer cache")
            .insert(entry.did.clone(), entry);
        self.changed.notify_waiters();
        true
    }

    fn live(&self) -> Vec<PeerEntry> {
        let now = now_ms();
        let mut map = self.inner.write().expect("peer cache");
        map.retain(|_, entry| entry.expires_at_ms > now);
        map.values().cloned().collect()
    }

    pub fn all(&self) -> Vec<PeerEntry> {
        self.live()
    }
    pub fn stars(&self) -> Vec<PeerEntry> {
        self.live()
            .into_iter()
            .filter(|p| p.peer_type == "star")
            .collect()
    }
    pub fn satellites(&self) -> Vec<PeerEntry> {
        self.live()
            .into_iter()
            .filter(|p| p.peer_type == "satellite")
            .collect()
    }
    pub fn get(&self, did: &str) -> Option<PeerEntry> {
        self.live().into_iter().find(|p| p.did == did)
    }

    pub fn known_peers(&self) -> Vec<KnownPeer> {
        let mut peers: Vec<KnownPeer> = self
            .live()
            .into_iter()
            .map(|p| KnownPeer {
                did: p.did,
                peer_type: p.peer_type,
                capabilities: p.capabilities,
                multiaddrs: p.multiaddrs,
                trust_level: p.trust_level,
                last_seen: iso8601(p.last_seen_ms),
            })
            .collect();
        peers.sort_by(|a, b| a.did.cmp(&b.did));
        peers
    }

    #[cfg(test)]
    pub(crate) fn expire_all_for_test(&self) {
        for entry in self.inner.write().expect("peer cache").values_mut() {
            entry.expires_at_ms = 0;
        }
    }
}

/// ISO 8601 UTC con milisegundos (como `Date.toISOString()`).
pub fn iso8601(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Algoritmo de días civiles (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn settle_waits_during_warm_up_until_the_provider_appears() {
        let cache = PeerCache::default();
        let waiting = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.settle(|p| !p.stars().is_empty()).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        cache.upsert(&advertise("did:key:zStar", ProviderType::Star));
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("settle debe terminar al llegar el anuncio")
            .unwrap();
        // Ya listo: no espera.
        tokio::time::timeout(
            Duration::from_millis(50),
            cache.settle(|p| !p.stars().is_empty()),
        )
        .await
        .unwrap();
    }

    fn advertise(did: &str, provider_type: ProviderType) -> NodeAdvertiseMessage {
        NodeAdvertiseMessage {
            did: did.into(),
            beacon: Some(fhs::Beacon {
                provider: Some(fhs::ProviderIdentity {
                    r#type: provider_type as i32,
                    name: "KB".into(),
                    ..Default::default()
                }),
                capabilities: vec![fhs::CapabilityDescriptor {
                    id: "knowledge.query".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ttl_seconds: 60,
            trust_level: "community".into(),
            ..Default::default()
        }
    }

    #[test]
    fn classifies_and_expires_providers() {
        let cache = PeerCache::default();
        assert!(cache.upsert(&advertise("did:key:zKB", ProviderType::Satellite)));
        assert!(cache.upsert(&advertise("did:key:zStar", ProviderType::Star)));
        assert_eq!(cache.satellites().len(), 1);
        assert_eq!(cache.stars().len(), 1);
        assert_eq!(
            cache.get("did:key:zKB").unwrap().capabilities,
            vec!["knowledge.query"]
        );
        cache.expire_all_for_test();
        assert!(cache.all().is_empty());
    }

    #[test]
    fn iso8601_matches_js_to_iso_string() {
        assert_eq!(iso8601(1_790_000_000_000), "2026-09-21T14:13:20.000Z");
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
    }
}
