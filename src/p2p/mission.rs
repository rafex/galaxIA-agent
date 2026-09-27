//! Ciclo de misión: offer → bids → selección → assign (`mission-cycle.ts`).

use std::time::Duration;
use std::time::Instant;

use uuid::Uuid;

use crate::p2p::node::NodeHandle;
use crate::p2p::wire::{self, TOPIC_MISSIONS_ASSIGN, TOPIC_MISSIONS_OFFER};
use crate::protocol::fhs::{MissionBidMessage, MissionOfferMessage};

pub const DEFAULT_BID_DEADLINE: Duration = Duration::from_secs(2);

pub struct MissionRequest<'a> {
    /// `chat` o `tool_call`.
    pub mission_type: &'a str,
    pub required_capabilities: Vec<String>,
    pub preferred_model: Option<String>,
    /// Provider que el runtime ya eligió: gana si pujó.
    pub preferred_provider: Option<String>,
    pub bid_deadline: Duration,
}

pub struct WinningBid {
    pub mission_id: String,
    pub bid: MissionBidMessage,
}

fn trust_rank(level: &str) -> i32 {
    match level {
        "delegated" => 4,
        "standard" => 3,
        "community" => 2,
        "unverified" => 1,
        _ => 0,
    }
}

/// El preferido gana si pujó; si no, trust → reputación → latencia
/// (misma regla que `selectWinningBid` del Navigator TS, E2E-030).
pub fn select_winning_bid<'a>(
    bids: &'a [MissionBidMessage],
    preferred: Option<&str>,
) -> Option<&'a MissionBidMessage> {
    if let Some(preferred) = preferred {
        if let Some(bid) = bids.iter().find(|b| b.provider_did == preferred) {
            return Some(bid);
        }
    }
    bids.iter().min_by(|a, b| {
        trust_rank(&b.trust_level)
            .cmp(&trust_rank(&a.trust_level))
            .then(b.reputation_score.total_cmp(&a.reputation_score))
            .then(a.estimated_latency_ms.cmp(&b.estimated_latency_ms))
    })
}

/// Publica la oferta, recoge pujas y publica la asignación del ganador.
pub async fn run_mission_cycle(
    node: &NodeHandle,
    request: MissionRequest<'_>,
) -> Option<WinningBid> {
    let mission_id = Uuid::new_v4().to_string();
    let navigator_multiaddrs = node
        .status()
        .await
        .map(|s| s.multiaddrs)
        .unwrap_or_default();
    let offer = MissionOfferMessage {
        mission_id: mission_id.clone(),
        navigator_multiaddrs,
        mission_type: request.mission_type.into(),
        required_capabilities: request.required_capabilities,
        preferred_model: request.preferred_model.unwrap_or_default(),
        bid_deadline_ms: request.bid_deadline.as_millis() as i64,
        ..Default::default()
    };
    let collecting = {
        let bids = node.bids.clone();
        let id = mission_id.clone();
        let deadline = request.bid_deadline;
        let preferred = request.preferred_provider.clone();
        tokio::spawn(async move { bids.collect(&id, deadline, preferred).await })
    };
    // La ventana se abre antes de publicar para no perder pujas rápidas.
    tokio::task::yield_now().await;
    let bid_wait_started = Instant::now();
    node.publish(
        TOPIC_MISSIONS_OFFER,
        wire::signed_mission_offer(&node.identity, offer),
    )
    .await;
    tracing::info!(
        "[mission] offer {mission_id} publicado (type={})",
        request.mission_type
    );

    let bids = collecting.await.unwrap_or_default();
    tracing::info!(
        mission_id = %mission_id,
        bid_count = bids.len(),
        bid_wait_ms = bid_wait_started.elapsed().as_millis() as u64,
        preferred_provider = %request.preferred_provider.as_deref().unwrap_or("none"),
        "[mission] bids collected",
    );
    let winner = select_winning_bid(&bids, request.preferred_provider.as_deref())?.clone();

    node.publish(
        TOPIC_MISSIONS_ASSIGN,
        wire::signed_mission_assign(&node.identity, &mission_id, &winner.provider_did),
    )
    .await;
    tracing::info!("[mission] assign {mission_id} → {}", winner.provider_did);
    Some(WinningBid {
        mission_id,
        bid: winner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bid(did: &str, trust: &str, reputation: f32, latency: i32) -> MissionBidMessage {
        MissionBidMessage {
            provider_did: did.into(),
            trust_level: trust.into(),
            reputation_score: reputation,
            estimated_latency_ms: latency,
            ..Default::default()
        }
    }

    #[test]
    fn preferred_provider_wins_if_it_bid() {
        let bids = [
            bid("a", "standard", 0.5, 100),
            bid("b", "community", 0.5, 100),
        ];
        assert_eq!(
            select_winning_bid(&bids, Some("b")).unwrap().provider_did,
            "b"
        );
    }

    #[test]
    fn otherwise_trust_then_reputation_then_latency() {
        let bids = [
            bid("a", "community", 0.9, 50),
            bid("b", "standard", 0.1, 900),
            bid("c", "standard", 0.1, 100),
        ];
        assert_eq!(
            select_winning_bid(&bids, Some("ausente"))
                .unwrap()
                .provider_did,
            "c"
        );
        assert!(select_winning_bid(&[], None).is_none());
    }
}
