use crate::{atlas::ProviderDescriptor, policy::PrivacyScope};
use thiserror::Error;
use tokio::time::{timeout, Duration};

#[derive(Clone, Debug)]
pub struct MissionOffer {
    pub mission_id: String,
    pub mission_type: String,
    pub required_capabilities: Vec<String>,
    pub preferred_model: Option<String>,
    pub scope: PrivacyScope,
    pub bid_deadline_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MissionAssignment {
    pub mission_id: String,
    pub provider: ProviderDescriptor,
    pub attempt: u8,
}

#[derive(Debug, Error)]
pub enum MissionError {
    #[error("no hay providers para la Mission")]
    NoProvider,
    #[error("la Mission agotó sus intentos de failover")]
    Exhausted,
    #[error("timeout esperando respuesta del provider")]
    Timeout,
    #[error("el provider falló: {0}")]
    Provider(String),
}

/// Agente determinista que coordina ofertas, asignaciones y failover.
#[derive(Clone, Debug)]
pub struct MissionManager {
    max_attempts: usize,
}

impl Default for MissionManager {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

impl MissionManager {
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }

    pub async fn assign_with_failover<F, Fut>(
        &self,
        offer: &MissionOffer,
        providers: Vec<ProviderDescriptor>,
        execute: F,
    ) -> Result<String, MissionError>
    where
        F: FnMut(MissionAssignment) -> Fut,
        Fut: std::future::Future<Output = Result<String, MissionError>>,
    {
        assign_with_failover_limited(offer, providers, self.max_attempts, execute).await
    }
}

pub async fn assign_with_failover<F, Fut>(
    offer: &MissionOffer,
    providers: Vec<ProviderDescriptor>,
    execute: F,
) -> Result<String, MissionError>
where
    F: FnMut(MissionAssignment) -> Fut,
    Fut: std::future::Future<Output = Result<String, MissionError>>,
{
    assign_with_failover_limited(offer, providers, usize::MAX, execute).await
}

async fn assign_with_failover_limited<F, Fut>(
    offer: &MissionOffer,
    mut providers: Vec<ProviderDescriptor>,
    max_attempts: usize,
    mut execute: F,
) -> Result<String, MissionError>
where
    F: FnMut(MissionAssignment) -> Fut,
    Fut: std::future::Future<Output = Result<String, MissionError>>,
{
    if providers.is_empty() {
        return Err(MissionError::NoProvider);
    }
    providers.sort_by(|a, b| {
        b.reputation
            .partial_cmp(&a.reputation)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.latency_ms.cmp(&b.latency_ms))
    });
    let mut last_error = MissionError::Exhausted;
    for (index, provider) in providers.into_iter().take(max_attempts).enumerate() {
        let assignment = MissionAssignment {
            mission_id: offer.mission_id.clone(),
            provider,
            attempt: index as u8 + 1,
        };
        match timeout(
            Duration::from_millis(offer.bid_deadline_ms),
            execute(assignment),
        )
        .await
        {
            Ok(Ok(answer)) => return Ok(answer),
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = MissionError::Timeout,
        }
    }
    Err(last_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atlas::ProviderDescriptor;

    fn p(id: &str, reputation: f32) -> ProviderDescriptor {
        ProviderDescriptor {
            provider_id: id.into(),
            provider_name: id.into(),
            provider_type: "star".into(),
            capabilities: vec!["chat".into()],
            models: vec!["m".into()],
            scope: PrivacyScope::Community,
            multiaddrs: vec![],
            reputation,
            latency_ms: 1,
        }
    }

    #[tokio::test]
    async fn retries_next_provider_after_failure() {
        let offer = MissionOffer {
            mission_id: "m".into(),
            mission_type: "chat".into(),
            required_capabilities: vec!["chat".into()],
            preferred_model: None,
            scope: PrivacyScope::Community,
            bid_deadline_ms: 100,
        };
        let mut calls = 0;
        let result = assign_with_failover(&offer, vec![p("a", 1.0), p("b", 0.5)], |assignment| {
            calls += 1;
            async move {
                if assignment.provider.provider_id == "a" {
                    Err(MissionError::Exhausted)
                } else {
                    Ok("ok".into())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, "ok");
        assert_eq!(calls, 2);
    }
}
