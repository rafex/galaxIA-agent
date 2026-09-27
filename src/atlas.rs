use crate::policy::PrivacyScope;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProviderDescriptor {
    pub provider_id: String,
    pub provider_name: String,
    pub provider_type: String,
    pub capabilities: Vec<String>,
    pub models: Vec<String>,
    pub scope: PrivacyScope,
    #[serde(default)]
    pub multiaddrs: Vec<String>,
    #[serde(default)]
    pub reputation: f32,
    #[serde(default)]
    pub latency_ms: u32,
}

#[derive(Clone, Default)]
pub struct AtlasClient {
    providers: Arc<RwLock<Vec<ProviderDescriptor>>>,
}

impl AtlasClient {
    pub async fn replace_snapshot(&self, providers: Vec<ProviderDescriptor>) {
        *self.providers.write().await = providers;
    }

    pub async fn providers_for(
        &self,
        capability: &str,
        scope: &PrivacyScope,
    ) -> Vec<ProviderDescriptor> {
        self.providers
            .read()
            .await
            .iter()
            .filter(|provider| {
                provider.capabilities.iter().any(|item| item == capability)
                    && scope_allowed(&provider.scope, scope)
            })
            .cloned()
            .collect()
    }
}

fn scope_allowed(provider: &PrivacyScope, requested: &PrivacyScope) -> bool {
    matches!(
        (provider, requested),
        (PrivacyScope::Local, PrivacyScope::Local)
            | (
                PrivacyScope::Network,
                PrivacyScope::Network | PrivacyScope::Community | PrivacyScope::External
            )
            | (
                PrivacyScope::Community,
                PrivacyScope::Community | PrivacyScope::External
            )
            | (PrivacyScope::External, PrivacyScope::External)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filters_provider_by_scope_before_selection() {
        let atlas = AtlasClient::default();
        atlas
            .replace_snapshot(vec![ProviderDescriptor {
                provider_id: "private".into(),
                provider_name: "private".into(),
                provider_type: "star".into(),
                capabilities: vec!["chat".into()],
                models: vec!["m".into()],
                scope: PrivacyScope::External,
                multiaddrs: vec![],
                reputation: 1.0,
                latency_ms: 1,
            }])
            .await;
        assert!(atlas
            .providers_for("chat", &PrivacyScope::Community)
            .await
            .is_empty());
    }
}
