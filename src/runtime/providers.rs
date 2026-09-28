//! Providers disponibles vistos desde la caché de anuncios: filtro por
//! ámbito de privacidad, tools anunciadas y KBs (`p2p-atlas-client.ts` +
//! `advertisedTools` + `matchesScope` del TS).

use crate::p2p::peer_cache::{PeerCache, PeerEntry};
use crate::protocol::fhs::Visibility;
use crate::runtime::kb::is_kb_capability;

/// Ámbito de privacidad pedido por el usuario (de más a menos restrictivo).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    Local = 0,
    Network = 1,
    Community = 2,
    External = 3,
}

impl Scope {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "local" => Some(Self::Local),
            "network" => Some(Self::Network),
            "community" => Some(Self::Community),
            "external" => Some(Self::External),
            _ => None,
        }
    }
}

/// Ámbito del provider según la visibilidad de su beacon firmado. Sin
/// visibilidad declarada cuenta como `community` (como el TS).
pub fn provider_scope(peer: &PeerEntry) -> Scope {
    match Visibility::try_from(peer.visibility()) {
        Ok(Visibility::Private) => Scope::Local,
        Ok(Visibility::Public) => Scope::External,
        _ => Scope::Community,
    }
}

pub fn allowed(peer: &PeerEntry, requested: Option<Scope>) -> bool {
    requested.is_none_or(|scope| provider_scope(peer) <= scope)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedTool {
    pub name: String,
    pub capability: String,
    pub provider_id: String,
    pub provider_name: String,
    pub description: String,
}

fn guess_capability(tool: &str) -> String {
    match tool {
        "extract_text" | "ocr_extract" => "document.ocr",
        "document_query" => "document.query",
        "document_index" | "index_document" => "document.index",
        "kb_query" | "search_kb" => "knowledge.query",
        other => other,
    }
    .to_string()
}

/// Tools anunciadas en el beacon (`tool:<nombre>`), asociadas a una
/// capability anunciada; sin tags, cada capability es su propia tool.
pub fn advertised_tools(peer: &PeerEntry) -> Vec<LoadedTool> {
    // `ipfs.native.<red>` describe acceso a IPFS, no es una tool.
    let capabilities: Vec<String> = peer
        .capabilities
        .iter()
        .filter(|c| !c.starts_with("ipfs.native."))
        .cloned()
        .collect();
    let names: Vec<String> = peer
        .tags()
        .iter()
        .filter_map(|t| t.strip_prefix("tool:").map(|n| n.trim().to_string()))
        .filter(|n| !n.is_empty())
        .collect();
    let candidates = if names.is_empty() {
        capabilities.clone()
    } else {
        names
    };
    candidates
        .into_iter()
        .filter_map(|name| {
            let capability = if capabilities.len() == 1 {
                capabilities[0].clone()
            } else {
                guess_capability(&name)
            };
            capabilities.contains(&capability).then(|| LoadedTool {
                description: format!("Capability '{capability}' vía satellite P2P"),
                name,
                capability,
                provider_id: peer.did.clone(),
                provider_name: peer.name(),
            })
        })
        .collect()
}

/// Tools de los Satellites en ámbito que atienden alguna de `capabilities`.
pub fn tools_for(
    peers: &PeerCache,
    capabilities: &[&str],
    scope: Option<Scope>,
) -> Vec<LoadedTool> {
    peers
        .satellites()
        .iter()
        .filter(|p| allowed(p, scope))
        .flat_map(advertised_tools)
        .filter(|t| capabilities.contains(&t.capability.as_str()))
        .collect()
}

/// Tools de `capability` de los Satellites en ámbito que además anuncian
/// todas las capacidades `also` (p. ej. `ipfs.native.public`).
pub fn tools_with(
    peers: &PeerCache,
    capability: &str,
    also: &[&str],
    scope: Option<Scope>,
) -> Vec<LoadedTool> {
    peers
        .satellites()
        .iter()
        .filter(|p| allowed(p, scope))
        .filter(|p| also.iter().all(|c| p.capabilities.iter().any(|pc| pc == c)))
        .flat_map(advertised_tools)
        .filter(|t| t.capability == capability)
        .collect()
}

#[derive(Clone, Debug)]
pub struct KbProvider {
    pub provider_id: String,
    pub provider_name: String,
    pub description: String,
    pub tags: Vec<String>,
}

pub fn kb_providers(peers: &PeerCache, scope: Option<Scope>) -> Vec<KbProvider> {
    peers
        .satellites()
        .into_iter()
        .filter(|p| allowed(p, scope) && p.capabilities.iter().any(|c| is_kb_capability(c)))
        .map(|p| KbProvider {
            provider_id: p.did.clone(),
            provider_name: p.name(),
            description: p.description(),
            tags: p.tags(),
        })
        .collect()
}

pub fn stars(peers: &PeerCache, scope: Option<Scope>) -> Vec<PeerEntry> {
    peers
        .stars()
        .into_iter()
        .filter(|p| allowed(p, scope))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fhs::{self, NodeAdvertiseMessage, ProviderType};

    fn satellite(did: &str, caps: &[&str], tags: &[&str], visibility: Visibility) -> PeerCache {
        let cache = PeerCache::default();
        cache.upsert(&NodeAdvertiseMessage {
            did: did.into(),
            beacon: Some(fhs::Beacon {
                provider: Some(fhs::ProviderIdentity {
                    r#type: ProviderType::Satellite as i32,
                    visibility: visibility as i32,
                    name: "Sat".into(),
                    tags: tags.iter().map(|s| s.to_string()).collect(),
                    description: "Constitución".into(),
                    ..Default::default()
                }),
                capabilities: caps
                    .iter()
                    .map(|c| fhs::CapabilityDescriptor {
                        id: c.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            ttl_seconds: 60,
            ..Default::default()
        });
        cache
    }

    #[test]
    fn maps_tool_tags_to_capabilities() {
        let rag = satellite(
            "did:rag",
            &["document.index", "document.query"],
            &["tool:document_index", "tool:document_query"],
            Visibility::Community,
        );
        let tools = tools_for(&rag, &["document.index"], None);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "document_index");
        let kb = satellite(
            "did:kb",
            &["knowledge.query"],
            &["tool:kb_query"],
            Visibility::Community,
        );
        assert_eq!(
            tools_for(&kb, &["knowledge.query"], None)[0].name,
            "kb_query"
        );
        assert_eq!(kb_providers(&kb, Some(Scope::Community)).len(), 1);
    }

    #[test]
    fn ipfs_access_filters_ocr_providers_and_is_not_a_tool() {
        let with = satellite(
            "did:con",
            &["document.ocr", "ipfs.native.public"],
            &["tool:extract_text"],
            Visibility::Community,
        );
        let tools = tools_with(&with, "document.ocr", &["ipfs.native.public"], None);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "extract_text");
        assert!(tools_with(&with, "document.ocr", &["ipfs.native.private"], None).is_empty());
        let without = satellite("did:sin", &["document.ocr"], &[], Visibility::Community);
        assert!(tools_with(&without, "document.ocr", &["ipfs.native.public"], None).is_empty());
        assert_eq!(tools_with(&without, "document.ocr", &[], None).len(), 1);
        // Sin tags, la capacidad de IPFS no se vuelve una tool.
        let untagged = satellite(
            "did:x",
            &["document.ocr", "ipfs.native.public"],
            &[],
            Visibility::Community,
        );
        let tools = tools_with(&untagged, "document.ocr", &[], None);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].capability, "document.ocr");
    }

    #[test]
    fn scope_filters_by_beacon_visibility() {
        let public = satellite("did:pub", &["knowledge.query"], &[], Visibility::Public);
        assert!(kb_providers(&public, Some(Scope::Community)).is_empty());
        assert_eq!(kb_providers(&public, Some(Scope::External)).len(), 1);
        let unspecified = satellite("did:x", &["knowledge.query"], &[], Visibility::Unspecified);
        assert_eq!(kb_providers(&unspecified, Some(Scope::Community)).len(), 1);
    }
}
