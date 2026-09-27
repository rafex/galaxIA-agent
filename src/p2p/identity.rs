//! Identidad del nodo: la misma que usa el Navigator TS.
//!
//! El archivo (`IDENTITY_KEY_PATH`) es JSON `{ "privateKeyHex": … }` con la
//! llave privada Ed25519 en la codificación protobuf de libp2p
//! (`privateKeyToProtobuf` en JS, `Keypair::to_protobuf_encoding` aquí). Así el
//! reemplazo conserva PeerId y DID usando el mismo volumen `navigator-data`.

use std::path::Path;

use libp2p::{identity::Keypair, PeerId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedIdentity {
    private_key_hex: String,
}

#[derive(Clone)]
pub struct NodeIdentity {
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub did: String,
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("no se pudo leer/escribir la identidad {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("identidad inválida en {path}: {reason}")]
    Invalid { path: String, reason: String },
}

impl NodeIdentity {
    pub fn from_keypair(keypair: Keypair) -> Result<Self, String> {
        let public = keypair
            .public()
            .try_into_ed25519()
            .map_err(|_| "la llave de identidad FHS debe ser Ed25519".to_string())?;
        let mut multicodec = vec![0xed, 0x01];
        multicodec.extend_from_slice(&public.to_bytes());
        Ok(Self {
            peer_id: keypair.public().to_peer_id(),
            did: format!("did:key:z{}", bs58::encode(multicodec).into_string()),
            keypair,
        })
    }

    /// Carga la identidad o la crea (y la persiste) si el archivo no existe.
    pub fn load_or_create(path: &Path) -> Result<Self, IdentityError> {
        let label = path.display().to_string();
        if path.exists() {
            let raw = std::fs::read_to_string(path).map_err(|source| IdentityError::Io {
                path: label.clone(),
                source,
            })?;
            let persisted: PersistedIdentity =
                serde_json::from_str(&raw).map_err(|e| IdentityError::Invalid {
                    path: label.clone(),
                    reason: e.to_string(),
                })?;
            let bytes = hex::decode(persisted.private_key_hex.trim()).map_err(|e| {
                IdentityError::Invalid {
                    path: label.clone(),
                    reason: e.to_string(),
                }
            })?;
            let keypair =
                Keypair::from_protobuf_encoding(&bytes).map_err(|e| IdentityError::Invalid {
                    path: label.clone(),
                    reason: e.to_string(),
                })?;
            return Self::from_keypair(keypair).map_err(|reason| IdentityError::Invalid {
                path: label,
                reason,
            });
        }
        let keypair = Keypair::generate_ed25519();
        let bytes = keypair
            .to_protobuf_encoding()
            .map_err(|e| IdentityError::Invalid {
                path: label.clone(),
                reason: e.to_string(),
            })?;
        let persisted = PersistedIdentity {
            private_key_hex: hex::encode(bytes),
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| IdentityError::Io {
                path: label.clone(),
                source,
            })?;
        }
        std::fs::write(
            path,
            serde_json::to_string_pretty(&persisted).expect("json"),
        )
        .map_err(|source| IdentityError::Io {
            path: label.clone(),
            source,
        })?;
        Self::from_keypair(keypair).map_err(|reason| IdentityError::Invalid {
            path: label,
            reason,
        })
    }

    /// Firma una cadena de firma FHS (UTF-8) con la llave del nodo.
    pub fn sign(&self, payload: &str) -> Vec<u8> {
        self.keypair
            .sign(payload.as_bytes())
            .expect("Ed25519 siempre firma")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_matches_the_ts_derivation_for_the_fixture_seed() {
        // Misma semilla que export-wire-fixtures.ts (bytes 1..=32).
        let seed: Vec<u8> = (1..=32).collect();
        let keypair = Keypair::ed25519_from_bytes(seed).unwrap();
        let identity = NodeIdentity::from_keypair(keypair).unwrap();
        assert_eq!(
            identity.did,
            "did:key:z6MkneMkZqwqRiU5mJzSG3kDwzt9P8C59N4NGTfBLfSGE7c7"
        );
        assert_eq!(
            identity.peer_id.to_string(),
            "12D3KooWJ1TsijH7H5F74hfAD5XishQz3sxrmAtVY37GtNd9CqYf"
        );
    }

    #[test]
    fn persists_and_reloads_the_same_identity() {
        let dir = std::env::temp_dir().join(format!("galaxia-id-{}", uuid::Uuid::new_v4()));
        let path = dir.join("id.json");
        let created = NodeIdentity::load_or_create(&path).unwrap();
        let loaded = NodeIdentity::load_or_create(&path).unwrap();
        assert_eq!(created.did, loaded.did);
        assert_eq!(created.peer_id, loaded.peer_id);
        std::fs::remove_dir_all(dir).ok();
    }
}
