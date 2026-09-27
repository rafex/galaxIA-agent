//! Firmas FHS compatibles con la implementación TS de producción.
//!
//! Cada mensaje firma una cadena de campos separados por `:` (ver
//! `@rafex/galaxia-fhs-protocol`, `identity.ts`) con Ed25519, sobre sus bytes
//! UTF-8. La llave pública sale del propio DID (`did:key:z…`, multicódec
//! `0xed 0x01`). Dos firmas dependen de re-codificar Protobuf: el sha256 del
//! beacon en `NodeAdvertise` y el hex del payload en `Envelope`. Por eso los
//! fixtures dorados (`tests/fixtures/wire.json`, generados desde el TS)
//! comprueban también que `prost` produce los mismos bytes que protobuf-es.

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::protocol::fhs::{
    self, envelope::Payload, Beacon, DhtBeaconRecord, MissionAssignMessage, MissionBidMessage,
    MissionOfferMessage, NodeAdvertiseMessage,
};

const DID_KEY_PREFIX: &str = "did:key:z";
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DidError {
    #[error("el DID no es did:key base58btc")]
    NotDidKey,
    #[error("base58 inválido en el DID")]
    Base58,
    #[error("el DID no contiene una llave Ed25519")]
    NotEd25519,
}

/// Llave pública Ed25519 (32 bytes) contenida en un `did:key:z…`.
pub fn did_public_key(did: &str) -> Result<[u8; 32], DidError> {
    let encoded = did
        .strip_prefix(DID_KEY_PREFIX)
        .ok_or(DidError::NotDidKey)?;
    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|_| DidError::Base58)?;
    if bytes.len() != 34 || bytes[..2] != ED25519_MULTICODEC {
        return Err(DidError::NotEd25519);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes[2..]);
    Ok(key)
}

/// Verifica una firma Ed25519 del `signer_did` sobre `payload` (UTF-8).
pub fn verify(signer_did: &str, payload: &str, signature: &[u8]) -> bool {
    let Ok(key) = did_public_key(signer_did) else {
        return false;
    };
    let Ok(verifying_key) = VerifyingKey::from_bytes(&key) else {
        return false;
    };
    let Ok(signature) = Signature::from_slice(signature) else {
        return false;
    };
    verifying_key.verify(payload.as_bytes(), &signature).is_ok()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// sha256 (hex) de la codificación Protobuf del beacon; ausente = beacon vacío.
pub fn beacon_sha256(beacon: Option<&Beacon>) -> String {
    sha256_hex(&beacon.cloned().unwrap_or_default().encode_to_vec())
}

pub fn node_advertise_payload(message: &NodeAdvertiseMessage) -> String {
    format!(
        "{}:{}:{}:{}",
        message.did,
        beacon_sha256(message.beacon.as_ref()),
        message.timestamp,
        message.ttl_seconds
    )
}

/// `dhtBeaconSignaturePayload` del SDK; el Portal lo verifica al leer el DHT.
pub fn dht_beacon_payload(record: &DhtBeaconRecord) -> String {
    format!(
        "{}:{}:{}:{}",
        record.did,
        beacon_sha256(record.beacon.as_ref()),
        record.published_at,
        record.expires_at
    )
}

pub fn verify_dht_beacon(record: &DhtBeaconRecord) -> bool {
    verify(&record.did, &dht_beacon_payload(record), &record.signature)
}

pub fn mission_offer_payload(message: &MissionOfferMessage) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        message.mission_id,
        message.navigator_did,
        message.mission_type,
        message.bid_deadline_ms,
        message.timestamp
    )
}

/// Las capabilities ofrecidas se firman ordenadas y unidas por comas.
pub fn mission_bid_payload(message: &MissionBidMessage) -> String {
    let mut capabilities = message.offered_capabilities.clone();
    capabilities.sort();
    format!(
        "{}:{}:{}:{}",
        message.mission_id,
        message.provider_did,
        capabilities.join(","),
        message.timestamp
    )
}

pub fn mission_assign_payload(message: &MissionAssignMessage) -> String {
    format!(
        "{}:{}:{}:{}",
        message.mission_id, message.navigator_did, message.assigned_provider, message.timestamp
    )
}

pub fn verify_node_advertise(message: &NodeAdvertiseMessage) -> bool {
    verify(
        &message.did,
        &node_advertise_payload(message),
        &message.signature,
    )
}

pub fn verify_mission_offer(message: &MissionOfferMessage) -> bool {
    verify(
        &message.navigator_did,
        &mission_offer_payload(message),
        &message.signature,
    )
}

pub fn verify_mission_bid(message: &MissionBidMessage) -> bool {
    verify(
        &message.provider_did,
        &mission_bid_payload(message),
        &message.signature,
    )
}

pub fn verify_mission_assign(message: &MissionAssignMessage) -> bool {
    verify(
        &message.navigator_did,
        &mission_assign_payload(message),
        &message.signature,
    )
}

/// Bytes Protobuf del mensaje interno del payload: lo que se firma en hex.
pub fn envelope_payload_bytes(payload: Option<&Payload>) -> Vec<u8> {
    macro_rules! inner {
        ($payload:expr, $($variant:ident),+ $(,)?) => {
            match $payload {
                None => Vec::new(),
                $(Some(Payload::$variant(message)) => message.encode_to_vec(),)+
            }
        };
    }
    inner!(
        payload,
        Handshake,
        HandshakeAck,
        Ping,
        Pong,
        Error,
        ChatRequest,
        ChatCancel,
        ChatDelta,
        ChatCompleted,
        ChatError,
        DispatchAck,
        ToolCall,
        ToolCancel,
        ToolResult,
        ToolError,
        ToolList,
        ToolListResp,
        NodeAdvertise,
        MissionOffer,
        MissionBid,
        MissionAssign,
        DhtBeacon,
        DhtReputation,
        AgentStart,
        AgentStatus,
        StarSelected,
        ToolSelected,
        AssistantDelta,
        AssistantCompleted,
        OcrExtracted,
        KbRecommended,
        KbDecision,
        MissionFeedback,
        ReputationUpdate,
    )
}

/// Cadena de firma de un Envelope a partir de los bytes de su payload.
fn envelope_signature_payload(envelope: &fhs::Envelope, payload_bytes: &[u8]) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        envelope.message_id,
        envelope.source_peer_id,
        envelope.dest_peer_id,
        envelope.timestamp,
        hex::encode(payload_bytes)
    )
}

/// Cadena que se firma al **enviar** un Envelope construido aquí: el payload
/// se codifica con prost y esos mismos bytes son los que viajan.
pub fn envelope_payload(envelope: &fhs::Envelope) -> String {
    envelope_signature_payload(envelope, &envelope_payload_bytes(envelope.payload.as_ref()))
}

/// Tags del `oneof payload` de `Envelope` (ver `protocol/fhs-protocol.proto`).
const ENVELOPE_PAYLOAD_TAGS: [u32; 34] = [
    10, 11, 12, 13, 14, 20, 21, 22, 23, 24, 25, 30, 31, 32, 33, 34, 35, 40, 50, 51, 52, 53, 54, 60,
    61, 62, 63, 64, 65, 66, 67, 69, 70, 80,
];

/// Bytes del payload **tal como llegaron** en el Envelope, sin re-codificar.
///
/// No se puede verificar re-codificando: `DynamicObject.fields` y
/// `ToolInputSchema.properties` son `map`, protobuf-es los escribe en orden de
/// inserción y prost (`HashMap`) en orden arbitrario. Un tool call con
/// argumentos de varias claves no verificaría (visto en la fase 0 de la
/// migración, fixture `envelope_tool_call`).
pub fn raw_envelope_payload(envelope_bytes: &[u8]) -> Result<&[u8], prost::DecodeError> {
    use prost::encoding::{decode_key, decode_varint, WireType};
    let mut buf = envelope_bytes;
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match wire_type {
            WireType::Varint => {
                decode_varint(&mut buf)?;
            }
            WireType::SixtyFourBit => skip(&mut buf, 8)?,
            WireType::ThirtyTwoBit => skip(&mut buf, 4)?,
            WireType::LengthDelimited => {
                let len = usize::try_from(decode_varint(&mut buf)?)
                    .map_err(|_| prost::DecodeError::new("longitud inválida"))?;
                if len > buf.len() {
                    return Err(prost::DecodeError::new("campo truncado"));
                }
                if ENVELOPE_PAYLOAD_TAGS.contains(&tag) {
                    return Ok(&buf[..len]);
                }
                buf = &buf[len..];
            }
            _ => {
                return Err(prost::DecodeError::new(
                    "wire type no soportado en Envelope",
                ))
            }
        }
    }
    Ok(&[])
}

fn skip(buf: &mut &[u8], len: usize) -> Result<(), prost::DecodeError> {
    if len > buf.len() {
        return Err(prost::DecodeError::new("campo truncado"));
    }
    *buf = &buf[len..];
    Ok(())
}

/// Decodifica un Envelope recibido y verifica su firma sobre los bytes crudos
/// del payload. Devuelve el Envelope solo si la firma es válida.
pub fn verify_envelope_bytes(
    envelope_bytes: &[u8],
) -> Result<Option<fhs::Envelope>, prost::DecodeError> {
    let envelope = fhs::Envelope::decode(envelope_bytes)?;
    if envelope.signature.is_empty() || envelope.source_peer_id.is_empty() {
        return Ok(None);
    }
    let payload = raw_envelope_payload(envelope_bytes)?;
    let valid = verify(
        &envelope.source_peer_id,
        &envelope_signature_payload(&envelope, payload),
        &envelope.signature,
    );
    Ok(valid.then_some(envelope))
}

/// Frame del stream `/fhs/v1/0.1.0`: longitud varint sin signo + Envelope.
pub fn encode_frame(envelope: &fhs::Envelope) -> Vec<u8> {
    envelope.encode_length_delimited_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_dids_that_are_not_ed25519_did_key() {
        assert_eq!(did_public_key("did:web:x"), Err(DidError::NotDidKey));
        assert_eq!(did_public_key("did:key:z0OIl"), Err(DidError::Base58));
        assert_eq!(did_public_key("did:key:z2"), Err(DidError::NotEd25519));
    }

    #[test]
    fn bid_payload_sorts_capabilities() {
        let bid = MissionBidMessage {
            mission_id: "m".into(),
            provider_did: "did:key:zX".into(),
            offered_capabilities: vec!["knowledge.query".into(), "document.query".into()],
            timestamp: 7,
            ..Default::default()
        };
        assert_eq!(
            mission_bid_payload(&bid),
            "m:did:key:zX:document.query,knowledge.query:7"
        );
    }
}
