//! Fase 0 de la migración: el Rust debe entender el wire exactamente como la
//! implementación TS de producción. `tests/fixtures/wire.json` se genera con
//! `galaxIA-Core/apps/navigator/scripts/export-wire-fixtures.ts`.

use galaxia_agent::{protocol::fhs, signing};
use prost::Message;
use serde_json::Value;

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/wire.json")).expect("wire.json válido")
}

fn bytes(entry: &Value, field: &str) -> Vec<u8> {
    hex::decode(entry[field].as_str().expect(field)).expect("hex válido")
}

fn text<'a>(entry: &'a Value, field: &str) -> &'a str {
    entry[field].as_str().expect(field)
}

#[test]
fn did_of_the_fixture_identity_holds_its_public_key() {
    let identity = &fixtures()["identity"];
    let key = signing::did_public_key(text(identity, "did")).unwrap();
    assert_eq!(hex::encode(key), text(identity, "public_key_hex"));
}

#[test]
fn gossip_messages_decode_reencode_and_verify() {
    let fixtures = fixtures();
    let entries = fixtures["gossip"].as_array().unwrap();
    assert!(!entries.is_empty());
    for entry in entries {
        let name = text(entry, "name");
        let raw = bytes(entry, "bytes_hex");
        let expected_payload = text(entry, "signature_payload");
        let (payload, verified, reencoded) = match text(entry, "type") {
            "NodeAdvertiseMessage" => {
                let m = fhs::NodeAdvertiseMessage::decode(raw.as_slice()).unwrap();
                assert_eq!(
                    signing::beacon_sha256(m.beacon.as_ref()),
                    text(entry, "beacon_sha256"),
                    "{name}: el beacon re-codificado por prost difiere del de protobuf-es"
                );
                (
                    signing::node_advertise_payload(&m),
                    signing::verify_node_advertise(&m),
                    m.encode_to_vec(),
                )
            }
            "MissionOfferMessage" => {
                let m = fhs::MissionOfferMessage::decode(raw.as_slice()).unwrap();
                (
                    signing::mission_offer_payload(&m),
                    signing::verify_mission_offer(&m),
                    m.encode_to_vec(),
                )
            }
            "MissionBidMessage" => {
                let m = fhs::MissionBidMessage::decode(raw.as_slice()).unwrap();
                (
                    signing::mission_bid_payload(&m),
                    signing::verify_mission_bid(&m),
                    m.encode_to_vec(),
                )
            }
            "MissionAssignMessage" => {
                let m = fhs::MissionAssignMessage::decode(raw.as_slice()).unwrap();
                (
                    signing::mission_assign_payload(&m),
                    signing::verify_mission_assign(&m),
                    m.encode_to_vec(),
                )
            }
            other => panic!("tipo sin cubrir: {other}"),
        };
        assert_eq!(
            payload, expected_payload,
            "{name}: cadena de firma distinta"
        );
        assert!(verified, "{name}: la firma del TS no verifica en Rust");
        assert_eq!(
            reencoded, raw,
            "{name}: prost no re-codifica los mismos bytes"
        );
    }
}

#[test]
fn tampered_gossip_message_does_not_verify() {
    let fixtures = fixtures();
    let entry = &fixtures["gossip"][0];
    let mut m = fhs::NodeAdvertiseMessage::decode(bytes(entry, "bytes_hex").as_slice()).unwrap();
    m.ttl_seconds += 1;
    assert!(!signing::verify_node_advertise(&m));
}

#[test]
fn envelopes_verify_over_the_raw_payload_bytes() {
    let fixtures = fixtures();
    for entry in fixtures["envelopes"].as_array().unwrap() {
        let name = text(entry, "name");
        let raw = bytes(entry, "envelope_hex");
        assert_eq!(
            hex::encode(signing::raw_envelope_payload(&raw).unwrap()),
            text(entry, "payload_hex"),
            "{name}: payload crudo distinto"
        );
        let envelope = signing::verify_envelope_bytes(&raw)
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: la firma del TS no verifica en Rust"));
        let mut frame = Vec::new();
        prost::encoding::encode_varint(raw.len() as u64, &mut frame);
        frame.extend_from_slice(&raw);
        assert_eq!(
            hex::encode(frame),
            text(entry, "frame_hex"),
            "{name}: frame distinto"
        );
        assert!(envelope.payload.is_some(), "{name}");
    }
}

#[test]
fn envelopes_without_maps_reencode_byte_for_byte() {
    // Solo los payloads sin `map` (sin DynamicObject ni ToolInputSchema con
    // varias propiedades) son re-codificables de forma idéntica.
    let fixtures = fixtures();
    for entry in fixtures["envelopes"].as_array().unwrap() {
        if entry["contains_multi_key_map"].as_bool().unwrap_or(false) {
            continue;
        }
        let raw = bytes(entry, "envelope_hex");
        let envelope = fhs::Envelope::decode(raw.as_slice()).unwrap();
        assert_eq!(envelope.encode_to_vec(), raw, "{}", text(entry, "name"));
        assert_eq!(
            signing::envelope_payload(&envelope),
            text(entry, "signature_payload")
        );
    }
}

#[test]
fn tampered_envelope_does_not_verify() {
    let fixtures = fixtures();
    let mut raw = bytes(&fixtures["envelopes"][0], "envelope_hex");
    let last = raw.len() - 1;
    raw[last] ^= 0x01;
    assert!(matches!(
        signing::verify_envelope_bytes(&raw),
        Ok(None) | Err(_)
    ));
}

#[test]
fn dynamic_values_decode_to_the_same_content() {
    // No se compara byte a byte: DynamicObject es un `map` y prost no conserva
    // el orden de las claves (ver signing::raw_envelope_payload).
    let fixtures = fixtures();
    for entry in fixtures["dynamic_values"].as_array().unwrap() {
        let raw = bytes(entry, "bytes_hex");
        let value = fhs::DynamicValue::decode(raw.as_slice()).unwrap();
        let again = fhs::DynamicValue::decode(value.encode_to_vec().as_slice()).unwrap();
        assert_eq!(value, again, "{}", text(entry, "name"));
    }
}
