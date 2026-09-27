use galaxia_agent::protocol::fhs;
use prost::Message;

#[test]
fn envelope_round_trip_is_protobuf() {
    let envelope = fhs::Envelope {
        message_id: "m".into(),
        source_peer_id: "did:key:test".into(),
        dest_peer_id: String::new(),
        timestamp: 1,
        version: "1".into(),
        signature: vec![],
        payload: Some(fhs::envelope::Payload::Handshake(fhs::HandshakeMessage {
            fhs_version: "1".into(),
            listen_addrs: vec![],
            beacon: None,
            delegation_token: None,
        })),
    };
    let encoded = envelope.encode_to_vec();
    let decoded = fhs::Envelope::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.message_id, "m");
    assert!(matches!(
        decoded.payload,
        Some(fhs::envelope::Payload::Handshake(_))
    ));
}
