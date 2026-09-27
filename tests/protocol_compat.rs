use galaxia_agent::{
    fhs::{chat_request, encode_envelope},
    policy::{AgentRequest, ModelPreferences, RequestPlan},
    protocol::fhs,
};
use prost::Message;

#[test]
#[allow(deprecated)]
fn chat_request_uses_document_chunks_not_deprecated_full_text() {
    let request = AgentRequest {
        conversation_id: "c".into(),
        request_id: "r".into(),
        message: "qué dice".into(),
        preferences: ModelPreferences::default(),
        document_id: None,
        document_context: vec![galaxia_agent::policy::DocumentChunk {
            chunk_id: "chunk-1".into(),
            filename: "doc.pdf".into(),
            chunk_index: 0,
            text: "fragmento".into(),
            score: 0.9,
            source: Some("network".into()),
        }],
        attachments: vec![],
    };
    let plan = RequestPlan::build(request).unwrap();
    let wire = chat_request(&plan, "qwen");
    let context = wire.document_context.expect("context");
    assert!(context.text.is_empty());
    assert_eq!(context.chunks.len(), 1);
    assert!(context.chunks[0].text.contains("fragmento"));
}

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
    let encoded = encode_envelope(&envelope).unwrap();
    let decoded = fhs::Envelope::decode(encoded.as_slice()).unwrap();
    assert_eq!(decoded.message_id, "m");
    assert!(matches!(
        decoded.payload,
        Some(fhs::envelope::Payload::Handshake(_))
    ));
}
