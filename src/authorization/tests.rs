use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::runtime::events::Collected;

fn digest_of(text: &str) -> [u8; 32] {
    digest::user_message_digest(text)
}

fn spec(id: &str, did: &str, depends_on: &[&str]) -> ItemSpec {
    let mut spec = ItemSpec::new(
        id,
        "document.ocr",
        did,
        "OCR de prueba",
        DataClass::Document,
        "archivo de prueba",
        digest_of(id),
    );
    spec.depends_on = depends_on.iter().map(|d| d.to_string()).collect();
    spec
}

/// Lanza `request` y devuelve el mensaje `authorization.requested` emitido.
async fn start(
    authorizer: &Authorizer,
    sink: Arc<Collected>,
    session: &'static str,
    items: Vec<ItemSpec>,
    ttl: Duration,
) -> (
    tokio::task::JoinHandle<Result<Resolution, AuthError>>,
    String,
    Vec<u8>,
) {
    let authorizer = authorizer.clone();
    let task_sink = sink.clone();
    let handle = tokio::spawn(async move {
        let ctx = Ctx {
            session,
            conversation: "conv",
            turn: "turn",
            sink: &*task_sink,
        };
        authorizer.request(&ctx, items, ttl).await
    });
    loop {
        let found = sink.0.lock().unwrap().iter().find_map(|e| match e {
            AgentEvent::AuthorizationRequested {
                authorization_id,
                batch_digest,
                ..
            } => Some((authorization_id.clone(), batch_digest.clone())),
            _ => None,
        });
        if let Some((id, batch)) = found {
            return (handle, id, batch);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn decision(id: &str, batch: Vec<u8>, allow: &[(&str, bool)]) -> fhs::AuthorizationDecisionMessage {
    fhs::AuthorizationDecisionMessage {
        authorization_id: id.into(),
        batch_digest: batch,
        decisions: allow
            .iter()
            .map(|(item, allow)| fhs::AuthorizationItemDecision {
                item_id: item.to_string(),
                allow: *allow,
            })
            .collect(),
    }
}

#[tokio::test]
async fn a_granted_item_is_single_use_and_bound_to_digest_and_node() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let (handle, id, batch) = start(
        &authorizer,
        sink,
        "s1",
        vec![spec("a", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    authorizer
        .decide("s1", &decision(&id, batch, &[("a", true)]))
        .unwrap();
    let resolution = handle.await.unwrap().unwrap();
    let grant = resolution.grant("a").expect("permitido").clone();
    assert_eq!(grant.basis(), Basis::Explicit);

    assert_eq!(
        grant.consume("document.ocr", "did:otro", digest_of("a")),
        Err(GrantError::ProviderMismatch)
    );
    assert_eq!(
        grant.consume("document.ocr", "did:ocr", digest_of("distinto")),
        Err(GrantError::DigestMismatch)
    );
    assert_eq!(
        grant.consume("document.index", "did:ocr", digest_of("a")),
        Err(GrantError::CapabilityMismatch)
    );
    // Los intentos fallidos no consumen el permiso.
    assert_eq!(
        grant.consume("document.ocr", "did:ocr", digest_of("a")),
        Ok(())
    );
    assert_eq!(
        grant.consume("document.ocr", "did:ocr", digest_of("a")),
        Err(GrantError::AlreadyConsumed)
    );
}

#[tokio::test]
async fn a_denied_dependency_cascades_but_independent_items_proceed() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let items = vec![
        spec("subir", "did:ipfs", &[]),
        spec("ocr", "did:ocr", &["subir"]),
        spec("kb", "did:kb", &[]),
    ];
    let (handle, id, batch) = start(&authorizer, sink.clone(), "s1", items, DEFAULT_TTL).await;
    authorizer
        .decide(
            "s1",
            &decision(&id, batch, &[("subir", false), ("ocr", true), ("kb", true)]),
        )
        .unwrap();
    let resolution = handle.await.unwrap().unwrap();
    assert!(resolution.grant("subir").is_none());
    assert!(
        resolution.grant("ocr").is_none(),
        "depende de un ítem denegado"
    );
    assert!(resolution.grant("kb").is_some());
    let resolved = sink.0.lock().unwrap().iter().find_map(|e| match e {
        AgentEvent::AuthorizationResolved { outcome, items, .. } => Some((*outcome, items.clone())),
        _ => None,
    });
    let (outcome, items) = resolved.expect("resolved");
    assert_eq!(outcome, Outcome::Partial as i32);
    let ocr = items.iter().find(|i| i.item_id == "ocr").unwrap();
    assert_eq!(ocr.outcome, Outcome::Denied as i32);
    assert!(ocr.reason.contains("subir"));
}

#[tokio::test]
async fn decisions_from_other_sessions_other_batches_or_repeated_are_ignored() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let (handle, id, batch) = start(
        &authorizer,
        sink,
        "s1",
        vec![spec("a", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    assert_eq!(
        authorizer.decide("otra", &decision(&id, batch.clone(), &[("a", true)])),
        Err(DecideError::WrongSession)
    );
    assert_eq!(
        authorizer.decide("s1", &decision(&id, vec![9; 32], &[("a", true)])),
        Err(DecideError::BatchMismatch)
    );
    assert_eq!(
        authorizer.decide("s1", &decision(&id, batch.clone(), &[("zzz", true)])),
        Err(DecideError::UnknownItem)
    );
    assert_eq!(
        authorizer.decide("s1", &decision("no-existe", batch.clone(), &[("a", true)])),
        Err(DecideError::Unknown)
    );
    authorizer
        .decide("s1", &decision(&id, batch.clone(), &[("a", true)]))
        .unwrap();
    assert_eq!(
        authorizer.decide("s1", &decision(&id, batch, &[("a", true)])),
        Err(DecideError::Unknown),
        "una decisión se usa una sola vez"
    );
    assert!(handle.await.unwrap().unwrap().grant("a").is_some());
}

#[tokio::test]
async fn without_a_decision_everything_expires_and_late_decisions_are_ignored() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let (handle, id, batch) = start(
        &authorizer,
        sink.clone(),
        "s1",
        vec![spec("a", "did:ocr", &[])],
        Duration::from_millis(60),
    )
    .await;
    let resolution = handle.await.unwrap().unwrap();
    assert!(resolution.all_expired());
    assert!(resolution.grant("a").is_none());
    assert_eq!(
        authorizer.decide("s1", &decision(&id, batch, &[("a", true)])),
        Err(DecideError::Unknown)
    );
    let status = authorizer.status("s1", &id);
    assert_eq!(status.outcome, Outcome::Expired as i32);
}

#[tokio::test]
async fn cancelling_a_conversation_cancels_only_its_pending_requests() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let (handle, _id, _batch) = start(
        &authorizer,
        sink,
        "s1",
        vec![spec("a", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    authorizer.cancel_conversation("s1", "otra-conversacion");
    assert!(!handle.is_finished());
    authorizer.cancel_conversation("s1", "conv");
    assert!(handle.await.unwrap().unwrap().all_cancelled());
}

#[tokio::test]
async fn concurrent_requests_do_not_overwrite_each_other() {
    let authorizer = Authorizer::for_tests();
    let sink_a = Arc::new(Collected::default());
    let sink_b = Arc::new(Collected::default());
    let (ha, ida, ba) = start(
        &authorizer,
        sink_a,
        "s1",
        vec![spec("a", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    let (hb, idb, bb) = start(
        &authorizer,
        sink_b,
        "s1",
        vec![spec("b", "did:kb", &[])],
        DEFAULT_TTL,
    )
    .await;
    assert_ne!(ida, idb);
    // La decisión del segundo no toca al primero.
    authorizer
        .decide("s1", &decision(&idb, bb, &[("b", true)]))
        .unwrap();
    assert!(hb.await.unwrap().unwrap().grant("b").is_some());
    assert!(!ha.is_finished());
    authorizer
        .decide("s1", &decision(&ida, ba, &[("a", false)]))
        .unwrap();
    assert!(ha.await.unwrap().unwrap().grant("a").is_none());
}

#[tokio::test]
async fn headless_policy_denies_by_default_and_allows_only_synthetic() {
    let sink = Collected::default();
    let ctx = Ctx {
        session: "cli",
        conversation: "c",
        turn: "t",
        sink: &sink,
    };
    let deny = Authorizer::headless_for_tests(HeadlessPolicy::Deny);
    let resolution = deny
        .request(&ctx, vec![spec("a", "did:ocr", &[])], DEFAULT_TTL)
        .await
        .unwrap();
    assert!(resolution.grant("a").is_none());

    let allow = Authorizer::headless_for_tests(HeadlessPolicy::AllowSynthetic);
    let resolution = allow
        .request(&ctx, vec![spec("a", "did:ocr", &[])], DEFAULT_TTL)
        .await
        .unwrap();
    assert_eq!(resolution.grant("a").unwrap().basis(), Basis::Headless);
}

#[tokio::test]
async fn invalid_requests_are_rejected_before_anything_is_asked() {
    let authorizer = Authorizer::for_tests();
    let sink = Collected::default();
    let ctx = Ctx {
        session: "s",
        conversation: "c",
        turn: "t",
        sink: &sink,
    };
    let mut no_digest = spec("a", "did:ocr", &[]);
    no_digest.digest = [0; 32];
    for bad in [
        vec![],
        vec![spec("a", "did:ocr", &[]), spec("a", "did:kb", &[])],
        vec![spec("a", "did:ocr", &["no-existe"])],
        vec![spec("a", "did:ocr", &["a"])],
        vec![no_digest],
        vec![spec("a", "", &[])],
    ] {
        assert!(authorizer
            .request(&ctx, bad, Duration::from_millis(20))
            .await
            .is_err());
    }
    assert!(
        sink.0.lock().unwrap().is_empty(),
        "no se mostró ninguna tarjeta"
    );
}

fn star(did: &str, visibility: fhs::Visibility) -> PeerEntry {
    PeerEntry {
        did: did.into(),
        beacon: fhs::Beacon {
            provider: Some(fhs::ProviderIdentity {
                id: did.into(),
                visibility: visibility as i32,
                ..Default::default()
            }),
            ..Default::default()
        },
        multiaddrs: vec![],
        trust_level: "community".into(),
        reputation_score: 0.5,
        peer_type: "star",
        capabilities: vec!["chat".into()],
        last_seen_ms: 0,
        expires_at_ms: i64::MAX,
        advert_expires_ms: i64::MAX,
        advert_timestamp_ms: 0,
    }
}

#[test]
fn the_implicit_grant_only_covers_the_literal_message_to_the_chosen_star_in_scope() {
    let authorizer = Authorizer::for_tests();
    let vetoed = HashSet::new();
    let grant = authorizer
        .implicit_user_message(
            &star("did:star", fhs::Visibility::Community),
            Some(Scope::Community),
            &vetoed,
            "hola",
        )
        .unwrap();
    assert_eq!(grant.basis(), Basis::Implicit);
    assert_eq!(
        grant.consume("chat", "did:star", digest_of("otro mensaje")),
        Err(GrantError::DigestMismatch)
    );
    assert_eq!(
        grant.consume("chat", "did:star", digest::user_message_digest("hola")),
        Ok(())
    );
    // Fuera del ámbito o vetado: no hay consentimiento implícito.
    assert!(authorizer
        .implicit_user_message(
            &star("did:pub", fhs::Visibility::Public),
            Some(Scope::Community),
            &vetoed,
            "hola",
        )
        .is_err());
    let vetoed: HashSet<String> = ["did:star".to_string()].into();
    assert!(authorizer
        .implicit_user_message(
            &star("did:star", fhs::Visibility::Community),
            Some(Scope::Community),
            &vetoed,
            "hola",
        )
        .is_err());
}

#[tokio::test]
async fn trusted_and_first_time_nodes_are_derived_by_the_navigator() {
    let trusted: HashSet<String> = ["did:ocr".to_string()].into();
    let authorizer = Authorizer::new(Arc::new(trusted), None, None);
    let sink = Arc::new(Collected::default());
    let (handle, id, batch) = start(
        &authorizer,
        sink.clone(),
        "s1",
        vec![spec("a", "did:ocr", &[]), spec("b", "did:desconocido", &[])],
        DEFAULT_TTL,
    )
    .await;
    let items = sink.0.lock().unwrap().iter().find_map(|e| match e {
        AgentEvent::AuthorizationRequested { items, .. } => Some(items.clone()),
        _ => None,
    });
    let items = items.unwrap();
    let a = items.iter().find(|i| i.item_id == "a").unwrap();
    let b = items.iter().find(|i| i.item_id == "b").unwrap();
    assert_eq!(a.trust_level, "operator");
    assert_eq!(b.trust_level, "community");
    assert!(a.first_time_node && b.first_time_node);
    authorizer
        .decide("s1", &decision(&id, batch, &[("a", true), ("b", false)]))
        .unwrap();
    handle.await.unwrap().unwrap();
    // Tras autorizar a un nodo, deja de ser la primera vez.
    let sink2 = Arc::new(Collected::default());
    let (_h, _id2, _b2) = start(
        &authorizer,
        sink2.clone(),
        "s1",
        vec![spec("c", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    let first = sink2.0.lock().unwrap().iter().find_map(|e| match e {
        AgentEvent::AuthorizationRequested { items, .. } => Some(items[0].first_time_node),
        _ => None,
    });
    assert_eq!(first, Some(false));
}

fn command_spec(fingerprint: &str) -> ItemSpec {
    let mut item = ItemSpec::new(
        "cmd-0",
        "math.arithmetic.solve",
        "did:phone",
        "Nodo de prueba",
        DataClass::CommandArgs,
        "comando /calc · 1 argumento · 5 caracteres",
        digest_of("cmd"),
    );
    item.contract = Some(Contract {
        fingerprint: fingerprint.into(),
        tool: "arithmetic_solve".into(),
        registry_digest: "cd".repeat(32),
    });
    item
}

#[tokio::test]
async fn a_command_grant_is_bound_to_its_contract_and_the_card_carries_it() {
    let authorizer = Authorizer::for_tests();
    let sink = Arc::new(Collected::default());
    let fingerprint = "ab".repeat(32);
    let (handle, id, batch) = start(
        &authorizer,
        sink.clone(),
        "s1",
        vec![command_spec(&fingerprint), spec("a", "did:ocr", &[])],
        DEFAULT_TTL,
    )
    .await;
    // La tarjeta lleva la ligadura de contexto (y solo en el ítem de comando).
    let items = sink
        .0
        .lock()
        .unwrap()
        .iter()
        .find_map(|e| match e {
            AgentEvent::AuthorizationRequested { items, .. } => Some(items.clone()),
            _ => None,
        })
        .unwrap();
    let card = items.iter().find(|i| i.item_id == "cmd-0").unwrap();
    assert_eq!(card.contract_fingerprint, fingerprint);
    assert_eq!(card.tool_name, "arithmetic_solve");
    assert_eq!(card.registry_digest, "cd".repeat(32));
    let plain = items.iter().find(|i| i.item_id == "a").unwrap();
    assert!(plain.contract_fingerprint.is_empty() && plain.tool_name.is_empty());

    authorizer
        .decide("s1", &decision(&id, batch, &[("cmd-0", true), ("a", true)]))
        .unwrap();
    let resolution = handle.await.unwrap().unwrap();
    let grant = resolution.grant("cmd-0").expect("permitido").clone();
    let same = grant.contract().cloned().unwrap();
    assert_eq!(grant.verify_contract(Some(&same)), Ok(()));
    // Un contrato distinto (el nodo cambió de huella) o ninguno: rechazado.
    let mut changed = same.clone();
    changed.fingerprint = "ef".repeat(32);
    assert_eq!(
        grant.verify_contract(Some(&changed)),
        Err(GrantError::ContractMismatch)
    );
    let mut registry = same.clone();
    registry.registry_digest = "00".repeat(32);
    assert_eq!(
        grant.verify_contract(Some(&registry)),
        Err(GrantError::ContractMismatch)
    );
    assert_eq!(
        grant.verify_contract(None),
        Err(GrantError::ContractMismatch)
    );
    // Un permiso que no es de comando tampoco acepta un contrato.
    let other = resolution.grant("a").unwrap();
    assert_eq!(other.verify_contract(None), Ok(()));
    assert_eq!(
        other.verify_contract(Some(&same)),
        Err(GrantError::ContractMismatch)
    );
}
