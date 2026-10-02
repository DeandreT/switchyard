use super::*;

use std::time::{SystemTime, UNIX_EPOCH};

fn named_coordinator(name: &str) -> Attach {
    let mut request = Peer::attach_request(CONTROL, CONTROL_HANDLE, "", Role::Sender);
    request.name = name.into();
    request.initial_delivery_count = Some(0);
    request.target = Some(Coordinator::default().into());
    request.source = Some(Source {
        outcomes: Some(
            vec![
                Symbol::from("amqp:declared:list"),
                Symbol::from("amqp:accepted:list"),
                Symbol::from("amqp:rejected:list"),
            ]
            .into(),
        ),
        ..Source::default()
    });
    request
}

pub(in super::super) async fn expired_authorization_then_same_connection_regrant_commits<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_secs(2))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    let mut peer = Peer::connect_cbs(node.address, security.certificate.clone(), "MSSBCBS").await?;
    peer.coordinator().await?;
    peer.open_cbs().await?;
    let before = node.snapshot()?;
    node.controls.reset_io();

    // This grant outlasts the fixed initial grace; expiry is observed through the wire Detach.
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(4)
        .ok_or("finite SAS expiry overflow")?;
    assert_eq!(
        peer.put_token(
            AUDIENCE,
            security::sas_token_with_expiry(AUDIENCE, security::SEND, expiry)?,
        )
        .await?,
        202
    );
    let old = peer.declare().await?;
    peer.unauthorized_controller().await?;
    inert(&node, &before)?;

    peer.attach(
        CONTROL,
        named_coordinator("cold-reauth-denied-without-grant"),
    )
    .await?;
    peer.detached(
        CONTROL,
        CONTROL_HANDLE,
        Some("amqp:unauthorized-access"),
        true,
    )
    .await?;
    peer.barrier(CONTROL).await?;
    inert(&node, &before)?;

    assert_eq!(
        peer.put_token(AUDIENCE, sas_token(AUDIENCE, security::SEND)?)
            .await?,
        202
    );
    peer.attach(CONTROL, named_coordinator("cold-reauth-current-grant"))
        .await?;
    peer.admitted(CONTROL, Role::Sender).await?;

    let unknown = peer.discharge(&old, false).await?;
    let refusal = peer.disposition(CONTROL, Role::Receiver, unknown).await?;
    assert!(refusal.settled);
    let Some(DeliveryState::Rejected(rejected)) = refusal.state else {
        return Err("old controller transaction must be rejected".into());
    };
    let error = rejected
        .error
        .ok_or("old transaction refusal condition missing")?;
    assert_eq!(
        error.condition.as_symbol().as_str(),
        "amqp:transaction:unknown-id"
    );
    assert_eq!(
        error.description.as_deref(),
        Some("unknown native transaction identity")
    );
    peer.barrier(CONTROL).await?;
    inert(&node, &before)?;

    let fresh = peer.declare().await?;
    assert_ne!(old, fresh);
    peer.producer(POST, "orders").await?;
    let id = "cold-reauth-commit";
    commit_post(&mut peer, &fresh, id).await?;
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.states(), [AtomicCommitState::Committed]);
    let records = node.messages()?;
    assert_eq!(records.len(), 2);
    let committed = records
        .iter()
        .find(|record| record.message_id == id)
        .ok_or("reauthorized commit missing")?;
    assert_eq!(committed.sequence.as_u64(), 3);
    assert_eq!(committed.body, id.as_bytes());
    peer.close().await?;
    let committed = node.snapshot()?;
    let (_provider, reopened, namespace) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, committed);
    assert_eq!(peek(&reopened, &namespace)?.len(), 2);
    Ok(())
}
