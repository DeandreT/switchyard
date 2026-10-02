use super::*;

#[path = "cold_first/security.rs"]
mod security;
use security::{Security, sas_token};
#[path = "cold_first/reauthorization.rs"]
mod reauthorization;
pub(super) use reauthorization::expired_authorization_then_same_connection_regrant_commits;

const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";

fn inert<P: StoreProvider>(node: &Node<P>, before: &StoreSnapshot) -> TestResult {
    node.unchanged(before)?;
    assert_eq!(node.controls.reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.receive_starts.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
    assert_eq!(node.controls.completed.load(Ordering::SeqCst), 0);
    Ok(())
}

fn accepted(disposition: Disposition) {
    assert!(disposition.settled);
    assert!(matches!(
        disposition.state,
        Some(DeliveryState::Accepted(_))
    ));
}

async fn commit_post(peer: &mut Peer, transaction: &TransactionId, id: &str) -> TestResult {
    let post = peer
        .transfer(
            POST,
            POST_HANDLE,
            Some(transaction),
            0,
            &message(id, id.as_bytes()),
        )
        .await?;
    peer.provisional(POST, Role::Receiver, post, transaction)
        .await?;
    let control = peer.discharge(transaction, false).await?;
    accepted(peer.disposition(POST, Role::Receiver, post).await?);
    accepted(peer.disposition(CONTROL, Role::Receiver, control).await?);
    Ok(())
}

pub(super) async fn initial_declare_then_cbs_commits_the_same_group<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_secs(20))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    for (mechanism, id, sequence) in [
        ("MSSBCBS", "cold-first-mssbcbs", 3),
        ("ANONYMOUS", "cold-first-anonymous", 4),
    ] {
        let mut peer =
            Peer::connect_cbs(node.address, security.certificate.clone(), mechanism).await?;
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.coordinator_with_sdk_defaults().await?;
        let transaction = peer.declare().await?;
        peer.barrier(CONTROL).await?;
        inert(&node, &before)?;
        peer.open_cbs().await?;
        assert_eq!(
            peer.put_token(AUDIENCE, sas_token(AUDIENCE, security::SEND)?)
                .await?,
            202
        );
        inert(&node, &before)?;
        peer.producer(POST, "orders").await?;
        assert_eq!(node.controls.binds.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 0);
        commit_post(&mut peer, &transaction, id).await?;
        assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.completed.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
        assert_eq!(node.controls.states(), [AtomicCommitState::Committed]);
        let retained = node.messages()?;
        let committed = retained
            .iter()
            .find(|message| message.message_id == id)
            .ok_or("cold-first committed message missing")?;
        assert_eq!(committed.sequence.as_u64(), sequence);
        assert_eq!(committed.body, id.as_bytes());
        peer.close().await?;
    }
    assert_eq!(node.messages()?.len(), 3);
    let committed = node.snapshot()?;
    let (_provider, reopened, namespace) = node.reopen().await?;
    assert_eq!(reopened.snapshot()?, committed);
    assert_eq!(peek(&reopened, &namespace)?.len(), 3);
    Ok(())
}

pub(super) async fn initial_rollback_is_inert_but_empty_commit_detaches<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_secs(20))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    for fail in [true, false] {
        let mut peer =
            Peer::connect_cbs(node.address, security.certificate.clone(), "MSSBCBS").await?;
        let before = node.snapshot()?;
        node.controls.reset_io();
        peer.coordinator().await?;
        let transaction = peer.declare().await?;
        let control = peer.discharge(&transaction, fail).await?;
        if fail {
            accepted(peer.disposition(CONTROL, Role::Receiver, control).await?);
            let next = peer.declare().await?;
            assert_ne!(transaction, next);
            let control = peer.discharge(&next, true).await?;
            accepted(peer.disposition(CONTROL, Role::Receiver, control).await?);
            peer.barrier(CONTROL).await?;
        } else {
            peer.unauthorized_controller().await?;
        }
        inert(&node, &before)?;
        peer.close().await?;
    }
    node.stop().await;
    Ok(())
}

pub(super) async fn cbs_failures_and_unrelated_grants_cannot_bind_orders<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_secs(20))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    let mut peer =
        Peer::connect_cbs(node.address, security.certificate.clone(), "ANONYMOUS").await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.coordinator().await?;
    let transaction = peer.declare().await?;
    peer.open_cbs().await?;
    peer.begin(POST).await?;
    for (attempt, (audience, token, status)) in [
        (AUDIENCE, "not-a-sas-token".to_owned(), 401),
        (
            "amqps://tenant.servicebus.windows.net/other",
            sas_token(
                "amqps://tenant.servicebus.windows.net/other",
                security::LISTEN_OTHER,
            )?,
            202,
        ),
        (
            "amqps://tenant.servicebus.windows.net/Orders",
            sas_token(
                "amqps://tenant.servicebus.windows.net/Orders",
                security::WRONG_CASE_SEND,
            )?,
            202,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(peer.put_token(audience, token).await?, status);
        let mut request = Peer::attach_request(POST, POST_HANDLE, "orders", Role::Sender);
        request.name = format!("cold-first-denied-producer-{attempt}");
        peer.attach(POST, request).await?;
        peer.detached(POST, POST_HANDLE, Some("amqp:unauthorized-access"), true)
            .await?;
        peer.barrier(POST).await?;
        inert(&node, &before)?;
    }
    assert_eq!(
        peer.put_token(AUDIENCE, sas_token(AUDIENCE, security::SEND)?)
            .await?,
        202
    );
    let mut request = Peer::attach_request(POST, POST_HANDLE, "orders", Role::Sender);
    request.name = "cold-first-authorized-producer".into();
    peer.attach(POST, request).await?;
    peer.admitted(POST, Role::Sender).await?;
    assert_eq!(node.controls.binds.load(Ordering::SeqCst), 1);
    commit_post(&mut peer, &transaction, "cold-first-after-denial").await?;
    assert_eq!(node.controls.handoffs.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.writes.load(Ordering::SeqCst), 1);
    assert_eq!(node.controls.clocks.load(Ordering::SeqCst), 1);
    assert_eq!(node.messages()?.len(), 2);
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn initial_declarations_are_bounded_without_broker_work<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_secs(20))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    let mut peer = Peer::connect_cbs(node.address, security.certificate.clone(), "MSSBCBS").await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.coordinator().await?;
    let mut transactions = Vec::new();
    timeout(Duration::from_secs(10), async {
        for _ in 0..32 {
            let transaction = peer.declare().await?;
            assert!(!transactions.contains(&transaction));
            transactions.push(transaction);
        }
        let id = peer
            .transfer(
                CONTROL,
                CONTROL_HANDLE,
                None,
                0,
                &control(TransactionCommand::Declare(Declare::default())),
            )
            .await?;
        let response = peer.disposition(CONTROL, Role::Receiver, id).await?;
        assert!(response.settled);
        let Some(DeliveryState::Rejected(rejected)) = response.state else {
            return Err::<(), Box<dyn Error + Send + Sync>>(
                "33rd initial declaration must be refused".into(),
            );
        };
        let error = rejected.error.ok_or("declaration quota error missing")?;
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:transaction:rollback"
        );
        assert_eq!(
            error.description.as_deref(),
            Some("native transaction declaration resource limit reached")
        );
        let control = peer.discharge(&transactions[0], true).await?;
        accepted(peer.disposition(CONTROL, Role::Receiver, control).await?);
        peer.barrier(CONTROL).await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    inert(&node, &before)?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn initial_authorization_deadline_is_a_real_close<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let security = Security::new(Duration::from_millis(500))?;
    let node =
        Node::start_with_security(provider, ListenerMode::Messaging, Some(security.server()))
            .await?;
    let mut peer = Peer::connect_cbs(node.address, security.certificate.clone(), "MSSBCBS").await?;
    let before = node.snapshot()?;
    node.controls.reset_io();
    peer.initial_authorization_close().await?;
    inert(&node, &before)?;
    node.stop().await;
    Ok(())
}
