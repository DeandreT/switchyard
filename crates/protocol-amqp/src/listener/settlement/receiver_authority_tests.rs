//! Real native receiving attachments and delayed dispositions over both stores.
//! Generation edits are owned fault injection, not a live deletion facility.

use super::super::test_support::{GatedStore, performative};
use super::*;
use crate::broker::ReceivingAuthority;
use crate::listener::settlement::{prepare_receiving_entry, serve_receiving_entry};
use amqp::{DeliveryState, Disposition, Fields, Flow, Modified, Outcome};
use domain::{EntityBinding, ReceiveMode, codec, keys};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use storage::WriteBatch;
use tokio::sync::Notify;

#[path = "session_authority_tests.rs"]
mod session_authority_tests;

struct StagedFrontier {
    snapshot: storage::StoreSnapshot,
    counts: (usize, usize, usize),
}

#[derive(Clone)]
struct Witness {
    actual: ActualBroker,
    captures: Arc<AtomicUsize>,
    operations: Arc<Mutex<Vec<EntityBinding>>>,
    commands: Arc<Mutex<Vec<CommandKind>>>,
    empty: Arc<Notify>,
    wake: Arc<Notify>,
    drift_after_receive: Arc<AtomicBool>,
    drift_after_bind: Arc<AtomicBool>,
    drift_after_grant: Arc<AtomicBool>,
    staged_frontier: Arc<Mutex<Option<StagedFrontier>>>,
    clock_calls: Arc<AtomicUsize>,
    store: GatedStore,
    namespace: NamespaceName,
    owner: EntityPath,
}

impl Witness {
    fn new(actor: &Actor) -> Self {
        Self {
            actual: actor.broker.as_ref().unwrap().clone(),
            captures: Arc::default(),
            operations: Arc::default(),
            commands: Arc::default(),
            empty: Arc::default(),
            wake: Arc::default(),
            drift_after_receive: Arc::default(),
            drift_after_bind: Arc::default(),
            drift_after_grant: Arc::default(),
            staged_frontier: Arc::default(),
            clock_calls: actor.clock_calls.clone(),
            store: actor.store().clone(),
            namespace: actor.namespace.clone(),
            owner: actor.entity.clone(),
        }
    }

    fn drift(&self) {
        self.store
            .apply(WriteBatch::default().put(
                keys::entity_metadata(&self.namespace, &self.owner),
                codec::encode(&(2_u64, 0_u32, false)).unwrap(),
            ))
            .unwrap();
    }

    fn assert_original(&self) {
        assert_eq!(self.captures.load(Ordering::SeqCst), 1);
        let operations = self.operations.lock().unwrap();
        assert!(!operations.is_empty());
        assert!(operations.iter().all(|binding| binding == &operations[0]));
    }
}

impl Broker for Witness {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        let binding = self.actual.bind_entity(namespace, entity).await?;
        if self.drift_after_bind.swap(false, Ordering::SeqCst) {
            self.drift();
            self.record_frontier();
        }
        Ok(binding)
    }
    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.operations.lock().unwrap().push(binding.clone());
        self.commands.lock().unwrap().push(kind.clone());
        let receiving = matches!(kind, CommandKind::Receive { .. });
        let granting = matches!(kind, CommandKind::AcceptSession { .. });
        let result = self.actual.submit_bound(binding, kind).await;
        if granting
            && matches!(result, Ok(CommandOutcome::SessionAccepted(Some(_))))
            && self.drift_after_grant.swap(false, Ordering::SeqCst)
        {
            self.drift();
            self.record_frontier();
        }
        if receiving && matches!(result, Ok(CommandOutcome::Received(None))) {
            self.empty.notify_one();
        }
        if receiving
            && matches!(result, Ok(CommandOutcome::Received(Some(_))))
            && self.drift_after_receive.swap(false, Ordering::SeqCst)
        {
            self.drift();
            *self.staged_frontier.lock().unwrap() = Some(StagedFrontier {
                snapshot: self.store.snapshot().unwrap(),
                counts: (
                    self.clock_calls.load(Ordering::SeqCst),
                    self.store.clock_reads.load(Ordering::SeqCst),
                    self.store.applies.load(Ordering::SeqCst),
                ),
            });
        }
        result
    }
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("receiving must never submit by name")
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        self.wake.notified().await;
    }
}

impl Witness {
    fn record_frontier(&self) {
        *self.staged_frontier.lock().unwrap() = Some(StagedFrontier {
            snapshot: self.store.snapshot().unwrap(),
            counts: (
                self.clock_calls.load(Ordering::SeqCst),
                self.store.clock_reads.load(Ordering::SeqCst),
                self.store.applies.load(Ordering::SeqCst),
            ),
        });
    }
}

async fn attached(
    actor: &Actor,
    broker: Witness,
    management: Arc<ConnectionManagement>,
    target: &EntityPath,
    mode: ReceiveMode,
    maximum: Option<u64>,
) -> (
    PendingWire,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) {
    attached_with_session(actor, broker, management, target, mode, maximum, None).await
}

async fn attached_with_session(
    actor: &Actor,
    broker: Witness,
    management: Arc<ConnectionManagement>,
    target: &EntityPath,
    mode: ReceiveMode,
    maximum: Option<u64>,
    request: Option<bool>,
) -> (
    PendingWire,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) {
    let (mut wire, mut session) = PendingWire::new().await;
    let mut source = Source::new(target.as_str());
    if let Some(next) = request {
        source
            .filter
            .get_or_insert_with(Default::default)
            .insert(Symbol::from(crate::SESSION_FILTER), super::filter(next));
    }
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Attach(Box::new(Attach {
                name: LINK.to_owned(),
                handle: HANDLE,
                role: Role::Receiver,
                snd_settle_mode: if mode == ReceiveMode::PeekLock {
                    SenderSettleMode::Unsettled
                } else {
                    SenderSettleMode::Settled
                },
                rcv_settle_mode: ReceiverSettleMode::Second,
                source: Some(source),
                target: None,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: None,
                max_message_size: maximum,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
        ),
    )
    .await
    .unwrap();
    let attach = timeout(WAIT, session.next_incoming_attach())
        .await
        .unwrap()
        .unwrap();
    let link = accept_entity_link(
        &session,
        &broker,
        &actor.namespace,
        target.as_str(),
        attach,
        None,
        &management,
    )
    .await
    .unwrap()
    .unwrap();
    let Performative::Attach(echo) = control(&mut wire.peer, CHANNEL).await else {
        panic!("native attach echo")
    };
    let authority = link.receiving_authority.unwrap();
    let binding = match (&authority, request) {
        (ReceivingAuthority::Ordinary(binding), None) => binding,
        (ReceivingAuthority::Session(binding), Some(_)) => {
            assert_eq!(
                crate::read_session_filter(echo.source.as_ref()).unwrap(),
                crate::SessionRequest::Named(link.accepted.as_ref().unwrap().session_id.clone())
            );
            binding
        }
        _ => panic!("receiving provenance"),
    };
    assert_eq!(binding.target(), target);
    assert_eq!(binding.owner(), &actor.entity);
    let LinkEndpoint::Sender(sender) = link.endpoint else {
        panic!("receiving endpoint")
    };
    let entry = prepare_receiving_entry(
        &sender,
        actor.namespace.clone(),
        target.clone(),
        broker,
        mode,
        link.accepted.as_ref().map(domain::AcceptedSession::hold),
        ReceivingLinkProtocol {
            authority,
            authorization: None,
            management,
            session_registration: link.registration,
        },
    );
    // Keep the exact native session alive until the original receiving future joins.
    let task = tokio::spawn(async move {
        let result = serve_receiving_entry(sender, entry).await;
        drop(session);
        result
    });
    (wire, task)
}

async fn credit(wire: &mut PendingWire, count: u32) {
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Flow(Flow {
                handle: Some(HANDLE),
                delivery_count: Some(0),
                link_credit: Some(count),
                incoming_window: 2048,
                outgoing_window: 2048,
                ..Flow::default()
            }),
        ),
    )
    .await
    .unwrap();
}

async fn stop(
    mut wire: PendingWire,
    task: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) {
    wire.detach().await;
    timeout(WAIT, task).await.unwrap().unwrap().unwrap();
    wire.connection.stop();
    timeout(WAIT, wire.connection.shutdown())
        .await
        .unwrap()
        .unwrap();
}

fn seed(actor: &Actor, dlq: bool) -> EntityPath {
    let sequence = actor.send("retained-receiver", None);
    if !dlq {
        return actor.entity.clone();
    }
    let delivery = actor.receive();
    assert_eq!(sequence, delivery.sequence);
    actor.intent(CommandKind::DeadLetter {
        sequence,
        lock_token: delivery.lock.unwrap().token,
        reason: "fixture".into(),
        description: "fixture".into(),
        replacement_envelope: None,
    });
    actor.entity.dead_letter_queue().unwrap()
}

fn observations(actor: &Actor) -> (usize, usize, usize) {
    (
        actor.clock_calls.load(Ordering::SeqCst),
        actor.store().clock_reads.load(Ordering::SeqCst),
        actor.store().applies.load(Ordering::SeqCst),
    )
}

async fn detached_not_found(wire: &mut PendingWire) {
    let Performative::Detach(detach) = control(&mut wire.peer, CHANNEL).await else {
        panic!("stale Receive detaches")
    };
    assert_eq!(
        detach.error.unwrap().condition.as_symbol().as_str(),
        crate::NOT_FOUND
    );
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Detach(Detach {
                handle: HANDLE,
                closed: true,
                error: None,
            }),
        ),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_attach_and_poll_retain_original_generation_in_both_modes_and_dlq() {
    for durable in [false, true] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            for dlq in [false, true] {
                let mut actor = Actor::new(durable, false);
                let target = if dlq {
                    actor.entity.dead_letter_queue().unwrap()
                } else {
                    actor.entity.clone()
                };
                let broker = Witness::new(&actor);
                let (mut wire, task) = attached(
                    &actor,
                    broker.clone(),
                    ConnectionManagement::new(),
                    &target,
                    mode,
                    None,
                )
                .await;
                credit(&mut wire, 1).await;
                timeout(WAIT, broker.empty.notified()).await.unwrap();
                let seeded = seed(&actor, dlq);
                assert_eq!(target, seeded);
                broker.drift();
                let before = actor.store().snapshot().unwrap();
                let counts = observations(&actor);
                let mut wake = Box::pin(
                    broker
                        .actual
                        .handle
                        .wait_until_deliverable(&actor.namespace, &target),
                );
                broker.wake.notify_one();
                detached_not_found(&mut wire).await;
                timeout(WAIT, task).await.unwrap().unwrap().unwrap();
                assert_eq!(observations(&actor), counts);
                assert_eq!(actor.store().snapshot().unwrap(), before);
                assert!(
                    timeout(std::time::Duration::from_millis(10), wake.as_mut())
                        .await
                        .is_err()
                );
                broker.assert_original();
                wire.connection.stop();
                timeout(WAIT, wire.connection.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                drop(wake);
                drop(broker);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
        }
    }
}

fn outcomes() -> Vec<Outcome> {
    let patch = Fields::from_iter([(Symbol::from("attempt"), Value::Int(2))]);
    vec![
        Outcome::Accepted(amqp::Accepted),
        Outcome::Released(amqp::Released),
        Outcome::Modified(Modified {
            undeliverable_here: Some(true),
            ..Modified::default()
        }),
        Outcome::Rejected(amqp::Rejected { error: None }),
        Outcome::Modified(Modified {
            message_annotations: Some(patch.clone()),
            ..Modified::default()
        }),
        Outcome::Modified(Modified {
            undeliverable_here: Some(true),
            message_annotations: Some(patch.clone()),
            ..Modified::default()
        }),
        Outcome::Rejected(amqp::Rejected {
            error: Some(amqp::Error::new(
                amqp::ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter")),
                "fixture",
                Some(patch),
            )),
        }),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn every_delayed_ordinary_disposition_and_patch_refuses_recreated_queue_and_dlq() {
    for durable in [false, true] {
        for dlq in [false, true] {
            for outcome in outcomes() {
                let mut actor = Actor::new(durable, false);
                let target = seed(&actor, dlq);
                let broker = Witness::new(&actor);
                let (mut wire, task) = attached(
                    &actor,
                    broker.clone(),
                    ConnectionManagement::new(),
                    &target,
                    ReceiveMode::PeekLock,
                    None,
                )
                .await;
                credit(&mut wire, 1).await;
                let Performative::Transfer(transfer) = performative(&mut wire.peer).await else {
                    panic!("real transfer")
                };
                broker.drift();
                // Complete normally bypasses queue configuration. Even corrupting
                // that configuration cannot turn stale authority into a valid lock.
                actor
                    .store()
                    .apply(WriteBatch::default().put(
                        keys::queue_config(&actor.namespace, &actor.entity),
                        vec![255],
                    ))
                    .unwrap();
                let before = actor.store().snapshot().unwrap();
                let counts = observations(&actor);
                let state = match outcome {
                    Outcome::Accepted(v) => DeliveryState::Accepted(v),
                    Outcome::Released(v) => DeliveryState::Released(v),
                    Outcome::Modified(v) => DeliveryState::Modified(v),
                    Outcome::Rejected(v) => DeliveryState::Rejected(v),
                };
                write_frame(
                    &mut wire.peer,
                    &frame(
                        CHANNEL,
                        Performative::Disposition(Disposition {
                            role: Role::Receiver,
                            first: transfer.delivery_id.unwrap(),
                            last: None,
                            settled: false,
                            state: Some(state),
                            batchable: false,
                        }),
                    ),
                )
                .await
                .unwrap();
                let Performative::Disposition(confirmation) =
                    control(&mut wire.peer, CHANNEL).await
                else {
                    panic!("durable refusal")
                };
                let Some(DeliveryState::Rejected(rejected)) = confirmation.state else {
                    panic!("not Accepted")
                };
                assert_eq!(
                    rejected.error.unwrap().condition.as_symbol().as_str(),
                    crate::NOT_FOUND
                );
                assert!(confirmation.settled);
                assert_eq!(observations(&actor), counts);
                assert_eq!(actor.store().snapshot().unwrap(), before);
                broker.assert_original();
                stop(wire, task).await;
                drop(broker);
                actor.reopen();
                assert_eq!(actor.store().snapshot().unwrap(), before);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn staged_receive_delete_complete_uses_the_original_binding() {
    for durable in [false, true] {
        for dlq in [false, true] {
            let mut actor = Actor::new(durable, false);
            let target = seed(&actor, dlq);
            let broker = Witness::new(&actor);
            broker.drift_after_receive.store(true, Ordering::SeqCst);
            let (mut wire, task) = attached(
                &actor,
                broker.clone(),
                ConnectionManagement::new(),
                &target,
                ReceiveMode::ReceiveAndDelete,
                Some(1024 * 1024),
            )
            .await;
            let before_receive = observations(&actor);
            credit(&mut wire, 1).await;
            detached_not_found(&mut wire).await;
            timeout(WAIT, task).await.unwrap().unwrap().unwrap();
            broker.assert_original();
            assert_eq!(
                broker.operations.lock().unwrap().len(),
                2,
                "staged Receive and Complete"
            );
            assert!(matches!(
                actor.log.lock().unwrap().last().unwrap().kind,
                CommandKind::Complete { .. }
            ));
            let before = actor.store().snapshot().unwrap();
            let counts = observations(&actor);
            let frontier = broker.staged_frontier.lock().unwrap().take().unwrap();
            assert_eq!(
                counts, frontier.counts,
                "stale staged Complete never touches clocks or apply"
            );
            assert_eq!(
                before, frontier.snapshot,
                "stale staged Complete leaves the measured copy locked"
            );
            assert_eq!(
                counts.0,
                before_receive.0 + 1,
                "only staged Receive reads the host clock, not stale Complete"
            );
            wire.connection.stop();
            timeout(WAIT, wire.connection.shutdown())
                .await
                .unwrap()
                .unwrap();
            drop(broker);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

async fn healthy(actor: &Actor, marker: &str) {
    actor.send(marker, None);
    let broker = Witness::new(actor);
    let (mut wire, task) = attached(
        actor,
        broker.clone(),
        ConnectionManagement::new(),
        &actor.entity,
        ReceiveMode::PeekLock,
        None,
    )
    .await;
    credit(&mut wire, 1).await;
    let Performative::Transfer(transfer) = performative(&mut wire.peer).await else {
        panic!("healthy transfer")
    };
    write_frame(
        &mut wire.peer,
        &frame(
            CHANNEL,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: transfer.delivery_id.unwrap(),
                last: None,
                settled: false,
                state: Some(DeliveryState::Accepted(amqp::Accepted)),
                batchable: false,
            }),
        ),
    )
    .await
    .unwrap();
    let Performative::Disposition(confirmed) = control(&mut wire.peer, CHANNEL).await else {
        panic!("healthy durable confirmation")
    };
    assert!(matches!(confirmed.state, Some(DeliveryState::Accepted(_))));
    broker.assert_original();
    stop(wire, task).await;
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_healthy_deliveries_settle_before_and_after_a_stale_delivery() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, false);
        actor.send("held-original", None);
        let broker = Witness::new(&actor);
        let (mut wire, task) = attached(
            &actor,
            broker.clone(),
            ConnectionManagement::new(),
            &actor.entity,
            ReceiveMode::PeekLock,
            None,
        )
        .await;
        credit(&mut wire, 1).await;
        let Performative::Transfer(transfer) = performative(&mut wire.peer).await else {
            panic!("original transfer")
        };
        broker.drift();
        healthy(&actor, "healthy-before").await;
        let before = actor.store().snapshot().unwrap();
        let counts = observations(&actor);
        write_frame(
            &mut wire.peer,
            &frame(
                CHANNEL,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: transfer.delivery_id.unwrap(),
                    last: None,
                    settled: false,
                    state: Some(DeliveryState::Accepted(amqp::Accepted)),
                    batchable: false,
                }),
            ),
        )
        .await
        .unwrap();
        let Performative::Disposition(confirmed) = control(&mut wire.peer, CHANNEL).await else {
            panic!("stale durable refusal")
        };
        let Some(DeliveryState::Rejected(rejected)) = confirmed.state else {
            panic!("stale must refuse")
        };
        assert_eq!(
            rejected.error.unwrap().condition.as_symbol().as_str(),
            crate::NOT_FOUND
        );
        assert_eq!(observations(&actor), counts);
        assert_eq!(actor.store().snapshot().unwrap(), before);
        healthy(&actor, "healthy-after").await;
        stop(wire, task).await;
        drop(broker);
        let committed = actor.store().snapshot().unwrap();
        actor.reopen();
        assert_eq!(actor.store().snapshot().unwrap(), committed);
    }
}
