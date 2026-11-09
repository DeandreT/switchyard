//! Combined #60/#372 authority and expiry frontiers over actual broker owners.
//! TLS proofs come from real handshakes; these are handler-frontier controls,
//! not a claim that every case traverses a network management exchange.

use super::*;
use crate::{listener::service::workload_tests::management_peer, workload::LeaseBroker};

#[derive(Clone)]
struct DispatchGate<B> {
    inner: B,
    selected: Arc<StdMutex<Vec<(EntityBinding, CommandKind)>>>,
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl<B> DispatchGate<B> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            selected: Arc::default(),
            reached: Arc::default(),
            release: Arc::default(),
        }
    }
}

impl<B: Broker> Broker for DispatchGate<B> {
    async fn bind_entity(
        &self,
        _: NamespaceName,
        _: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        panic!("request captures and selected capabilities cannot be recaptured")
    }

    async fn submit_bound(&self, binding: EntityBinding, kind: CommandKind) -> RawResult {
        self.selected
            .lock()
            .unwrap()
            .push((binding.clone(), kind.clone()));
        self.reached.notify_one();
        self.release.notified().await;
        self.inner.submit_bound(binding, kind).await
    }

    async fn submit(&self, _: NamespaceName, _: EntityPath, _: CommandKind) -> RawResult {
        panic!("retained management cannot submit by name")
    }

    async fn deliverable(&self, namespace: &NamespaceName, entity: &EntityPath) {
        self.inner.deliverable(namespace, entity).await;
    }
}

fn expired(response: &ManagementResponse) {
    assert_eq!(response.status_code, 401, "{response:?}");
    assert_eq!(response.error_condition, Some("amqp:unauthorized-access"));
}

#[tokio::test(flavor = "current_thread")]
async fn workload_request_and_selected_delivery_keep_authority_at_the_final_expiry_frontier() {
    for durable in [false, true] {
        for operation in [
            PEEK_MESSAGE_OPERATION,
            RENEW_LOCK_OPERATION,
            UPDATE_DISPOSITION_OPERATION,
        ] {
            let mut actor = Actor::new(durable, false);
            let management = ConnectionManagement::new();
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let binding = actor.authority().binding().clone();
            let registration = management
                .register_request_response_delivery(actor.entity.clone(), delivery, binding.clone())
                .await;
            let witness = Witness::new(&actor);
            let (proof, server, client) = management_peer().await;
            let gate = DispatchGate::new(LeaseBroker::new(witness.clone(), Some(proof.clone())));
            let capture = if operation == PEEK_MESSAGE_OPERATION {
                Ok(binding.clone())
            } else {
                Err(BrokerRejection::Unavailable(
                    "unrelated request capture".into(),
                ))
            };
            let broker = RequestBroker::with_sender(gate.clone(), Arc::new(capture));
            let message = if operation == PEEK_MESSAGE_OPERATION {
                peek_request(false)
            } else {
                let mut entries = vec![(LOCK_TOKENS, token_value(token))];
                if operation == UPDATE_DISPOSITION_OPERATION {
                    entries.push(("disposition-status", Value::String("completed".into())));
                }
                request(operation, false, entries)
            };
            let mut original = Box::pin(process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &message,
                None,
            ));
            timeout(WAIT, async {
                tokio::select! {
                    _ = original.as_mut() => panic!("operation must reach the held dispatch"),
                    () = gate.reached.notified() => {},
                }
            })
            .await
            .unwrap();
            proof.expired().await;
            drift(&actor, &actor.entity);
            let before = actor.snapshot();
            let observed = counts(&actor);
            let retained = management
                .request_response_deliveries
                .read()
                .await
                .get(&registration.key)
                .unwrap()
                .clone();
            gate.release.notify_one();
            expired(&timeout(WAIT, original.as_mut()).await.unwrap());
            drop(original);
            no_effects(&actor, &before, observed);
            assert_eq!(gate.selected.lock().unwrap()[0].0, binding);
            assert!(witness.operations.lock().unwrap().is_empty());
            assert_eq!(witness.captures.load(Ordering::SeqCst), 0);
            assert_eq!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .get(&registration.key),
                Some(&retained)
            );
            drop((broker, gate, witness, proof, server, client));
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn workload_selected_session_operations_do_not_upgrade_authority_or_run_after_cutoff() {
    for durable in [false, true] {
        for operation in [
            GET_SESSION_STATE_OPERATION,
            SET_SESSION_STATE_OPERATION,
            RENEW_SESSION_LOCK_OPERATION,
            PEEK_MESSAGE_OPERATION,
            RECEIVE_BY_SEQUENCE_NUMBER_OPERATION,
        ] {
            let mut actor = Actor::new(durable, true);
            let (sequence, hold) = prepare_deferred(&actor, true);
            let hold = hold.unwrap();
            let management = ConnectionManagement::new();
            install_hold(&management, &actor, hold.clone()).await;
            let binding = actor.authority().binding().clone();
            let witness = Witness::new(&actor);
            let (proof, server, client) = management_peer().await;
            let gate = DispatchGate::new(LeaseBroker::new(witness.clone(), Some(proof.clone())));
            let broker = RequestBroker::with_sender(
                gate.clone(),
                Arc::new(Err(BrokerRejection::Unavailable(
                    "unrelated request capture".into(),
                ))),
            );
            let message = match operation {
                PEEK_MESSAGE_OPERATION => peek_request(true),
                RECEIVE_BY_SEQUENCE_NUMBER_OPERATION => deferred_request(sequence, false, true),
                SET_SESSION_STATE_OPERATION => state_request(),
                _ => request(
                    operation,
                    true,
                    [(SESSION_ID, Value::String(session_id().as_str().into()))],
                ),
            };
            let mut original = Box::pin(process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &message,
                None,
            ));
            timeout(WAIT, async {
                tokio::select! {
                    _ = original.as_mut() => panic!("selected session dispatch must be held"),
                    () = gate.reached.notified() => {},
                }
            })
            .await
            .unwrap();
            proof.expired().await;
            drift(&actor, &actor.entity);
            let fresh = capture(&actor, &actor.entity);
            let claim = management.claim_session(LINK, actor.entity.clone());
            let replacement = management
                .install_bound_session(&claim, hold, fresh, || true)
                .await
                .unwrap();
            let before = actor.snapshot();
            let observed = counts(&actor);
            gate.release.notify_one();
            expired(&timeout(WAIT, original.as_mut()).await.unwrap());
            drop(original);
            no_effects(&actor, &before, observed);
            assert_eq!(gate.selected.lock().unwrap()[0].0, binding);
            assert!(witness.operations.lock().unwrap().is_empty());
            assert_eq!(witness.captures.load(Ordering::SeqCst), 0);
            assert_eq!(management.session(LINK).await, Some(replacement));
            drop((broker, gate, witness, proof, server, client));
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn workload_staged_deferred_delete_keeps_its_lock_when_expiry_precedes_completion() {
    for durable in [false, true] {
        for session in [false, true] {
            let mut actor = Actor::new(durable, session);
            let (sequence, hold) = prepare_deferred(&actor, session);
            let management = ConnectionManagement::new();
            if let Some(hold) = hold {
                install_hold(&management, &actor, hold).await;
            }
            let binding = actor.authority().binding().clone();
            let witness = Witness::new(&actor);
            witness.after_deferred.store(true, Ordering::SeqCst);
            let (proof, server, client) = management_peer().await;
            let broker = RequestBroker::with_sender(
                LeaseBroker::new(witness.clone(), Some(proof.clone())),
                Arc::new(Ok(binding.clone())),
            );
            let message = deferred_request(sequence, true, session);
            let mut original = Box::pin(process(
                &actor,
                &actor.entity,
                &broker,
                &management,
                &message,
                Some(128 * 1024),
            ));
            timeout(WAIT, async {
                tokio::select! {
                    _ = original.as_mut() => panic!("original receive result must remain held"),
                    () = witness.reached.notified() => {},
                }
            })
            .await
            .unwrap();
            proof.expired().await;
            let before = actor.snapshot();
            let observed = counts(&actor);
            witness.release.notify_one();
            expired(&timeout(WAIT, original.as_mut()).await.unwrap());
            drop(original);
            no_effects(&actor, &before, observed);
            let operations = witness.operations.lock().unwrap().clone();
            assert!(matches!(
                &operations[..],
                [(_, CommandKind::ReceiveDeferred { .. })]
            ));
            assert_eq!(operations[0].0, binding);
            assert!(matches!(
                actor.message(sequence).unwrap().state,
                MessageState::Locked { .. }
            ));
            assert!(
                management
                    .request_response_deliveries
                    .read()
                    .await
                    .is_empty()
            );
            drop((broker, witness, proof, server, client));
            actor.reopen(&before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn workload_begun_session_original_survives_expiry_and_retired_borrower_once() {
    for durable in [false, true] {
        let mut actor = Actor::new(durable, true);
        let hold = actor.accept();
        let management = ConnectionManagement::new();
        install_hold(&management, &actor, hold).await;
        let witness = Witness::new(&actor);
        let binding = actor.authority().binding().clone();
        let (proof, server, client) = management_peer().await;
        let broker = RequestBroker::with_sender(
            LeaseBroker::new(witness.clone(), Some(proof.clone())),
            Arc::new(Err(BrokerRejection::Unavailable(
                "unrelated request capture".into(),
            ))),
        );
        actor.gate.arm(
            keys::session(&actor.namespace, &actor.entity, &session_id()),
            true,
        );
        let message = state_request();
        let mut original = PendingOperation::new(
            process(&actor, &actor.entity, &broker, &management, &message, None),
            broker.control(),
        );
        {
            let observing = original.observe();
            tokio::pin!(observing);
            drive_to_apply(observing.as_mut(), &actor.gate).await;
        }
        proof.expired().await;
        original.retire();
        actor.gate.release(false);
        actor.gate.reached(true).await;
        actor.gate.release(true);
        let response = take_response(&mut original).await;
        assert_eq!(
            response.status_code, 200,
            "retain actual admitted result, not a new lease check"
        );
        drop(original);
        assert_eq!(actor.session().state, STATE);
        assert_eq!(actor.gate.progress.lock().unwrap().commits, 1);
        assert_eq!(witness.operations.lock().unwrap()[0].0, binding);
        assert_eq!(witness.operations.lock().unwrap().len(), 1);
        assert_eq!(witness.captures.load(Ordering::SeqCst), 0);
        let committed = actor.snapshot();
        drop((broker, witness, proof, server, client));
        actor.reopen(&committed);
        assert_eq!(actor.session().state, STATE);
    }
}
