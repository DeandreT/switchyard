use super::*;

#[tokio::test(flavor = "current_thread")]
async fn actual_control_processed_end_and_reuse_keep_original_admission_identity() {
    for role in ROLES {
        for cached in [false, true] {
            let (mut wire, environment, attach, _baseline) = fixture(role).await;
            let mut custody =
                ControlAttachmentCustody::new(&environment.session, Some(&environment.notice));
            assert!(custody.begin_acceptance(attach));
            if cached {
                assert!(timeout(WAIT, custody.observe_acceptance()).await.unwrap());
                acceptance_frames(&mut wire, role).await;
                assert!(matches!(custody.acceptance_result(), Some(Ok(_))));
            }
            let mut original_end = Box::pin(environment.session.on_end_owned());
            wire.writes.arm(false);
            peer_end(&mut wire).await;
            timeout(WAIT, wire.writes.reached()).await.unwrap();
            timeout(WAIT, original_end.as_mut()).await.unwrap();
            assert!(wire.writes.state.lock().unwrap().held);
            assert!(!environment.notice.is_requested());
            assert!(environment.session.is_ended());
            assert!(!custody.observe_acceptance().await);
            custody.set_primary(Ok(Ok(())));
            timeout(WAIT, custody.finish()).await.unwrap();
            assert!(custody.acceptance_result().is_none());
            assert!(custody.resolve(None).unwrap().is_none());
            wire.writes.release();
            assert!(matches!(
                wire.control(ADMISSION_CHANNEL).await,
                Performative::End(_)
            ));
            wire.begin(ADMISSION_CHANNEL).await;
            let mut replacement_session = wire.sessions.pop().unwrap();
            let replacement_attach = offer(
                &mut wire,
                &mut replacement_session,
                role.attach(ReceiverSettleMode::First, false),
            )
            .await;
            assert!(!custody.begin_acceptance(replacement_attach.clone()));
            assert!(environment.session.on_end_owned().now_or_never().is_some());
            assert!(!replacement_session.is_ended());
            let replacement_context = context(
                &replacement_session,
                &environment.broker,
                &environment.namespace,
                &environment.authorization,
                &environment.management,
                Some(&environment.notice),
            );
            let admitted = timeout(
                WAIT,
                serve_control_attachment(replacement_context, replacement_attach),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            acceptance_frames(&mut wire, role).await;
            wire.barrier().await;
            assert!(!environment.notice.is_requested());
            drop(custody);
            wire.stop().await;
            timeout(WAIT, admitted.task).await.unwrap().unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_route_lock_keeps_one_original_across_cancelled_finish() {
    for role in [ControlRole::CbsReply, ControlRole::ManagementReply] {
        let (mut wire, environment, attach, _baseline) = fixture(role).await;
        let mut custody =
            ControlAttachmentCustody::new(&environment.session, Some(&environment.notice));
        assert!(custody.begin_acceptance(attach));
        assert!(timeout(WAIT, custody.observe_acceptance()).await.unwrap());
        acceptance_frames(&mut wire, role).await;
        let held = route_lock(role, &environment).await;
        assert!(begin_route(&mut custody, role, &environment));
        for _ in 0..2 {
            let mut observer = Box::pin(custody.observe_route());
            pending_once(observer.as_mut()).await;
        }
        custody.retire();
        custody.set_primary(Ok(Ok(())));
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
        }
        assert!(custody.route_result().is_none());
        assert!(!environment.notice.is_requested());
        drop(held);
        timeout(WAIT, custody.finish()).await.unwrap();
        let probe = RouteProbe::captured(custody.route_result().unwrap());
        probe.check(custody.route_result().unwrap(), true);
        timeout(WAIT, custody.finish()).await.unwrap();
        probe.check(custody.route_result().unwrap(), true);
        assert!(!begin_route(&mut custody, role, &environment));
        assert!(custody.resolve(None).unwrap().is_none());
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

async fn reached_fault<F: Future>(serving: Pin<&mut F>, fault: &AdmissionFault) {
    tokio::select! {
        biased;
        result = timeout(WAIT, fault.reached.notified()) => { result.unwrap(); },
        _ = serving => panic!("outer fault checkpoint precedes adoption"),
    }
}

struct CloneFault(Arc<str>);

impl Clone for CloneFault {
    fn clone(&self) -> Self {
        panic_any(Arc::clone(&self.0));
    }
}

impl Broker for CloneFault {
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("clone-failed preparation must never start a broker operation");
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_ready_fault_keeps_endpoint_and_route_before_adoption() {
    // Ten real wrapper frontiers, plus one actual fallible Broker::clone preparation.
    for role in ROLES {
        for point in [
            AdmissionPoint::AcceptancePacket,
            AdmissionPoint::RoutePacket,
            AdmissionPoint::Prepared,
        ] {
            if !role.reply() && point == AdmissionPoint::RoutePacket {
                continue;
            }
            let (mut wire, environment, attach, _baseline) = fixture(role).await;
            let fault = AdmissionFault::new(point);
            let mut serving = Box::pin(
                ADMISSION_FAULT.scope(
                    Arc::clone(&fault),
                    AssertUnwindSafe(serve_control_attachment(environment.context(), attach))
                        .catch_unwind(),
                ),
            );
            reached_fault(serving.as_mut(), &fault).await;
            acceptance_frames(&mut wire, role).await;
            let replacement = if role.reply() && point != AdmissionPoint::AcceptancePacket {
                Some(replacement(role, &environment).await)
            } else {
                None
            };
            let held = if replacement.is_some() {
                Some(route_lock(role, &environment).await)
            } else {
                None
            };
            fault.trigger.notify_one();
            if held.is_some() {
                for _ in 0..2 {
                    pending_once(serving.as_mut()).await;
                }
                assert!(environment.notice.is_requested());
            }
            drop(held);
            exact_panic(
                timeout(WAIT, serving.as_mut()).await.unwrap(),
                &fault.payload,
            );
            assert!(environment.notice.is_requested());
            if let Some(replacement) = replacement {
                replacement_usable(replacement).await;
            }
            wire.stop().await;
        }
    }
    let (mut wire, environment, attach, _baseline) = fixture(ControlRole::ManagementRequest).await;
    let payload: Arc<str> = Arc::from("actual broker clone failed before ownership take");
    let broker = CloneFault(Arc::clone(&payload));
    let result = timeout(
        WAIT,
        AssertUnwindSafe(serve_control_attachment(
            environment.with_broker(&broker),
            attach,
        ))
        .catch_unwind(),
    )
    .await
    .unwrap();
    exact_panic(result, &payload);
    assert!(environment.notice.is_requested());
    // Acceptance preceded the clone; captured Stop, not a new Close, retired its endpoint.
    acceptance_frames(&mut wire, ControlRole::ManagementRequest).await;
    wire.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_conditional_cleanup_preserves_same_address_replacement() {
    for role in [ControlRole::CbsReply, ControlRole::ManagementReply] {
        let (mut wire, environment, attach, _baseline) = fixture(role).await;
        let mut custody = ControlAttachmentCustody::new(&environment.session, None);
        assert!(custody.begin_acceptance(attach));
        assert!(timeout(WAIT, custody.observe_acceptance()).await.unwrap());
        acceptance_frames(&mut wire, role).await;
        assert!(begin_route(&mut custody, role, &environment));
        assert!(timeout(WAIT, custody.observe_route()).await.unwrap());
        let probe = RouteProbe::captured(custody.route_result().unwrap());
        probe.check(custody.route_result().unwrap(), false);
        let replacement = replacement(role, &environment).await;
        let held = route_lock(role, &environment).await;
        let primary: Arc<str> = Arc::from("retained primary before conditional route cleanup");
        custody.set_primary(Err(Box::new(Arc::clone(&primary))));
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
        }
        probe.check(custody.route_result().unwrap(), true);
        assert!(matches!(
            custody.acceptance_result(),
            Some(Ok(LinkEndpoint::Sender(_)))
        ));
        drop(held);
        timeout(WAIT, custody.finish()).await.unwrap();
        probe.check(custody.route_result().unwrap(), true);
        assert!(custody.acceptance_result().is_none());
        exact_panic(
            catch_unwind(AssertUnwindSafe(|| custody.resolve(None))),
            &primary,
        );
        replacement_usable(replacement).await;
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;
    }
}

#[derive(Clone, Copy)]
enum PoisonPhase {
    Acceptance,
    Route,
    Refusal,
    Unregister,
}

async fn start_poison(custody: &mut ControlAttachmentCustody<'_>, phase: PoisonPhase) {
    match phase {
        PoisonPhase::Acceptance => {
            let mut observer = Box::pin(custody.observe_acceptance());
            pending_once(observer.as_mut()).await;
        }
        PoisonPhase::Route => {
            let mut observer = Box::pin(custody.observe_route());
            pending_once(observer.as_mut()).await;
        }
        PoisonPhase::Refusal => {
            let mut observer = Box::pin(custody.observe_refusal());
            pending_once(observer.as_mut()).await;
        }
        PoisonPhase::Unregister => {
            let mut observer = Box::pin(custody.observe_unregister());
            pending_once(observer.as_mut()).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_control_cached_or_poisoned_phases_survive_cancelled_finish() {
    // Eight synthetic owner compositions, not ordinary admission or a native future panic.
    for phase in [
        PoisonPhase::Acceptance,
        PoisonPhase::Route,
        PoisonPhase::Refusal,
        PoisonPhase::Unregister,
    ] {
        for primary in [false, true] {
            let (mut wire, environment, _attach, _baseline) = fixture(ControlRole::CbsReply).await;
            let mut custody = ControlAttachmentCustody::new(&environment.session, None);
            let payload: Arc<str> = Arc::from("injected original poison is terminal");
            let polls = Arc::new(AtomicUsize::new(0));
            let (poison_tx, poison_rx) = tokio::sync::oneshot::channel::<()>();
            let raw = Arc::clone(&payload);
            match phase {
                PoisonPhase::Acceptance => custody.seed_acceptance(witness(
                    async move {
                        poison_rx.await.unwrap();
                        panic_any(raw)
                    },
                    &polls,
                )),
                PoisonPhase::Route => custody.seed_route(witness(
                    async move {
                        poison_rx.await.unwrap();
                        panic_any(raw)
                    },
                    &polls,
                )),
                PoisonPhase::Refusal => custody.seed_refusal(witness(
                    async move {
                        poison_rx.await.unwrap();
                        panic_any(raw)
                    },
                    &polls,
                )),
                PoisonPhase::Unregister => custody.seed_unregister(witness(
                    async move {
                        poison_rx.await.unwrap();
                        panic_any(raw)
                    },
                    &polls,
                )),
            }
            start_poison(&mut custody, phase).await;
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            let pending_polls = Arc::new(AtomicUsize::new(0));
            let (pending_tx, pending_rx) = tokio::sync::oneshot::channel::<()>();
            if matches!(phase, PoisonPhase::Acceptance) {
                let packet = replacement(ControlRole::CbsReply, &environment).await;
                custody.seed_route(witness(
                    async move {
                        pending_rx.await.unwrap();
                        packet
                    },
                    &pending_polls,
                ));
                let mut observer = Box::pin(custody.observe_route());
                pending_once(observer.as_mut()).await;
            } else if matches!(phase, PoisonPhase::Route) {
                custody.seed_refusal(witness(
                    async move {
                        pending_rx.await.unwrap();
                        Err(EngineError::Stopped)
                    },
                    &pending_polls,
                ));
                let mut observer = Box::pin(custody.observe_refusal());
                pending_once(observer.as_mut()).await;
            } else if matches!(phase, PoisonPhase::Refusal) {
                custody.seed_unregister(witness(
                    async move {
                        pending_rx.await.unwrap();
                    },
                    &pending_polls,
                ));
                let mut observer = Box::pin(custody.observe_unregister());
                pending_once(observer.as_mut()).await;
            } else {
                // Last-phase poison has no later production phase; cancellation precedes it.
                for _ in 0..2 {
                    let mut finish = Box::pin(custody.finish());
                    pending_once(finish.as_mut()).await;
                }
                drop(pending_rx);
            }
            let primary_payload: Arc<str> =
                Arc::from("same retained primary outranks original poison");
            custody.set_primary(if primary {
                Err(Box::new(Arc::clone(&primary_payload)))
            } else {
                Ok(Ok(()))
            });
            poison_tx.send(()).unwrap();
            if !matches!(phase, PoisonPhase::Unregister) {
                for _ in 0..2 {
                    let mut finish = Box::pin(custody.finish());
                    pending_once(finish.as_mut()).await;
                }
                assert!(Arc::ptr_eq(
                    custody
                        .cleanup_payload()
                        .unwrap()
                        .downcast_ref::<Arc<str>>()
                        .unwrap(),
                    &payload
                ));
                assert_eq!(polls.load(Ordering::SeqCst), 2);
                pending_tx.send(()).unwrap();
            }
            timeout(WAIT, custody.finish()).await.unwrap();
            let terminal_polls = polls.load(Ordering::SeqCst);
            let terminal_pending_polls = pending_polls.load(Ordering::SeqCst);
            timeout(WAIT, custody.finish()).await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), terminal_polls);
            assert_eq!(pending_polls.load(Ordering::SeqCst), terminal_pending_polls);
            assert!(Arc::ptr_eq(
                custody
                    .cleanup_payload()
                    .unwrap()
                    .downcast_ref::<Arc<str>>()
                    .unwrap(),
                &payload
            ));
            exact_panic(
                catch_unwind(AssertUnwindSafe(|| custody.resolve(None))),
                if primary { &primary_payload } else { &payload },
            );
            assert!(!environment.notice.is_requested());
            wire.stop().await;
        }
    }
    // Two completed-first/pending-second compositions retain allocation and poll identity.
    for role in [ControlRole::CbsReply, ControlRole::ManagementReply] {
        let (mut wire, environment, _attach, _baseline) = fixture(role).await;
        let mut custody = ControlAttachmentCustody::new(&environment.session, None);
        let polls = Arc::new(AtomicUsize::new(0));
        let message = "cached original error allocation".to_owned();
        let pointer = message.as_ptr() as usize;
        custody.seed_acceptance(witness(
            async move { Err(EngineError::InvalidState(message)) },
            &polls,
        ));
        assert!(custody.observe_acceptance().await);
        let held = route_lock(role, &environment).await;
        assert!(begin_route(&mut custody, role, &environment));
        {
            let mut observer = Box::pin(custody.observe_route());
            pending_once(observer.as_mut()).await;
        }
        custody.set_primary(Ok(Ok(())));
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
        }
        let Some(Err(EngineError::InvalidState(cached))) = custody.acceptance_result() else {
            panic!("cached raw error")
        };
        assert_eq!(cached.as_ptr() as usize, pointer);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(held);
        timeout(WAIT, custody.finish()).await.unwrap();
        let Err(EngineError::InvalidState(cached)) = custody.resolve(None) else {
            panic!("original cached error resumes")
        };
        assert_eq!(cached.as_ptr() as usize, pointer);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        wire.stop().await;
    }
}

fn ungranted_authorization() -> Arc<ConnectionAuthorization> {
    let rule = SharedAccessRule::new(
        "ungranted",
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::LISTEN,
    )
    .unwrap();
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(
            SharedAccessPolicy::new([rule]).unwrap(),
            "tenant.servicebus.windows.net",
        )
        .unwrap(),
        None,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn actual_control_refusal_and_retired_unstarted_work_preserve_existing_policy() {
    for role in ROLES {
        // Invalid service shapes are actually offered, then answered and refused without a leaf.
        let (mut wire, _baseline) = Wire::new(
            if role.reply() {
                Role::Sender
            } else {
                Role::Receiver
            },
            0,
            ReceiverSettleMode::First,
        )
        .await;
        wire.begin(ADMISSION_CHANNEL).await;
        let mut session = wire.sessions.pop().unwrap();
        let mut invalid = role.attach(ReceiverSettleMode::First, false);
        if role.cbs() {
            invalid.source = Some(Source::new(crate::CBS_NODE));
            invalid.target = Some(Target::new(""));
        } else if role.reply() {
            invalid.source = Some(Source::new("/$management"));
        } else {
            invalid.target = Some(Target::new("/$management"));
        }
        let invalid = offer(&mut wire, &mut session, invalid).await;
        let environment = Environment {
            session,
            broker: NoBroker,
            namespace: NamespaceName::new("tenant").unwrap(),
            authorization: Arc::clone(&authorization().connection),
            management: ConnectionManagement::new(),
            notice: ConnectionRetirementRequest::capture(&wire.connection),
        };
        assert!(
            timeout(
                WAIT,
                serve_control_attachment(environment.context(), invalid)
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        acceptance_frames(&mut wire, role).await;
        let Performative::Detach(detach) = wire.control(ADMISSION_CHANNEL).await else {
            panic!("actual invalid-field refusal")
        };
        assert_eq!(detach.handle, ADMISSION_HANDLE);
        assert_eq!(
            detach.error.unwrap().condition,
            amqp::ErrorCondition::Amqp(amqp::AmqpError::InvalidField)
        );
        assert!(!environment.notice.is_requested());
        wire.barrier().await;
        wire.stop().await;

        // A positively answered remote Detach removes the pending offer before acceptance starts.
        let (mut wire, environment, attach, _baseline) = fixture(role).await;
        peer_detach(&mut wire).await;
        assert!(matches!(
            wire.control(ADMISSION_CHANNEL).await,
            Performative::Detach(_)
        ));
        wire.barrier().await;
        assert!(
            timeout(
                WAIT,
                serve_control_attachment(environment.context(), attach)
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        assert!(!environment.notice.is_requested());
        wire.barrier().await;
        wire.no_frame_yet().await;
        wire.stop().await;

        // A begun native refusal remains owned; its returned Stopped is still nonfatal policy.
        let (mut wire, environment, attach, _baseline) = fixture(role).await;
        let mut custody =
            ControlAttachmentCustody::new(&environment.session, Some(&environment.notice));
        assert!(custody.begin_acceptance(attach));
        assert!(timeout(WAIT, custody.observe_acceptance()).await.unwrap());
        acceptance_frames(&mut wire, role).await;
        wire.writes.arm(false);
        assert!(custody.begin_refusal(amqp::Error::new(
            amqp::AmqpError::InvalidField,
            "qualified refusal",
            None
        )));
        {
            let mut observer = Box::pin(custody.observe_refusal());
            pending_once(observer.as_mut()).await;
        }
        timeout(WAIT, wire.writes.reached()).await.unwrap();
        wire.connection.stop();
        timeout(WAIT, environment.session.on_end_owned())
            .await
            .unwrap();
        custody.set_primary(Ok(Ok(())));
        timeout(WAIT, custody.finish()).await.unwrap();
        assert!(matches!(
            custody.refusal_result(),
            Some(Err(EngineError::Stopped))
        ));
        assert!(
            custody
                .resolve(Some(Box::new(Arc::<str>::from(
                    "suppressed live-refusal report"
                ))))
                .unwrap()
                .is_none()
        );
        assert!(!environment.notice.is_requested());
        assert!(wire.writes.state.lock().unwrap().held);
        wire.stop().await;

        // This seed only witnesses zero invocation after captured retirement, not native admission.
        let (mut wire, environment, _attach, _baseline) = fixture(role).await;
        let mut custody =
            ControlAttachmentCustody::new(&environment.session, Some(&environment.notice));
        let polls = Arc::new(AtomicUsize::new(0));
        custody.seed_acceptance(witness(std::future::pending(), &polls));
        environment.notice.request();
        assert!(!custody.observe_acceptance().await);
        assert!(!custody.begin_acceptance(role.attach(ReceiverSettleMode::First, false)));
        assert!(!begin_route(&mut custody, role, &environment));
        assert!(!custody.begin_refusal(amqp::Error::new(
            amqp::AmqpError::InvalidField,
            "never started",
            None
        )));
        custody.set_primary(Ok(Ok(())));
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(
            custody
                .resolve(Some(Box::new(Arc::<str>::from(
                    "suppressed retired report"
                ))))
                .unwrap()
                .is_none()
        );
        wire.stop().await;
    }
    for role in [ControlRole::ManagementRequest, ControlRole::ManagementReply] {
        let (mut wire, mut environment, attach, _baseline) = fixture(role).await;
        environment.authorization = ungranted_authorization();
        assert!(
            timeout(
                WAIT,
                serve_control_attachment(environment.context(), attach)
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        acceptance_frames(&mut wire, role).await;
        let Performative::Detach(detach) = wire.control(ADMISSION_CHANNEL).await else {
            panic!("actual management authorization refusal")
        };
        assert_eq!(
            detach.error.unwrap().condition,
            amqp::ErrorCondition::Amqp(amqp::AmqpError::UnauthorizedAccess)
        );
        assert!(!environment.notice.is_requested());
        wire.barrier().await;
        wire.stop().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn control_primary_error_and_report_only_results_keep_existing_priority() {
    // Six explicit resolver compositions, not actual native errors or reporting callbacks.
    for case in 0..6 {
        let (mut wire, environment, _attach, _baseline) = fixture(ControlRole::CbsRequest).await;
        let mut custody =
            ControlAttachmentCustody::new(&environment.session, Some(&environment.notice));
        let primary: Arc<str> = Arc::from("raw primary takes precedence");
        let diagnostic: Arc<str> = Arc::from("report-only raw payload");
        let cleanup: Arc<str> = Arc::from("injected cleanup original panic");
        if case < 5 {
            custody.seed_acceptance(async move {
                Err(if case == 4 {
                    EngineError::RemoteDetached
                } else {
                    EngineError::Stopped
                })
            });
            assert!(custody.observe_acceptance().await);
        }
        if case < 3 {
            let raw = Arc::clone(&cleanup);
            custody.seed_unregister(async move {
                panic_any(raw);
            });
            AssertUnwindSafe(custody.observe_unregister())
                .catch_unwind()
                .await
                .expect("observer catches and caches original poison");
        }
        // observe_unregister caches poison instead of unwinding: finish owns its raw payload.
        custody.set_primary(match case {
            0 => Err(Box::new(Arc::clone(&primary))),
            1 => Ok(Err(EngineError::InvalidState(
                "active-primary-error".to_owned(),
            ))),
            _ => Ok(Ok(())),
        });
        timeout(WAIT, custody.finish()).await.unwrap();
        if case < 3 {
            assert!(Arc::ptr_eq(
                custody
                    .cleanup_payload()
                    .unwrap()
                    .downcast_ref::<Arc<str>>()
                    .unwrap(),
                &cleanup
            ));
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            custody.resolve(Some(Box::new(Arc::clone(&diagnostic))))
        }));
        match case {
            0 => exact_panic(result, &primary),
            1 => assert!(
                matches!(result.unwrap(), Err(EngineError::InvalidState(message)) if message == "active-primary-error")
            ),
            2 | 3 => assert!(matches!(result.unwrap(), Err(EngineError::Stopped))),
            4 => assert!(result.unwrap().unwrap().is_none()),
            5 => exact_panic(result, &diagnostic),
            _ => unreachable!(),
        }
        assert_eq!(
            environment.notice.is_requested(),
            case < 2,
            "only the retained primary fault requests retirement"
        );
        wire.stop().await;
    }
}
