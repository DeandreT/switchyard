//! Raw near-MAX counter seeds followed by actual ownership operations.
//! These are not natural exhaustion histories or snapshot-health certificates.
//! Unexpected-success diagnostics run only in RED; conservation after the
//! intended refusal assertion is unreached there. GREEN never runs diagnostics.

use std::error::Error;

use domain::{
    AcceptedSession, BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, Delivery,
    EntityPath, LockToken, MAX_DEFERRED_RECEIVE_BATCH, MessageInput, MessageRecord, QueueConfig,
    QueueConfigUpdate, QueueCounters, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    StateMachine, Timestamp, TopicConfig, codec, keys,
};
use storage::{Key, StateStore, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;
const LOCK: u64 = 20;

#[path = "lock_token_exhaustion/holds.rs"]
mod holds;

#[path = "lock_token_exhaustion/indexed.rs"]
mod indexed;

fn config(sessions: bool) -> QueueConfig {
    QueueConfig {
        requires_session: sessions,
        lock_duration_millis: LOCK,
        max_delivery_count: 100,
        ..QueueConfig::default()
    }
}

fn fixture<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(provider, "t", "initial", config(false))?)
}

fn case<P: StoreProvider>(
    f: &mut QueueFixture<P>,
    name: &str,
    at: u64,
    sessions: bool,
) -> TestResult {
    f.entity = EntityPath::new(name)?;
    assert_eq!(
        f.at(
            at,
            CommandKind::CreateQueue {
                config: config(sessions)
            }
        )?,
        CommandOutcome::QueueCreated
    );
    Ok(())
}

fn id() -> SessionId {
    SessionId::new("S").unwrap()
}

fn send<P: StoreProvider>(
    f: &QueueFixture<P>,
    at: u64,
    name: &str,
    ttl: Option<u64>,
    session: Option<SessionId>,
) -> Result<SequenceNumber, Box<dyn Error>> {
    let CommandOutcome::Sent { sequence } = f.at(at, send_kind(name, ttl, session))? else {
        panic!("expected the ordinary setup send");
    };
    Ok(sequence)
}

fn send_kind(name: &str, ttl: Option<u64>, session: Option<SessionId>) -> CommandKind {
    CommandKind::Send {
        message_id: name.to_owned(),
        body: name.as_bytes().to_vec(),
        time_to_live_millis: ttl,
        session_id: session,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

fn take(mode: ReceiveMode, session: Option<SessionHold>) -> CommandKind {
    CommandKind::Receive {
        mode,
        lock_duration_millis: Some(LOCK),
        session,
    }
}

fn retrieve(
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    session: Option<SessionHold>,
) -> CommandKind {
    CommandKind::ReceiveDeferred {
        sequences,
        mode,
        lock_duration_millis: Some(LOCK),
        session,
    }
}

fn accept(session_id: Option<SessionId>) -> CommandKind {
    CommandKind::AcceptSession {
        session_id,
        lock_duration_millis: Some(LOCK),
    }
}

fn delivery(outcome: CommandOutcome) -> Delivery {
    let CommandOutcome::Received(Some(value)) = outcome else {
        panic!("expected delivery: {outcome:?}")
    };
    value
}

fn accepted(outcome: CommandOutcome) -> AcceptedSession {
    let CommandOutcome::SessionAccepted(Some(value)) = outcome else {
        panic!("expected session: {outcome:?}")
    };
    value
}

fn counters<P: StoreProvider>(f: &QueueFixture<P>) -> Result<QueueCounters, Box<dyn Error>> {
    match f
        .machine
        .store()
        .get(&keys::queue_counters(&f.namespace, &f.entity))?
    {
        Some(raw) => Ok(codec::decode(&raw)?),
        None => Ok(QueueCounters::default()),
    }
}

fn raw<P: StoreProvider>(f: &QueueFixture<P>, key: Key, value: Option<Value>) -> TestResult {
    let mut batch = WriteBatch::default();
    match value {
        Some(value) => batch.push_put(key, value),
        None => batch.push_delete(key),
    }
    f.machine.store().apply(batch)?;
    Ok(())
}

fn seed<P: StoreProvider>(f: &QueueFixture<P>, next: u64) -> TestResult {
    let mut value = counters(f)?;
    value.next_lock_token = next;
    raw(
        f,
        keys::queue_counters(&f.namespace, &f.entity),
        Some(codec::encode(&value)?),
    )
}

fn exact_exhaustion(result: &Result<CommandOutcome, BrokerError>, diagnostic: &str) {
    // No reference to the not-yet-added unit variant: unchanged RED can compile.
    assert!(
        result.is_err(),
        "expected token exhaustion, got {result:?}; {diagnostic}"
    );
    assert_eq!(
        format!("{:?}", result.as_ref().unwrap_err()),
        "LockTokenExhausted"
    );
}

fn refuse<P: StoreProvider>(f: &QueueFixture<P>, at: u64, kind: CommandKind) -> TestResult {
    let before = f.machine.store().snapshot()?;
    let clock = f.machine.last_applied_time()?;
    let original = counters(f)?;
    let result = f.at(at, kind);
    let after = f.machine.store().snapshot()?;
    let after_clock = f.machine.last_applied_time()?;
    let after_counters = counters(f)?;
    exact_exhaustion(
        &result,
        &format!(
            "post-call counters {after_counters:?}, Clock {after_clock:?}, image changed {}; later conservation is unreached on RED success",
            after != before
        ),
    );
    assert_eq!(after, before);
    assert_eq!(after_clock, clock);
    assert_eq!(after_counters, original);
    Ok(())
}

fn unchanged_error<P: StoreProvider>(
    f: &QueueFixture<P>,
    command: &Command,
    expected: BrokerError,
) -> TestResult {
    let before = f.machine.store().snapshot()?;
    let clock = f.machine.last_applied_time()?;
    assert_eq!(f.machine.apply(command), Err(expected));
    assert_eq!(f.machine.store().snapshot()?, before);
    assert_eq!(f.machine.last_applied_time()?, clock);
    Ok(())
}

fn restart<P: StoreProvider>(f: QueueFixture<P>) -> Result<QueueFixture<P>, Box<dyn Error>> {
    let before = f.machine.store().snapshot()?;
    // This consumes the sole store handle. No clone survives real Fjall reopen.
    let f = f.restart()?;
    assert_eq!(f.machine.store().snapshot()?, before);
    Ok(f)
}

fn terminal_receive<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    for expire in [false, true] {
        let base = if expire { 100 } else { 10 };
        case(
            &mut f,
            if expire { "expire" } else { "abandon" },
            base,
            false,
        )?;
        let sequence = send(&f, base + 1, "same-message", None, None)?;
        seed(&f, u64::MAX)?;
        let before = f.machine.store().snapshot()?;
        let clock = f.machine.last_applied_time()?;
        let original = counters(&f)?;
        let result = f.at(base + 2, take(ReceiveMode::PeekLock, None));
        let mut diagnostic = String::new();
        if let Ok(CommandOutcome::Received(Some(first))) = &result {
            let first_record = f.machine.message(&f.namespace, &f.entity, sequence)?;
            let lock = first.lock.unwrap();
            let again_at = if expire {
                assert_eq!(
                    f.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
                    CommandOutcome::LocksExpired {
                        returned_to_ready: 1,
                        dead_lettered: 0
                    }
                );
                lock.locked_until.as_millis() + 1
            } else {
                assert_eq!(
                    f.at(
                        base + 3,
                        CommandKind::Abandon {
                            sequence,
                            lock_token: lock.token,
                            replacement_envelope: None
                        }
                    )?,
                    CommandOutcome::Abandoned {
                        dead_lettered: false
                    }
                );
                base + 4
            };
            let second = delivery(f.at(again_at, take(ReceiveMode::PeekLock, None))?);
            let second_record = f.machine.message(&f.namespace, &f.entity, sequence)?;
            assert_eq!(second.sequence, sequence);
            let stale_renew = f.at(
                again_at + 1,
                CommandKind::RenewLock {
                    sequence,
                    lock_token: lock.token,
                    lock_duration_millis: Some(LOCK),
                },
            );
            let stale_complete = f.at(
                again_at + 2,
                CommandKind::Complete {
                    sequence,
                    lock_token: lock.token,
                },
            );
            diagnostic = format!(
                "first token {:?}, second token {:?}; first {first_record:?}; second {second_record:?}; stale renew {stale_renew:?}; stale complete {stale_complete:?}",
                lock.token,
                second.lock.unwrap().token
            );
        }
        exact_exhaustion(&result, &diagnostic);
        assert_eq!(f.machine.store().snapshot()?, before);
        assert_eq!(f.machine.last_applied_time()?, clock);
        assert_eq!(counters(&f)?, original);
        assert_eq!(
            f.machine.ready_sequences(&f.namespace, &f.entity, 10)?,
            vec![sequence]
        );
        f = restart(f)?;
        refuse(&f, base + 2, take(ReceiveMode::PeekLock, None))?;
    }
    Ok(())
}

fn terminal_session<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    for (index, (named, cleanup)) in [
        (true, 0),
        (false, 0),
        (true, 1),
        (false, 1),
        (true, 2),
        (false, 2),
    ]
    .into_iter()
    .enumerate()
    {
        let base = 100 * (index as u64 + 1);
        case(&mut f, &format!("session-{index}"), base, true)?;
        if !named {
            send(&f, base + 1, "available", None, Some(id()))?;
        }
        seed(&f, u64::MAX)?;
        let before = f.machine.store().snapshot()?;
        let clock = f.machine.last_applied_time()?;
        let original = counters(&f)?;
        let kind = accept(named.then(id));
        let result = f.at(base + 2, kind.clone());
        let mut diagnostic = String::new();
        if let Ok(CommandOutcome::SessionAccepted(Some(first))) = &result {
            let first_record = f.machine.session(&f.namespace, &f.entity, &id())?;
            let again_at = match cleanup {
                0 => {
                    assert_eq!(
                        f.at(
                            base + 3,
                            CommandKind::ReleaseSession {
                                session: first.hold()
                            }
                        )?,
                        CommandOutcome::SessionReleased
                    );
                    base + 4
                }
                1 => {
                    assert_eq!(
                        f.at(
                            first.lock.locked_until.as_millis(),
                            CommandKind::ExpireSessionLocks
                        )?,
                        CommandOutcome::SessionLocksExpired { released: 1 }
                    );
                    first.lock.locked_until.as_millis() + 1
                }
                _ => first.lock.locked_until.as_millis(),
            };
            let second = accepted(f.at(again_at, kind.clone())?);
            let second_record = f.machine.session(&f.namespace, &f.entity, &id())?;
            assert_eq!(second.session_id, first.session_id);
            let stale_state = f.at(
                again_at + 1,
                CommandKind::GetSessionState {
                    session: first.hold(),
                },
            );
            let stale_release = f.at(
                again_at + 2,
                CommandKind::ReleaseSession {
                    session: first.hold(),
                },
            );
            diagnostic = format!(
                "first token {:?}, second token {:?}; first {first_record:?}; second {second_record:?}; stale state {stale_state:?}; stale release {stale_release:?}",
                first.lock.token, second.lock.token
            );
        }
        exact_exhaustion(&result, &diagnostic);
        assert_eq!(f.machine.store().snapshot()?, before);
        assert_eq!(f.machine.last_applied_time()?, clock);
        assert_eq!(counters(&f)?, original);
        assert_eq!(f.machine.session(&f.namespace, &f.entity, &id())?, None);
        f = restart(f)?;
        refuse(&f, base + 2, kind)?;
    }
    Ok(())
}

fn final_slot<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    case(&mut f, "final-message", 10, false)?;
    let sequence = send(&f, 11, "last", None, None)?;
    send(&f, 12, "next", None, None)?;
    seed(&f, u64::MAX - 1)?;
    let original_sequence = counters(&f)?.next_sequence;
    let last = delivery(f.at(13, take(ReceiveMode::PeekLock, None))?);
    assert_eq!(last.sequence, sequence);
    assert_eq!(last.lock.unwrap().token, LockToken::new(u64::MAX - 1));
    assert_eq!(
        counters(&f)?,
        QueueCounters {
            next_sequence: original_sequence,
            next_lock_token: u64::MAX
        }
    );
    refuse(&f, 14, take(ReceiveMode::PeekLock, None))?;
    assert_eq!(
        f.at(
            15,
            CommandKind::Defer {
                sequence,
                lock_token: last.lock.unwrap().token,
                replacement_envelope: None
            }
        )?,
        CommandOutcome::Deferred
    );
    unchanged_error(
        &f,
        &f.command(
            16,
            CommandKind::Complete {
                sequence,
                lock_token: last.lock.unwrap().token,
            },
        ),
        BrokerError::MessageNotLocked { sequence },
    )?;
    f = restart(f)?;
    refuse(
        &f,
        16,
        retrieve(vec![sequence], ReceiveMode::PeekLock, None),
    )?;
    refuse(&f, 16, take(ReceiveMode::PeekLock, None))?;
    case(&mut f, "final-session", 100, true)?;
    seed(&f, u64::MAX - 1)?;
    let last = accepted(f.at(101, accept(Some(id())))?);
    assert_eq!(last.lock.token, LockToken::new(u64::MAX - 1));
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    assert_eq!(
        f.at(102, take(ReceiveMode::PeekLock, Some(last.hold())))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(
        f.at(
            103,
            CommandKind::ReleaseSession {
                session: last.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    refuse(&f, 104, accept(Some(id())))?;
    unchanged_error(
        &f,
        &f.command(
            105,
            CommandKind::GetSessionState {
                session: last.hold(),
            },
        ),
        BrokerError::SessionLockNotHeld { session_id: id() },
    )?;
    case(&mut f, "shared-counter", 200, true)?;
    let sequence = send(&f, 201, "shared-first", None, Some(id()))?;
    send(&f, 202, "shared-next", None, Some(id()))?;
    seed(&f, u64::MAX - 2)?;
    let session = accepted(f.at(203, accept(Some(id())))?);
    let message = delivery(f.at(204, take(ReceiveMode::PeekLock, Some(session.hold())))?);
    assert_eq!(session.lock.token, LockToken::new(u64::MAX - 2));
    assert_eq!(message.lock.unwrap().token, LockToken::new(u64::MAX - 1));
    refuse(&f, 205, take(ReceiveMode::PeekLock, Some(session.hold())))?;
    refuse(&f, 205, accept(Some(SessionId::new("other")?)))?;
    assert_eq!(
        f.at(
            206,
            CommandKind::ReleaseSession {
                session: session.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    assert_eq!(
        f.at(
            207,
            CommandKind::Complete {
                sequence,
                lock_token: message.lock.unwrap().token
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    f = restart(f)?;
    refuse(&f, 208, accept(None))?;
    Ok(())
}

fn defer_setup<P: StoreProvider>(
    f: &QueueFixture<P>,
    at: u64,
    name: &str,
    ttl: Option<u64>,
) -> Result<SequenceNumber, Box<dyn Error>> {
    let sequence = send(f, at, name, ttl, None)?;
    let value = delivery(f.at(at + 1, take(ReceiveMode::PeekLock, None))?);
    assert_eq!(value.sequence, sequence);
    assert_eq!(
        f.at(
            at + 2,
            CommandKind::Defer {
                sequence,
                lock_token: value.lock.unwrap().token,
                replacement_envelope: None
            }
        )?,
        CommandOutcome::Deferred
    );
    Ok(sequence)
}

fn deferred_batches<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    for crossing in [false, true] {
        let base = if crossing { 1_000 } else { 100 };
        case(
            &mut f,
            if crossing { "crossing" } else { "fitting" },
            base,
            false,
        )?;
        let expired = defer_setup(&f, base + 1, "expired", Some(10))?;
        let first = defer_setup(&f, base + 4, "first", Some(1_000))?;
        let second = defer_setup(&f, base + 7, "second", Some(1_000))?;
        let request = vec![expired, second, first];
        seed(&f, if crossing { u64::MAX - 1 } else { u64::MAX - 2 })?;
        if crossing {
            refuse(
                &f,
                base + 30,
                retrieve(request.clone(), ReceiveMode::PeekLock, None),
            )?;
            assert_eq!(
                f.machine.deferred_sequences(&f.namespace, &f.entity, 10)?,
                vec![expired, first, second]
            );
            assert_eq!(
                f.machine
                    .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
                Vec::new()
            );
            f = restart(f)?;
            refuse(
                &f,
                base + 30,
                retrieve(request, ReceiveMode::PeekLock, None),
            )?;
        } else {
            let CommandOutcome::DeferredReceived(values) =
                f.at(base + 30, retrieve(request, ReceiveMode::PeekLock, None))?
            else {
                panic!("expected fitting deferred batch")
            };
            assert_eq!(
                values.iter().map(|v| v.sequence).collect::<Vec<_>>(),
                vec![second, first]
            );
            assert_eq!(
                values
                    .iter()
                    .map(|v| v.lock.unwrap().token)
                    .collect::<Vec<_>>(),
                vec![LockToken::new(u64::MAX - 2), LockToken::new(u64::MAX - 1)]
            );
            assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
            assert_eq!(
                f.machine
                    .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
                vec![expired]
            );
            for value in values {
                f.at(
                    base + 31,
                    CommandKind::Abandon {
                        sequence: value.sequence,
                        lock_token: value.lock.unwrap().token,
                        replacement_envelope: None,
                    },
                )?;
            }
        }
    }
    case(&mut f, "expired-prefix", 2_000, false)?;
    let expired = send(&f, 2_001, "expired", Some(10), None)?;
    let live = send(&f, 2_002, "live", None, None)?;
    seed(&f, u64::MAX)?;
    refuse(&f, 2_030, take(ReceiveMode::PeekLock, None))?;
    assert_eq!(
        f.machine.ready_sequences(&f.namespace, &f.entity, 10)?,
        vec![expired, live]
    );
    assert_eq!(
        f.machine
            .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
        Vec::new()
    );
    Ok(())
}

fn no_new_hold<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    case(&mut f, "lazy", 10, false)?;
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(11, take(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    assert_eq!(
        f.machine
            .store()
            .get(&keys::queue_counters(&f.namespace, &f.entity))?,
        None
    );
    seed(&f, u64::MAX)?;
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(12, take(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    let expired = send(&f, 13, "expires", Some(10), None)?;
    assert_eq!(
        f.at(23, take(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(
        f.machine
            .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
        vec![expired]
    );
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    let parent_counters = counters(&f)?;
    let mut shadow = f.command(24, take(ReceiveMode::PeekLock, None));
    shadow.entity = f.entity.dead_letter_queue()?;
    let shadow_delivery = delivery(f.machine.apply(&shadow)?);
    assert_eq!(shadow_delivery.sequence, expired);
    assert_eq!(shadow_delivery.lock.unwrap().token, LockToken::new(1));
    shadow.issued_at = Timestamp::from_millis(25);
    shadow.kind = CommandKind::Complete {
        sequence: expired,
        lock_token: shadow_delivery.lock.unwrap().token,
    };
    assert_eq!(f.machine.apply(&shadow)?, CommandOutcome::Completed);
    assert_eq!(counters(&f)?, parent_counters);
    let live = send(&f, 26, "delete", None, None)?;
    let deleted = delivery(f.at(27, take(ReceiveMode::ReceiveAndDelete, None))?);
    assert_eq!(deleted.sequence, live);
    assert_eq!(deleted.lock, None);
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    case(&mut f, "deferred-delete", 100, false)?;
    let expired = defer_setup(&f, 101, "deferred-expired", Some(10))?;
    let live = defer_setup(&f, 104, "deferred-live", None)?;
    seed(&f, u64::MAX)?;
    assert_eq!(
        f.at(130, retrieve(vec![expired], ReceiveMode::PeekLock, None))?,
        CommandOutcome::DeferredReceived(Vec::new())
    );
    let CommandOutcome::DeferredReceived(values) = f.at(
        131,
        retrieve(vec![live], ReceiveMode::ReceiveAndDelete, None),
    )?
    else {
        panic!("expected deferred delete")
    };
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].sequence, live);
    assert_eq!(values[0].lock, None);
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    assert_eq!(
        f.machine
            .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
        vec![expired]
    );
    case(&mut f, "empty-sessions", 200, true)?;
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(201, accept(None))?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    assert_eq!(
        f.machine
            .store()
            .get(&keys::queue_counters(&f.namespace, &f.entity))?,
        None
    );
    let session = accepted(f.at(202, accept(Some(id())))?);
    send(&f, 203, "session-expired", Some(5), Some(id()))?;
    seed(&f, u64::MAX)?;
    assert_eq!(
        f.at(208, take(ReceiveMode::PeekLock, Some(session.hold())))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(
        f.at(209, accept(None))?,
        CommandOutcome::SessionAccepted(None)
    );
    let live = send(&f, 210, "session-delete", None, Some(id()))?;
    let deleted = delivery(f.at(
        211,
        take(ReceiveMode::ReceiveAndDelete, Some(session.hold())),
    )?);
    assert_eq!(deleted.sequence, live);
    assert_eq!(deleted.lock, None);
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    case(&mut f, "all-held", 1_000, true)?;
    for index in 0..33 {
        let sid = SessionId::new(format!("S{index:02}"))?;
        send(
            &f,
            1_001 + index * 2,
            &format!("message-{index}"),
            None,
            Some(sid.clone()),
        )?;
        if index < 32 {
            accepted(f.at(
                1_002 + index * 2,
                CommandKind::AcceptSession {
                    session_id: Some(sid),
                    lock_duration_millis: Some(10_000),
                },
            )?);
        }
    }
    seed(&f, u64::MAX)?;
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(1_100, accept(None))?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    // The 33rd available session is beyond the unchanged 32-session scan bound.
    f = restart(f)?;
    assert_eq!(
        f.at(1_100, accept(None))?,
        CommandOutcome::SessionAccepted(None)
    );
    Ok(())
}

fn priorities<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    case(&mut f, "q", 10, false)?;
    let sequence = send(&f, 11, "ready", None, None)?;
    seed(&f, u64::MAX)?;
    unchanged_error(
        &f,
        &f.command(10, take(ReceiveMode::PeekLock, None)),
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(11),
            proposed: Timestamp::from_millis(10),
        },
    )?;
    unchanged_error(
        &f,
        &f.command(
            12,
            take(
                ReceiveMode::PeekLock,
                Some(SessionHold::new(id(), LockToken::new(1))),
            ),
        ),
        BrokerError::SessionNotSupported,
    )?;
    let mut missing = f.command(12, take(ReceiveMode::PeekLock, None));
    missing.entity = EntityPath::new("missing")?;
    unchanged_error(&f, &missing, BrokerError::QueueNotFound)?;
    let binding = f.machine.bind_entity(&f.namespace, &f.entity)?;
    let before = f.machine.store().snapshot()?;
    missing.issued_at = Timestamp::UNIX_EPOCH;
    assert_eq!(
        f.machine.apply_bound(&BoundCommand::new(binding, missing)),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    let counter_key = keys::queue_counters(&f.namespace, &f.entity);
    let original_counter = f.machine.store().get(&counter_key)?.unwrap();
    let bad = vec![0];
    let counter_error = BrokerError::Codec(codec::decode::<QueueCounters>(&bad).unwrap_err());
    raw(&f, counter_key.clone(), Some(bad.clone()))?;
    unchanged_error(
        &f,
        &f.command(12, take(ReceiveMode::PeekLock, None)),
        counter_error.clone(),
    )?;
    let message_key = keys::message(&f.namespace, &f.entity, sequence);
    let original_message = f.machine.store().get(&message_key)?.unwrap();
    raw(&f, message_key.clone(), None)?;
    unchanged_error(
        &f,
        &f.command(12, take(ReceiveMode::PeekLock, None)),
        BrokerError::DanglingIndexEntry { sequence },
    )?;
    raw(&f, message_key.clone(), Some(bad.clone()))?;
    let message_error = BrokerError::Codec(MessageRecord::decode(&bad).unwrap_err());
    unchanged_error(
        &f,
        &f.command(12, take(ReceiveMode::PeekLock, None)),
        message_error,
    )?;
    raw(&f, message_key, Some(original_message))?;
    let prefix = keys::ready_prefix(&f.namespace, &f.entity);
    assert!(
        prefix.len() < 8,
        "the injected short key must reach trailing_sequence's failure"
    );
    raw(&f, prefix.clone(), Some(Vec::new()))?;
    unchanged_error(
        &f,
        &f.command(12, take(ReceiveMode::PeekLock, None)),
        BrokerError::MalformedIndexKey,
    )?;
    raw(&f, prefix, None)?;
    raw(&f, counter_key, Some(original_counter))?;
    case(&mut f, "deferred-priority", 100, false)?;
    let first = defer_setup(&f, 101, "first", None)?;
    let second = defer_setup(&f, 104, "second", None)?;
    let expired = defer_setup(&f, 107, "expired-counter-read", Some(10))?;
    let ready = send(&f, 110, "not-deferred", None, None)?;
    seed(&f, u64::MAX)?;
    for (sequences, expected) in [
        (Vec::new(), BrokerError::EmptyDeferredReceive),
        (
            vec![first; MAX_DEFERRED_RECEIVE_BATCH + 1],
            BrokerError::DeferredReceiveBatchTooLarge {
                count: MAX_DEFERRED_RECEIVE_BATCH + 1,
                maximum: MAX_DEFERRED_RECEIVE_BATCH,
            },
        ),
        (
            vec![first, first],
            BrokerError::DuplicateDeferredSequence { sequence: first },
        ),
        (
            vec![first, SequenceNumber::new(999)],
            BrokerError::MessageNotFound {
                sequence: SequenceNumber::new(999),
            },
        ),
        (
            vec![first, ready],
            BrokerError::MessageNotDeferred { sequence: ready },
        ),
    ] {
        unchanged_error(
            &f,
            &f.command(120, retrieve(sequences, ReceiveMode::PeekLock, None)),
            expected,
        )?;
    }
    let key = keys::message(&f.namespace, &f.entity, second);
    let original = f.machine.store().get(&key)?.unwrap();
    let mut value = f.machine.message(&f.namespace, &f.entity, second)?.unwrap();
    value.session_id = Some(id());
    raw(&f, key.clone(), Some(codec::encode(&value)?))?;
    unchanged_error(
        &f,
        &f.command(
            120,
            retrieve(vec![first, second], ReceiveMode::PeekLock, None),
        ),
        BrokerError::DeferredMessageSessionMismatch { sequence: second },
    )?;
    raw(&f, key, Some(original))?;
    let counter_key = keys::queue_counters(&f.namespace, &f.entity);
    let original = f.machine.store().get(&counter_key)?.unwrap();
    raw(&f, counter_key.clone(), Some(bad.clone()))?;
    for sequences in [vec![expired], vec![first, second]] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            unchanged_error(
                &f,
                &f.command(120, retrieve(sequences.clone(), mode, None)),
                counter_error.clone(),
            )?;
        }
    }
    raw(&f, counter_key, Some(original))?;
    case(&mut f, "session-priority", 200, true)?;
    let held = accepted(f.at(201, accept(Some(id())))?);
    seed(&f, u64::MAX)?;
    unchanged_error(
        &f,
        &f.command(202, accept(Some(id()))),
        BrokerError::SessionAlreadyLocked { session_id: id() },
    )?;
    unchanged_error(
        &f,
        &f.command(202, take(ReceiveMode::PeekLock, None)),
        BrokerError::SessionRequired,
    )?;
    unchanged_error(
        &f,
        &f.command(
            202,
            take(
                ReceiveMode::PeekLock,
                Some(SessionHold::new(id(), LockToken::new(99))),
            ),
        ),
        BrokerError::SessionLockNotHeld { session_id: id() },
    )?;
    unchanged_error(
        &f,
        &f.command(
            held.lock.locked_until.as_millis(),
            take(ReceiveMode::PeekLock, Some(held.hold())),
        ),
        BrokerError::SessionLockExpired {
            session_id: id(),
            locked_until: held.lock.locked_until,
        },
    )?;
    let prefix = keys::entity_session_ready_prefix(&f.namespace, &f.entity);
    let mut malformed = prefix;
    malformed.extend_from_slice(&[0xff, 0]);
    malformed.extend_from_slice(&1_u64.to_be_bytes());
    raw(&f, malformed.clone(), Some(Vec::new()))?;
    unchanged_error(
        &f,
        &f.command(202, accept(None)),
        BrokerError::MalformedIndexKey,
    )?;
    raw(&f, malformed, None)?;
    let key = keys::session(&f.namespace, &f.entity, &id());
    let original = f.machine.store().get(&key)?.unwrap();
    raw(&f, key.clone(), Some(bad.clone()))?;
    unchanged_error(
        &f,
        &f.command(202, accept(Some(id()))),
        BrokerError::Codec(codec::decode::<domain::SessionRecord>(&bad).unwrap_err()),
    )?;
    raw(&f, key, Some(original))?;
    case(&mut f, "counter-ignored", 300, false)?;
    raw(&f, keys::queue_counters(&f.namespace, &f.entity), Some(bad))?;
    let before = f.machine.store().snapshot()?;
    assert_eq!(
        f.at(301, take(ReceiveMode::PeekLock, None))?,
        CommandOutcome::Received(None)
    );
    assert_eq!(f.machine.store().snapshot()?, before);
    f.entity = EntityPath::new("topic")?;
    f.at(
        400,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    unchanged_error(
        &f,
        &f.command(401, take(ReceiveMode::PeekLock, None)),
        BrokerError::TopicReceiveNotSupported,
    )?;
    Ok(())
}

#[test]
fn memory_qualified_terminal_receive_captures_repeated_token_before_refusal() -> TestResult {
    terminal_receive(MemoryProvider::new())
}
#[test]
fn fjall_qualified_terminal_receive_captures_repeated_token_before_refusal() -> TestResult {
    terminal_receive(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_terminal_session_captures_stale_hold_before_refusal() -> TestResult {
    terminal_session(MemoryProvider::new())
}
#[test]
fn fjall_qualified_terminal_session_captures_stale_hold_before_refusal() -> TestResult {
    terminal_session(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_final_token_and_shared_counter_preserve_legal_holds() -> TestResult {
    final_slot(MemoryProvider::new())
}
#[test]
fn fjall_qualified_final_token_and_shared_counter_preserve_legal_holds() -> TestResult {
    final_slot(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_deferred_crossing_discards_locks_and_expired_routes() -> TestResult {
    deferred_batches(MemoryProvider::new())
}
#[test]
fn fjall_qualified_deferred_crossing_discards_locks_and_expired_routes() -> TestResult {
    deferred_batches(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_no_new_hold_preserves_lazy_and_terminal_policy() -> TestResult {
    no_new_hold(MemoryProvider::new())
}
#[test]
fn fjall_qualified_no_new_hold_preserves_lazy_and_terminal_policy() -> TestResult {
    no_new_hold(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_request_record_and_counter_priorities_stay_exact() -> TestResult {
    priorities(MemoryProvider::new())
}
#[test]
fn fjall_qualified_request_record_and_counter_priorities_stay_exact() -> TestResult {
    priorities(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_existing_holds_timers_and_reopen_remain_usable() -> TestResult {
    holds::existing_holds(MemoryProvider::new())
}
#[test]
fn fjall_qualified_existing_holds_timers_and_reopen_remain_usable() -> TestResult {
    holds::existing_holds(DurableProvider::temporary()?)
}
#[test]
fn memory_qualified_indexed_token_refusal_advances_only_checkpoint() -> TestResult {
    indexed::indexed(MemoryProvider::new())
}
#[test]
fn fjall_qualified_indexed_token_refusal_advances_only_checkpoint() -> TestResult {
    indexed::indexed(DurableProvider::temporary()?)
}
