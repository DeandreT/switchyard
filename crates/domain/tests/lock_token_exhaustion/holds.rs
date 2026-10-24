use super::*;

pub(super) fn existing_holds<P: StoreProvider>(provider: P) -> TestResult {
    let mut f = fixture(provider)?;
    case(&mut f, "renew-ready", 10, false)?;
    let sequence = send(&f, 11, "held", Some(10), None)?;
    seed(&f, u64::MAX - 1)?;
    let value = delivery(f.at(12, take(ReceiveMode::PeekLock, None))?);
    let lock = value.lock.unwrap();
    assert_eq!(
        f.at(
            13,
            CommandKind::RenewLock {
                sequence,
                lock_token: lock.token,
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::LockRenewed {
            locked_until: Timestamp::from_millis(113),
            lock_duration_millis: 100
        }
    );
    assert_eq!(
        f.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0
        }
    );
    assert_eq!(
        f.at(
            33,
            CommandKind::Complete {
                sequence,
                lock_token: lock.token
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    case(&mut f, "renew-session", 100, true)?;
    let sequence = send(&f, 101, "inside", None, Some(id()))?;
    seed(&f, u64::MAX - 2)?;
    let session = accepted(f.at(102, accept(Some(id())))?);
    let message = delivery(f.at(103, take(ReceiveMode::PeekLock, Some(session.hold())))?);
    let token = message.lock.unwrap().token;
    f.at(
        104,
        CommandKind::SetSessionState {
            session: session.hold(),
            state: b"retained".to_vec(),
        },
    )?;
    assert_eq!(
        f.at(
            105,
            CommandKind::RenewSessionLock {
                session: session.hold(),
                lock_duration_millis: Some(100)
            }
        )?,
        CommandOutcome::SessionLockRenewed {
            locked_until: Timestamp::from_millis(205)
        }
    );
    f.at(
        106,
        CommandKind::RenewLock {
            sequence,
            lock_token: token,
            lock_duration_millis: Some(100),
        },
    )?;
    assert_eq!(
        f.at(
            session.lock.locked_until.as_millis(),
            CommandKind::ExpireSessionLocks
        )?,
        CommandOutcome::SessionLocksExpired { released: 0 }
    );
    assert_eq!(
        f.at(123, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0
        }
    );
    assert_eq!(
        f.at(
            124,
            CommandKind::ReleaseSession {
                session: session.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    assert_eq!(
        f.at(
            125,
            CommandKind::Complete {
                sequence,
                lock_token: token
            }
        )?,
        CommandOutcome::Completed
    );
    unchanged_error(
        &f,
        &f.command(
            126,
            CommandKind::GetSessionState {
                session: session.hold(),
            },
        ),
        BrokerError::SessionLockNotHeld { session_id: id() },
    )?;
    f = restart(f)?;
    assert_eq!(
        f.machine.session_state(&f.namespace, &f.entity, &id())?,
        b"retained".to_vec()
    );
    refuse(&f, 127, accept(Some(id())))?;
    case(&mut f, "expiry-session", 200, true)?;
    send(&f, 201, "available-after-expiry", None, Some(id()))?;
    seed(&f, u64::MAX - 1)?;
    let session = accepted(f.at(202, accept(Some(id())))?);
    assert_eq!(
        f.at(
            session.lock.locked_until.as_millis(),
            CommandKind::ExpireSessionLocks
        )?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    unchanged_error(
        &f,
        &f.command(
            223,
            CommandKind::GetSessionState {
                session: session.hold(),
            },
        ),
        BrokerError::SessionLockNotHeld { session_id: id() },
    )?;
    refuse(&f, 223, accept(None))?;
    case(&mut f, "expiry-return", 300, false)?;
    let sequence = send(&f, 301, "returns", None, None)?;
    seed(&f, u64::MAX - 1)?;
    let value = delivery(f.at(302, take(ReceiveMode::PeekLock, None))?);
    let lock = value.lock.unwrap();
    assert_eq!(
        f.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0
        }
    );
    unchanged_error(
        &f,
        &f.command(
            323,
            CommandKind::Complete {
                sequence,
                lock_token: lock.token,
            },
        ),
        BrokerError::MessageNotLocked { sequence },
    )?;
    refuse(&f, 323, take(ReceiveMode::PeekLock, None))?;
    case(&mut f, "deferred-renew", 400, false)?;
    let sequence = defer_setup(&f, 401, "deferred", None)?;
    seed(&f, u64::MAX - 1)?;
    let CommandOutcome::DeferredReceived(values) =
        f.at(410, retrieve(vec![sequence], ReceiveMode::PeekLock, None))?
    else {
        panic!("expected last deferred lock")
    };
    let lock = values[0].lock.unwrap();
    f.at(
        411,
        CommandKind::RenewLock {
            sequence,
            lock_token: lock.token,
            lock_duration_millis: Some(100),
        },
    )?;
    assert_eq!(
        f.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 0,
            dead_lettered: 0
        }
    );
    assert_eq!(
        f.at(
            431,
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
    refuse(
        &f,
        432,
        retrieve(vec![sequence], ReceiveMode::PeekLock, None),
    )?;
    for (index, ttl) in [Some(5), None].into_iter().enumerate() {
        let base = 600 + index as u64 * 100;
        case(&mut f, &format!("dead-letter-{index}"), base, false)?;
        if ttl.is_none() {
            f.at(
                base + 1,
                CommandKind::UpdateQueue {
                    update: QueueConfigUpdate {
                        max_delivery_count: Some(1),
                        ..QueueConfigUpdate::default()
                    },
                },
            )?;
        }
        let sequence = send(&f, base + 2, "dead-letter", ttl, None)?;
        seed(&f, u64::MAX - 1)?;
        let value = delivery(f.at(base + 3, take(ReceiveMode::PeekLock, None))?);
        let lock = value.lock.unwrap();
        assert_eq!(
            f.at(lock.locked_until.as_millis(), CommandKind::ExpireLocks)?,
            CommandOutcome::LocksExpired {
                returned_to_ready: 0,
                dead_lettered: 1
            }
        );
        assert_eq!(
            f.machine
                .dead_lettered_sequences(&f.namespace, &f.entity, 10)?,
            vec![sequence]
        );
        assert_eq!(counters(&f)?.next_lock_token, u64::MAX);
    }
    Ok(())
}
