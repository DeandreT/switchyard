//! Session ownership and session-ordered delivery.
//!
//! Like the queue semantics suite, every case is generic over the backend its
//! state lives on and is run once in memory and once on a real store directory.

use std::error::Error;

use domain::{
    AcceptedSession, BrokerError, CommandKind, CommandOutcome, Delivery, LockToken, QueueConfig,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, Timestamp,
};
use testkit::{QueueFixture, StoreProvider};

const LOCK_MILLIS: u64 = 30_000;

fn session_queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            lock_duration_millis: LOCK_MILLIS,
            max_delivery_count: 2,
            requires_session: true,
            ..QueueConfig::default()
        },
    )?)
}

fn plain_queue<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::with_defaults(provider, "tenant", "orders")?)
}

fn id(value: &str) -> SessionId {
    SessionId::new(value).expect("a valid session id")
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    message_id: &str,
    session_id: Option<SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Send {
            message_id: message_id.to_owned(),
            body: message_id.as_bytes().to_vec(),
            time_to_live_millis: None,
            session_id,
            envelope: None,
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected a send outcome, got {other:?}"),
    }
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session_id: Option<SessionId>,
) -> Result<Option<AcceptedSession>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::AcceptSession {
            session_id,
            lock_duration_millis: None,
        },
    )? {
        CommandOutcome::SessionAccepted(accepted) => Ok(accepted),
        other => panic!("expected a session acceptance, got {other:?}"),
    }
}

fn receive<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: &SessionHold,
) -> Result<Option<Delivery>, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(session.clone()),
        },
    )? {
        CommandOutcome::Received(delivery) => Ok(delivery),
        other => panic!("expected a receive outcome, got {other:?}"),
    }
}

// ---- the suite -------------------------------------------------------------

fn a_session_queue_refuses_a_command_that_names_no_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;

    assert_eq!(
        fixture.at(
            10,
            CommandKind::Send {
                message_id: String::from("first"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                envelope: None,
            }
        ),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(
        fixture.at(
            11,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            }
        ),
        Err(BrokerError::SessionRequired)
    );
    Ok(())
}

fn a_plain_queue_refuses_a_command_that_names_a_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = plain_queue(provider)?;

    // A session identifier here would promise an ordering the queue does not
    // keep, so it is refused rather than ignored.
    assert_eq!(
        fixture.at(
            10,
            CommandKind::Send {
                message_id: String::from("first"),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: Some(id("cart-1")),
                envelope: None,
            }
        ),
        Err(BrokerError::SessionNotSupported)
    );
    assert_eq!(
        fixture.at(
            11,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: Some(SessionHold::new(id("cart-1"), LockToken::new(1))),
            }
        ),
        Err(BrokerError::SessionNotSupported)
    );
    assert_eq!(
        fixture.at(
            12,
            CommandKind::AcceptSession {
                session_id: Some(id("cart-1")),
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::SessionNotSupported)
    );
    Ok(())
}

fn accepting_a_session_grants_an_exclusive_lock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;

    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("a named session is accepted");
    assert_eq!(accepted.session_id, id("cart-1"));
    assert_eq!(
        accepted.lock.locked_until,
        Timestamp::from_millis(20 + LOCK_MILLIS)
    );
    assert_eq!(accepted.state, Vec::<u8>::new());

    // Nobody else can take it while the lock is live.
    assert_eq!(
        fixture.at(
            21,
            CommandKind::AcceptSession {
                session_id: Some(id("cart-1")),
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::SessionAlreadyLocked {
            session_id: id("cart-1")
        })
    );
    Ok(())
}

fn a_named_session_can_be_accepted_before_it_holds_anything<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;

    // Waiting on a session a sender has not reached yet is legitimate.
    let accepted = accept(&fixture, 10, Some(id("cart-1")))?.expect("an empty session is accepted");
    assert_eq!(receive(&fixture, 11, &accepted.hold())?, None);

    send(&fixture, 12, "first", Some(id("cart-1")))?;
    let delivery = receive(&fixture, 13, &accepted.hold())?.expect("the message arrived");
    assert_eq!(delivery.message_id, "first");
    assert_eq!(delivery.session_id, Some(id("cart-1")));
    Ok(())
}

fn a_session_delivers_its_own_messages_in_send_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "cart-1-first", Some(id("cart-1")))?;
    send(&fixture, 11, "cart-2-first", Some(id("cart-2")))?;
    send(&fixture, 12, "cart-1-second", Some(id("cart-1")))?;

    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let hold = accepted.hold();

    // FIFO within the session, and nothing from any other session.
    let mut delivered = Vec::new();
    for tick in 0..3 {
        match receive(&fixture, 30 + tick, &hold)? {
            Some(delivery) => delivered.push(delivery.message_id),
            None => break,
        }
    }
    assert_eq!(delivered, vec!["cart-1-first", "cart-1-second"]);
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &id("cart-2"),
            16
        )?,
        vec![SequenceNumber::new(2)]
    );
    Ok(())
}

fn accepting_the_next_session_skips_the_ones_already_held<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "a-first", Some(id("cart-a")))?;
    send(&fixture, 11, "a-second", Some(id("cart-a")))?;
    send(&fixture, 12, "b-first", Some(id("cart-b")))?;

    let first = accept(&fixture, 20, None)?.expect("a session is available");
    assert_eq!(first.session_id, id("cart-a"));

    // The walk resumes past every message of the held session rather than
    // stalling on its backlog.
    let second = accept(&fixture, 21, None)?.expect("the other session is available");
    assert_eq!(second.session_id, id("cart-b"));

    assert_eq!(accept(&fixture, 22, None)?, None);
    Ok(())
}

fn accepting_the_next_session_finds_nothing_in_an_empty_queue<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    assert_eq!(accept(&fixture, 10, None)?, None);
    Ok(())
}

fn a_session_hold_cannot_reach_another_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "for-b", Some(id("cart-b")))?;

    let accepted = accept(&fixture, 20, Some(id("cart-a")))?.expect("the session is accepted");
    // Holding one session reveals nothing about another, even an unheld one.
    assert_eq!(receive(&fixture, 21, &accepted.hold())?, None);

    let forged = SessionHold::new(id("cart-b"), accepted.lock.token);
    assert_eq!(
        receive(&fixture, 22, &forged),
        Err(BrokerError::SessionLockNotHeld {
            session_id: id("cart-b")
        })
    );
    Ok(())
}

fn a_foreign_session_token_cannot_receive<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");

    let forged = SessionHold::new(
        id("cart-1"),
        LockToken::new(accepted.lock.token.as_u64() + 1),
    );
    assert_eq!(
        receive(&fixture, 21, &forged),
        Err(BrokerError::SessionLockNotHeld {
            session_id: id("cart-1")
        })
    );
    // The rejection left the real hold working.
    assert!(receive(&fixture, 22, &accepted.hold())?.is_some());
    Ok(())
}

fn receiving_after_the_session_lock_elapsed_is_rejected<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let locked_until = accepted.lock.locked_until;

    assert_eq!(
        receive(&fixture, locked_until.as_millis(), &accepted.hold()),
        Err(BrokerError::SessionLockExpired {
            session_id: id("cart-1"),
            locked_until
        })
    );
    Ok(())
}

fn an_elapsed_session_lock_frees_the_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let locked_until = accepted.lock.locked_until.as_millis();

    // Before the deadline the sweep changes nothing.
    assert_eq!(
        fixture.at(locked_until - 1, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 0 }
    );
    assert_eq!(
        fixture.at(locked_until, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );

    // Another receiver can take it, and the old hold is dead.
    let next = accept(&fixture, locked_until + 1, None)?.expect("the session was freed");
    assert_eq!(next.session_id, id("cart-1"));
    assert_ne!(next.lock.token, accepted.lock.token);
    assert_eq!(
        receive(&fixture, locked_until + 2, &accepted.hold()),
        Err(BrokerError::SessionLockNotHeld {
            session_id: id("cart-1")
        })
    );
    Ok(())
}

fn a_session_can_be_reaccepted_once_its_lock_elapsed_even_without_a_sweep<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let locked_until = accepted.lock.locked_until.as_millis();

    // The timer has not run, but an elapsed lock is not a held session.
    let next = accept(&fixture, locked_until, Some(id("cart-1")))?.expect("the lock had elapsed");
    assert_ne!(next.lock.token, accepted.lock.token);
    assert!(receive(&fixture, locked_until + 1, &next.hold())?.is_some());
    Ok(())
}

fn releasing_a_session_frees_it_immediately<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");

    assert_eq!(
        fixture.at(
            30,
            CommandKind::ReleaseSession {
                session: accepted.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    assert_eq!(
        receive(&fixture, 31, &accepted.hold()),
        Err(BrokerError::SessionLockNotHeld {
            session_id: id("cart-1")
        })
    );

    let next = accept(&fixture, 32, None)?.expect("the released session is available");
    assert_eq!(next.session_id, id("cart-1"));
    Ok(())
}

fn renewing_a_session_lock_extends_it_without_changing_its_token<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let original_deadline = accepted.lock.locked_until.as_millis();

    assert_eq!(
        fixture.at(
            100,
            CommandKind::RenewSessionLock {
                session: accepted.hold(),
                lock_duration_millis: None,
            }
        )?,
        CommandOutcome::SessionLockRenewed {
            locked_until: Timestamp::from_millis(100 + LOCK_MILLIS)
        }
    );

    // The same hold keeps working past the deadline it originally had, and the
    // sweep no longer finds a lock to release at that deadline.
    assert_eq!(
        fixture.at(original_deadline, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 0 }
    );
    assert!(receive(&fixture, original_deadline + 1, &accepted.hold())?.is_some());
    Ok(())
}

fn session_state_outlives_the_receiver_that_set_it<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    let accepted = accept(&fixture, 10, Some(id("cart-1")))?.expect("the session is accepted");

    assert_eq!(
        fixture.at(
            20,
            CommandKind::SetSessionState {
                session: accepted.hold(),
                state: b"checkout-step-2".to_vec(),
            }
        )?,
        CommandOutcome::SessionStateSet
    );
    assert_eq!(
        fixture.at(
            21,
            CommandKind::GetSessionState {
                session: accepted.hold(),
            }
        )?,
        CommandOutcome::SessionState(b"checkout-step-2".to_vec())
    );
    fixture.at(
        22,
        CommandKind::ReleaseSession {
            session: accepted.hold(),
        },
    )?;

    // Releasing the session keeps its state, and the next owner is handed it.
    assert_eq!(
        fixture
            .machine
            .session_state(&fixture.namespace, &fixture.entity, &id("cart-1"))?,
        b"checkout-step-2".to_vec()
    );
    let next = accept(&fixture, 23, Some(id("cart-1")))?.expect("the session is available");
    assert_eq!(next.state, b"checkout-step-2".to_vec());
    Ok(())
}

fn session_state_cannot_be_set_without_the_session_lock<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    let accepted = accept(&fixture, 10, Some(id("cart-1")))?.expect("the session is accepted");
    let forged = SessionHold::new(
        id("cart-1"),
        LockToken::new(accepted.lock.token.as_u64() + 1),
    );

    assert_eq!(
        fixture.at(
            20,
            CommandKind::SetSessionState {
                session: forged,
                state: b"forged".to_vec(),
            }
        ),
        Err(BrokerError::SessionLockNotHeld {
            session_id: id("cart-1")
        })
    );
    assert_eq!(
        fixture
            .machine
            .session_state(&fixture.namespace, &fixture.entity, &id("cart-1"))?,
        Vec::<u8>::new()
    );
    Ok(())
}

fn an_abandoned_message_returns_to_its_own_session_order<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    send(&fixture, 11, "second", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    let hold = accepted.hold();

    let first = receive(&fixture, 30, &hold)?.expect("the session holds messages");
    fixture.at(
        31,
        CommandKind::Abandon {
            sequence: first.sequence,
            lock_token: first.lock.expect("a peek-lock delivery is locked").token,
            replacement_envelope: None,
        },
    )?;

    // Back at the head of its own session, not of the entity.
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &id("cart-1"),
            16
        )?,
        vec![SequenceNumber::new(1), SequenceNumber::new(2)]
    );
    let redelivered = receive(&fixture, 32, &hold)?.expect("the abandoned message is ready");
    assert_eq!(redelivered.sequence, first.sequence);
    assert_eq!(redelivered.delivery_count, 2);
    Ok(())
}

fn an_expired_session_message_is_dead_lettered_out_of_its_session<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    fixture.at(
        10,
        CommandKind::Send {
            message_id: String::from("perishable"),
            body: b"perishable".to_vec(),
            time_to_live_millis: Some(100),
            session_id: Some(id("cart-1")),
            envelope: None,
        },
    )?;

    assert_eq!(
        fixture.at(110, CommandKind::ExpireMessages)?,
        CommandOutcome::MessagesExpired { dead_lettered: 1 }
    );
    // The sweep cleared the session's index entry, not the entity-wide one.
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &id("cart-1"),
            16
        )?,
        Vec::new()
    );
    assert_eq!(
        fixture
            .machine
            .dead_lettered_sequences(&fixture.namespace, &fixture.entity, 16)?,
        vec![SequenceNumber::new(1)]
    );
    Ok(())
}

fn a_restart_preserves_session_locks_and_state<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = session_queue(provider)?;
    send(&fixture, 10, "first", Some(id("cart-1")))?;
    let accepted = accept(&fixture, 20, Some(id("cart-1")))?.expect("the session is accepted");
    fixture.at(
        21,
        CommandKind::SetSessionState {
            session: accepted.hold(),
            state: b"checkout-step-2".to_vec(),
        },
    )?;

    let fixture = fixture.restart()?;

    // The session hold and its state were replicated state, not process state.
    assert_eq!(
        fixture
            .machine
            .session_state(&fixture.namespace, &fixture.entity, &id("cart-1"))?,
        b"checkout-step-2".to_vec()
    );
    assert_eq!(
        fixture.at(
            30,
            CommandKind::AcceptSession {
                session_id: Some(id("cart-1")),
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::SessionAlreadyLocked {
            session_id: id("cart-1")
        })
    );
    let delivery = receive(&fixture, 31, &accepted.hold())?.expect("the session still holds one");
    assert_eq!(delivery.message_id, "first");
    Ok(())
}

// ---- instantiation ---------------------------------------------------------

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    a_session_queue_refuses_a_command_that_names_no_session,
    a_plain_queue_refuses_a_command_that_names_a_session,
    accepting_a_session_grants_an_exclusive_lock,
    a_named_session_can_be_accepted_before_it_holds_anything,
    a_session_delivers_its_own_messages_in_send_order,
    accepting_the_next_session_skips_the_ones_already_held,
    accepting_the_next_session_finds_nothing_in_an_empty_queue,
    a_session_hold_cannot_reach_another_session,
    a_foreign_session_token_cannot_receive,
    receiving_after_the_session_lock_elapsed_is_rejected,
    an_elapsed_session_lock_frees_the_session,
    a_session_can_be_reaccepted_once_its_lock_elapsed_even_without_a_sweep,
    releasing_a_session_frees_it_immediately,
    renewing_a_session_lock_extends_it_without_changing_its_token,
    session_state_outlives_the_receiver_that_set_it,
    session_state_cannot_be_set_without_the_session_lock,
    an_abandoned_message_returns_to_its_own_session_order,
    an_expired_session_message_is_dead_lettered_out_of_its_session,
    a_restart_preserves_session_locks_and_state,
}
