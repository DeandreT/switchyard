//! Session acceptance pages preserve the legacy bounded operation and profiles.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, LockToken,
    MAX_ENTITY_PATH_BYTES, MAX_NAMESPACE_NAME_BYTES, MAX_SESSION_ID_BYTES, MAX_SESSION_PAGE_GROUPS,
    NamespaceName, QueueConfig, ReceiveMode, SequenceNumber, SessionCursor, SessionHold, SessionId,
    SessionPageOutcome, SettlementDisposition, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, codec, keys,
};
use serde::Serialize;
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScanCall {
    prefix: Vec<u8>,
    start: Vec<u8>,
    limit: usize,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    scans: Arc<Mutex<Vec<ScanCall>>>,
    reads: Arc<Mutex<Vec<Vec<u8>>>>,
    commits: Arc<AtomicUsize>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.reads.lock().expect("read recorder").push(key.to_vec());
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        self.scans.lock().expect("scan recorder").push(ScanCall {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
        });
        self.inner.scan_from(prefix, start, limit)
    }
}

struct Node<P: StoreProvider> {
    machine: StateMachine<ObservedStore<P::Store>>,
    namespace: NamespaceName,
    entity: EntityPath,
    scans: Arc<Mutex<Vec<ScanCall>>>,
    reads: Arc<Mutex<Vec<Vec<u8>>>>,
    commits: Arc<AtomicUsize>,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn new(provider: P, requires_session: bool) -> TestResult<Self> {
        let scans = Arc::new(Mutex::new(Vec::new()));
        let reads = Arc::new(Mutex::new(Vec::new()));
        let commits = Arc::new(AtomicUsize::new(0));
        let node = Self {
            machine: StateMachine::new(ObservedStore {
                inner: provider.open()?,
                scans: Arc::clone(&scans),
                reads: Arc::clone(&reads),
                commits: Arc::clone(&commits),
            }),
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            scans,
            reads,
            commits,
            provider,
        };
        node.at(
            0,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session,
                    lock_duration_millis: 1_000,
                    ..QueueConfig::default()
                },
            },
        )?;
        Ok(node)
    }

    fn restart(self) -> TestResult<Self> {
        let Self {
            machine,
            namespace,
            entity,
            scans,
            reads,
            commits,
            provider,
        } = self;
        drop(machine);
        let machine = StateMachine::new(ObservedStore {
            inner: provider.open()?,
            scans: Arc::clone(&scans),
            reads: Arc::clone(&reads),
            commits: Arc::clone(&commits),
        });
        Ok(Self {
            machine,
            namespace,
            entity,
            scans,
            reads,
            commits,
            provider,
        })
    }

    fn at(&self, at: u64, kind: CommandKind) -> Result<CommandOutcome, BrokerError> {
        self.at_entity(&self.entity, at, kind)
    }

    fn at_entity(
        &self,
        entity: &EntityPath,
        at: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        ))
    }

    fn send(&self, id: &SessionId, count: usize) -> TestResult {
        for index in 0..count {
            assert!(matches!(
                self.at(
                    1,
                    CommandKind::Send {
                        message_id: format!("{id}-{index}"),
                        body: vec![1],
                        time_to_live_millis: None,
                        session_id: Some(id.clone()),
                    }
                )?,
                CommandOutcome::Sent { .. }
            ));
        }
        Ok(())
    }

    fn hold(&self, at: u64, id: &SessionId) -> TestResult<SessionHold> {
        let CommandOutcome::SessionAccepted(Some(accepted)) = self.at(
            at,
            CommandKind::AcceptSession {
                session_id: Some(id.clone()),
                lock_duration_millis: Some(100_000),
            },
        )?
        else {
            panic!("named session")
        };
        Ok(accepted.hold())
    }

    fn page(
        &self,
        at: u64,
        after: Option<SessionCursor>,
        duration: Option<u64>,
    ) -> Result<SessionPageOutcome, BrokerError> {
        let CommandOutcome::SessionPage(page) = self.at(
            at,
            CommandKind::AcceptNextSessionPage {
                after,
                lock_duration_millis: duration,
            },
        )?
        else {
            panic!("session page")
        };
        Ok(page)
    }

    fn cursor(&self, id: &str) -> SessionCursor {
        SessionCursor {
            namespace: self.namespace.clone(),
            entity: self.entity.clone(),
            session_id: SessionId::new(id).expect("session cursor"),
        }
    }

    fn reset_observation(&self) {
        self.scans.lock().expect("scan recorder").clear();
        self.reads.lock().expect("read recorder").clear();
        self.commits.store(0, Ordering::SeqCst);
    }

    fn assert_page_reads(&self, count: usize) {
        let prefix = keys::entity_session_ready_prefix(&self.namespace, &self.entity);
        let calls = self.scans.lock().expect("scan recorder").clone();
        let ready: Vec<_> = calls.iter().filter(|call| call.prefix == prefix).collect();
        let probes: Vec<_> = calls
            .iter()
            .filter(|call| call.prefix.first() == Some(&0x14))
            .collect();
        assert_eq!(ready.len(), count);
        assert_eq!(calls.len(), ready.len() + probes.len());
        assert!(probes.len() <= count);
        let mut seen = std::collections::BTreeSet::new();
        for call in &probes {
            assert_eq!(call.limit, 1);
            assert!(
                seen.insert(&call.prefix),
                "one grant probe per eligible session"
            );
        }
        for call in &ready {
            assert_eq!(call.prefix, prefix);
            assert_eq!(call.limit, 1);
        }
        assert!(ready.windows(2).all(|pair| pair[0].start < pair[1].start));
    }

    fn session_reads(&self) -> Vec<Vec<u8>> {
        let prefix = keys::entity_session_prefix(&self.namespace, &self.entity);
        self.reads
            .lock()
            .expect("read recorder")
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect()
    }
}

fn numbered(index: usize) -> SessionId {
    SessionId::new(format!("S{index:03}")).expect("numbered session")
}

fn acceptance_passes_stable_32_and_64_held_prefixes<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider, true)?;
    for index in 0..65 {
        node.send(&numbered(index), 1)?;
    }
    let candidate = node.hold(2, &numbered(32))?;
    node.at(
        2,
        CommandKind::SetSessionState {
            session: candidate.clone(),
            state: vec![3, 2, 1],
        },
    )?;
    node.at(2, CommandKind::ReleaseSession { session: candidate })?;
    for index in 0..32 {
        node.hold(2, &numbered(index))?;
    }
    let before = node.machine.store().snapshot()?;
    node.reset_observation();
    assert_eq!(
        node.at(
            50,
            CommandKind::AcceptSession {
                session_id: None,
                lock_duration_millis: None
            }
        )?,
        CommandOutcome::SessionAccepted(None)
    );
    assert_eq!(node.machine.store().snapshot()?, before);
    node.assert_page_reads(32);
    node.reset_observation();
    let SessionPageOutcome::Continue(cursor) = node.page(50, None, None)? else {
        panic!("first page")
    };
    assert_eq!(cursor, node.cursor("S031"));
    assert_eq!(node.machine.store().snapshot()?, before);
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    node.assert_page_reads(32);
    assert_eq!(node.session_reads().len(), 32);
    node.reset_observation();
    let SessionPageOutcome::Accepted(accepted) = node.page(51, Some(cursor), None)? else {
        panic!("33rd grant")
    };
    assert_eq!(accepted.session_id, numbered(32));
    assert_eq!(accepted.lock.locked_until, Timestamp::from_millis(1_051));
    assert_eq!(accepted.state, vec![3, 2, 1]);
    node.assert_page_reads(1);
    assert_eq!(node.session_reads().len(), 1);
    assert_eq!(node.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.machine.last_applied_time()?,
        Timestamp::from_millis(51)
    );
    for index in 33..64 {
        node.hold(52, &numbered(index))?;
    }
    let before = node.machine.store().snapshot()?;
    node.reset_observation();
    let SessionPageOutcome::Continue(first) = node.page(53, None, None)? else {
        panic!("first full page")
    };
    node.assert_page_reads(32);
    node.reset_observation();
    let SessionPageOutcome::Continue(second) = node.page(54, Some(first), None)? else {
        panic!("second full page")
    };
    assert_eq!(second, node.cursor("S063"));
    node.assert_page_reads(32);
    assert_eq!(node.machine.store().snapshot()?, before);
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    node.reset_observation();
    let SessionPageOutcome::Accepted(accepted) = node.page(55, Some(second), Some(7))? else {
        panic!("65th grant")
    };
    assert_eq!(accepted.session_id, numbered(64));
    assert_eq!(accepted.lock.locked_until, Timestamp::from_millis(62));
    node.assert_page_reads(1);
    assert_eq!(node.commits.load(Ordering::SeqCst), 1);
    Ok(())
}

fn full_held_and_empty_pages_have_no_clock_or_storage_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider, true)?;
    let empty = node.machine.store().snapshot()?;
    node.reset_observation();
    assert_eq!(node.page(100, None, None)?, SessionPageOutcome::End);
    assert_eq!(node.machine.store().snapshot()?, empty);
    assert_eq!(node.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    node.assert_page_reads(1);
    assert!(node.session_reads().is_empty());
    for index in 0..MAX_SESSION_PAGE_GROUPS {
        node.send(&numbered(index), 1)?;
    }
    for index in 0..MAX_SESSION_PAGE_GROUPS {
        node.hold(2, &numbered(index))?;
    }
    let before = node.machine.store().snapshot()?;
    node.reset_observation();
    let SessionPageOutcome::Continue(cursor) = node.page(200, None, Some(99))? else {
        panic!("full held page")
    };
    assert_eq!(cursor, node.cursor("S031"));
    node.assert_page_reads(32);
    assert_eq!(node.session_reads().len(), 32);
    assert_eq!(node.machine.store().snapshot()?, before);
    assert_eq!(node.machine.last_applied_time()?, Timestamp::from_millis(2));
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    node.reset_observation();
    assert_eq!(node.page(201, Some(cursor), None)?, SessionPageOutcome::End);
    node.assert_page_reads(1);
    assert!(node.session_reads().is_empty());
    assert_eq!(node.machine.store().snapshot()?, before);
    assert_eq!(node.machine.last_applied_time()?, Timestamp::from_millis(2));
    assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    Ok(())
}

fn backlogs_and_prefix_related_identifiers_cost_one_group_each<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider, true)?;
    let names = ["A", "A-long", "B", "\u{e9}", "\u{e9}-long"];
    for name in names {
        node.send(&SessionId::new(name)?, 64)?;
    }
    let mut holds = Vec::new();
    for name in &names[..4] {
        holds.push(node.hold(2, &SessionId::new(*name)?)?);
    }
    node.reset_observation();
    let SessionPageOutcome::Accepted(accepted) = node.page(3, None, None)? else {
        panic!("prefix-related grant")
    };
    assert_eq!(accepted.session_id, SessionId::new(names[4])?);
    node.assert_page_reads(5);
    assert_eq!(
        node.session_reads(),
        names
            .iter()
            .map(|name| keys::session(
                &node.namespace,
                &node.entity,
                &SessionId::new(*name).expect("session")
            ))
            .collect::<Vec<_>>()
    );
    let calls = node.scans.lock().expect("scan recorder").clone();
    assert_eq!(
        calls[0].start,
        keys::entity_session_ready_prefix(&node.namespace, &node.entity)
    );
    for index in 1..names.len() {
        assert_eq!(
            calls[index].start,
            keys::after_session_ready(
                &node.namespace,
                &node.entity,
                &SessionId::new(names[index - 1])?
            )
        );
    }
    node.reset_observation();
    let SessionPageOutcome::Accepted(expired) = node.page(100_002, None, None)? else {
        panic!("elapsed first session grant")
    };
    assert_eq!(expired.session_id, holds[0].session_id);
    assert_ne!(expired.lock.token, holds[0].token);
    assert_eq!(expired.lock.locked_until, Timestamp::from_millis(101_002));
    node.assert_page_reads(1);
    assert_eq!(node.session_reads().len(), 1);
    assert_eq!(node.commits.load(Ordering::SeqCst), 1);
    assert!(
        node.machine
            .store()
            .get(&keys::session_lock(
                &node.namespace,
                &node.entity,
                Timestamp::from_millis(100_002),
                &expired.session_id
            ))?
            .is_none()
    );
    assert!(
        node.machine
            .store()
            .get(&keys::session_lock(
                &node.namespace,
                &node.entity,
                expired.lock.locked_until,
                &expired.session_id
            ))?
            .is_some()
    );
    Ok(())
}

#[derive(Serialize)]
struct CursorWire<'a> {
    namespace: &'a str,
    entity: &'a str,
    session_id: &'a str,
}

fn decoded_cursor(namespace: &str, entity: &str, session_id: &str) -> TestResult<SessionCursor> {
    Ok(postcard::from_bytes(&postcard::to_stdvec(&CursorWire {
        namespace,
        entity,
        session_id,
    })?)?)
}

fn cursor_validation_and_scope_refusals_precede_all_range_work<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider, true)?;
    let long_namespace = "n".repeat(MAX_NAMESPACE_NAME_BYTES + 1);
    let long_entity = "e".repeat(MAX_ENTITY_PATH_BYTES + 1);
    let long_session = "s".repeat(MAX_SESSION_ID_BYTES + 1);
    let invalid = [
        ("", "orders", "A"),
        ("tenant", "", "A"),
        ("tenant", "orders", ""),
        ("tenant\0", "orders", "A"),
        ("tenant", "orders\0", "A"),
        ("tenant", "orders", "A\0"),
        (long_namespace.as_str(), "orders", "A"),
        ("tenant", long_entity.as_str(), "A"),
        ("tenant", "orders", long_session.as_str()),
    ];
    let before = node.machine.store().snapshot()?;
    for (namespace, entity, id) in invalid {
        let cursor = decoded_cursor(namespace, entity, id)?;
        node.reset_observation();
        assert_eq!(
            node.page(1, Some(cursor), None),
            Err(BrokerError::InvalidSessionCursor)
        );
        assert_eq!(node.machine.store().snapshot()?, before);
        assert!(node.scans.lock().expect("scan recorder").is_empty());
        assert!(node.session_reads().is_empty());
        assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    }
    for cursor in [
        decoded_cursor("other", "orders", "A")?,
        decoded_cursor("tenant", "other", "A")?,
    ] {
        node.reset_observation();
        assert_eq!(
            node.page(1, Some(cursor.clone()), None),
            Err(BrokerError::SessionCursorScopeMismatch {
                namespace: node.namespace.clone(),
                entity: node.entity.clone(),
                cursor_namespace: cursor.namespace,
                cursor_entity: cursor.entity,
            })
        );
        assert_eq!(node.machine.store().snapshot()?, before);
        assert!(node.scans.lock().expect("scan recorder").is_empty());
        assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    }
    let missing = EntityPath::new("missing")?;
    node.reset_observation();
    assert_eq!(
        node.at_entity(
            &missing,
            1,
            CommandKind::AcceptNextSessionPage {
                after: Some(decoded_cursor("", "", "")?),
                lock_duration_millis: None,
            }
        ),
        Err(BrokerError::QueueNotFound)
    );
    assert!(node.scans.lock().expect("scan recorder").is_empty());
    assert_eq!(node.machine.store().snapshot()?, before);
    Ok(())
}

fn deleted_cursor_and_insertions_resume_exclusively_after_restart<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::new(provider, true)?;
    for name in ["a", "m", "z"] {
        node.send(&SessionId::new(name)?, 1)?;
    }
    let hold = node.hold(2, &SessionId::new("a")?)?;
    let CommandOutcome::Received(Some(delivery)) = node.at(
        3,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: Some(hold),
        },
    )?
    else {
        panic!("cursor message")
    };
    node.at(
        4,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.expect("message lock").token,
        },
    )?;
    assert!(
        node.machine
            .store()
            .scan_prefix(
                &keys::session_ready_prefix(&node.namespace, &node.entity, &SessionId::new("a")?),
                1
            )?
            .is_empty()
    );
    let cursor = node.cursor("a");
    let before = node.machine.store().snapshot()?;
    node = node.restart()?;
    assert_eq!(node.machine.store().snapshot()?, before);
    for name in ["0-before", "b-after"] {
        node.at(
            5,
            CommandKind::Send {
                message_id: name.to_owned(),
                body: vec![1],
                time_to_live_millis: None,
                session_id: Some(SessionId::new(name)?),
            },
        )?;
    }
    node.reset_observation();
    let SessionPageOutcome::Accepted(accepted) = node.page(6, Some(cursor), None)? else {
        panic!("exclusive resume")
    };
    assert_eq!(accepted.session_id, SessionId::new("b-after")?);
    node.assert_page_reads(1);
    assert_eq!(
        node.scans.lock().expect("scan recorder")[0].start,
        keys::after_session_ready(&node.namespace, &node.entity, &SessionId::new("a")?)
    );
    let SessionPageOutcome::Accepted(behind) = node.page(7, None, None)? else {
        panic!("new walk sees insertion behind cursor")
    };
    assert_eq!(behind.session_id, SessionId::new("0-before")?);
    Ok(())
}

fn only_required_queues_and_subscriptions_admit_session_pages<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider, true)?;
    let plain = EntityPath::new("plain")?;
    node.at_entity(
        &plain,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )?;
    node.at_entity(
        &plain,
        1,
        CommandKind::Send {
            message_id: "metadata".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: Some(SessionId::new("metadata")?),
        },
    )?;
    let shadow = node.entity.dead_letter_queue()?;
    for entity in [&plain, &shadow] {
        let before = node.machine.store().snapshot()?;
        node.reset_observation();
        assert_eq!(
            node.at_entity(
                entity,
                2,
                CommandKind::AcceptNextSessionPage {
                    after: Some(decoded_cursor("", "", "")?),
                    lock_duration_millis: None,
                }
            ),
            Err(BrokerError::SessionNotSupported)
        );
        assert_eq!(node.machine.store().snapshot()?, before);
        assert!(node.scans.lock().expect("scan recorder").is_empty());
        assert_eq!(node.commits.load(Ordering::SeqCst), 0);
    }
    let topic = EntityPath::new("events")?;
    let subscription = SubscriptionName::new("required")?;
    node.at_entity(
        &topic,
        2,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    )?;
    node.at_entity(
        &topic,
        2,
        CommandKind::CreateSubscription {
            name: subscription.clone(),
            config: SubscriptionConfig {
                requires_session: true,
                lock_duration_millis: 1_000,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    let id = SessionId::new("subscription-session")?;
    node.at_entity(
        &topic,
        3,
        CommandKind::Send {
            message_id: "publication".into(),
            body: vec![1],
            time_to_live_millis: None,
            session_id: Some(id.clone()),
        },
    )?;
    let backing = topic.subscription(&subscription)?;
    let CommandOutcome::SessionPage(SessionPageOutcome::Accepted(accepted)) = node.at_entity(
        &backing,
        4,
        CommandKind::AcceptNextSessionPage {
            after: None,
            lock_duration_millis: None,
        },
    )?
    else {
        panic!("required subscription grant")
    };
    assert_eq!(accepted.session_id, id);
    assert_eq!(accepted.lock.locked_until, Timestamp::from_millis(1_004));
    assert_eq!(
        node.machine
            .session(&node.namespace, &backing, &id)?
            .expect("session")
            .lock,
        Some(accepted.lock)
    );
    Ok(())
}

#[test]
fn page_command_is_appended_with_scoped_cursor_roundtrips_and_unchanged_versions() -> TestResult {
    let old_acceptance = CommandKind::AcceptSession {
        session_id: None,
        lock_duration_millis: None,
    };
    assert_eq!(postcard::to_stdvec(&old_acceptance)?, vec![12, 0, 0]);
    let old_renewal = CommandKind::RenewLockHeld {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        session: None,
        lock_duration_millis: None,
    };
    assert_eq!(postcard::to_stdvec(&old_renewal)?, vec![39, 7, 9, 0, 0]);
    let old_settlement = CommandKind::SettleHeld {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
        session: None,
        disposition: SettlementDisposition::Complete,
        properties_to_modify: BTreeMap::new(),
    };
    assert_eq!(
        postcard::to_stdvec(&old_settlement)?,
        vec![38, 7, 9, 0, 0, 0]
    );
    let empty = CommandKind::AcceptNextSessionPage {
        after: None,
        lock_duration_millis: None,
    };
    assert_eq!(postcard::to_stdvec(&empty)?, vec![40, 0, 0]);
    let resumed = CommandKind::AcceptNextSessionPage {
        after: Some(SessionCursor {
            namespace: NamespaceName::new("tenant")?,
            entity: EntityPath::new("orders")?,
            session_id: SessionId::new("A")?,
        }),
        lock_duration_millis: Some(13),
    };
    assert_eq!(
        postcard::to_stdvec(&resumed)?,
        vec![
            40, 1, 6, b't', b'e', b'n', b'a', b'n', b't', 6, b'o', b'r', b'd', b'e', b'r', b's', 1,
            b'A', 1, 13
        ]
    );
    for command in [empty, resumed, old_acceptance, old_renewal, old_settlement] {
        let encoded = codec::encode(&command)?;
        assert_eq!(encoded[0], 11);
        assert_eq!(codec::decode::<CommandKind>(&encoded)?, command);
    }
    assert_eq!(codec::ACTIVE_VALUE_FORMAT, 11);
    assert_eq!(storage::ACTIVE_STORE_FORMAT, 17);
    Ok(())
}

#[test]
fn session_pages_remain_outside_the_closed_atomic_profile() -> TestResult {
    let node = Node::new(testkit::MemoryProvider::new(), false)?;
    let binding = node
        .machine
        .bind_entity(
            &node.namespace,
            &node.entity,
            &node.entity,
            domain::EntityIncarnationKind::Queue,
        )?
        .expect("binding");
    let before = node.machine.store().snapshot()?;
    let mut usage = domain::AtomicMessagingInputUsage::default();
    usage.try_extend(&CommandKind::Complete {
        sequence: SequenceNumber::new(7),
        lock_token: LockToken::new(9),
    })?;
    let original_usage = usage;
    for after in [None, Some(node.cursor("A"))] {
        let kind = CommandKind::AcceptNextSessionPage {
            after,
            lock_duration_millis: None,
        };
        assert_eq!(
            usage.try_extend(&kind),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(usage, original_usage);
        assert_eq!(
            domain::validate_atomic_messaging_kinds(std::slice::from_ref(&kind)),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        let transaction = domain::AtomicMessagingCommand {
            binding: binding.clone(),
            issued_at: Timestamp::from_millis(1),
            commands: vec![
                Command::new(
                    node.namespace.clone(),
                    node.entity.clone(),
                    Timestamp::from_millis(1),
                    CommandKind::Send {
                        message_id: "uncommitted-prefix".into(),
                        body: vec![1],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                ),
                Command::new(
                    node.namespace.clone(),
                    node.entity.clone(),
                    Timestamp::from_millis(1),
                    kind,
                ),
            ],
        };
        assert_eq!(
            node.machine.validate_atomic_messaging(&transaction),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(
            node.machine.apply_atomic_messaging(&transaction),
            Err(BrokerError::AtomicMessagingOperationNotSupported)
        );
        assert_eq!(node.machine.store().snapshot()?, before);
        assert_eq!(node.machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend!(
    acceptance_passes_stable_32_and_64_held_prefixes,
    full_held_and_empty_pages_have_no_clock_or_storage_effects,
    backlogs_and_prefix_related_identifiers_cost_one_group_each,
    cursor_validation_and_scope_refusals_precede_all_range_work,
    deleted_cursor_and_insertions_resume_exclusively_after_restart,
    only_required_queues_and_subscriptions_admit_session_pages,
);
