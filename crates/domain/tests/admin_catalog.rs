use std::sync::{Arc, Mutex};

use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogError, AdminCatalogKind, AdminCatalogPage,
    Command, CommandKind, EntityPath, MAX_ADMIN_CATALOG_PAGE, MAX_ADMIN_CATALOG_SCAN,
    NamespaceName, QueueConfig, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, codec, keys,
};
use storage::{StateStore, StorageError, StoreSnapshot, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

fn ns(value: &str) -> NamespaceName {
    NamespaceName::new(value).expect("namespace")
}

fn path(value: &str) -> EntityPath {
    EntityPath::new(value).expect("path")
}

fn name(value: &str) -> SubscriptionName {
    SubscriptionName::new(value).expect("subscription")
}

fn apply<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    kind: CommandKind,
) {
    machine
        .apply(&Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(47),
            kind,
        ))
        .expect("seed command");
}

fn queue<S: StateStore>(machine: &StateMachine<S>, namespace: &NamespaceName, entity: &EntityPath) {
    apply(
        machine,
        namespace,
        entity,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
}

fn topic<S: StateStore>(machine: &StateMachine<S>, namespace: &NamespaceName, entity: &EntityPath) {
    apply(
        machine,
        namespace,
        entity,
        CommandKind::CreateTopic {
            config: TopicConfig {
                max_message_bytes: 17_000,
                default_time_to_live_millis: Some(900),
            },
        },
    );
}

fn subscription<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    entity: &EntityPath,
    child: &SubscriptionName,
) {
    apply(
        machine,
        namespace,
        entity,
        CommandKind::CreateSubscription {
            name: child.clone(),
            config: SubscriptionConfig {
                lock_duration_millis: 5_000,
                max_delivery_count: 3,
                default_time_to_live_millis: Some(700),
            },
        },
    );
}

fn put<S: StateStore>(store: &S, key: Vec<u8>, value: Vec<u8>) {
    store
        .apply(WriteBatch::default().put(key, value))
        .expect("fixture mutation");
}

fn delete<S: StateStore>(store: &S, key: Vec<u8>) {
    store
        .apply(WriteBatch::default().delete(key))
        .expect("fixture deletion");
}

#[derive(Clone)]
struct ReadOnly<S> {
    store: S,
    namespace: NamespaceName,
    reads: Arc<Mutex<(usize, usize)>>,
}

impl<S: StateStore> StateStore for ReadOnly<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        assert_ne!(key, keys::clock(), "administration must not observe Clock");
        self.reads.lock().expect("reads").0 += 1;
        self.store.get(key)
    }

    fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
        panic!("administration must not write")
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        panic!("administration must not scan the full store")
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        assert!(limit <= MAX_ADMIN_CATALOG_SCAN + 1, "bounded scan request");
        assert!(start.starts_with(prefix));
        assert!(
            [
                keys::namespace_queue_config_prefix(&self.namespace),
                keys::namespace_topic_config_prefix(&self.namespace),
                keys::namespace_subscription_prefix(&self.namespace)
            ]
            .iter()
            .any(|expected| prefix.starts_with(expected)),
            "no global or foreign namespace scans: {prefix:?}"
        );
        let result = self.store.scan_from(prefix, start, limit)?;
        let mut reads = self.reads.lock().expect("reads");
        reads.0 += 1;
        reads.1 += result.len();
        Ok(result)
    }
}

fn readonly<S: StateStore>(
    machine: &StateMachine<S>,
    namespace: &NamespaceName,
    check: impl FnOnce(&StateMachine<ReadOnly<S>>),
) {
    let before = machine.store().snapshot().expect("before");
    let reader = StateMachine::new(ReadOnly {
        store: machine.store().clone(),
        namespace: namespace.clone(),
        reads: Arc::default(),
    });
    check(&reader);
    assert_eq!(
        machine.store().snapshot().expect("after"),
        before,
        "every byte including Clock is preserved"
    );
}

fn page<S: StateStore>(
    reader: &StateMachine<ReadOnly<S>>,
    namespace: &NamespaceName,
    kind: AdminCatalogKind,
    cursor: Option<&AdminCatalogCursor>,
    size: usize,
) -> Result<AdminCatalogPage, AdminCatalogError> {
    *reader.store().reads.lock().expect("reads") = (0, 0);
    let result = reader.admin_catalog_page(namespace, kind, cursor, size);
    assert!(reader.store().reads.lock().expect("reads").1 <= MAX_ADMIN_CATALOG_SCAN + 1);
    result
}

fn list<S: StateStore>(
    reader: &StateMachine<ReadOnly<S>>,
    namespace: &NamespaceName,
    kind: AdminCatalogKind,
    size: usize,
) -> Vec<AdminCatalogEntry> {
    let mut result = Vec::new();
    let mut after = None;
    for _ in 0..100 {
        let next = page(reader, namespace, kind, after.as_ref(), size).expect("page");
        result.extend(next.entries);
        let Some(next) = next.continuation else {
            return result;
        };
        assert_ne!(after.as_ref(), Some(&next), "cursor makes progress");
        after =
            Some(AdminCatalogCursor::from_bytes(next.as_bytes()).expect("round-trip continuation"));
    }
    panic!("bounded fixture did not finish");
}

fn identity(entry: &AdminCatalogEntry) -> String {
    match entry {
        AdminCatalogEntry::Queue(value) => format!("q:{}", value.path),
        AdminCatalogEntry::Topic(value) => format!("t:{}", value.path),
        AdminCatalogEntry::Subscription(value) => format!("s:{}", value.path),
    }
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $body:ident) => {
        #[test]
        fn $memory() {
            $body(MemoryProvider::new());
        }
        #[test]
        fn $fjall() {
            $body(DurableProvider::temporary().expect("durable provider"));
        }
    };
}

fn controls<P: StoreProvider>(provider: P) {
    let namespace = ns("Tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    for entity in ["z", "a", "a-archive"] {
        queue(&machine, &namespace, &path(entity));
    }
    for entity in ["t", "t-archive"] {
        topic(&machine, &namespace, &path(entity));
        for child in ["b", "a"] {
            subscription(&machine, &namespace, &path(entity), &name(child));
        }
    }
    for foreign in ["tenan", "tenant-a", "tenantz"] {
        queue(&machine, &ns(foreign), &path("foreign"));
        topic(&machine, &ns(foreign), &path("foreign-topic"));
        subscription(
            &machine,
            &ns(foreign),
            &path("foreign-topic"),
            &name("foreign"),
        );
        // Broken foreign values are outside the selected namespace, not global health failures.
        put(
            machine.store(),
            keys::topic_config(&ns(foreign), &path("broken")),
            vec![255],
        );
    }
    let expected = [
        "q:a",
        "q:a-archive",
        "q:z",
        "t:t",
        "t:t-archive",
        "s:t/subscriptions/a",
        "s:t/subscriptions/b",
        "s:t-archive/subscriptions/a",
        "s:t-archive/subscriptions/b",
    ];
    readonly(&machine, &namespace, |reader| {
        let q = reader
            .admin_queue(&ns("TENANT"), &path("A"))
            .expect("queue")
            .expect("present");
        assert_eq!(q.config, QueueConfig::default());
        let t = reader
            .admin_topic(&namespace, &path("T"))
            .expect("topic")
            .expect("present");
        assert_eq!(t.config.max_message_bytes, 17_000);
        let s = reader
            .admin_subscription(&namespace, &path("T"), &name("A"))
            .expect("subscription")
            .expect("present");
        assert_eq!(s.path, path("t").subscription(&name("a")).expect("child"));
        assert_eq!(s.config.lock_duration_millis, 5_000);
        assert_eq!(reader.admin_queue(&namespace, &path("t")), Ok(None));
        assert_eq!(reader.admin_topic(&namespace, &path("a")), Ok(None));
        assert_eq!(reader.admin_queue(&namespace, &path("absent")), Ok(None));
        assert_eq!(reader.admin_topic(&namespace, &path("absent")), Ok(None));
        assert_eq!(
            reader.admin_subscription(&namespace, &path("t"), &name("absent")),
            Ok(None)
        );
        assert_eq!(
            reader.admin_subscription(&namespace, &path("absent"), &name("absent")),
            Ok(None)
        );
        for reserved in [
            path("a").dead_letter_queue().expect("shadow"),
            s.path,
            path("a/$management"),
            path("t/subscriptions"),
        ] {
            assert_eq!(
                reader.admin_queue(&namespace, &reserved),
                Err(AdminCatalogError::InvalidEntityPath)
            );
            assert_eq!(
                reader.admin_topic(&namespace, &reserved),
                Err(AdminCatalogError::InvalidEntityPath)
            );
            assert_eq!(
                reader.admin_subscription(&namespace, &reserved, &name("a")),
                Err(AdminCatalogError::InvalidEntityPath)
            );
        }
        for size in [1, 2, 3, MAX_ADMIN_CATALOG_PAGE] {
            let rows = list(reader, &namespace, AdminCatalogKind::All, size);
            assert_eq!(rows.iter().map(identity).collect::<Vec<_>>(), expected);
        }
        for (kind, count) in [
            (AdminCatalogKind::Queues, 3),
            (AdminCatalogKind::Topics, 2),
            (AdminCatalogKind::Subscriptions, 4),
        ] {
            assert_eq!(list(reader, &namespace, kind, 1).len(), count);
        }
    });
    let before = machine.store().snapshot().expect("image");
    drop(machine); // No reader, machine, or cloned Fjall handle survives this boundary.
    let reopened = StateMachine::new(provider.open().expect("real reopen"));
    assert_eq!(reopened.store().snapshot().expect("reopened bytes"), before);
    readonly(&reopened, &namespace, |reader| {
        assert_eq!(
            list(reader, &namespace, AdminCatalogKind::All, 2)
                .iter()
                .map(identity)
                .collect::<Vec<_>>(),
            expected
        );
    });
}
paired!(controls_memory, controls_fjall, controls);

fn exact_pages<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    readonly(&machine, &namespace, |reader| {
        for kind in [
            AdminCatalogKind::All,
            AdminCatalogKind::Queues,
            AdminCatalogKind::Topics,
            AdminCatalogKind::Subscriptions,
        ] {
            assert_eq!(
                page(reader, &namespace, kind, None, 1).expect("empty"),
                AdminCatalogPage {
                    entries: vec![],
                    continuation: None
                }
            );
        }
    });
    for entity in ["a", "b", "c"] {
        topic(&machine, &namespace, &path(entity));
    }
    queue(&machine, &namespace, &path("q"));
    for child in ["a", "b", "c"] {
        subscription(&machine, &namespace, &path("a"), &name(child));
    }
    readonly(&machine, &namespace, |reader| {
        for (kind, size) in [
            (AdminCatalogKind::Topics, 3),
            (AdminCatalogKind::Queues, 1),
            (AdminCatalogKind::Subscriptions, 3),
            (AdminCatalogKind::All, 7),
        ] {
            let exact = page(reader, &namespace, kind, None, size).expect("exact full");
            assert_eq!(exact.entries.len(), size);
            assert!(
                exact.continuation.is_none(),
                "no false continuation at exact end"
            );
        }
    });
    topic(&machine, &namespace, &path("d"));
    subscription(&machine, &namespace, &path("a"), &name("d"));
    readonly(&machine, &namespace, |reader| {
        for kind in [AdminCatalogKind::Topics, AdminCatalogKind::Subscriptions] {
            let first = page(reader, &namespace, kind, None, 3).expect("full");
            assert_eq!(first.entries.len(), 3);
            let after = first.continuation.expect("one more exists");
            let final_page = page(reader, &namespace, kind, Some(&after), 3).expect("last");
            assert_eq!(final_page.entries.len(), 1);
            assert!(final_page.continuation.is_none());
        }
    });
}
paired!(exact_pages_memory, exact_pages_fjall, exact_pages);

fn boundaries<P: StoreProvider>(provider: P) {
    let namespace = ns(&"N".repeat(domain::MAX_NAMESPACE_NAME_BYTES));
    let q = path(&"Q".repeat(domain::MAX_ENTITY_PATH_BYTES));
    let t = path(&"T".repeat(domain::MAX_ENTITY_PATH_BYTES));
    let s = name(&"S".repeat(domain::MAX_SUBSCRIPTION_NAME_CHARACTERS));
    let machine = StateMachine::new(provider.open().expect("open"));
    queue(&machine, &namespace, &q);
    topic(&machine, &namespace, &t);
    subscription(&machine, &namespace, &t, &s);
    readonly(&machine, &namespace, |reader| {
        assert!(reader.admin_queue(&namespace, &q).expect("queue").is_some());
        assert!(reader.admin_topic(&namespace, &t).expect("topic").is_some());
        assert!(
            reader
                .admin_subscription(&namespace, &t, &s)
                .expect("child")
                .is_some()
        );
        assert_eq!(list(reader, &namespace, AdminCatalogKind::All, 1).len(), 3);
    });
}
paired!(boundaries_memory, boundaries_fjall, boundaries);

fn bounded_internal_rows<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    // Canonical internal keys are not public entities. Their orphaned profiles
    // are deliberately invalid: this operation is not whole-store validation.
    let mut batch = WriteBatch::default();
    for index in 0..MAX_ADMIN_CATALOG_SCAN {
        let shadow = path(&format!("a{index:04}"))
            .dead_letter_queue()
            .expect("shadow");
        batch.push_put(keys::queue_config(&namespace, &shadow), vec![255]);
    }
    machine.store().apply(batch).expect("internal rows");
    readonly(&machine, &namespace, |reader| {
        for kind in [AdminCatalogKind::Queues, AdminCatalogKind::All] {
            let exact = page(reader, &namespace, kind, None, 1).expect("exact scan end");
            assert!(exact.entries.is_empty());
            assert!(exact.continuation.is_none());
        }
    });
    let one_more = path(&format!("a{:04}", MAX_ADMIN_CATALOG_SCAN))
        .dead_letter_queue()
        .expect("shadow");
    put(
        machine.store(),
        keys::queue_config(&namespace, &one_more),
        vec![255],
    );
    readonly(&machine, &namespace, |reader| {
        let first =
            page(reader, &namespace, AdminCatalogKind::Queues, None, 1).expect("one more raw row");
        assert!(first.entries.is_empty());
        let next = first.continuation.expect("accurate raw continuation");
        let last = page(reader, &namespace, AdminCatalogKind::Queues, Some(&next), 1)
            .expect("last raw row");
        assert!(last.entries.is_empty());
        assert!(last.continuation.is_none());
    });
    queue(&machine, &namespace, &path("z"));
    readonly(&machine, &namespace, |reader| {
        let first = page(reader, &namespace, AdminCatalogKind::Queues, None, 1).expect("bounded");
        assert!(first.entries.is_empty());
        let next = first.continuation.expect("raw work remains");
        let final_page =
            page(reader, &namespace, AdminCatalogKind::Queues, Some(&next), 1).expect("progress");
        assert_eq!(
            final_page.entries.iter().map(identity).collect::<Vec<_>>(),
            ["q:z"]
        );
        assert!(final_page.continuation.is_none());
    });
}
paired!(
    bounded_internal_memory,
    bounded_internal_fjall,
    bounded_internal_rows
);

fn mixed_kind_budget_boundaries<P: StoreProvider>(provider: P) {
    let machine = StateMachine::new(provider.open().expect("open"));
    for topics_before_boundary in [0, 2] {
        let namespace = ns(if topics_before_boundary == 0 {
            "queue-boundary"
        } else {
            "subscription-boundary"
        });
        queue(&machine, &namespace, &path("a"));
        for entity in ["t", "u"] {
            topic(&machine, &namespace, &path(entity));
        }
        subscription(&machine, &namespace, &path("t"), &name("s"));

        // The public queue and subscription each contribute two queue rows.
        // Exhaust either all 256 rows in queues, or 254 queue + two topic rows,
        // leaving the first row of the next kind as the sole lookahead.
        let padding = MAX_ADMIN_CATALOG_SCAN - 4 - topics_before_boundary;
        let mut batch = WriteBatch::default();
        for index in 0..padding {
            let shadow = path(&format!("z{index:04}"))
                .dead_letter_queue()
                .expect("padding shadow");
            batch.push_put(keys::queue_config(&namespace, &shadow), vec![255]);
        }
        machine.store().apply(batch).expect("padding rows");

        let (expected_first, expected_next, position, corrupt_key) = if topics_before_boundary == 0
        {
            let last_shadow = path(&format!("z{:04}", padding - 1))
                .dead_letter_queue()
                .expect("last queue row");
            (
                vec!["q:a"],
                vec!["t:t", "t:u", "s:t/subscriptions/s"],
                keys::queue_config(&namespace, &last_shadow),
                keys::topic_config(&namespace, &path("t")),
            )
        } else {
            (
                vec!["q:a", "t:t", "t:u"],
                vec!["s:t/subscriptions/s"],
                keys::topic_config(&namespace, &path("u")),
                keys::topic_subscription(&namespace, &path("t"), &name("s")),
            )
        };
        readonly(&machine, &namespace, |reader| {
            let first = page(
                reader,
                &namespace,
                AdminCatalogKind::All,
                None,
                MAX_ADMIN_CATALOG_PAGE,
            )
            .expect("healthy next-kind lookahead");
            assert_eq!(
                reader.store().reads.lock().expect("reads").1,
                MAX_ADMIN_CATALOG_SCAN + 1,
                "one shared raw-row budget across phases"
            );
            assert_eq!(
                first.entries.iter().map(identity).collect::<Vec<_>>(),
                expected_first
            );
            let after = first.continuation.expect("next phase remains");
            assert!(
                after.as_bytes().ends_with(&position),
                "cursor names the last consumed row, not the next-kind lookahead"
            );
            let after =
                AdminCatalogCursor::from_bytes(after.as_bytes()).expect("cursor round trip");
            let next = page(
                reader,
                &namespace,
                AdminCatalogKind::All,
                Some(&after),
                MAX_ADMIN_CATALOG_PAGE,
            )
            .expect("advance into next phase");
            assert_eq!(
                next.entries.iter().map(identity).collect::<Vec<_>>(),
                expected_next,
                "no consumed rows repeated or lookahead rows skipped"
            );
            assert!(next.continuation.is_none());
        });

        put(machine.store(), corrupt_key, vec![255]);
        readonly(&machine, &namespace, |reader| {
            assert_eq!(
                page(
                    reader,
                    &namespace,
                    AdminCatalogKind::All,
                    None,
                    MAX_ADMIN_CATALOG_PAGE,
                ),
                Err(AdminCatalogError::CorruptCatalog),
                "next-kind corruption rejects already collected entries after the full budget"
            );
            assert_eq!(
                reader.store().reads.lock().expect("reads").1,
                MAX_ADMIN_CATALOG_SCAN + 1,
                "corruption is checked in the single cross-phase lookahead"
            );
        });
    }
}
paired!(
    mixed_kind_budget_boundaries_memory,
    mixed_kind_budget_boundaries_fjall,
    mixed_kind_budget_boundaries
);

fn cursor_controls<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    for entity in ["a", "b"] {
        topic(&machine, &namespace, &path(entity));
    }
    let after = machine
        .admin_catalog_page(&namespace, AdminCatalogKind::Topics, None, 1)
        .expect("first")
        .continuation
        .expect("cursor");
    assert_eq!(
        after.validate_scope(&namespace, AdminCatalogKind::Topics),
        Ok(())
    );
    let mut malformed = vec![vec![], vec![1], vec![1, 9], vec![0; 513], vec![255, 1]];
    for change in 0..5 {
        let mut raw = after.as_bytes().to_vec();
        match change {
            0 => raw.push(0),
            1 => {
                raw.pop();
            }
            2 => raw[2] = keys::queue_config_prefix()[0],
            3 => raw[3] = b'T',
            _ => {
                let last = raw.len() - 2;
                raw[last] = b'A';
            }
        }
        malformed.push(raw);
    }
    for raw in malformed {
        assert_eq!(
            AdminCatalogCursor::from_bytes(&raw),
            Err(AdminCatalogError::InvalidCursor)
        );
    }
    readonly(&machine, &namespace, |reader| {
        for size in [0, MAX_ADMIN_CATALOG_PAGE + 1, usize::MAX] {
            assert_eq!(
                page(reader, &namespace, AdminCatalogKind::All, None, size),
                Err(AdminCatalogError::InvalidPageSize { requested: size })
            );
            assert_eq!(reader.store().reads.lock().expect("reads").0, 0);
        }
        for (foreign, kind) in [
            (ns("tenant-a"), AdminCatalogKind::Topics),
            (namespace.clone(), AdminCatalogKind::All),
            (namespace.clone(), AdminCatalogKind::Queues),
            (namespace.clone(), AdminCatalogKind::Subscriptions),
        ] {
            assert_eq!(
                after.validate_scope(&foreign, kind),
                Err(AdminCatalogError::CursorScopeMismatch)
            );
            assert_eq!(
                page(reader, &foreign, kind, Some(&after), 1),
                Err(AdminCatalogError::CursorScopeMismatch)
            );
            assert_eq!(reader.store().reads.lock().expect("reads").0, 0);
        }
    });
    delete(machine.store(), keys::topic_config(&namespace, &path("a")));
    delete(
        machine.store(),
        keys::entity_metadata(&namespace, &path("a")),
    );
    readonly(&machine, &namespace, |reader| {
        let next = page(
            reader,
            &namespace,
            AdminCatalogKind::Topics,
            Some(&after),
            1,
        )
        .expect("deleted position is valid");
        assert_eq!(
            next.entries.iter().map(identity).collect::<Vec<_>>(),
            ["t:b"]
        );
        assert!(next.continuation.is_none());
    });
}
paired!(
    cursor_controls_memory,
    cursor_controls_fjall,
    cursor_controls
);

#[derive(Clone, Copy, Debug)]
enum Damage {
    QueueValue,
    QueueNoncanonical,
    QueueOwnerMissing,
    QueueOwnerWrongKind,
    QueueOwnerZero,
    QueueOwnerRetired,
    QueueShadowOwner,
    QueueShadowMissing,
    QueueShadowWrong,
    QueueTopic,
    QueueMembership,
    QueuePrimaryMissing,
    TopicValue,
    TopicOwnerMissing,
    TopicOwnerWrongKind,
    TopicShadowQueue,
    TopicShadowTopic,
    TopicQueue,
    TopicPrimaryMissing,
    SubscriptionMembership,
    SubscriptionCase,
    SubscriptionMissing,
    SubscriptionBackingMissing,
    SubscriptionBackingWrong,
    SubscriptionBackingNoncanonical,
    SubscriptionShadowMissing,
    SubscriptionShadowWrong,
    SubscriptionOwnerMissing,
    SubscriptionOwnerWrongKind,
    SubscriptionShadowOwner,
    SubscriptionTopic,
    SubscriptionShadowTopic,
}

fn damaged_gets<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let q = path("a");
    let t = path("t");
    let n = name("s");
    let s = t.subscription(&n).expect("child");
    for damage in [
        Damage::QueueValue,
        Damage::QueueNoncanonical,
        Damage::QueueOwnerMissing,
        Damage::QueueOwnerWrongKind,
        Damage::QueueOwnerZero,
        Damage::QueueOwnerRetired,
        Damage::QueueShadowOwner,
        Damage::QueueShadowMissing,
        Damage::QueueShadowWrong,
        Damage::QueueTopic,
        Damage::QueueMembership,
        Damage::QueuePrimaryMissing,
        Damage::TopicValue,
        Damage::TopicOwnerMissing,
        Damage::TopicOwnerWrongKind,
        Damage::TopicShadowQueue,
        Damage::TopicShadowTopic,
        Damage::TopicQueue,
        Damage::TopicPrimaryMissing,
        Damage::SubscriptionMembership,
        Damage::SubscriptionCase,
        Damage::SubscriptionMissing,
        Damage::SubscriptionBackingMissing,
        Damage::SubscriptionBackingWrong,
        Damage::SubscriptionBackingNoncanonical,
        Damage::SubscriptionShadowMissing,
        Damage::SubscriptionShadowWrong,
        Damage::SubscriptionOwnerMissing,
        Damage::SubscriptionOwnerWrongKind,
        Damage::SubscriptionShadowOwner,
        Damage::SubscriptionTopic,
        Damage::SubscriptionShadowTopic,
    ] {
        let machine = StateMachine::new(provider.open().expect("open"));
        // Restore the same complete baseline between corruptions, without
        // retaining any durable handle across the next open.
        let all = machine.store().snapshot().expect("previous rows");
        let mut clean = WriteBatch::default();
        for (key, _) in all.entries() {
            clean.push_delete(key.clone());
        }
        machine.store().apply(clean).expect("fixture reset");
        queue(&machine, &namespace, &q);
        topic(&machine, &namespace, &t);
        subscription(&machine, &namespace, &t, &n);
        let qs = q.dead_letter_queue().expect("q shadow");
        let ss = s.dead_letter_queue().expect("s shadow");
        let ts = t.dead_letter_queue().expect("t shadow");
        let qkey = keys::queue_config(&namespace, &q);
        let tkey = keys::topic_config(&namespace, &t);
        let skey = keys::queue_config(&namespace, &s);
        let membership = keys::topic_subscription(&namespace, &t, &n);
        let qowner = machine
            .store()
            .get(&keys::entity_metadata(&namespace, &q))
            .expect("read")
            .expect("q owner");
        let towner = machine
            .store()
            .get(&keys::entity_metadata(&namespace, &t))
            .expect("read")
            .expect("t owner");
        let (key, value) = match damage {
            Damage::QueueValue => (
                qkey,
                Some(
                    codec::encode(&QueueConfig {
                        max_message_bytes: 0,
                        ..QueueConfig::default()
                    })
                    .expect("config"),
                ),
            ),
            Damage::QueueNoncanonical => {
                let mut value = machine.store().get(&qkey).expect("read").expect("q");
                value.push(0);
                (qkey, Some(value))
            }
            Damage::QueueOwnerMissing => (keys::entity_metadata(&namespace, &q), None),
            Damage::QueueOwnerWrongKind => (keys::entity_metadata(&namespace, &q), Some(towner)),
            Damage::QueueOwnerZero => (
                keys::entity_metadata(&namespace, &q),
                Some(codec::encode(&(0_u64, 0_u8, false)).expect("zero owner")),
            ),
            Damage::QueueOwnerRetired => (
                keys::entity_metadata(&namespace, &q),
                Some(codec::encode(&(1_u64, 0_u8, true)).expect("retired owner")),
            ),
            Damage::QueueShadowOwner => (keys::entity_metadata(&namespace, &qs), Some(qowner)),
            Damage::QueueShadowMissing => (keys::queue_config(&namespace, &qs), None),
            Damage::QueueShadowWrong => (
                keys::queue_config(&namespace, &qs),
                Some(codec::encode(&QueueConfig::default()).expect("wrong shadow")),
            ),
            Damage::QueueTopic => (
                keys::topic_config(&namespace, &q),
                Some(codec::encode(&TopicConfig::default()).expect("topic")),
            ),
            Damage::QueueMembership => (
                keys::topic_subscription(&namespace, &q, &n),
                Some(codec::encode(&s).expect("membership")),
            ),
            Damage::QueuePrimaryMissing => (qkey, None),
            Damage::TopicValue => (
                tkey,
                Some(
                    codec::encode(&TopicConfig {
                        max_message_bytes: 0,
                        ..TopicConfig::default()
                    })
                    .expect("topic"),
                ),
            ),
            Damage::TopicOwnerMissing => (keys::entity_metadata(&namespace, &t), None),
            Damage::TopicOwnerWrongKind => (keys::entity_metadata(&namespace, &t), Some(qowner)),
            Damage::TopicShadowQueue => (
                keys::queue_config(&namespace, &ts),
                Some(codec::encode(&QueueConfig::default()).expect("q")),
            ),
            Damage::TopicShadowTopic => (
                keys::topic_config(&namespace, &ts),
                Some(codec::encode(&TopicConfig::default()).expect("t")),
            ),
            Damage::TopicQueue => (
                keys::queue_config(&namespace, &t),
                Some(codec::encode(&QueueConfig::default()).expect("q")),
            ),
            Damage::TopicPrimaryMissing => (tkey, None),
            Damage::SubscriptionMembership => {
                (membership, Some(codec::encode(&q).expect("wrong member")))
            }
            Damage::SubscriptionCase => (
                membership,
                Some(codec::encode(&"T/subscriptions/S").expect("case alias")),
            ),
            Damage::SubscriptionMissing => (membership, None),
            Damage::SubscriptionBackingMissing => (skey, None),
            Damage::SubscriptionBackingWrong => (
                skey,
                Some(codec::encode(&QueueConfig::default()).expect("wrong inherited profile")),
            ),
            Damage::SubscriptionBackingNoncanonical => {
                let mut value = machine.store().get(&skey).expect("read").expect("backing");
                value.push(0);
                (skey, Some(value))
            }
            Damage::SubscriptionShadowMissing => (keys::queue_config(&namespace, &ss), None),
            Damage::SubscriptionShadowWrong => (
                keys::queue_config(&namespace, &ss),
                Some(codec::encode(&QueueConfig::default()).expect("wrong shadow")),
            ),
            Damage::SubscriptionOwnerMissing => (keys::entity_metadata(&namespace, &s), None),
            Damage::SubscriptionOwnerWrongKind => {
                (keys::entity_metadata(&namespace, &s), Some(qowner))
            }
            Damage::SubscriptionShadowOwner => {
                (keys::entity_metadata(&namespace, &ss), Some(qowner))
            }
            Damage::SubscriptionTopic => (
                keys::topic_config(&namespace, &s),
                Some(codec::encode(&TopicConfig::default()).expect("t")),
            ),
            Damage::SubscriptionShadowTopic => (
                keys::topic_config(&namespace, &ss),
                Some(codec::encode(&TopicConfig::default()).expect("t")),
            ),
        };
        if let Some(value) = value {
            put(machine.store(), key, value);
        } else {
            delete(machine.store(), key);
        }
        let check = |reader: &StateMachine<ReadOnly<P::Store>>| {
            let result = match damage {
                Damage::QueueValue
                | Damage::QueueNoncanonical
                | Damage::QueueOwnerMissing
                | Damage::QueueOwnerWrongKind
                | Damage::QueueOwnerZero
                | Damage::QueueOwnerRetired
                | Damage::QueueShadowOwner
                | Damage::QueueShadowMissing
                | Damage::QueueShadowWrong
                | Damage::QueueTopic
                | Damage::QueueMembership
                | Damage::QueuePrimaryMissing => reader.admin_queue(&namespace, &q).map(|_| ()),
                Damage::TopicValue
                | Damage::TopicOwnerMissing
                | Damage::TopicOwnerWrongKind
                | Damage::TopicShadowQueue
                | Damage::TopicShadowTopic
                | Damage::TopicQueue
                | Damage::TopicPrimaryMissing => reader.admin_topic(&namespace, &t).map(|_| ()),
                _ => reader.admin_subscription(&namespace, &t, &n).map(|_| ()),
            };
            assert_eq!(result, Err(AdminCatalogError::CorruptCatalog), "{damage:?}");
            if !matches!(
                damage,
                Damage::QueuePrimaryMissing | Damage::SubscriptionMissing
            ) {
                assert_eq!(
                    page(
                        reader,
                        &namespace,
                        AdminCatalogKind::All,
                        None,
                        MAX_ADMIN_CATALOG_PAGE
                    ),
                    Err(AdminCatalogError::CorruptCatalog),
                    "no partial page: {damage:?}"
                );
            }
        };
        readonly(&machine, &namespace, check);
        let before = machine.store().snapshot().expect("corrupt image");
        drop(machine);
        let reopened = StateMachine::new(provider.open().expect("reopen corrupt state"));
        assert_eq!(reopened.store().snapshot().expect("image survives"), before);
        readonly(&reopened, &namespace, check);
        drop(reopened);
    }
}
paired!(damaged_gets_memory, damaged_gets_fjall, damaged_gets);

fn malformed_selected_keys<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    queue(&machine, &namespace, &path("a"));
    topic(&machine, &namespace, &path("t"));
    subscription(&machine, &namespace, &path("t"), &name("s"));
    for kind in [
        AdminCatalogKind::Queues,
        AdminCatalogKind::Topics,
        AdminCatalogKind::Subscriptions,
    ] {
        let valid = match kind {
            AdminCatalogKind::Queues => keys::queue_config(&namespace, &path("z")),
            AdminCatalogKind::Topics => keys::topic_config(&namespace, &path("z")),
            _ => keys::topic_subscription(&namespace, &path("t"), &name("z")),
        };
        for change in 0..4 {
            let mut bad = valid.clone();
            match change {
                0 => bad.push(0),
                1 => {
                    let index = bad.len()
                        - if kind == AdminCatalogKind::Subscriptions {
                            1
                        } else {
                            2
                        };
                    bad[index] = b'Z';
                }
                2 => {
                    bad.pop();
                }
                _ => bad.push(255),
            }
            put(machine.store(), bad.clone(), vec![255]);
            readonly(&machine, &namespace, |reader| {
                assert_eq!(
                    page(reader, &namespace, kind, None, MAX_ADMIN_CATALOG_PAGE),
                    Err(AdminCatalogError::CorruptCatalog)
                );
                // A malformed selected lookahead must not turn a valid prefix into success.
                assert_eq!(
                    page(reader, &namespace, kind, None, 1),
                    Err(AdminCatalogError::CorruptCatalog)
                );
            });
            delete(machine.store(), bad);
        }
    }
}
paired!(
    malformed_keys_memory,
    malformed_keys_fjall,
    malformed_selected_keys
);

fn selected_not_global<P: StoreProvider>(provider: P) {
    let namespace = ns("tenant");
    let machine = StateMachine::new(provider.open().expect("open"));
    queue(&machine, &namespace, &path("a"));
    topic(&machine, &namespace, &path("t"));
    subscription(&machine, &namespace, &path("t"), &name("s"));
    put(
        machine.store(),
        keys::queue_config(&namespace, &path("t").subscription(&name("s")).expect("s")),
        vec![255],
    );
    put(machine.store(), keys::clock(), vec![255]); // Even a corrupt Clock is outside this read surface.
    readonly(&machine, &namespace, |reader| {
        assert_eq!(
            list(reader, &namespace, AdminCatalogKind::Queues, 1).len(),
            1
        );
        assert_eq!(
            list(reader, &namespace, AdminCatalogKind::Topics, 1).len(),
            1
        );
        assert_eq!(
            page(reader, &namespace, AdminCatalogKind::Subscriptions, None, 1),
            Err(AdminCatalogError::CorruptCatalog)
        );
        // All kinds must reach the selected corrupt child, even with a small page.
        let first = page(reader, &namespace, AdminCatalogKind::All, None, 1).expect("queue page");
        assert_eq!(first.entries.len(), 1);
        assert_eq!(
            page(
                reader,
                &namespace,
                AdminCatalogKind::All,
                first.continuation.as_ref(),
                1
            ),
            Err(AdminCatalogError::CorruptCatalog)
        );
    });
}
paired!(
    selected_not_global_memory,
    selected_not_global_fjall,
    selected_not_global
);

fn storage_errors<P: StoreProvider>(provider: P) {
    #[derive(Clone)]
    struct Failed<S>(S);
    impl<S: StateStore> StateStore for Failed<S> {
        fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            Err(StorageError::LockPoisoned)
        }
        fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
            panic!("no writes")
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            panic!("no snapshots")
        }
        fn scan_from(
            &self,
            _: &[u8],
            _: &[u8],
            _: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            Err(StorageError::LockPoisoned)
        }
    }
    let machine = StateMachine::new(Failed(provider.open().expect("open")));
    let expected = Err(AdminCatalogError::Storage(StorageError::LockPoisoned));
    assert_eq!(
        machine.admin_queue(&ns("tenant"), &path("q")).map(|_| ()),
        expected
    );
    assert_eq!(
        machine.admin_topic(&ns("tenant"), &path("t")).map(|_| ()),
        expected
    );
    assert_eq!(
        machine
            .admin_subscription(&ns("tenant"), &path("t"), &name("s"))
            .map(|_| ()),
        expected
    );
    assert_eq!(
        machine
            .admin_catalog_page(&ns("tenant"), AdminCatalogKind::All, None, 1)
            .map(|_| ()),
        expected
    );
}
paired!(storage_errors_memory, storage_errors_fjall, storage_errors);
