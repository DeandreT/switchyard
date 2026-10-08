//! Registry ownership and actual write-lock admission, using committed grants.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Poll,
};

use domain::{
    AcceptedSession, Command, QueueConfig, SessionId, SessionRecord, StateMachine, Timestamp,
};
use storage::{FjallStore, MemoryStore, StateStore, StoreSnapshot};

use super::*;

const LINK: &str = "shared-session-link";

enum Machine {
    Memory(StateMachine<MemoryStore>),
    Fjall(StateMachine<FjallStore>),
}

struct Grants {
    machine: Option<Machine>,
    memory: Option<MemoryStore>,
    directory: Option<tempfile::TempDir>,
    namespace: NamespaceName,
}

impl Grants {
    fn new(durable: bool) -> Self {
        let memory = (!durable).then(MemoryStore::default);
        let directory = durable.then(|| tempfile::tempdir().unwrap());
        let machine = match &directory {
            Some(directory) => Machine::Fjall(StateMachine::new(
                FjallStore::open(directory.path()).unwrap(),
            )),
            None => Machine::Memory(StateMachine::new(memory.as_ref().unwrap().clone())),
        };
        let grants = Self {
            machine: Some(machine),
            memory,
            directory,
            namespace: NamespaceName::new("tenant").unwrap(),
        };
        for name in ["orders", "invoices"] {
            assert_eq!(
                grants.apply(
                    &entity(name),
                    1_000,
                    CommandKind::CreateQueue {
                        config: QueueConfig {
                            requires_session: true,
                            ..QueueConfig::default()
                        },
                    },
                ),
                CommandOutcome::QueueCreated,
            );
        }
        grants
    }

    fn apply(&self, entity: &EntityPath, at: u64, kind: CommandKind) -> CommandOutcome {
        let command = Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        );
        match self.machine.as_ref().unwrap() {
            Machine::Memory(machine) => machine.apply(&command).unwrap(),
            Machine::Fjall(machine) => machine.apply(&command).unwrap(),
        }
    }

    fn accept(&self, entity: &EntityPath, at: u64) -> AcceptedSession {
        match self.apply(
            entity,
            at,
            CommandKind::AcceptSession {
                session_id: Some(session_id()),
                lock_duration_millis: None,
            },
        ) {
            CommandOutcome::SessionAccepted(Some(accepted)) => accepted,
            other => panic!("a named session grant committed: {other:?}"),
        }
    }

    fn snapshot(&self) -> StoreSnapshot {
        match self.machine.as_ref().unwrap() {
            Machine::Memory(machine) => machine.store().snapshot().unwrap(),
            Machine::Fjall(machine) => machine.store().snapshot().unwrap(),
        }
    }

    fn session(&self, entity: &EntityPath) -> SessionRecord {
        match self.machine.as_ref().unwrap() {
            Machine::Memory(machine) => machine
                .session(&self.namespace, entity, &session_id())
                .unwrap()
                .unwrap(),
            Machine::Fjall(machine) => machine
                .session(&self.namespace, entity, &session_id())
                .unwrap()
                .unwrap(),
        }
    }

    fn reopen_unchanged(
        &mut self,
        before: &StoreSnapshot,
        expected: &[(&EntityPath, &AcceptedSession)],
    ) {
        assert_eq!(&self.snapshot(), before);
        drop(self.machine.take());
        self.machine = Some(match &self.directory {
            Some(directory) => Machine::Fjall(StateMachine::new(
                FjallStore::open(directory.path()).unwrap(),
            )),
            None => Machine::Memory(StateMachine::new(self.memory.as_ref().unwrap().clone())),
        });
        assert_eq!(&self.snapshot(), before);
        for (entity, accepted) in expected {
            let stored = self.session(entity);
            assert_eq!(stored.lock, Some(accepted.lock));
            assert_eq!(stored.state, accepted.state);
        }
    }
}

fn entity(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}

fn session_id() -> SessionId {
    SessionId::new("shared-session").unwrap()
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn latest_cross_entity_claim_wins_even_when_old_install_is_first_in_write_lock_fifo() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let first = entity("orders");
        let second = entity("invoices");
        let old_grant = grants.accept(&first, 1_000);
        let management = ConnectionManagement::new();
        let held_write = management.sessions.write().await;
        let old_claim = management.claim_session(LINK, first.clone());
        let mut old_install =
            Box::pin(management.install_session(&old_claim, old_grant.hold(), || true));
        pending_once(old_install.as_mut()).await;

        let new_claim = management.claim_session(LINK, second.clone());
        let new_grant = grants.accept(&second, 1_000);
        // These are real first queue-local grants, not manufactured aliases.
        assert_eq!(old_grant.hold(), new_grant.hold());
        let before = grants.snapshot();
        let mut new_install =
            Box::pin(management.install_session(&new_claim, new_grant.hold(), || true));
        pending_once(new_install.as_mut()).await;
        drop(held_write);

        assert!(old_install.await.is_none());
        let owner = new_install.await.expect("the latest live claim installs");
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(owner.clone()),
        );
        drop(old_claim);
        drop(new_claim);
        assert_eq!(
            management.registered_session(LINK).await,
            Some((second.clone(), new_grant.hold())),
        );
        grants.reopen_unchanged(&before, &[(&first, &old_grant), (&second, &new_grant)]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn captured_old_owner_cannot_remove_cross_entity_replacement_with_equal_full_hold() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let first = entity("orders");
        let second = entity("invoices");
        let old_grant = grants.accept(&first, 1_000);
        let new_grant = grants.accept(&second, 1_000);
        assert_eq!(old_grant.hold(), new_grant.hold());
        let before = grants.snapshot();
        let management = ConnectionManagement::new();
        let old_claim = management.claim_session(LINK, first.clone());
        let old_owner = management
            .install_session(&old_claim, old_grant.hold(), || true)
            .await
            .unwrap();
        let new_claim = management.claim_session(LINK, second.clone());
        let new_owner = management
            .install_session(&new_claim, new_grant.hold(), || true)
            .await
            .unwrap();

        assert_ne!(old_owner, new_owner);
        management.unregister_session(&old_owner).await;
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(new_owner.clone()),
        );
        assert_eq!(
            management.registered_session(LINK).await,
            Some((second.clone(), new_grant.hold())),
        );
        management.unregister_session(&new_owner).await;
        assert_eq!(management.registered_session(LINK).await, None);
        grants.reopen_unchanged(&before, &[(&first, &old_grant), (&second, &new_grant)]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registration_identity_distinguishes_owners_even_when_the_domain_tuple_is_identical() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let entity = entity("orders");
        let grant = grants.accept(&entity, 1_000);
        let before = grants.snapshot();
        let management = ConnectionManagement::new();
        let old_claim = management.claim_session(LINK, entity.clone());
        let old_owner = management
            .install_session(&old_claim, grant.hold(), || true)
            .await
            .unwrap();
        let new_claim = management.claim_session(LINK, entity.clone());
        let new_owner = management
            .install_session(&new_claim, grant.hold(), || true)
            .await
            .unwrap();

        // Reuse one committed grant solely to isolate protocol-owner identity.
        assert_eq!(old_owner.entity, new_owner.entity);
        assert_eq!(old_owner.hold, new_owner.hold);
        assert_ne!(old_owner, new_owner);
        management.unregister_session(&old_owner).await;
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(new_owner.clone()),
        );
        management.unregister_session(&new_owner).await;
        assert_eq!(management.registered_session(LINK).await, None);
        grants.reopen_unchanged(&before, &[(&entity, &grant)]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn same_entity_reaccept_after_expiry_fences_late_install_and_old_owner_cleanup() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let entity = entity("orders");
        let old_grant = grants.accept(&entity, 1_000);
        let management = ConnectionManagement::new();
        let installed_claim = management.claim_session(LINK, entity.clone());
        let old_owner = management
            .install_session(&installed_claim, old_grant.hold(), || true)
            .await
            .unwrap();
        let held_write = management.sessions.write().await;
        let delayed_claim = management.claim_session(LINK, entity.clone());
        let mut delayed_install =
            Box::pin(management.install_session(&delayed_claim, old_grant.hold(), || true));
        pending_once(delayed_install.as_mut()).await;

        let new_claim = management.claim_session(LINK, entity.clone());
        let new_grant = grants.accept(&entity, old_grant.lock.locked_until.as_millis());
        assert_eq!(old_grant.session_id, new_grant.session_id);
        assert_ne!(old_grant.hold(), new_grant.hold());
        let before = grants.snapshot();
        let mut new_install =
            Box::pin(management.install_session(&new_claim, new_grant.hold(), || true));
        pending_once(new_install.as_mut()).await;
        drop(held_write);

        assert!(delayed_install.await.is_none());
        let new_owner = new_install.await.unwrap();
        management.unregister_session(&old_owner).await;
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(new_owner),
        );
        assert_eq!(
            management.registered_session(LINK).await,
            Some((entity.clone(), new_grant.hold())),
        );
        grants.reopen_unchanged(&before, &[(&entity, &new_grant)]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn failed_latest_claim_preserves_installed_row_without_reviving_older_pending_claim() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let first = entity("orders");
        let second = entity("invoices");
        let installed_grant = grants.accept(&first, 1_000);
        let pending_grant = grants.accept(&second, 1_000);
        let before = grants.snapshot();
        let management = ConnectionManagement::new();
        let installed_claim = management.claim_session(LINK, first.clone());
        let installed_owner = management
            .install_session(&installed_claim, installed_grant.hold(), || true)
            .await
            .unwrap();
        let held_write = management.sessions.write().await;
        let older_pending = management.claim_session(LINK, second.clone());
        let mut delayed_install =
            Box::pin(management.install_session(&older_pending, pending_grant.hold(), || true));
        pending_once(delayed_install.as_mut()).await;
        let latest_failed = management.claim_session(LINK, second.clone());
        drop(latest_failed);
        drop(held_write);

        assert!(delayed_install.await.is_none());
        drop(older_pending);
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(installed_owner),
        );
        assert_eq!(
            management.registered_session(LINK).await,
            Some((first.clone(), installed_grant.hold())),
        );
        grants.reopen_unchanged(
            &before,
            &[(&first, &installed_grant), (&second, &pending_grant)],
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retiring_older_claim_does_not_remove_latest_pending_claim() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let first = entity("orders");
        let second = entity("invoices");
        let old_grant = grants.accept(&first, 1_000);
        let new_grant = grants.accept(&second, 1_000);
        let before = grants.snapshot();
        let management = ConnectionManagement::new();
        let old_claim = management.claim_session(LINK, first.clone());
        let held_write = management.sessions.write().await;
        let new_claim = management.claim_session(LINK, second.clone());
        let mut new_install =
            Box::pin(management.install_session(&new_claim, new_grant.hold(), || true));
        pending_once(new_install.as_mut()).await;
        drop(old_claim);
        drop(held_write);

        let owner = new_install
            .await
            .expect("old retirement preserves new pending claim");
        assert_eq!(management.registered_session_owner(LINK).await, Some(owner));
        assert_eq!(
            management.registered_session(LINK).await,
            Some((second.clone(), new_grant.hold())),
        );
        grants.reopen_unchanged(&before, &[(&first, &old_grant), (&second, &new_grant)]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn owner_liveness_is_rechecked_only_after_actual_registry_write_lock_admission() {
    for durable in [false, true] {
        let mut grants = Grants::new(durable);
        let first = entity("orders");
        let second = entity("invoices");
        let installed_grant = grants.accept(&first, 1_000);
        let new_grant = grants.accept(&second, 1_000);
        let before = grants.snapshot();
        let management = ConnectionManagement::new();
        let installed_claim = management.claim_session(LINK, first.clone());
        let installed_owner = management
            .install_session(&installed_claim, installed_grant.hold(), || true)
            .await
            .unwrap();
        let held_write = management.sessions.write().await;
        let claim = management.claim_session(LINK, second.clone());
        let live = AtomicBool::new(true);
        let checks = AtomicUsize::new(0);
        let mut install = Box::pin(management.install_session(&claim, new_grant.hold(), || {
            checks.fetch_add(1, Ordering::SeqCst);
            live.load(Ordering::SeqCst)
        }));
        pending_once(install.as_mut()).await;
        assert_eq!(checks.load(Ordering::SeqCst), 0);
        live.store(false, Ordering::SeqCst);
        drop(held_write);

        assert!(install.await.is_none());
        assert_eq!(checks.load(Ordering::SeqCst), 1);
        drop(claim);
        assert_eq!(
            management.registered_session_owner(LINK).await,
            Some(installed_owner),
        );
        grants.reopen_unchanged(
            &before,
            &[(&first, &installed_grant), (&second, &new_grant)],
        );
    }
}
