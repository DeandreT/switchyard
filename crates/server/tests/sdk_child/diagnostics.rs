//! Opt-in bounded observations and delegation of explicit diagnostic delay probes.
//! Ordinary diagnostic observations never drive time, consume messages or retry.

use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use domain::{CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName};
use server::BrokerHandle;
use storage::StateStore;

static LINES: AtomicUsize = AtomicUsize::new(0);
static START: OnceLock<Instant> = OnceLock::new();
static DELAY_ARMED: AtomicBool = AtomicBool::new(false);
static DELAY_RECEIVES: AtomicUsize = AtomicUsize::new(0);

pub fn enabled() -> bool {
    std::env::var_os("SWITCHYARD_SDK_DIAGNOSTICS").is_some()
}

pub fn emit(event: &str, detail: impl std::fmt::Display) {
    if !enabled() {
        return;
    }
    let line = LINES.fetch_add(1, Ordering::Relaxed);
    if line >= 1200 {
        if line == 1200 {
            println!("SDK_NATIVE_DIAGNOSTIC observation limit reached (1200)");
        }
        return;
    }
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let elapsed = START.get_or_init(Instant::now).elapsed().as_millis();
    let detail: String = detail.to_string().chars().take(2048).collect();
    println!("SDK_NATIVE_DIAGNOSTIC wall_ms={wall} elapsed_ms={elapsed} {event} {detail}");
}

#[derive(Clone)]
pub struct ObservedBroker(pub BrokerHandle);

impl ObservedBroker {
    async fn submit_observed(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
        binding: Option<EntityBinding>,
    ) -> Result<CommandOutcome, protocol_amqp::BrokerRejection> {
        let started = Instant::now();
        let control = std::env::var("SWITCHYARD_SDK_DIAG_DELAY_RECEIVE").unwrap_or_default();
        let default_subscription = entity.as_str().ends_with("/subscriptions/accounting")
            || entity.as_str().ends_with("/subscriptions/first");
        let filtered_subscription =
            entity.as_str().contains("filtered-") && entity.as_str().contains("/subscriptions/");
        let selected = (control == "scheduled-topic" && default_subscription)
            || (control == "empty-rule" && filtered_subscription);
        if selected
            && DELAY_ARMED.load(Ordering::SeqCst)
            && matches!(kind, CommandKind::Receive { .. })
        {
            let index = DELAY_RECEIVES.fetch_add(1, Ordering::SeqCst);
            if index == 0 {
                let millis: u64 = std::env::var("SWITCHYARD_SDK_DIAG_DELAY_MS")
                    .expect("controlled delay duration")
                    .parse()
                    .expect("numeric delay");
                assert!(
                    (1..=30_000).contains(&millis),
                    "diagnostic delay must be 1..30000ms"
                );
                emit(
                    "CONTROLLED-original-receive-delay",
                    format!("mode={control} entity={entity} millis={millis}; NOT qualification"),
                );
                tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
                emit(
                    "CONTROLLED-original-receive-resumed",
                    format!("entity={entity}"),
                );
            }
        }
        if enabled() {
            emit("submit", format!("entity={entity} kind={kind:?}"));
        }
        super::diagnostic_controls::scheduled(&kind);
        let arm_on_empty_rules = control == "empty-rule"
            && filtered_subscription
            && matches!(kind, CommandKind::ListRules { .. });
        let arm_on_scheduled_peek =
            control == "scheduled-topic" && matches!(kind, CommandKind::Peek { .. });
        let result = match binding {
            Some(binding) => protocol_amqp::Broker::submit_bound(&self.0, binding, kind).await,
            None => protocol_amqp::Broker::submit(&self.0, namespace, entity.clone(), kind).await,
        };
        if (arm_on_scheduled_peek
            && matches!(&result, Ok(CommandOutcome::Peeked(messages)) if messages.iter().any(|message| message.origin == domain::DeliveryOrigin::Scheduled)))
            || (arm_on_empty_rules
                && matches!(&result, Ok(CommandOutcome::RulesListed { rules }) if rules.is_empty()))
        {
            DELAY_ARMED.store(true, Ordering::SeqCst);
        }
        if enabled() {
            emit(
                "outcome",
                format!(
                    "entity={entity} took_ms={} result={result:?}",
                    started.elapsed().as_millis()
                ),
            );
        }
        result
    }
}

impl protocol_amqp::Broker for ObservedBroker {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, protocol_amqp::BrokerRejection> {
        protocol_amqp::Broker::bind_entity(&self.0, namespace, entity).await
    }

    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, protocol_amqp::BrokerRejection> {
        self.submit_observed(
            binding.namespace().clone(),
            binding.target().clone(),
            kind,
            Some(binding),
        )
        .await
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, protocol_amqp::BrokerRejection> {
        self.submit_observed(namespace, entity, kind, None).await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        protocol_amqp::Broker::deliverable(&self.0, namespace, entity)
    }
}

/// Snapshot is atomic and read-only, including while the original owner runs.
pub fn state<S: StateStore>(store: &S, phase: &str, entities: &[&str]) {
    if !enabled() {
        return;
    }
    let snapshot = store.snapshot().expect("diagnostic snapshot");
    let namespace = NamespaceName::new("tenant").unwrap();
    let clock = snapshot
        .entries()
        .iter()
        .find(|(key, _)| *key == domain::keys::clock())
        .map(|(_, value)| domain::codec::decode::<domain::Timestamp>(value));
    emit(
        "snapshot",
        format!(
            "phase={phase} rows={} persisted_clock={clock:?}",
            snapshot.entries().len()
        ),
    );
    for name in entities {
        let entity = EntityPath::new(*name).unwrap();
        for selected in [entity.clone(), entity.dead_letter_queue().unwrap()] {
            let prefix = domain::keys::message_prefix(&namespace, &selected);
            let mut count = 0;
            for (_, value) in snapshot
                .entries()
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .take(32)
            {
                let message = domain::MessageRecord::decode(value).unwrap();
                emit(
                    "message",
                    format!(
                        "phase={phase} entity={selected} id={} seq={} enqueue={} scheduled={:?} expiry={:?} delivery_count={} state={:?} dead_letter={:?}",
                        message.message_id,
                        message.sequence,
                        message.enqueued_at.as_millis(),
                        message.scheduled_enqueue_at,
                        message.expires_at,
                        message.delivery_count,
                        message.state,
                        message.dead_letter
                    ),
                );
                count += 1;
            }
            emit(
                "inventory",
                format!("phase={phase} entity={selected} selected_messages={count}"),
            );
        }
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    #[tokio::test]
    async fn observed_facade_delegates_capture_and_original_bound_submission() {
        let broker = server::Broker::spawn(server::LocalProposer::new(
            domain::StateMachine::new(storage::MemoryStore::default()),
            server::ManualClock::at(1_000),
        ));
        let observed = ObservedBroker(broker.handle());
        let namespace = NamespaceName::new("tenant").unwrap();
        let entity = EntityPath::new("bound-fixture").unwrap();
        protocol_amqp::Broker::submit(
            &observed,
            namespace.clone(),
            entity.clone(),
            CommandKind::CreateQueue {
                config: domain::QueueConfig::default(),
            },
        )
        .await
        .unwrap();
        let binding = protocol_amqp::Broker::bind_entity(&observed, namespace, entity)
            .await
            .unwrap();
        assert!(matches!(
            protocol_amqp::Broker::submit_bound(
                &observed,
                binding,
                CommandKind::SendContent {
                    content: domain::MessageContent::Raw(domain::RawMessage::default())
                },
            )
            .await,
            Ok(CommandOutcome::Sent { .. })
        ));
        drop(observed);
        drop(broker);
    }
}
