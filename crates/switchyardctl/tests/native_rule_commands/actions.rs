use domain::{
    CommandKind, CommandOutcome, EntityPath, MessageBody, MessageEnvelope, MessageProperties,
    MessageValue, NamespaceName, ReceiveMode, SequenceNumber, StateMachine,
};
use storage::{FjallStore, MemoryStore};

use super::*;

#[path = "actions/fixture.rs"]
mod fixture;
#[path = "actions/legacy.rs"]
pub(super) mod legacy;
#[path = "actions/validation.rs"]
mod validation;

use fixture::ActionNode;

const FIRST: &str = " /* retained source */ REMOVE user.[colour]; REMOVE [RuleName]; ";
const SECOND: &str = "\nREMOVE [audit]; REMOVE [missing]; REMOVE [RuleName];\n";

fn message() -> MessageEnvelope {
    MessageEnvelope {
        properties: MessageProperties {
            subject: Some("preserved".into()),
            content_type: Some("text/plain".into()),
            ..Default::default()
        },
        application_properties: [
            ("colour".into(), MessageValue::String("red".into())),
            ("audit".into(), MessageValue::String("lower".into())),
            ("Audit".into(), MessageValue::String("capital".into())),
            (
                "RuleName".into(),
                MessageValue::String("producer-name".into()),
            ),
            ("nullable".into(), MessageValue::Null),
        ]
        .into(),
        body: MessageBody::Data(vec![b"retained body".to_vec()]),
        ..Default::default()
    }
}

fn expected(name: Option<&str>) -> MessageEnvelope {
    let mut content = message();
    if let Some(name) = name {
        content.application_properties.remove("RuleName");
        content
            .application_properties
            .remove(if name == "a-remove" {
                "colour"
            } else {
                "audit"
            });
        content
            .application_properties
            .insert("RuleName".into(), MessageValue::String(name.into()));
    }
    content
}

fn check_records<S: StateStore>(store: &S, completed: bool) -> TestResult {
    let machine = StateMachine::new(store.clone());
    let namespace = NamespaceName::new("tenant")?;
    let alpha = EntityPath::new(CHILD)?;
    for (sequence, name) in [(1, None), (2, Some("a-remove")), (3, Some("b-remove"))] {
        let record = machine.message(&namespace, &alpha, SequenceNumber::new(sequence))?;
        if completed && sequence == 2 {
            assert!(record.is_none());
            continue;
        }
        let record = record.expect("independent copy");
        assert_eq!(record.message_id, "cli-action-message");
        assert_eq!(record.body, b"retained body");
        assert_eq!(*record.envelope.expect("envelope"), expected(name));
    }
    let beta = EntityPath::new("Orders/subscriptions/Beta")?;
    let record = machine
        .message(&namespace, &beta, SequenceNumber::new(1))?
        .expect("unaltered sibling");
    assert_eq!(*record.envelope.expect("envelope"), message());
    assert!(
        machine
            .message(&namespace, &alpha, SequenceNumber::new(4))?
            .is_none()
    );
    Ok(())
}

async fn workflow<S: StateStore>(node: &ActionNode<S>) -> TestResult<Value> {
    node.json(&["topic", "create", "Orders"]).await?;
    for name in ["Alpha", "Beta"] {
        node.json(&["subscription", "create", "Orders", name])
            .await?;
    }
    let plain = node.file("plain.json", &json!({"type":"true"}))?;
    node.json(&[
        "rule",
        "create",
        "Orders",
        "Alpha",
        "overlap",
        "--filter-file",
        &plain,
    ])
    .await?;
    for (name, source, filter) in [
        (
            "a-remove",
            FIRST,
            json!({"type":"correlation","properties":[{"name":"colour","value":{"type":"string","value":"red"}}]}),
        ),
        (
            "b-remove",
            SECOND,
            json!({"type":"sql","expression":"colour = 'red'"}),
        ),
    ] {
        let filter_file = node.file(&format!("{name}-filter.json"), &filter)?;
        let action_file = node.file(
            &format!("{name}-action.json"),
            &json!({"type":"sql","expression":source}),
        )?;
        assert_eq!(
            node.json(&[
                "rule",
                "create",
                "Orders",
                "Alpha",
                name,
                "--filter-file",
                &filter_file,
                "--action-file",
                &action_file
            ])
            .await?,
            json!({"namespace":"tenant","subscription_path":CHILD,"name":name,"completed":true})
        );
        let output = node.json(&["rule", "get", "Orders", "Alpha", name]).await?;
        assert_eq!(
            output["action"],
            json!({"type":"sql","expression":source,"semantic_version":1})
        );
    }
    let before = node.store().snapshot()?;
    let applied = node.handle().last_applied_blocking()?;
    node.clock.set(0);
    let persisted = node.json(&["rule", "list", "Orders", "Alpha"]).await?;
    let rules = persisted["rules"].as_array().expect("rules");
    assert_eq!(
        rules
            .iter()
            .map(|rule| rule["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["$Default", "a-remove", "b-remove", "overlap"]
    );
    assert!(rules[0].get("action").is_none());
    assert!(rules[3].get("action").is_none());
    assert_eq!(node.store().snapshot()?, before);
    assert_eq!(node.handle().last_applied_blocking()?, applied);
    node.clock.set(10_000);

    let namespace = NamespaceName::new("tenant")?;
    timeout(
        DEADLINE,
        node.handle().submit(
            namespace.clone(),
            EntityPath::new("Orders")?,
            CommandKind::SendEnvelope {
                message_id: "cli-action-message".into(),
                body: b"retained body".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                envelope: Box::new(message()),
            },
        ),
    )
    .await??;
    check_records(node.store(), false)?;
    let alpha = EntityPath::new(CHILD)?;
    let mut held = Vec::new();
    for _ in 0..3 {
        let result = timeout(
            DEADLINE,
            node.handle().submit(
                namespace.clone(),
                alpha.clone(),
                CommandKind::Receive {
                    mode: ReceiveMode::PeekLock,
                    lock_duration_millis: None,
                    session: None,
                },
            ),
        )
        .await??;
        let CommandOutcome::Received(Some(delivery)) = result else {
            panic!("held copy");
        };
        held.push(delivery);
    }
    assert_eq!(
        held.iter()
            .map(|delivery| delivery.sequence.as_u64())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    timeout(
        DEADLINE,
        node.handle().submit(
            namespace,
            alpha,
            CommandKind::Complete {
                sequence: held[1].sequence,
                lock_token: held[1].lock.as_ref().expect("held lock").token,
            },
        ),
    )
    .await??;
    check_records(node.store(), true)?;
    Ok(persisted)
}

pub(super) async fn round_trip() -> TestResult {
    let memory = ActionNode::start(MemoryStore::default()).await?;
    workflow(&memory).await?;
    let memory = memory.stop().await?;
    check_records(&memory, true)?;

    let directory = TempDir::new()?;
    let path = directory.path().join("store");
    let durable = ActionNode::start(FjallStore::open(&path)?).await?;
    let persisted = workflow(&durable).await?;
    let before = durable.store().snapshot()?;
    drop(durable.stop().await?);
    let reopened = ActionNode::start(FjallStore::open(&path)?).await?;
    reopened.clock.set(0);
    assert_eq!(
        reopened.json(&["rule", "list", "Orders", "Alpha"]).await?,
        persisted
    );
    assert_eq!(reopened.store().snapshot()?, before);
    check_records(reopened.store(), true)?;
    drop(reopened.stop().await?);
    Ok(())
}

pub(super) async fn validation() -> TestResult {
    validation::server_statuses().await?;
    validation::local_inputs().await
}
