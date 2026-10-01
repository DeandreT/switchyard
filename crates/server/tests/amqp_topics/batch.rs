use super::*;

fn batch(messages: &[Message]) -> TestResult<Message> {
    Ok(Message {
        body: Body::Data(
            messages
                .iter()
                .map(encode_message)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(Into::into)
                .collect(),
        ),
        ..Message::default()
    })
}

async fn topic_batch_deduplicates_once_and_rejects_late_members_atomically<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(
        provider,
        "orders",
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )
    .await?;
    let alpha = node.subscription("Alpha").await?;
    let beta = node.subscription("beta").await?;
    let mut connection = node.connect().await?;
    let mut session = connection.begin().await?;
    let mut sender = ClientSender::attach(&mut session, "batch-publisher", "orders").await?;
    let originals = [rich(0), rich(0), rich(1)];
    accepted(
        sender
            .send_with_message_format(
                batch(&originals)?,
                protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
            )
            .await?,
    );
    for (name, target) in [("alpha-batch", &alpha), ("beta-batch", &beta)] {
        let mut receiver = ClientReceiver::attach(&mut session, name, target.as_str()).await?;
        for (sequence_number, original) in [(1, &originals[0]), (3, &originals[2])] {
            let delivery = recv(&mut receiver).await?;
            content(delivery.message(), original);
            assert_eq!(sequence(delivery.message()), sequence_number);
            receiver.accept(&delivery).await?;
        }
        node.wait_len(target, 0).await?;
        receiver.close().await?;
        assert!(
            node.store
                .as_ref()
                .expect("store")
                .scan_prefix(&keys::duplicate_history_prefix(&node.namespace, target), 8)?
                .is_empty()
        );
    }
    let before = node.snapshot()?;
    let mut bad = rich(3);
    bad.properties.as_mut().expect("properties").message_id = Some("x".repeat(129).into());
    let Outcome::Rejected(rejected) = sender
        .send_with_message_format(
            batch(&[rich(2), bad])?,
            protocol_amqp::SERVICE_BUS_BATCH_MESSAGE_FORMAT,
        )
        .await?
    else {
        panic!("invalid batch accepted")
    };
    assert_eq!(
        rejected.error.expect("reason").condition.as_symbol(),
        Symbol::from("amqp:invalid-field")
    );
    assert_eq!(node.snapshot()?, before);
    accepted(sender.send(rich(2)).await?);
    for (name, target) in [
        ("alpha-after-refusal", &alpha),
        ("beta-after-refusal", &beta),
    ] {
        let mut receiver = ClientReceiver::attach(&mut session, name, target.as_str()).await?;
        let delivery = recv(&mut receiver).await?;
        content(delivery.message(), &rich(2));
        assert_eq!(sequence(delivery.message()), 4);
        receiver.accept(&delivery).await?;
        node.wait_len(target, 0).await?;
    }
    let counters: QueueCounters = codec::decode(
        &node
            .store
            .as_ref()
            .expect("store")
            .get(&keys::queue_counters(&node.namespace, &node.topic))?
            .expect("topic counters"),
    )?;
    assert_eq!(counters.next_sequence, 5);
    connection.close().await?;
    Ok(())
}

macro_rules! for_each_backend {
    ($case:ident) => {
        mod memory {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(
                    super::DEADLINE * 8,
                    super::$case(::testkit::MemoryProvider::new()),
                )
                .await?
            }
        }
        mod durable {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(
                    super::DEADLINE * 8,
                    super::$case(::testkit::DurableProvider::temporary()?),
                )
                .await?
            }
        }
    };
}

for_each_backend!(topic_batch_deduplicates_once_and_rejects_late_members_atomically);
