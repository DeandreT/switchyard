use amqp::{FilterSet, ReceiverSettleMode, SenderSettleMode, Source};

use super::*;

async fn first_mode_receiver(
    session: &mut ClientSession,
    name: &str,
    entity: &EntityPath,
    id: &str,
) -> TestResult<ClientReceiver> {
    let mut filter = FilterSet::default();
    filter.insert(
        Symbol::from(protocol_amqp::SESSION_FILTER),
        Value::String(id.into()),
    );
    Ok(timeout(
        DEADLINE,
        ClientReceiver::builder()
            .name(name)
            .source(
                Source::builder()
                    .address(entity.as_str())
                    .filter(filter)
                    .build(),
            )
            .sender_settle_mode(SenderSettleMode::Unsettled)
            .receiver_settle_mode(ReceiverSettleMode::First)
            .attach(session),
    )
    .await??)
}

async fn at_stage<T, E: std::fmt::Display>(
    stage: &str,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> TestResult<T> {
    operation
        .await
        .map_err(|error| std::io::Error::other(format!("{stage}: {error}")).into())
}

async fn accept_then_immediate_close_releases_only_its_session<P: StoreProvider>(
    provider: P,
    subscription: bool,
) -> TestResult {
    let node = Node::start_for_peek(provider, !subscription).await?;
    let entity = if subscription {
        node.alpha.clone()
    } else {
        EntityPath::new("Sessions")?
    };
    let publishing_entity = if subscription { &node.topic } else { &entity };
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(&mut session, "close-publisher", publishing_entity.as_str()),
    )
    .await??;
    let b = first_mode_receiver(&mut session, "untouched-B", &entity, "B").await?;
    node.wait_waiter_count(&entity, 1).await?;
    let b_hold = node.session(&entity, "B")?.lock.expect("B hold");

    for iteration in 0..4 {
        let stage = format!("first settlement iteration {iteration}");
        let mut a = first_mode_receiver(
            &mut session,
            &format!("closing-A-{iteration}"),
            &entity,
            "A",
        )
        .await?;
        node.wait_waiter_count(&entity, 2).await?;
        let a_hold = node.session(&entity, "A")?.lock.expect("A hold");
        assert!(a_hold.locked_until > Timestamp::from_millis(1_000));
        accepted(
            timeout(
                DEADLINE,
                sender.send(message(&format!("accepted-{iteration}"), Some("A"))),
            )
            .await??,
        );
        let delivery = at_stage(&format!("{stage} receive"), recv(&mut a)).await?;
        let number = sequence(delivery.message());
        assert_eq!(number, iteration + 1);
        assert_eq!(group(delivery.message()), Some("A"));
        let record = node.record(&entity, number)?.expect("locked message");
        assert!(matches!(record.state, domain::MessageState::Locked { .. }));
        assert_eq!(record.delivery_count, 1);

        // First-mode accept confirms the local Disposition flush, not the
        // broker commit. Closing immediately is a valid peer lifecycle.
        at_stage(
            &format!("{stage} accept"),
            timeout(DEADLINE, a.accept(&delivery)),
        )
        .await??;
        at_stage(&format!("{stage} close"), timeout(DEADLINE, a.close())).await??;
        at_stage(
            &format!("{stage} removal"),
            node.wait_removed(&entity, number),
        )
        .await?;
        at_stage(
            &format!("{stage} release"),
            node.wait_released(&entity, "A"),
        )
        .await?;
        assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));
        assert!(node.record(&entity, number)?.is_none());
    }
    timeout(DEADLINE, b.close()).await??;
    node.wait_released(&entity, "B").await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn close_unsettled_releases_hold_without_changing_message_lock<P: StoreProvider>(
    provider: P,
    subscription: bool,
) -> TestResult {
    let node = Node::start_for_peek(provider, !subscription).await?;
    let entity = if subscription {
        node.alpha.clone()
    } else {
        EntityPath::new("Sessions")?
    };
    let publishing_entity = if subscription { &node.topic } else { &entity };
    let mut connection = node.connect().await?;
    let mut session = timeout(DEADLINE, connection.begin()).await??;
    let mut sender = timeout(
        DEADLINE,
        ClientSender::attach(
            &mut session,
            "unsettled-publisher",
            publishing_entity.as_str(),
        ),
    )
    .await??;
    let b = first_mode_receiver(&mut session, "untouched-B", &entity, "B").await?;
    node.wait_waiter_count(&entity, 1).await?;
    let b_hold = node.session(&entity, "B")?.lock.expect("B hold");

    for iteration in 0..4 {
        let stage = format!("unsettled iteration {iteration}");
        let mut a = first_mode_receiver(
            &mut session,
            &format!("unsettled-A-{iteration}"),
            &entity,
            "A",
        )
        .await?;
        node.wait_waiter_count(&entity, 2).await?;
        let old_hold = node.session(&entity, "A")?.lock.expect("A hold");
        assert!(old_hold.locked_until > Timestamp::from_millis(1_000));
        accepted(
            timeout(
                DEADLINE,
                sender.send(message(&format!("unsettled-{iteration}"), Some("A"))),
            )
            .await??,
        );
        let delivery = at_stage(&format!("{stage} receive"), recv(&mut a)).await?;
        let number = sequence(delivery.message());
        assert_eq!(number, iteration + 1);
        let before = node.record(&entity, number)?.expect("locked message");
        assert!(matches!(&before.state, domain::MessageState::Locked { .. }));
        assert_eq!(before.delivery_count, 1);
        at_stage(&format!("{stage} close"), timeout(DEADLINE, a.close())).await??;
        at_stage(
            &format!("{stage} release"),
            node.wait_released(&entity, "A"),
        )
        .await?;
        assert_eq!(node.record(&entity, number)?, Some(before.clone()));
        assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));

        let pending_before = node.snapshot()?;
        let mut pending = first_mode_receiver(
            &mut session,
            &format!("pending-A-{iteration}"),
            &entity,
            "A",
        )
        .await?;
        assert!(pending.source().is_none());
        assert!(matches!(
            timeout(DEADLINE, pending.recv()).await?,
            Err(amqp::EngineError::RemoteDetached)
        ));
        assert_eq!(node.snapshot()?, pending_before);
        assert_eq!(node.record(&entity, number)?, Some(before.clone()));
        let domain::MessageState::Locked { token, .. } = &before.state else {
            unreachable!()
        };
        assert_eq!(
            node.submit_entity(
                &entity,
                CommandKind::Settle {
                    sequence: SequenceNumber::new(number),
                    lock_token: *token,
                    disposition: domain::SettlementDisposition::Complete,
                    properties_to_modify: Default::default(),
                }
            )
            .await?,
            CommandOutcome::Completed
        );
        assert!(node.record(&entity, number)?.is_none());
        assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));

        let replacement = first_mode_receiver(
            &mut session,
            &format!("replacement-A-{iteration}"),
            &entity,
            "A",
        )
        .await?;
        assert_eq!(granted(&replacement), Some("A"));
        let new_hold = node.session(&entity, "A")?.lock.expect("fresh A hold");
        assert_ne!(new_hold.token, old_hold.token);
        // The clock never moves, so this is release/reacquisition, not expiry.
        assert_eq!(new_hold.locked_until, old_hold.locked_until);
        at_stage(
            &format!("{stage} replacement idle"),
            node.wait_waiter_count(&entity, 2),
        )
        .await?;
        assert!(node.record(&entity, number)?.is_none());
        assert_eq!(node.session(&entity, "B")?.lock, Some(b_hold));
        timeout(DEADLINE, replacement.close()).await??;
        node.wait_released(&entity, "A").await?;
    }
    timeout(DEADLINE, b.close()).await??;
    node.wait_released(&entity, "B").await?;
    timeout(DEADLINE, connection.close()).await??;
    Ok(())
}

async fn required_queue_accept_then_close_releases_hold<P: StoreProvider>(
    provider: P,
) -> TestResult {
    accept_then_immediate_close_releases_only_its_session(provider, false).await
}

async fn required_subscription_accept_then_close_releases_hold<P: StoreProvider>(
    provider: P,
) -> TestResult {
    accept_then_immediate_close_releases_only_its_session(provider, true).await
}

async fn required_queue_unsettled_close_preserves_message_lock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    close_unsettled_releases_hold_without_changing_message_lock(provider, false).await
}

async fn required_subscription_unsettled_close_preserves_message_lock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    close_unsettled_releases_hold_without_changing_message_lock(provider, true).await
}

for_each_backend!(
    required_queue_accept_then_close_releases_hold,
    required_subscription_accept_then_close_releases_hold,
    required_queue_unsettled_close_preserves_message_lock,
    required_subscription_unsettled_close_preserves_message_lock,
);
