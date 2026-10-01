use super::*;

pub(super) async fn coordinator_attach_is_refused_before_broker_admission<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(MAIN, 0).await?;
    peer.healthy_session().await?;
    for (index, capabilities) in [
        None,
        Some(Array::from(vec![
            Symbol::from("amqp:local-transactions"),
            Symbol::from("amqp:multi-ssns-per-txn"),
        ])),
    ]
    .into_iter()
    .enumerate()
    {
        let channel = MAIN + index as u16;
        if index != 0 {
            peer.begin(channel, 0).await?;
        }
        let before = node.snapshot()?;
        node.reset();
        peer.attach(
            channel,
            MAIN_HANDLE,
            Role::Sender,
            Some(Coordinator { capabilities }.into()),
        )
        .await?;
        peer.refused(channel).await?;
        node.unchanged_without_owner_io(&before)?;
        peer.publish(
            HEALTHY,
            HEALTHY_HANDLE,
            &format!("coordinator-sibling-{index}"),
        )
        .await?;
    }
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn transactional_transfer_never_enqueues_a_message<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(MAIN, 0).await?;
    peer.healthy_session().await?;
    let states = [
        transactional(None)?,
        transactional(Some(Outcome::Accepted(Accepted)))?,
        DeliveryState::Declared(Declared {
            txn_id: TransactionId::new([])?,
        }),
    ];
    for (index, state) in states.into_iter().enumerate() {
        let channel = MAIN + index as u16;
        if index != 0 {
            peer.begin(channel, 0).await?;
        }
        peer.ordinary(channel, MAIN_HANDLE, Role::Sender).await?;
        let before = node.snapshot()?;
        node.reset();
        peer.send(
            channel,
            Performative::Transfer(transfer(MAIN_HANDLE, Some(0), false, Some(state))),
            encode_message(&message("must-not-enqueue"))?,
        )
        .await?;
        peer.refused(channel).await?;
        node.unchanged_without_owner_io(&before)?;
        peer.publish(
            HEALTHY,
            HEALTHY_HANDLE,
            &format!("transfer-sibling-{index}"),
        )
        .await?;
    }
    // A transaction marker on a continuation must not turn an ordinary partial
    // post into a silently accepted non-transactional delivery.
    let channel = MAIN + 3;
    peer.begin(channel, 0).await?;
    peer.ordinary(channel, MAIN_HANDLE, Role::Sender).await?;
    let payload = encode_message(&message("partial-must-not-enqueue"))?;
    let middle = payload.len() / 2;
    peer.send(
        channel,
        Performative::Transfer(transfer(MAIN_HANDLE, Some(0), true, None)),
        payload[..middle].to_vec(),
    )
    .await?;
    peer.barrier(channel).await?;
    let before = node.snapshot()?;
    node.reset();
    peer.send(
        channel,
        Performative::Transfer(transfer(
            MAIN_HANDLE,
            None,
            false,
            Some(transactional(None)?),
        )),
        payload[middle..].to_vec(),
    )
    .await?;
    peer.refused(channel).await?;
    node.unchanged_without_owner_io(&before)?;
    peer.publish(HEALTHY, HEALTHY_HANDLE, "partial-sibling")
        .await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn transactional_disposition_does_not_settle_a_held_message<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    node.broker
        .handle()
        .submit(
            node.namespace.clone(),
            node.entity.clone(),
            CommandKind::Send {
                message_id: "held".into(),
                body: b"held".to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )
        .await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin(MAIN, 0).await?;
    peer.healthy_session().await?;
    peer.ordinary(MAIN, MAIN_HANDLE, Role::Receiver).await?;
    let id = peer.delivery(MAIN, MAIN_HANDLE).await?;
    let sequence = domain::SequenceNumber::new(1);
    let original = node
        .store
        .inner
        .get(&keys::message(&node.namespace, &node.entity, sequence))?
        .expect("held record");
    let held = domain::MessageRecord::decode(&original)?;
    assert!(matches!(held.state, domain::MessageState::Locked { .. }));
    let before = node.snapshot()?;
    node.reset();
    peer.send(
        MAIN,
        Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: id,
            last: None,
            settled: false,
            state: Some(transactional(Some(Outcome::Accepted(Accepted)))?),
            batchable: false,
        }),
        vec![],
    )
    .await?;
    peer.refused(MAIN).await?;
    node.unchanged_without_owner_io(&before)?;
    assert_eq!(
        node.store
            .inner
            .get(&keys::message(&node.namespace, &node.entity, sequence))?,
        Some(original)
    );
    peer.publish(HEALTHY, HEALTHY_HANDLE, "disposition-sibling")
        .await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}

pub(super) async fn transactional_flow_is_refused_before_ordinary_flow_validation<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider).await?;
    let mut peer = Peer::connect(node.address).await?;
    peer.begin_with_handle_max(MAIN, 0, 1).await?;
    peer.healthy_session().await?;
    peer.ordinary(MAIN, MAIN_HANDLE, Role::Sender).await?;
    peer.ordinary_at(MAIN, 77, Role::Sender, 1).await?;
    let before = node.snapshot()?;
    node.reset();
    let mut properties = Fields::default();
    properties.insert(
        Symbol::from("txn-id"),
        Value::Binary(TransactionId::new([7])?.into_binary()),
    );
    // These counters would be inconsistent in an ordinary Flow. Transactional
    // acquisition must be refused before updating the session or link.
    peer.send(
        MAIN,
        Performative::Flow(Flow {
            next_incoming_id: Some(u32::MAX),
            incoming_window: 0,
            next_outgoing_id: u32::MAX,
            outgoing_window: 0,
            handle: Some(MAIN_HANDLE),
            delivery_count: Some(u32::MAX),
            link_credit: Some(u32::MAX),
            properties: Some(properties.clone()),
            ..Flow::default()
        }),
        vec![],
    )
    .await?;
    peer.detached(MAIN, MAIN_HANDLE, 0).await?;
    peer.barrier(MAIN).await?;
    node.unchanged_without_owner_io(&before)?;
    peer.publish(MAIN, 77, "same-session-flow-sibling").await?;
    peer.publish(HEALTHY, HEALTHY_HANDLE, "other-session-flow-sibling")
        .await?;

    // No installed link owns a session-level transaction request, so that form
    // refuses the session rather than claiming acquisition support.
    let before = node.snapshot()?;
    node.reset();
    properties.insert(Symbol::from("txn-id"), Value::Null);
    peer.send(
        MAIN,
        Performative::Flow(Flow {
            next_incoming_id: Some(u32::MAX),
            next_outgoing_id: u32::MAX,
            properties: Some(properties),
            ..Flow::default()
        }),
        vec![],
    )
    .await?;
    peer.refused(MAIN).await?;
    node.unchanged_without_owner_io(&before)?;
    peer.publish(HEALTHY, HEALTHY_HANDLE, "sessionless-flow-sibling")
        .await?;
    peer.close().await?;
    node.stop().await;
    Ok(())
}
