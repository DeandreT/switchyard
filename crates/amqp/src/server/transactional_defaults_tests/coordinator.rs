use super::*;

#[tokio::test]
async fn missing_coordinator_count_is_refused_before_approval_by_every_strict_profile() {
    for (policy, condition) in [
        (Policy::Disabled, "amqp:not-implemented"),
        (Policy::Posting, "amqp:invalid-field"),
        (Policy::Work, "amqp:invalid-field"),
    ] {
        let mut fixture = Fixture::new(policy).await;
        let mut session = fixture.session().await;
        fixture
            .send(
                Performative::Attach(Box::new(coordinator_request(None))),
                Vec::new(),
            )
            .await;
        let (channel, frame) = fixture.control().await;
        assert_eq!(channel, fixture.local());
        let Performative::End(end) = frame else {
            panic!("strict coordinator admission refusal");
        };
        assert_eq!(
            end.error
                .expect("typed refusal")
                .condition
                .as_symbol()
                .as_str(),
            condition
        );
        assert!(bounded(session.next_incoming_attach()).await.is_none());
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn default_profile_preserves_original_count_and_uses_exact_effective_credit() {
    for count in [None, Some(37)] {
        let mut fixture = Fixture::new(Policy::Defaults).await;
        let mut session = fixture.session().await;
        let incoming = fixture
            .incoming(&mut session, coordinator_request(count))
            .await;
        assert_eq!(incoming.initial_delivery_count, count);
        let native_transactions::NativeAttachKind::Coordinator(profile) =
            incoming.approval().kind()
        else {
            panic!("actor-approved coordinator profile");
        };
        assert_eq!(profile.defaults_initial_delivery_count(), count.is_none());
        let (coordinator, response) = bounded(async {
            tokio::join!(
                session.accept_coordinator(incoming, 4096),
                fixture.attached()
            )
        })
        .await;
        let mut coordinator = coordinator.expect("approved coordinator");
        assert_eq!(response.role, Role::Receiver);
        assert!(response.initial_delivery_count.is_none());
        assert!(
            response
                .target
                .as_ref()
                .and_then(TargetTerminus::as_coordinator)
                .is_some()
        );
        let credit = fixture.credit(response.handle).await;
        assert_eq!(credit.delivery_count, Some(count.unwrap_or(0)));
        fixture.declare(&mut coordinator).await;
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn coordinator_count_nullness_mutation_cannot_launder_immutable_approval() {
    for count in [None, Some(0)] {
        let mut fixture = Fixture::new(Policy::Defaults).await;
        let mut session = fixture.session().await;
        let incoming = fixture
            .incoming(&mut session, coordinator_request(count))
            .await;
        let mut changed = incoming.clone();
        changed.initial_delivery_count = if count.is_none() { Some(0) } else { None };
        assert!(
            bounded(session.accept_coordinator(changed, 4096))
                .await
                .is_err()
        );
        fixture.barrier().await;
        let (coordinator, response) = bounded(async {
            tokio::join!(
                session.accept_coordinator(incoming, 4096),
                fixture.attached()
            )
        })
        .await;
        assert!(
            coordinator
                .expect("original remains usable")
                .controller_identity()
                .is_active()
        );
        assert_eq!(
            fixture.credit(response.handle).await.delivery_count,
            Some(0)
        );
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn default_profile_does_not_default_missing_count_for_an_ordinary_producer() {
    let mut fixture = Fixture::new(Policy::Defaults).await;
    let mut session = fixture.session().await;
    let mut request = request(Role::Sender);
    request.initial_delivery_count = None;
    let incoming = fixture.incoming(&mut session, request).await;
    assert!(matches!(
        incoming.approval().kind(),
        native_transactions::NativeAttachKind::Ordinary
    ));
    assert!(
        bounded(session.accept_transactional_receiver(incoming.clone(), 4096))
            .await
            .is_err()
    );
    fixture.barrier().await;
    let mut changed = incoming;
    changed.initial_delivery_count = Some(0);
    let (receiver, response) = bounded(async {
        tokio::join!(
            session.accept_transactional_receiver(changed, 4096),
            fixture.attached()
        )
    })
    .await;
    assert!(
        receiver
            .expect("existing ordinary caller normalization")
            .receiver_identity()
            .is_active()
    );
    assert_eq!(
        fixture.credit(response.handle).await.delivery_count,
        Some(0)
    );
    fixture.shutdown().await;
}
