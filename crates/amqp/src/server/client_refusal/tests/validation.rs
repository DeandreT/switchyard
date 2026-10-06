use super::fixture::*;
use super::*;

#[tokio::test]
async fn null_terminus_does_not_bypass_role_count_or_recovery_validation() {
    mismatched_buffered_count_is_rejected_before_parking();
    for case in 0..5 {
        let mut fixture = Fixture::new().await;
        let observed = caught(async {
            let role = if case == 1 {
                Role::Receiver
            } else {
                Role::Sender
            };
            let caller = fixture.start(role.clone(), "invalid");
            let request = fixture.request().await;
            let mut response = request.response(None, None);
            response.handle = 17;
            match case {
                0 => response.role = role,
                1 => response.initial_delivery_count = None,
                2 => response.incomplete_unsettled = true,
                3 => {
                    response.source = Some(crate::Source {
                        outcomes: Some(vec![crate::Symbol::from("amqp:declared:list")].into()),
                        ..crate::Source::default()
                    });
                }
                _ => {
                    let mut source = crate::Source::new("entity");
                    source.default_outcome = Some(crate::DeliveryState::Received {
                        section_number: 0,
                        section_offset: 0,
                    });
                    response.source = Some(source);
                }
            }
            if case == 4 {
                fixture.write_nonterminal_source_default(response).await;
                fixture.complete(caller).await;
                assert!(fixture.failed(caller));
                let error = bounded(read_frame(&mut fixture.peer))
                    .await
                    .expect_err("malformed source stops the original connection");
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
                fixture.no_activity(request.handle).await;
                return;
            }
            fixture
                .write(Performative::Attach(Box::new(response)))
                .await;
            let frame = fixture.read().await;
            if case == 2 {
                assert!(matches!(
                    frame,
                    Frame::Amqp {
                        performative: Some(Performative::Detach(Detach { error: Some(_), .. })),
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    frame,
                    Frame::Amqp {
                        performative: Some(Performative::End(End { error: Some(_), .. })),
                        ..
                    }
                ));
            }
            fixture.complete(caller).await;
            assert!(fixture.failed(caller));
            if case == 2 {
                fixture
                    .write(Performative::Detach(Detach {
                        handle: 17,
                        closed: true,
                        error: None,
                    }))
                    .await;
                fixture.barrier().await;
                let mut historical = request.response(None, None);
                historical.handle = 99;
                historical.unsettled = Some(crate::OrderedMap::default());
                fixture
                    .write(Performative::Attach(Box::new(historical)))
                    .await;
                assert!(matches!(
                    fixture.read().await,
                    Frame::Amqp {
                        performative: Some(Performative::End(End { error: Some(_), .. })),
                        ..
                    }
                ));
            }
            fixture.no_activity(request.handle).await;
        })
        .await;
        fixture.finish().await;
        fixture.joined();
        rethrow(observed);
    }
    let mut fixture = Fixture::new().await;
    let observed = caught(async {
        let first = fixture.start(Role::Sender, "first");
        let first_request = fixture.request().await;
        fixture.null_response(&first_request, 17).await;
        fixture.barrier().await;
        let second = fixture.start(Role::Sender, "second");
        let request = fixture.request().await;
        fixture.null_response(&request, 17).await;
        assert!(matches!(
            fixture.read().await,
            Frame::Amqp {
                performative: Some(Performative::Close(Close { error: Some(_), .. })),
                ..
            }
        ));
        fixture.complete(first).await;
        fixture.complete(second).await;
        assert!(fixture.failed(first) && fixture.failed(second));
        fixture.no_activity(first_request.handle).await;
        fixture.no_activity(request.handle).await;
    })
    .await;
    fixture.finish().await;
    fixture.joined();
    rethrow(observed);
}

#[tokio::test]
async fn optional_opposite_terminus_and_positive_session_echo_remain_unchanged() {
    for role in [Role::Sender, Role::Receiver] {
        let mut fixture = Fixture::new().await;
        let observed = caught(async {
            let caller = fixture.start(role.clone(), "positive");
            let request = fixture.request().await;
            let mut source = crate::Source::default();
            let mut filter = crate::FilterSet::default();
            filter.insert("com.microsoft:session-filter".into(), serde_amqp::Value::String("session-A".into()));
            source.filter = Some(filter);
            let mut response = if role == Role::Sender {
                request.response(None, Some(crate::Target::default().into()))
            } else { request.response(Some(source.clone()), None) };
            response.handle = 17;
            fixture.write(Performative::Attach(Box::new(response))).await;
            fixture.complete(caller).await;
            match fixture.endpoint(caller) {
                Endpoint::Sender(sender) => assert!(!sender.identity.is_retired()),
                Endpoint::Receiver(receiver) => {
                    assert!(!receiver.identity.is_retired());
                    assert_eq!(receiver.source(), &Some(source));
                }
            }
            if role == Role::Receiver {
                assert!(matches!(fixture.read().await, Frame::Amqp {
                    performative: Some(Performative::Flow(Flow { handle: Some(handle), link_credit: Some(credit), .. })), ..
                } if handle == request.handle && credit > 0));
            }
            fixture.detach(&request, 17).await;
            assert!(!is_refusal(&request.response(
                Some(crate::Source::default()), Some(crate::Target::default().into()))));
        }).await;
        fixture.finish().await;
        fixture.joined();
        rethrow(observed);
    }
}
