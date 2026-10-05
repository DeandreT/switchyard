use super::*;
use crate::{
    Attach, Begin, Coordinator, Frame, Performative, ReceiverSettleMode, Role as AmqpRole,
    SenderSettleMode, Source, read_frame, write_frame,
};

type Failure = Box<dyn std::any::Any + Send>;

async fn probe(
    connection: &mut crate::ServerConnection,
    peer: &mut tokio::io::DuplexStream,
    count: Option<u32>,
    refusal: Option<&str>,
) -> Result<bool, Failure> {
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 9,
            performative: Some(Performative::Begin(Begin::default())),
            payload: Vec::new(),
        },
    )
    .await
    .map_err(|error| Box::new(error) as Failure)?;
    let Some(incoming) = connection.next_incoming_session().await else {
        return Ok(false);
    };
    let pair = tokio::join!(connection.accept_session(incoming), read_frame(peer));
    let (mut session, frame) = match pair {
        (Ok(session), Ok(frame)) => (session, frame),
        original => return Err(Box::new(original)),
    };
    let begin_matches = matches!(
        frame,
        Frame::Amqp {
            performative: Some(Performative::Begin(Begin {
                remote_channel: Some(9),
                ..
            })),
            ..
        }
    );
    let attach = Attach {
        name: "scoped-native-policy".to_owned(),
        handle: 0,
        role: AmqpRole::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("control")),
        target: Some(Coordinator::default().into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: count,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    };
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 9,
            performative: Some(Performative::Attach(Box::new(attach))),
            payload: Vec::new(),
        },
    )
    .await
    .map_err(|error| Box::new(error) as Failure)?;
    if let Some(expected) = refusal {
        let mut condition_matches = false;
        for _ in 0..16 {
            let frame = read_frame(peer)
                .await
                .map_err(|error| Box::new(error) as Failure)?;
            if let Frame::Amqp {
                performative: Some(Performative::End(end)),
                ..
            } = frame
            {
                condition_matches = end
                    .error
                    .as_ref()
                    .is_some_and(|error| error.condition.as_symbol().as_str() == expected);
                break;
            }
        }
        let no_approval = session.next_incoming_attach().await.is_none();
        Ok(begin_matches && condition_matches && no_approval)
    } else {
        let incoming = session.next_incoming_attach().await;
        Ok(begin_matches
            && incoming.as_ref().is_some_and(|incoming| {
                incoming.initial_delivery_count == count
                    && matches!(
                        incoming.approval().kind(),
                        crate::server::native_transactions::NativeAttachKind::Coordinator(profile)
                            if profile.defaults_initial_delivery_count() == count.is_none()
                    )
            }))
    }
}

#[tokio::test]
async fn public_three_admission_methods_keep_real_coordinator_policies() -> TestResult {
    for (mode, count, refusal) in [
        (Mode::Ordinary, Some(0), Some("amqp:not-implemented")),
        (Mode::Ordinary, None, Some("amqp:not-implemented")),
        (Mode::Posting, None, Some("amqp:invalid-field")),
        (Mode::Posting, Some(0), None),
        (Mode::WorkDefaults, None, None),
        (Mode::WorkDefaults, Some(37), None),
    ] {
        let (io, mut peer, _) = fixture::transport();
        let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
        let mut setup = open(acceptor, io, &mut peer, mode).await;
        let probed = tokio::time::timeout(Duration::from_secs(2), async {
            match setup.connection.as_mut() {
                Some(connection) => probe(connection, &mut peer, count, refusal).await,
                None => Err(Box::new(()) as Failure),
            }
        })
        .await;
        drop(setup.connection.take());
        let report = owner.finish().await.expect("actual report");
        drop(peer);
        setup.checked()?;
        let verified = match probed {
            Ok(Ok(verified)) => verified,
            original => {
                drop(original);
                return Err(
                    io::Error::other("scoped native policy observation did not finish").into(),
                );
            }
        };
        assert!(verified);
        assert!(report.actor().is_some_and(|result| result.is_ok()));
    }
    Ok(())
}
