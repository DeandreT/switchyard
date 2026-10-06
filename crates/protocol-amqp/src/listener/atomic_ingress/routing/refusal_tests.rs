use std::time::Duration;

use amqp::{
    Attach, Begin, Close, ConnectionOptions, Detach, Frame, Open, Performative, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, ServerConnection, Source, Target, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{io::DuplexStream, time::timeout};

use super::{AmqpError, error_for, refuse};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn send(peer: &mut DuplexStream, channel: u16, performative: Performative) -> TestResult {
    write_frame(
        peer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload: Vec::new(),
        },
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn ordinary_refusal_keeps_both_termini_null_and_preserves_authorization_error() -> TestResult
{
    timeout(Duration::from_secs(5), async {
        let (server, mut peer) = tokio::io::duplex(16_384);
        let opening = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
            assert_eq!(read_protocol_header(&mut peer).await?, ProtocolHeader::AMQP);
            send(&mut peer, 0, Performative::Open(Open::new("refusal-peer"))).await?;
            assert!(matches!(
                read_frame(&mut peer).await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (connection, opened) = tokio::join!(
            ServerConnection::accept_with_transactional_work_defaults(
                server,
                "refusal-server",
                None,
                ConnectionOptions::default()
            ),
            opening,
        );
        opened?;
        let mut connection = connection?;
        send(
            &mut peer,
            7,
            Performative::Begin(Begin {
                remote_channel: None,
                next_outgoing_id: 0,
                incoming_window: 1_000,
                outgoing_window: 1_000,
                handle_max: 32,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            }),
        )
        .await?;
        let incoming = connection
            .next_incoming_session()
            .await
            .ok_or("session missing")?;
        let mut session = connection.accept_session(incoming).await?;
        let Frame::Amqp {
            channel,
            performative: Some(Performative::Begin(begin)),
            payload,
        } = read_frame(&mut peer).await?
        else {
            return Err("Begin missing".into());
        };
        assert_eq!(begin.remote_channel, Some(7));
        assert!(payload.is_empty());

        for (handle, role) in [(11, Role::Receiver), (13, Role::Sender)] {
            let name = format!("denied-{handle}");
            send(
                &mut peer,
                7,
                Performative::Attach(Box::new(Attach {
                    name: name.clone(),
                    handle,
                    role: role.clone(),
                    snd_settle_mode: SenderSettleMode::Mixed,
                    rcv_settle_mode: ReceiverSettleMode::Second,
                    source: Some(Source::new("orders")),
                    target: Some(Target::new("orders").into()),
                    unsettled: None,
                    incomplete_unsettled: false,
                    initial_delivery_count: (role == Role::Sender).then_some(0),
                    max_message_size: None,
                    offered_capabilities: None,
                    desired_capabilities: None,
                    properties: None,
                })),
            )
            .await?;
            let incoming = session
                .next_incoming_attach()
                .await
                .ok_or("Attach missing")?;
            let denied = error_for(AmqpError::UnauthorizedAccess, "permission denied".into());
            refuse(&session, incoming, denied.clone()).await?;
            let Frame::Amqp {
                channel: reply_channel,
                performative: Some(Performative::Attach(reply)),
                payload,
            } = read_frame(&mut peer).await?
            else {
                return Err("refusal Attach missing".into());
            };
            assert_eq!(reply_channel, channel);
            assert_eq!(reply.name, name);
            assert_eq!(reply.role, role.opposite());
            assert!(reply.source.is_none() && reply.target.is_none());
            assert!(payload.is_empty());
            let Frame::Amqp {
                channel: reply_channel,
                performative: Some(Performative::Detach(detach)),
                payload,
            } = read_frame(&mut peer).await?
            else {
                return Err("error Detach missing or credit granted".into());
            };
            assert_eq!(reply_channel, channel);
            assert_eq!(detach.handle, reply.handle);
            assert!(detach.closed);
            assert_eq!(detach.error, Some(denied));
            assert!(payload.is_empty());
            send(
                &mut peer,
                7,
                Performative::Detach(Detach {
                    handle,
                    closed: true,
                    error: None,
                }),
            )
            .await?;
        }

        let closing = async {
            assert!(matches!(
                read_frame(&mut peer).await?,
                Frame::Amqp {
                    channel: 0,
                    performative: Some(Performative::Close(Close { error: None })),
                    ..
                }
            ));
            send(&mut peer, 0, Performative::Close(Close { error: None })).await
        };
        let (closed, replied) = tokio::join!(connection.close(), closing);
        closed?;
        replied?;
        Ok(())
    })
    .await?
}
