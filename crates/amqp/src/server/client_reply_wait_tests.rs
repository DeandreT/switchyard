//! Admitted-command reply custody, distinct from native task joins.

use super::*;
use crate::test_deadline::WAIT;
use std::{future::poll_fn, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    task::JoinHandle,
    time::timeout,
};

async fn old_wait_is_pending(response: &mut oneshot::Receiver<Result<(), EngineError>>) {
    let original_reply_only_wait = async { response.await.map_err(|_| EngineError::Stopped)? };
    tokio::pin!(original_reply_only_wait);
    assert!(
        poll_fn(|cx| Poll::Ready(original_reply_only_wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

#[tokio::test]
async fn admitted_close_published_after_original_inbox_drop_retires_its_exact_reply() {
    let (commands, original_inbox) = mpsc::channel::<ClientCommand>(1);
    let original_permit = commands.reserve().await.unwrap();
    drop(original_inbox);
    assert!(commands.is_closed());
    let (reply, mut response) = oneshot::channel();
    original_permit.send(ClientCommand::Close { reply });
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    old_wait_is_pending(&mut response).await;
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        client_response(response, commands.closed()).await,
        Err(EngineError::Stopped)
    ));
}

fn original_result(success: bool) -> Result<u32, EngineError> {
    if success {
        Ok(17)
    } else {
        Err(EngineError::InvalidState("original refusal".into()))
    }
}

fn assert_original_result(result: Result<u32, EngineError>, success: bool) {
    if success {
        assert!(matches!(result, Ok(17)));
    } else {
        assert!(
            matches!(result, Err(EngineError::InvalidState(ref value)) if value == "original refusal")
        );
    }
}

#[tokio::test]
async fn ready_original_success_and_error_precede_closed_inbox() {
    for success in [true, false] {
        let (commands, original_inbox) = mpsc::channel::<ClientCommand>(1);
        let (reply, response) = oneshot::channel();
        reply.send(original_result(success)).unwrap();
        drop(original_inbox);
        assert!(commands.is_closed());
        assert_original_result(client_response(response, commands.closed()).await, success);
    }
}

#[tokio::test]
async fn original_reply_published_between_response_and_closure_polls_is_preserved() {
    for success in [true, false] {
        let (commands, original_inbox) = mpsc::channel::<ClientCommand>(1);
        let (reply, mut response) = oneshot::channel();
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        // The production select polls the original response Pending first. On
        // this future's first poll, publish that exact reply and close the real
        // original inbox. Closure is then Ready in the same select iteration;
        // biased ordering alone cannot revisit the already-polled response.
        let closed = async {
            reply.send(original_result(success)).unwrap();
            drop(original_inbox);
            commands.closed().await;
        };
        assert_original_result(client_response(response, closed).await, success);
    }
}

#[tokio::test]
async fn dropped_original_reply_and_closed_inbox_return_stopped() {
    let (commands, original_inbox) = mpsc::channel::<ClientCommand>(1);
    let (reply, response) = oneshot::channel::<Result<(), EngineError>>();
    drop(reply);
    drop(original_inbox);
    assert!(matches!(
        client_response(response, commands.closed()).await,
        Err(EngineError::Stopped)
    ));
}

struct Native {
    connection: ClientConnection,
    session: ClientSession,
    peer: DuplexStream,
    driver: JoinHandle<Result<(), EngineError>>,
    observations: watch::Receiver<Option<ClientReaderObservation>>,
    original_reader: tokio::task::Id,
}

async fn control(peer: &mut DuplexStream) -> (u16, Performative) {
    loop {
        let Frame::Amqp {
            channel,
            performative: Some(value),
            payload,
        } = timeout(WAIT, read_frame(peer)).await.unwrap().unwrap()
        else {
            panic!("one native AMQP control frame")
        };
        assert!(payload.is_empty());
        if matches!(&value, Performative::Flow(flow) if flow.handle.is_none()) {
            continue;
        }
        return (channel, value);
    }
}

impl Native {
    async fn new() -> Self {
        let (stream, mut peer) = tokio::io::duplex(64 * 1024);
        let (commands, command_rx) = mpsc::channel(256);
        let (closed_tx, closed) = watch::channel(false);
        let (observer, observations) = watch::channel(None);
        let driver = tokio::spawn(CLIENT_READER_CUSTODY.scope(
            observer,
            run_client(stream, 256 * 1024, command_rx, closed_tx),
        ));
        let mut connection = ClientConnection { commands, closed };
        let (session, ()) = timeout(WAIT, async {
            tokio::join!(connection.begin(), async {
                let (channel, begin) = control(&mut peer).await;
                assert!(matches!(begin, Performative::Begin(_)));
                write_amqp(
                    &mut peer,
                    channel,
                    Performative::Begin(Begin {
                        remote_channel: Some(channel),
                        ..Begin::default()
                    }),
                    vec![],
                )
                .await
                .unwrap();
            })
        })
        .await
        .unwrap();
        let Some(ClientReaderObservation::Spawned(original_reader)) = *observations.borrow() else {
            panic!("the original native reader was spawned")
        };
        Self {
            connection,
            session: session.unwrap(),
            peer,
            driver,
            observations,
            original_reader,
        }
    }

    async fn attach(&mut self, name: &str) -> ClientReceiver {
        let (receiver, ()) = timeout(WAIT, async {
            tokio::join!(self.session.attach_receiver(name, "orders"), async {
                let (channel, offer) = control(&mut self.peer).await;
                let Performative::Attach(mut offer) = offer else {
                    panic!("native receiving Attach")
                };
                assert_eq!(offer.role, Role::Receiver);
                let handle = offer.handle;
                offer.role = Role::Sender;
                offer.initial_delivery_count = Some(0);
                write_amqp(&mut self.peer, channel, Performative::Attach(offer), vec![])
                    .await
                    .unwrap();
                let (_, flow) = control(&mut self.peer).await;
                assert!(matches!(flow, Performative::Flow(flow) if flow.handle == Some(handle)));
            })
        })
        .await
        .unwrap();
        receiver.unwrap()
    }

    async fn healthy_progress(&mut self, receiver: &mut ClientReceiver) {
        let message = Message::data(b"independent healthy link".to_vec());
        write_amqp(
            &mut self.peer,
            self.session.channel,
            Performative::Transfer(Transfer {
                handle: receiver.handle,
                delivery_id: Some(0),
                delivery_tag: Some(vec![1].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            }),
            encode_message(&message).unwrap(),
        )
        .await
        .unwrap();
        let delivery = timeout(WAIT, receiver.recv()).await.unwrap().unwrap();
        assert_eq!(delivery.message(), &message);
        timeout(WAIT, receiver.accept(&delivery))
            .await
            .unwrap()
            .unwrap();
        let (_, acknowledgement) = control(&mut self.peer).await;
        assert!(matches!(
            acknowledgement,
            Performative::Disposition(Disposition {
                first: 0,
                settled: true,
                state: Some(DeliveryState::Accepted(_)),
                ..
            })
        ));
        assert_eq!(
            *self.observations.borrow(),
            Some(ClientReaderObservation::Spawned(self.original_reader))
        );
        assert!(!*self.connection.closed.borrow());
    }

    async fn joined(&mut self) {
        timeout(WAIT, &mut self.driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(WAIT, self.connection.on_close()).await.unwrap();
        assert_eq!(
            *self.observations.borrow(),
            Some(ClientReaderObservation::Joined {
                id: self.original_reader,
                success: true
            })
        );
        assert!(*self.connection.closed.borrow());
        let mut byte = [0];
        assert_eq!(
            timeout(WAIT, self.peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0,
            "no extra Detach or Close work after original task joins"
        );
    }
}

#[tokio::test]
async fn late_admitted_close_after_native_eof_does_not_wait_for_a_dead_writer() {
    let mut native = Native::new().await;
    let mut healthy = native.attach("healthy").await;
    native.healthy_progress(&mut healthy).await;
    let commands = native.connection.commands.clone();
    let original_permit = commands.reserve().await.unwrap();
    // Half-close only the peer's writer. Keep its reader open to prove native
    // transport release, rather than rescuing the original through peer drop.
    native.peer.shutdown().await.unwrap();
    native.joined().await;
    assert!(commands.is_closed());
    let (reply, mut response) = oneshot::channel();
    original_permit.send(ClientCommand::Close { reply });
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    old_wait_is_pending(&mut response).await;
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        client_response(response, commands.closed()).await,
        Err(EngineError::Stopped)
    ));
}

#[tokio::test]
async fn physical_detach_ack_and_normal_close_keep_the_original_reader_join_distinct() {
    let mut native = Native::new().await;
    let mut retiring = native.attach("retiring").await;
    let mut healthy = native.attach("healthy").await;
    write_amqp(
        &mut native.peer,
        native.session.channel,
        Performative::Detach(Detach {
            handle: retiring.handle,
            closed: true,
            error: None,
        }),
        vec![],
    )
    .await
    .unwrap();
    let (_, acknowledgement) = control(&mut native.peer).await;
    assert!(
        matches!(acknowledgement, Performative::Detach(Detach { handle, closed: true, error: None }) if handle == retiring.handle)
    );
    timeout(WAIT, wait_for_detach(&mut retiring.detached))
        .await
        .unwrap();
    native.healthy_progress(&mut healthy).await;
    let (closed, ()) = timeout(WAIT, async {
        tokio::join!(native.connection.close(), async {
            let (_, close) = control(&mut native.peer).await;
            assert!(matches!(close, Performative::Close(_)));
            write_amqp(
                &mut native.peer,
                0,
                Performative::Close(Close::default()),
                vec![],
            )
            .await
            .unwrap();
        })
    })
    .await
    .unwrap();
    closed.unwrap();
    native.joined().await;
}
