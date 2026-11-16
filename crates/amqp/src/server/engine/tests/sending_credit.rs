use super::*;
use std::{io, pin::Pin, task::Context};

fn link(sessions: &HashMap<u16, SessionState>) -> &SendingLink {
    let LinkState::Sending(link) = &sessions[&1].links[&1] else {
        panic!("sending link")
    };
    link
}

fn link_mut(sessions: &mut HashMap<u16, SessionState>) -> &mut SendingLink {
    let LinkState::Sending(link) = sessions.get_mut(&1).unwrap().links.get_mut(&1).unwrap() else {
        panic!("sending link")
    };
    link
}

fn grant(count: u32, credit: u32) -> Flow {
    Flow {
        handle: Some(1),
        delivery_count: Some(count),
        link_credit: Some(credit),
        drain: true,
        ..Flow::default()
    }
}

/// A write frontier, not a timer: complete exactly this many bytes, then either
/// block the original future or fail it. Partial frames never count as written.
struct Frontier {
    bytes: Vec<u8>,
    budget: usize,
    block: bool,
}

impl AsyncWrite for Frontier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.budget == 0 {
            return if self.block {
                Poll::Pending
            } else {
                Poll::Ready(Err(io::Error::other("owned write frontier")))
            };
        }
        let n = bytes.len().min(self.budget);
        self.bytes.extend_from_slice(&bytes[..n]);
        self.budget -= n;
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn full_uint_drain_commits_only_completed_control_steps_and_no_delivery_resources() {
    // Obtain exact encoded frame budgets from this actor, including its real
    // session fields, rather than assuming an encoder's frame lengths.
    let mut reference = queued_sending_session(1, 1, 0, VecDeque::new());
    reference.get_mut(&1).unwrap().outgoing = OutgoingWindow::new(0, 0);
    let mut expected = Vec::new();
    apply_flow(1, grant(0, u32::MAX), &mut expected, &mut reference, 512)
        .await
        .unwrap();
    assert!(
        expected.is_empty(),
        "positive drain requires explicit source-empty acknowledgement"
    );
    let request = link(&reference).drain.current.unwrap().request;
    let (reply, response) = oneshot::channel();
    acknowledge_drain(request, reply, &mut reference, &mut expected)
        .await
        .unwrap();
    response.await.unwrap().unwrap();
    let mut input = expected.as_slice();
    let mut lengths = vec![0];
    for (count, remaining) in [(2147483647, 2147483648), (4294967294, 1), (u32::MAX, 0)] {
        let Frame::Amqp {
            performative: Some(Performative::Flow(flow)),
            ..
        } = read_frame(&mut input).await.unwrap()
        else {
            panic!("Flow only")
        };
        assert_eq!(
            (flow.delivery_count, flow.link_credit),
            (Some(count), Some(remaining))
        );
        assert!(flow.drain && !flow.echo);
        assert_eq!(flow.next_outgoing_id, 0);
        lengths.push(expected.len() - input.len());
    }
    assert!(input.is_empty());
    for complete in 0..3 {
        for partial in [0, 1] {
            for block in [false, true] {
                let mut sessions = queued_sending_session(1, 1, 0, VecDeque::new());
                sessions.get_mut(&1).unwrap().outgoing = OutgoingWindow::new(0, 0);
                let mut writer = Frontier {
                    bytes: Vec::new(),
                    budget: lengths[complete] + partial,
                    block,
                };
                apply_flow(1, grant(0, u32::MAX), &mut writer, &mut sessions, 512)
                    .await
                    .unwrap();
                let request = link(&sessions).drain.current.unwrap().request;
                let (reply, mut response) = oneshot::channel();
                let mut original = Box::pin(acknowledge_drain(
                    request,
                    reply,
                    &mut sessions,
                    &mut writer,
                ));
                let state =
                    std::future::poll_fn(|cx| Poll::Ready(original.as_mut().poll(cx))).await;
                if block {
                    assert!(state.is_pending());
                    assert!(response.try_recv().is_err());
                } else {
                    assert!(matches!(state, Poll::Ready(Err(EngineError::Io(_)))));
                }
                drop(original);
                let count = [0, 2147483647, 4294967294][complete];
                let current = link(&sessions);
                assert_eq!(current.progress.count(), count);
                assert_eq!(current.drain.current.unwrap().request, request);
                assert!(current.progress.update(Some(count), Some(0), 0).is_ok());
                assert!(
                    current
                        .progress
                        .update(Some(count.wrapping_add(1)), Some(1), 0)
                        .is_err()
                );
                assert!(
                    current.queued.is_empty()
                        && current.transmitting.is_none()
                        && current.unsettled.is_empty()
                );
                assert!(current.credit_reservations.is_empty());
                assert_eq!(sessions[&1].next_delivery_id, 0);
                assert_eq!(sessions[&1].outgoing.next_id(), 0);
                assert!(!sessions[&1].outgoing.has_capacity());
                assert_eq!(writer.bytes, expected[..lengths[complete] + partial]);
            }
        }
    }
}

#[tokio::test]
async fn first_write_publishes_wrap_but_drain_waits_for_original_final_fragment_and_settlement() {
    let capacity = Arc::new(Semaphore::new(1));
    let (mut queued, mut started, mut outcome) = queued_send(&capacity, 7, 0);
    queued.message = Message::data(vec![7; 1400]);
    let mut sessions = queued_sending_session(1, 1, 0, VecDeque::from([queued]));
    link_mut(&mut sessions).progress = sending_credit(u32::MAX, 1);
    sessions.get_mut(&1).unwrap().outgoing = OutgoingWindow::new(0, 1);
    let mut bytes = Vec::new();
    flush_sends(1, 1, sessions.get_mut(&1).unwrap(), &mut bytes, 512)
        .await
        .unwrap();
    assert_eq!(link(&sessions).progress.count(), 0);
    assert!(link(&sessions).progress.update(Some(0), Some(0), 0).is_ok());
    assert!(started.try_recv().is_err() && outcome.try_recv().is_err());
    apply_flow(1, grant(0, 3), &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    let old = link(&sessions).drain.current.unwrap().request;
    apply_flow(1, grant(0, 0), &mut bytes, &mut sessions, 512)
        .await
        .unwrap();
    let current = link(&sessions).drain.current.unwrap().request;
    assert_ne!(old, current);
    for (request, allowed) in [(old, true), (current, false)] {
        let (reply, response) = oneshot::channel();
        acknowledge_drain(request, reply, &mut sessions, &mut bytes)
            .await
            .unwrap();
        assert_eq!(response.await.unwrap().is_ok(), allowed);
    }
    let mut input = bytes.as_slice();
    let Frame::Amqp {
        performative: Some(Performative::Transfer(first)),
        ..
    } = read_frame(&mut input).await.unwrap()
    else {
        panic!("first Transfer")
    };
    assert_eq!(first.delivery_id, Some(0));
    assert!(
        first.more && input.is_empty(),
        "no drain completion ahead of final fragments"
    );
    apply_disposition(
        1,
        Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        },
        &mut sessions,
    );
    assert_eq!(capacity.available_permits(), 0);
    let offset = bytes.len();
    apply_session_flow(
        1,
        Flow {
            next_incoming_id: Some(1),
            incoming_window: 4096,
            outgoing_window: SESSION_WINDOW,
            ..Flow::default()
        },
        &mut bytes,
        &mut sessions,
        512,
    )
    .await
    .unwrap();
    assert_eq!(started.await.unwrap().unwrap().delivery_id, 0);
    assert!(matches!(
        outcome.await.unwrap().unwrap().outcome,
        Outcome::Accepted(_)
    ));
    assert_eq!(capacity.available_permits(), 1);
    assert_eq!(sessions[&1].next_delivery_id, 1);
    assert_eq!(link(&sessions).progress.count(), 0);
    assert!(link(&sessions).drain.current.is_none());
    let mut input = &bytes[offset..];
    let mut frames = 1;
    loop {
        let Frame::Amqp {
            performative: Some(p),
            ..
        } = read_frame(&mut input).await.unwrap()
        else {
            panic!("performative")
        };
        match p {
            Performative::Transfer(t) => {
                assert!(t.delivery_id.is_none() && t.delivery_tag.is_none());
                frames += 1;
            }
            Performative::Flow(f) => {
                assert!(f.drain);
                assert_eq!(f.delivery_count, Some(0));
                assert_eq!(f.link_credit, Some(0));
                assert_eq!(f.next_outgoing_id, frames);
                break;
            }
            _ => panic!("unexpected control"),
        }
    }
    assert!(input.is_empty());
}

#[tokio::test]
async fn uncompleted_first_write_is_not_peer_observable_progress() {
    let capacity = Arc::new(Semaphore::new(1));
    let (queued, start, outcome) = queued_send(&capacity, 1, 0);
    let mut sessions = queued_sending_session(1, 1, 1, VecDeque::from([queued]));
    link_mut(&mut sessions).progress = sending_credit(u32::MAX, 1);
    let mut writer = Frontier {
        bytes: Vec::new(),
        budget: 1,
        block: true,
    };
    let mut original = Box::pin(flush_sends(
        1,
        1,
        sessions.get_mut(&1).unwrap(),
        &mut writer,
        512,
    ));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(original.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(original);
    assert_eq!(link(&sessions).progress.count(), 0);
    assert!(
        link(&sessions)
            .progress
            .update(Some(0), Some(1), 0)
            .is_err()
    );
    stop_link(sessions.get_mut(&1).unwrap().links.get_mut(&1).unwrap());
    assert!(start.await.is_err() && outcome.await.is_err());
    assert_eq!(capacity.available_permits(), 1);
}
