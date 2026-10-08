use std::{task::Waker, time::Duration};

use super::super::link_flow::has_unreserved_credit;
use super::*;

struct Admission {
    sender: Sender,
    commands: mpsc::Receiver<Command>,
    cleanup: mpsc::UnboundedReceiver<CleanupCommand>,
    detached: watch::Sender<bool>,
}

fn admission() -> Admission {
    let (commands, command_rx) = mpsc::channel(1);
    let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
    let (detached_tx, detached) = watch::channel(false);
    Admission {
        sender: Sender {
            name: String::from("admission"),
            channel: 3,
            handle: 1,
            incarnation: 1,
            commands,
            cleanup,
            detached,
            drains: watch::channel(None).1,
            credits: watch::channel(false).1,
            send_capacity: Arc::new(Semaphore::new(1)),
            pending_confirmation: None,
        },
        commands: command_rx,
        cleanup: cleanup_rx,
        detached: detached_tx,
    }
}

fn assert_pending<F: Future>(future: Pin<&mut F>) {
    let mut future = tokio::task::unconstrained(future);
    assert!(
        Pin::new(&mut future)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

#[tokio::test]
async fn detached_admission_wins_over_available_capacity_and_closed_channels() {
    let mut admission = admission();
    admission
        .detached
        .send(true)
        .expect("detach remains observed");
    assert!(matches!(
        admission
            .sender
            .send_pending(Message::data(vec![1]), Binary::from(vec![1]))
            .await,
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        admission.commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(admission.sender.send_capacity.available_permits(), 1);
    drop(admission.detached);
    drop(admission.commands);
    assert!(matches!(
        admission.sender.acquire_send_capacity().await,
        Err(EngineError::RemoteDetached)
    ));
    assert_eq!(admission.sender.send_capacity.available_permits(), 1);
}

#[tokio::test]
async fn closed_command_receiver_wakes_admission_without_releasing_held_capacity() {
    let admission = admission();
    let capacity = Arc::clone(&admission.sender.send_capacity);
    let held = Arc::clone(&capacity)
        .try_acquire_owned()
        .expect("capacity is held externally");
    let mut wait = Box::pin(admission.sender.acquire_send_capacity());
    assert_pending(wait.as_mut());
    drop(admission.commands);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("command closure wakes admission"),
        Err(EngineError::Stopped)
    ));
    assert_eq!(capacity.available_permits(), 0);
    drop(held);
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn closed_false_detach_watch_refuses_available_and_held_capacity() {
    for exhausted in [false, true] {
        let admission = admission();
        let capacity = Arc::clone(&admission.sender.send_capacity);
        let held = exhausted.then(|| {
            Arc::clone(&capacity)
                .try_acquire_owned()
                .expect("capacity is held")
        });
        let mut wait = Box::pin(admission.sender.acquire_send_capacity());
        if exhausted {
            assert_pending(wait.as_mut());
        }
        drop(admission.detached);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), wait)
                .await
                .expect("watch closure wakes admission"),
            Err(EngineError::Stopped)
        ));
        assert_eq!(capacity.available_permits(), usize::from(!exhausted));
        drop(held);
        assert_eq!(capacity.available_permits(), 1);
    }
}

#[tokio::test]
async fn a_closed_semaphore_refuses_admission_without_a_command() {
    let mut admission = admission();
    admission.sender.send_capacity.close();
    assert!(matches!(
        admission
            .sender
            .send_pending(Message::data(vec![1]), Binary::from(vec![1]))
            .await,
        Err(EngineError::Stopped)
    ));
    assert!(matches!(
        admission.commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn queued_capacity_is_refunded_when_terminal_state_precedes_repoll() {
    for detach in [true, false] {
        let admission = admission();
        let capacity = Arc::clone(&admission.sender.send_capacity);
        let held = Arc::clone(&capacity)
            .try_acquire_owned()
            .expect("capacity is held");
        let mut wait = Box::pin(admission.sender.acquire_send_capacity());
        assert_pending(wait.as_mut());
        drop(held);
        assert_eq!(
            capacity.available_permits(),
            0,
            "permit is assigned to the queued acquisition"
        );
        if detach {
            admission
                .detached
                .send(true)
                .expect("detach remains observed");
        } else {
            drop(admission.commands);
        }
        let result = wait.await;
        assert!(if detach {
            matches!(result, Err(EngineError::RemoteDetached))
        } else {
            matches!(result, Err(EngineError::Stopped))
        });
        assert_eq!(capacity.available_permits(), 1);
    }
}

#[tokio::test]
async fn a_benign_watch_update_preserves_semaphore_waiter_fifo() {
    let admission = admission();
    let capacity = Arc::clone(&admission.sender.send_capacity);
    let held = Arc::clone(&capacity)
        .try_acquire_owned()
        .expect("capacity is held");
    let mut first = Box::pin(admission.sender.acquire_send_capacity());
    let mut second = Box::pin(admission.sender.acquire_send_capacity());
    assert_pending(first.as_mut());
    assert_pending(second.as_mut());
    admission
        .detached
        .send(false)
        .expect("benign update is observed");
    assert_pending(first.as_mut());
    drop(held);
    let first_permit = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("first keeps its queue position")
        .expect("first acquires");
    assert_pending(second.as_mut());
    assert_eq!(capacity.available_permits(), 0);
    drop(first_permit);
    let second_permit = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("second waiter wakes")
        .expect("second acquires after first releases");
    assert_eq!(capacity.available_permits(), 0);
    drop(second_permit);
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn cancelling_admission_removes_only_that_semaphore_waiter() {
    let admission = admission();
    let capacity = Arc::clone(&admission.sender.send_capacity);
    let held = Arc::clone(&capacity)
        .try_acquire_owned()
        .expect("capacity is held");
    let mut first = Box::pin(admission.sender.acquire_send_capacity());
    let mut second = Box::pin(admission.sender.acquire_send_capacity());
    assert_pending(first.as_mut());
    assert_pending(second.as_mut());
    drop(first);
    assert_eq!(capacity.available_permits(), 0);
    drop(held);
    let second_permit = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("remaining waiter acquires")
        .expect("admission succeeds");
    drop(second_permit);
    assert_eq!(capacity.available_permits(), 1);
}

async fn reservation_cleanup_after_capacity_wait(cancel: bool) {
    let mut admission = admission();
    let channel = admission.sender.channel;
    let handle = admission.sender.handle;
    let mut sessions = queued_sending_session(channel, handle, 1, VecDeque::new());
    let Some(LinkState::Sending(link)) = sessions
        .get_mut(&channel)
        .and_then(|session| session.links.get_mut(&handle))
    else {
        panic!("sending link exists");
    };
    admission.sender.incarnation = link.drain.incarnation;
    let (reply, reserved) = oneshot::channel();
    reserve_credit(
        channel,
        handle,
        admission.sender.incarnation,
        admission.sender.commands.clone(),
        admission.sender.cleanup.clone(),
        reply,
        &mut sessions,
    );
    let reservation = reserved
        .await
        .expect("reservation response remains live")
        .expect("reservation succeeds")
        .expect("one credit is available");
    let identity = reservation.identity;
    let capacity = Arc::clone(&admission.sender.send_capacity);
    let held = Arc::clone(&capacity)
        .try_acquire_owned()
        .expect("capacity is held");
    let (reply, _response) = oneshot::channel();
    admission
        .sender
        .commands
        .try_send(Command::Close { error: None, reply })
        .expect("the command queue is filled");
    let mut wait = Box::pin(admission.sender.send_pending_with_credit(
        reservation,
        Message::data(vec![1]),
        Binary::from(vec![1]),
    ));
    assert_pending(wait.as_mut());
    assert!(matches!(
        admission.cleanup.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    if cancel {
        drop(wait);
    } else {
        admission
            .detached
            .send(true)
            .expect("detach remains observed");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), wait)
                .await
                .expect("detach wakes reserved admission"),
            Err(EngineError::RemoteDetached)
        ));
    }
    let cleanup = admission
        .cleanup
        .try_recv()
        .expect("the active reservation queues cleanup");
    let CleanupCommand::ReleaseCredit { reservation } = &cleanup;
    assert_eq!(*reservation, identity);
    let (mut writer, _peer) = tokio::io::duplex(64 * 1024);
    handle_cleanup(cleanup, &mut writer, &mut sessions)
        .await
        .expect("cleanup releases actual reserved credit");
    let Some(LinkState::Sending(link)) = sessions[&channel].links.get(&handle) else {
        panic!("sending link remains attached");
    };
    assert!(link.credit_reservations.is_empty());
    assert!(has_unreserved_credit(link));
    assert!(matches!(
        admission.cleanup.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        admission.commands.try_recv(),
        Ok(Command::Close { .. })
    ));
    assert!(matches!(
        admission.commands.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(capacity.available_permits(), 0);
    drop(held);
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn cancelling_a_capacity_wait_refunds_reserved_credit_once_without_enqueue() {
    reservation_cleanup_after_capacity_wait(true).await;
}

#[tokio::test]
async fn rejecting_a_detached_capacity_wait_refunds_reserved_credit_once_without_enqueue() {
    reservation_cleanup_after_capacity_wait(false).await;
}
