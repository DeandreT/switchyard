use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use super::*;
use native_transactions::{NativeIngressPolicy, NativeTransactionBook};

#[tokio::test]
async fn negotiating_acceptance_installs_unsettled_not_the_original_mixed_request() {
    for requested in [SenderSettleMode::Mixed, SenderSettleMode::Unsettled] {
        let connection = NativeConnectionIdentity::new();
        let (terminated, _) = watch::channel(false);
        let _exit = connection_identity::ConnectionActorExit::new(connection.clone(), terminated);
        let (commands, mut queued) = mpsc::channel(4);
        let (attaches, incoming_attaches) = mpsc::channel(4);
        let (incoming_sessions, _incoming) = mpsc::channel(1);
        let mut state = SessionState::for_connection(&Begin::default(), &connection);
        state.local_begin_sent = true;
        state.peer_channel = Some(CHANNEL);
        state.attach_tx = Some(attaches);
        let mut session = ServerSession {
            channel: 0,
            identity: state.identity.clone(),
            commands,
            incoming_attaches,
            consumed: Arc::new(Notify::new()),
        };
        let mut sessions = HashMap::from([(0, state)]);
        let mut book = NativeTransactionBook::new(&connection, NativeIngressPolicy::WorkDefaults);
        let mut writer = FrameWriter::new(tokio::io::sink(), 4096).expect("bounded actor writer");
        let mut attach = request(Role::Receiver);
        attach.snd_settle_mode = requested.clone();
        handle_frame_scoped(
            Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Attach(Box::new(attach))),
                payload: Vec::new(),
            },
            &mut writer,
            &incoming_sessions,
            &mut sessions,
            4096,
            u16::MAX,
            false,
            ConnectionScope::Native(&connection),
            &mut book,
        )
        .await
        .expect("actual Attach actor path");
        let incoming = session
            .incoming_attaches
            .try_recv()
            .expect("actor-created approval");
        assert_eq!(incoming.snd_settle_mode, requested);
        let owner = incoming.approval().link_identity().clone();
        let handle = incoming.approval().local_handle();
        let mut accepting =
            Box::pin(session.accept_transactional_sender_negotiating_unsettled(incoming, 0));
        poll_fn(|cx| {
            assert!(accepting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let Command::NativeTransactions(command) =
            queued.try_recv().expect("exact native acceptance")
        else {
            panic!("native acceptance command");
        };
        native_transactions::handle_native_command(command, &mut book, &mut sessions, &mut writer)
            .await
            .expect("actor sender acceptance");
        let sender = bounded(accepting).await.expect("actual sender reply");
        let LinkState::Sending(link) = &sessions[&0].links[&handle] else {
            panic!("installed sender");
        };
        assert!(link.identity.same_link(&owner));
        assert_eq!(link.settle_mode, SenderSettleMode::Unsettled);
        assert_eq!(link.receiver_settle_mode, ReceiverSettleMode::Second);
        assert!(sender.sender_identity().is_active());
    }
}

#[test]
fn defaulting_coordinator_count_does_not_relax_original_role_recovery_or_source_guards() {
    for variant in 0..5 {
        let mut attach = coordinator_request(None);
        match variant {
            0 => attach.role = Role::Receiver,
            1 => attach.snd_settle_mode = SenderSettleMode::Settled,
            2 => attach.incomplete_unsettled = true,
            3 => attach.source.as_mut().expect("source").dynamic = true,
            _ => {
                attach.source.as_mut().expect("source").default_outcome =
                    Some(DeliveryState::Declared(crate::Declared {
                        txn_id: TransactionId::new([1]).expect("bounded ID"),
                    }))
            }
        }
        assert!(
            native_transactions::classify_attach(&attach, NativeIngressPolicy::WorkDefaults)
                .is_err()
        );
    }
}
