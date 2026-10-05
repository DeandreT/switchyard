use std::{
    future::Future,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    Attach, Begin, ConnectionOptions, Coordinator, End, Frame, Open, Performative, ProtocolHeader,
    ReceiverSettleMode, Role, ScopedConnectionAcceptance, SenderSettleMode, ServerConnection,
    ServerConnectionJoinReport, ServerConnectionOwner, Source, Target, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{io::DuplexStream, runtime::Handle, time::timeout};

use super::super::super::IngressMode;
use super::super::{
    Refused, Report, Root,
    controls::{Controls, Gate},
    launch,
};
use super::recorder::Recorder;
use crate::authorization::ConnectionAuthorization;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) const DEADLINE: Duration = Duration::from_secs(5);
pub(super) const CHANNEL: u16 = 7;

pub(super) struct Anchor {
    pub(super) drops: Arc<AtomicUsize>,
    _not_send: Rc<()>,
}
impl Drop for Anchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct Cleanup {
    pub(super) report: Option<Report<Anchor>>,
    pub(super) socket: Option<ServerConnectionJoinReport<()>>,
    pub(super) refused: Option<Refused<Anchor>>,
    pub(super) unused_fault: Option<super::super::controls::Fault>,
    pub(super) unused_session_fault: Option<super::super::controls::Fault>,
    pub(super) counts: Option<(usize, usize, bool)>,
}

pub(super) struct Fixture {
    pub(super) root: Option<Root<Anchor, Recorder>>,
    pub(super) controls: Arc<Controls>,
    pub(super) recorder: Recorder,
    pub(super) anchor_drops: Arc<AtomicUsize>,
    pub(super) peer: Option<DuplexStream>,
    connection: Option<ServerConnection>,
    socket: ServerConnectionOwner<()>,
    pub(super) report: Option<Report<Anchor>>,
    refused: Option<Refused<Anchor>>,
}

impl Fixture {
    pub(super) async fn new(
        mode: IngressMode,
        limit: usize,
        authorization: Option<Arc<ConnectionAuthorization>>,
        controls: Arc<Controls>,
    ) -> TestResult<Self> {
        let namespace = domain::NamespaceName::new("tenant")?;
        let (server, mut peer) = tokio::io::duplex(16_384);
        let (mut socket, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
        let negotiation = async {
            write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
            let header = read_protocol_header(&mut peer).await?;
            if header != ProtocolHeader::AMQP {
                return Err("unexpected header".into());
            }
            send(
                &mut peer,
                0,
                Performative::Open(Open::new("retained-session-peer")),
            )
            .await?;
            if !matches!(
                read_frame(&mut peer).await?,
                Frame::Amqp {
                    performative: Some(Performative::Open(_)),
                    ..
                }
            ) {
                return Err("missing Open".into());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let acceptance = async {
            match mode {
                IngressMode::Posting => {
                    acceptor
                        .accept_with_transactional_ingress(
                            server,
                            "retained-session-server",
                            None,
                            ConnectionOptions::default(),
                        )
                        .await
                }
                IngressMode::Messaging => {
                    acceptor
                        .accept_with_transactional_work_defaults(
                            server,
                            "retained-session-server",
                            None,
                            ConnectionOptions::default(),
                        )
                        .await
                }
            }
        };
        let mut paired = Box::pin(async { tokio::join!(acceptance, negotiation) });
        let observed = timeout(DEADLINE, paired.as_mut()).await;
        let (accepted, negotiated) = match observed {
            Ok(results) => results,
            Err(error) => {
                socket.stop();
                let joined = socket.finish().await;
                drop(paired);
                drop(peer);
                drop(joined);
                return Err(error.into());
            }
        };
        drop(paired);
        let connection = match (accepted, negotiated) {
            (Ok(ScopedConnectionAcceptance::Accepted(connection)), Ok(())) => connection,
            (accepted, negotiated) => {
                socket.stop();
                drop(peer);
                let joined = socket.finish().await;
                // Every observed original setup outcome survives actual socket barriers.
                drop(joined);
                negotiated?;
                match accepted {
                    Err(error) => return Err(error.into()),
                    Ok(_) => return Err("unexpected refused acceptance".into()),
                }
            }
        };
        if let Err(error) = send(&mut peer, CHANNEL, Performative::Begin(Begin::default())).await {
            socket.stop();
            drop(peer);
            let joined = socket.finish().await;
            drop(joined);
            return Err(error);
        }
        let mut connection = connection;
        let incoming = timeout(DEADLINE, connection.next_incoming_session()).await;
        let incoming = match incoming {
            Ok(Some(incoming)) => incoming,
            other => {
                socket.stop();
                drop(peer);
                let joined = socket.finish().await;
                drop(joined);
                other?;
                return Err("missing incoming Session".into());
            }
        };
        let mut paired = Box::pin(async {
            tokio::join!(connection.accept_session(incoming), read_frame(&mut peer))
        });
        let observed = timeout(DEADLINE, paired.as_mut()).await;
        let (session, response) = match observed {
            Ok(results) => results,
            Err(error) => {
                socket.stop();
                let joined = socket.finish().await;
                drop(paired);
                drop(peer);
                drop(joined);
                return Err(error.into());
            }
        };
        drop(paired);
        let session = match (session, response) {
            (
                Ok(session),
                Ok(Frame::Amqp {
                    channel: CHANNEL,
                    performative: Some(Performative::Begin(_)),
                    ..
                }),
            ) => session,
            (session, response) => {
                socket.stop();
                drop(peer);
                let joined = socket.finish().await;
                drop(joined);
                response?;
                session?;
                return Err("missing Begin echo".into());
            }
        };
        let recorder = Recorder::default();
        let anchor_drops = Arc::new(AtomicUsize::new(0));
        let anchor = Anchor {
            drops: Arc::clone(&anchor_drops),
            _not_send: Rc::new(()),
        };
        let launched = launch(
            session,
            connection.connection_identity().clone(),
            namespace,
            recorder.clone(),
            authorization,
            mode,
            limit,
            Handle::current(),
            anchor,
            Arc::clone(&controls),
        );
        let (root, refused) = match launched {
            Ok(root) => (Some(root), None),
            Err(refused) => (None, Some(refused)),
        };
        Ok(Self {
            root,
            controls,
            recorder,
            anchor_drops,
            peer: Some(peer),
            connection: Some(connection),
            socket,
            report: None,
            refused,
        })
    }
    pub(super) fn counts(&self) -> (usize, usize, bool) {
        self.root.as_ref().expect("created root").budget_counts()
    }
    pub(super) fn take_refused(&mut self) -> Option<Refused<Anchor>> {
        self.refused.take()
    }
    pub(super) fn connection_identity(&self) -> Option<amqp::NativeConnectionIdentity> {
        self.connection
            .as_ref()
            .map(|connection| connection.connection_identity().clone())
    }
    pub(super) async fn drive<T>(
        &mut self,
        observed: impl Future<Output = TestResult<T>>,
    ) -> TestResult<T> {
        drive_parts(&mut self.root, &mut self.report, observed).await
    }
    pub(super) async fn send_attach(&mut self, attach: Attach) -> TestResult {
        self.send_performative(Performative::Attach(Box::new(attach)))
            .await
    }
    pub(super) async fn send_performative(&mut self, performative: Performative) -> TestResult {
        let Self {
            root, report, peer, ..
        } = self;
        drive_parts(
            root,
            report,
            send(peer.as_mut().expect("retained peer"), CHANNEL, performative),
        )
        .await
    }
    pub(super) async fn frame(&mut self) -> TestResult<Frame> {
        let Self {
            root, report, peer, ..
        } = self;
        drive_parts(root, report, async {
            Ok(read_frame(peer.as_mut().expect("retained peer")).await?)
        })
        .await
    }
    pub(super) async fn attached(&mut self, handle: u32) -> TestResult {
        for _ in 0..64 {
            if matches!(self.frame().await?, Frame::Amqp { performative: Some(Performative::Attach(attach)), .. } if attach.handle == handle)
            {
                return Ok(());
            }
        }
        Err("Attach echo exceeds bounded frame allowance".into())
    }
    pub(super) async fn wait_gate(&mut self, gate: &Gate) -> TestResult {
        self.drive(async {
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn wait_committed(&mut self, expected: usize) -> TestResult {
        let budget = Arc::clone(&self.root.as_ref().expect("created root").budget);
        self.drive(async {
            while budget.counts().1 < expected {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
    }
    pub(super) async fn peer_end(&mut self) -> TestResult {
        self.send_performative(Performative::End(End::default()))
            .await
    }
    pub(super) async fn observe_natural_finish(&mut self) -> TestResult {
        if self.report.is_none() {
            let observed =
                timeout(DEADLINE, self.root.as_mut().expect("created root").finish()).await;
            match observed {
                Ok(report) => self.report = Some(report),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    pub(super) async fn complete(mut self) -> Cleanup {
        if self.report.is_none()
            && let Some(root) = &mut self.root
        {
            root.stop();
            // Release the caller-owned observation gates, not blocked task bodies.
            self.controls.session_ready.release();
            self.controls.close_ack.release();
            self.controls.finish_after_row.release();
            self.report = Some(root.finish().await);
        }
        self.controls.release_all();
        // Atomic barriers do not cover the engine pair: retain and join that separately.
        self.socket.stop();
        drop(self.peer.take());
        drop(self.connection.take());
        let socket = self.socket.finish().await;
        let unused_fault = self.controls.take_fault();
        let unused_session_fault = self.controls.take_session_fault();
        let counts = self.root.as_ref().map(Root::budget_counts);
        Cleanup {
            report: self.report.take(),
            socket,
            refused: self.refused.take(),
            unused_fault,
            unused_session_fault,
            counts,
        }
    }
}

async fn drive_parts<T>(
    root: &mut Option<Root<Anchor, Recorder>>,
    report: &mut Option<Report<Anchor>>,
    observed: impl Future<Output = TestResult<T>>,
) -> TestResult<T> {
    let Some(root) = root.as_mut() else {
        return observed.await;
    };
    timeout(DEADLINE, async {
        tokio::select! {
            result = observed => result,
            completed = root.finish(), if report.is_none() => {
                *report = Some(completed);
                Err("Session completed before observation".into())
            },
        }
    })
    .await?
}

pub(super) async fn send(
    peer: &mut DuplexStream,
    channel: u16,
    performative: Performative,
) -> TestResult {
    Ok(write_frame(
        peer,
        &Frame::Amqp {
            channel,
            performative: Some(performative),
            payload: Vec::new(),
        },
    )
    .await?)
}

pub(super) fn producer(handle: u32) -> Attach {
    Attach {
        name: format!("producer-{handle}"),
        handle,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source::default()),
        target: Some(Target::new("orders").into()),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}
pub(super) fn controller(handle: u32) -> Attach {
    let mut attach = producer(handle);
    attach.target = Some(Coordinator::default().into());
    attach
}
pub(super) fn consumer(handle: u32) -> Attach {
    let mut attach = producer(handle);
    attach.name = format!("consumer-{handle}");
    attach.role = Role::Receiver;
    attach.source = Some(Source::new("orders"));
    attach.target = Some(Target::default().into());
    attach.rcv_settle_mode = ReceiverSettleMode::Second;
    attach.initial_delivery_count = None;
    attach
}

pub(super) fn assert_barriers(cleanup: &Cleanup, expected: usize) {
    let report = cleanup.report.as_ref().expect("actual Session report");
    assert_eq!(report.launches.len(), expected);
    assert_eq!(report.rows.len(), expected);
    for (ordinal, launch) in report.launches.iter().enumerate() {
        assert_eq!(launch.ordinal, ordinal);
        assert_eq!(
            report.rows.iter().filter(|row| row.id == launch.id).count(),
            1
        );
    }
    let socket = cleanup
        .socket
        .as_ref()
        .expect("separate actual socket pair report");
    assert!(socket.actor().is_some_and(Result::is_ok));
    assert!(socket.reader().is_some_and(|row| row.is_ok() || row.as_ref().is_err_and(|error| error.is_cancelled())));
}
