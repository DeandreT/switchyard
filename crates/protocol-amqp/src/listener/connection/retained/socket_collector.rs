//! Private fixed accepted-socket experiment. External root and live A are mandatory.

use super::{AdmissionMode, RetainedDriver, ready};
use crate::listener::{
    AmqpListener,
    atomic_ingress::retained_collector::{self as atomic, admissions::Record},
    retained_connection::{
        RetainedConnectionJoinReport, RetainedConnectionOwner, RetainedConnectionStartError,
        RetainedConnectionStarter,
    },
};
use crate::{NativeAtomicBroker, authorization::ConnectionAuthorization};
use amqp::ServerConnection;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, runtime::Handle, sync::watch};

mod control;
mod handoff;
mod hooks;
mod tests;
use control::{Binding, Control, Open, Publisher};
use handoff::{Cell, Packet, Port};

pub(super) struct Refused<A> {
    pub(super) limit: usize,
    pub(super) anchor: A,
}
pub(super) struct Launch<B, const MESSAGING: bool> {
    starter: RetainedConnectionStarter,
    bridge: Bridge<B, MESSAGING>,
}
struct Bridge<B, const MESSAGING: bool> {
    control: Arc<Control<B>>,
    publisher: Publisher<B>,
    ports: [Port; 2],
}
pub(super) struct Report<A, B> {
    pub(super) socket: Option<RetainedConnectionJoinReport<()>>,
    pub(super) collector: Option<atomic::Report<()>>,
    histories: [Packet; 2],
    pub(super) stopped: [Option<Record>; 2],
    context: Option<Open<B>>,
    binding_refused: Option<Box<atomic::Refused<(), B>>>,
    pub(super) anchor: A,
}
pub(super) struct Root<A, B: NativeAtomicBroker> {
    socket: RetainedConnectionOwner<()>,
    collector: Option<atomic::Root<(), B>>,
    control: Arc<Control<B>>,
    changed: watch::Receiver<()>,
    cells: [Arc<Cell>; 2],
    context: Option<Open<B>>,
    entered_binding: bool,
    binding_refused: Option<Box<atomic::Refused<(), B>>>,
    socket_report: Option<RetainedConnectionJoinReport<()>>,
    collector_report: Option<atomic::Report<()>>,
    histories: [Option<Packet>; 2],
    stopped: [Option<Record>; 2],
    runtime: Handle,
    limit: usize,
    anchor: Option<A>,
}

impl<A, B: NativeAtomicBroker> Root<A, B> {
    pub(super) fn new<const MESSAGING: bool>(
        runtime: Handle,
        limit: usize,
        anchor: A,
    ) -> Result<(Self, Launch<B, MESSAGING>), Box<Refused<A>>> {
        if !(1..=128).contains(&limit) {
            return Err(Box::new(Refused { limit, anchor }));
        }
        let (socket, starter) = RetainedConnectionOwner::new(runtime.clone(), ());
        let (control, publisher) = Control::new();
        let changed = control.subscribe();
        let (first, first_port) = Cell::new();
        let (second, second_port) = Cell::new();
        Ok((
            Self {
                socket,
                collector: None,
                control: control.clone(),
                changed,
                cells: [first, second],
                context: None,
                entered_binding: false,
                binding_refused: None,
                socket_report: None,
                collector_report: None,
                histories: [None, None],
                stopped: [None, None],
                runtime,
                limit,
                anchor: Some(anchor),
            },
            Launch {
                starter,
                bridge: Bridge {
                    control,
                    publisher,
                    ports: [first_port, second_port],
                },
            },
        ))
    }
    async fn bind(&mut self) {
        if self.entered_binding {
            // A canceled pre-creation binding is closed unbound, never retried.
            if self.collector.is_none() && self.context.is_some() {
                self.stop();
            }
            return;
        }
        if let Some(context) = self.control.take_context() {
            self.context = Some(context);
        }
        let Some(context) = self.context.as_ref() else {
            return;
        };
        self.entered_binding = true;
        self.control.hooks.bind.hold().await;
        if self.control.sealed() {
            self.control.closed();
            return;
        }
        let settings = atomic::Settings::from_open(
            context.identity.clone(),
            context.namespace.clone(),
            context.broker.clone(),
            context.authorization.clone(),
            self.runtime.clone(),
            context.messaging,
        );
        match atomic::Root::new(settings, self.limit, ()) {
            Ok(collector) => self.collector = Some(collector),
            Err(refused) => {
                self.binding_refused = Some(refused);
                self.control.seal();
                self.control.closed();
                return;
            }
        }
        let reservations = self
            .collector
            .as_mut()
            .expect("installed sole collector")
            .reserved_pair();
        let Some([first, second]) = reservations else {
            self.stop();
            return;
        };
        self.cells[0].arm(first);
        self.cells[1].arm(second);
        if self.control.sealed() {
            self.stop();
        } else {
            self.control.bound();
        }
    }
    fn transfer(&mut self) {
        for (index, cell) in self.cells.iter().enumerate() {
            if let Some(record) = cell.take_record() {
                let mut record = Some(record);
                if let Some(collector) = self.collector.as_mut()
                    && collector.install_reserved(&mut record).is_some()
                {
                    continue;
                }
                self.stopped[index] = record;
            }
        }
    }
    pub(super) fn stop(&mut self) {
        self.control.seal();
        if let Some(collector) = &mut self.collector {
            collector.close_only();
        }
        // Authority closure is published before root-requested cancellation.
        self.control.closed();
        if let Some(collector) = &mut self.collector {
            collector.stop();
        }
        self.socket.stop();
    }
    pub(super) async fn drive(&mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            self.bind().await;
            self.transfer();
            if self.control.requested()
                || self.control.sealed()
                || self.collector.as_ref().is_some_and(atomic::Root::is_closed)
            {
                self.stop();
            }
            if let Some(collector) = &mut self.collector {
                tokio::select! {
                    () = collector.step_scoped(&mut tick) => {},
                    _ = self.changed.changed() => {},
                }
            } else {
                let _ = self.changed.changed().await;
            }
        }
    }
    pub(super) async fn finish(&mut self) -> Report<A, B> {
        self.stop();
        if self.socket_report.is_none() {
            let observed = ready::observe(
                self.socket.finish(),
                |report| {
                    self.socket_report = report;
                },
                || {},
            );
            let mut observed = std::pin::pin!(observed);
            if let Some(collector) = &mut self.collector {
                tokio::select! {
                    () = observed.as_mut() => {},
                    () = collector.drive_closed() => unreachable!("borrowed closed pump"),
                }
            } else {
                observed.as_mut().await;
            }
            self.control.hooks.socket_ready.hold().await;
        }
        // Actual Wrapper creator join ends every accepted no-await storage obligation.
        if self.context.is_none() {
            self.context = self.control.take_context();
        }
        self.transfer();
        for (index, cell) in self.cells.iter().enumerate() {
            if self.histories[index].is_none() {
                let mut packet = cell.take();
                drop(packet.ticket.take());
                self.histories[index] = Some(packet);
            }
        }
        for record in self.stopped.iter_mut().flatten() {
            record.refund();
        }
        if let Some(collector) = &mut self.collector
            && self.collector_report.is_none()
        {
            ready::observe(
                collector.finish(),
                |report| {
                    self.collector_report = Some(report);
                },
                || {},
            )
            .await;
            self.control.hooks.collector_ready.hold().await;
        }
        Report {
            socket: self.socket_report.take(),
            collector: self.collector_report.take(),
            histories: std::array::from_fn(|index| {
                self.histories[index].take().expect("reconciled fixed port")
            }),
            stopped: std::mem::take(&mut self.stopped),
            context: self.context.take(),
            binding_refused: self.binding_refused.take(),
            anchor: self
                .anchor
                .take()
                .expect("one-shot external anchor after both barriers"),
        }
    }
}

impl<B: NativeAtomicBroker, const MESSAGING: bool> Launch<B, MESSAGING> {
    pub(super) fn start(
        self,
        listener: AmqpListener<B>,
        stream: TcpStream,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        listener.start_retained_with_driver(stream, self.starter, self.bridge)
    }
}
impl<B: NativeAtomicBroker, const MESSAGING: bool> RetainedDriver<B> for Bridge<B, MESSAGING> {
    const ADMISSION: AdmissionMode = if MESSAGING {
        AdmissionMode::AtomicMessaging
    } else {
        AdmissionMode::AtomicPosting
    };
    async fn serve_retained_open(
        self,
        connection: &mut ServerConnection,
        namespace: domain::NamespaceName,
        broker: B,
        authorization: Option<Arc<ConnectionAuthorization>>,
    ) -> crate::listener::retained_connection::RetainedConnectionResult {
        self.publisher.publish(Open {
            identity: connection.connection_identity().clone(),
            namespace,
            broker,
            authorization,
            messaging: MESSAGING,
        });
        if self.control.binding().await == Binding::Bound {
            for port in self.ports {
                if !port.discover(connection, &self.control).await {
                    self.control.request_stop();
                    break;
                }
            }
        }
        self.control.terminal().await;
        self.control.hooks.wrapper_return.hold().await;
        self.control.hooks.terminal_result()
    }
}
