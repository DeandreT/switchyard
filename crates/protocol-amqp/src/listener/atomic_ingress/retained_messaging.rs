//! Caller-driven, finite-history messaging for one explicitly accepted socket.
//! Ordinary listener defaults and broker/provider custody are unchanged.

mod admission;
mod bridge;
mod collector;
mod outcomes;
mod root;
mod worker_history;

pub use outcomes::{
    RetainedAtomicMessagingAdmissionOutcome, RetainedAtomicMessagingBuildError,
    RetainedAtomicMessagingControl, RetainedAtomicMessagingDrain, RetainedAtomicMessagingLimits,
    RetainedAtomicMessagingLimitsError, RetainedAtomicMessagingProgress,
    RetainedAtomicMessagingReport, RetainedAtomicMessagingSessionOutcome,
    RetainedAtomicMessagingWorkerBranch, RetainedAtomicMessagingWorkerOutcome,
};
pub use root::{RetainedAtomicMessagingOwner, RetainedAtomicMessagingStarter};

use crate::{AmqpListener, NativeAtomicBroker, RetainedConnectionStartError};

impl<B: NativeAtomicBroker> AmqpListener<B> {
    /// Start exactly one already accepted socket on the starter's captured runtime.
    ///
    /// Retain and drive its separate owner on externally live Runtime A. A Handle
    /// does not keep that runtime alive or drive its I/O/timers. This endpoint
    /// uses the existing atomic messaging/CBS policy, refuses management links,
    /// and does not activate SDK transactions on the ordinary listener.
    ///
    /// History is append-only: ended sessions/links do not refund their lifetime
    /// slots. Limits cover logical metadata, not native buffers, backend work or
    /// RSS. Reports retain original role results and an optional actual native
    /// Close result; they do not certify broker cleanup, store reopen or health.
    ///
    /// # Unavailable Capabilities
    ///
    /// The owner cannot be cloned.
    /// ```compile_fail
    /// fn owner_clone_is_unavailable<A, B: protocol_amqp::NativeAtomicBroker>(
    ///     owner: protocol_amqp::RetainedAtomicMessagingOwner<A, B>) {
    ///     let _copy = owner.clone();
    /// }
    /// ```
    /// The starter cannot be cloned.
    /// ```compile_fail
    /// fn starter_clone_is_unavailable<B: protocol_amqp::NativeAtomicBroker>(
    ///     starter: protocol_amqp::RetainedAtomicMessagingStarter<B>) {
    ///     let _copy = starter.clone();
    /// }
    /// ```
    /// Reports have no public constructor or writable fields.
    /// ```compile_fail
    /// fn caller_cannot_construct_report() {
    ///     let _report = protocol_amqp::RetainedAtomicMessagingReport { anchor: () };
    /// }
    /// ```
    /// Original engine handles remain private.
    /// ```compile_fail
    /// fn caller_cannot_extract_actor_handle<A, B: protocol_amqp::NativeAtomicBroker>(
    ///     owner: protocol_amqp::RetainedAtomicMessagingOwner<A, B>) {
    ///     let _actor = owner.actor_handle();
    /// }
    /// ```
    /// Progress is data, not a launch capability.
    /// ```compile_fail
    /// fn progress_cannot_be_used_as_starter<B: protocol_amqp::NativeAtomicBroker>(
    ///     listener: protocol_amqp::AmqpListener<B>, stream: tokio::net::TcpStream,
    ///     progress: protocol_amqp::RetainedAtomicMessagingProgress) {
    ///     let _ = listener.start_retained_collected_atomic_messaging(stream, progress);
    /// }
    /// ```
    /// A cloned control cannot join or publish a report.
    /// ```compile_fail
    /// async fn control_cannot_finish(control: protocol_amqp::RetainedAtomicMessagingControl) {
    ///     let _report = control.finish().await;
    /// }
    /// ```
    /// This root does not grant native writer or broker access.
    /// ```compile_fail
    /// fn root_does_not_expose_broker_or_storage_writer<A, B: protocol_amqp::NativeAtomicBroker>(
    ///     owner: protocol_amqp::RetainedAtomicMessagingOwner<A, B>) {
    ///     let _writer = owner.storage_writer();
    ///     let _broker = owner.broker();
    /// }
    /// ```
    ///
    /// # Caller-Owned Anchor
    /// ```no_run
    /// async fn captured_runtime_permit_anchor_example<B: protocol_amqp::NativeAtomicBroker>(
    ///     runtime: tokio::runtime::Handle, listener: protocol_amqp::AmqpListener<B>,
    ///     stream: tokio::net::TcpStream, permit: tokio::sync::OwnedSemaphorePermit,
    /// ) -> Result<(), Box<dyn std::error::Error>> {
    ///     let witness = std::rc::Rc::new(());
    ///     let limits = protocol_amqp::RetainedAtomicMessagingLimits::new(4, 16)?;
    ///     let (mut owner, starter) = protocol_amqp::RetainedAtomicMessagingOwner::new(
    ///         runtime, limits, (permit, witness))?;
    ///     listener.start_retained_collected_atomic_messaging(stream, starter)
    ///         .map_err(|_| std::io::Error::other("retained socket setup refused"))?;
    ///     // Application code drives owner.drive_step() beside its original client work.
    ///     let report = owner.finish().await.expect("first complete covered-role report");
    ///     assert!(report.socket().wrapper().is_some());
    ///     // Keep the raw report and its permit through application postconditions.
    ///     drop(report);
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Borrowed cancellation leaves the original owner resumable. Dropping an
    /// unreported owner instead permanently forgets one preallocated holder with
    /// its anchor/tokens/raw results after logical closure and cancellation.
    /// This deliberately refuses reusable capacity; no rescue task or actual
    /// join is implied, and caller-created abandoned roots have no global bound.
    pub fn start_retained_collected_atomic_messaging(
        self,
        stream: tokio::net::TcpStream,
        starter: RetainedAtomicMessagingStarter<B>,
    ) -> Result<(), RetainedConnectionStartError<B>> {
        let RetainedAtomicMessagingStarter { socket, bridge } = starter;
        self.start_retained_with_driver(stream, socket, bridge)
    }
}

#[cfg(test)]
mod tests;
