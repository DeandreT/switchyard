use std::{collections::BTreeMap, future::pending, time::Instant};

use amqp::{
    AmqpError, CoordinatorRequest, Error as AmqpProtocolError, ErrorCondition,
    MAX_NATIVE_TRANSACTION_POSTINGS, MAX_NATIVE_TRANSACTIONS, NativeConnectionIdentity,
    NativeControllerIdentity, NativeDeclarationRefusal, NativePreparedWork, NativeTransactionState,
};
use domain::EntityIncarnationKind;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_amqp::primitives::Symbol;

use super::groups::{
    Consumer, Controller, Group, Posting, Producer, QueuedWork, RetirementCompletion, id_key,
};
use super::operations::{Operation, OperationFuture};
use super::{Event, MAX_LINKS, WorkerClose, WorkerIdentity, same_connection};
use crate::{
    ATOMIC_TRANSACTION_TIMEOUT, AtomicCommitState, AtomicTransactionDischarge,
    AtomicTransactionRegistry, NativeAtomicBroker, OwnedNativeAtomicMessagingSubmission,
    batch::read_ingress,
};

// Includes declaration refusals and negative/final flushes, not only postings.
const MAX_OPERATIONS: usize = 128;

mod postings;
mod retirements;

pub(super) struct Owner<B: NativeAtomicBroker> {
    registry: AtomicTransactionRegistry,
    connection: NativeConnectionIdentity,
    broker: B,
    controllers: Vec<Controller>,
    producers: Vec<Producer>,
    consumers: Vec<Consumer>,
    groups: BTreeMap<u64, Group>,
    operations: FuturesUnordered<OperationFuture>,
    next_producer: u64,
    next_consumer: u64,
    closed: bool,
}

impl<B: NativeAtomicBroker> Owner<B> {
    pub(super) fn new(connection: NativeConnectionIdentity, broker: B) -> Self {
        let (registry, _) = AtomicTransactionRegistry::new();
        Self {
            registry,
            connection,
            broker,
            controllers: Vec::new(),
            producers: Vec::new(),
            consumers: Vec::new(),
            groups: BTreeMap::new(),
            operations: FuturesUnordered::new(),
            next_producer: 0,
            next_consumer: 0,
            closed: false,
        }
    }

    pub(super) fn process(&mut self, event: Event) {
        if self.closed {
            return;
        }
        match event {
            Event::RegisterConsumer {
                identity,
                admission,
                authorization,
                close,
                reply,
            } => {
                self.register_consumer(identity, admission, authorization, close, reply);
            }
            Event::RegisterHeld {
                source,
                delivery,
                reply,
            } => {
                self.register_held(source, delivery, reply);
            }
            Event::Retirement {
                source,
                receipt,
                reply,
            } => {
                self.retirement(source, receipt, reply);
            }
            Event::ClearHeld {
                source,
                original,
                reply,
            } => {
                self.clear_held(source, original, reply);
            }
            Event::RegisterProducer {
                identity,
                admission,
                authorization,
                close,
                reply,
            } => {
                let valid = same_connection(&self.connection, identity.connection_identity())
                    && identity.is_active()
                    && !admission.config.requires_session
                    && admission.binding.kind() == EntityIncarnationKind::Queue
                    && admission.binding.target() == admission.binding.owner()
                    && !admission.binding.target().is_dead_letter_queue()
                    && !admission.binding.target().is_subscription_path()
                    && !self
                        .producers
                        .iter()
                        .any(|row| row.identity.same_receiver(&identity));
                let key = self.next_producer.checked_add(1);
                if !valid
                    || key.is_none()
                    || self.producers.len() + self.consumers.len() >= MAX_LINKS
                {
                    let error = refused("the transactional producer admission is unavailable");
                    let _ = close.try_send(WorkerClose::Close(Some(error.clone())));
                    let _ = reply.send(Err(error));
                    return;
                }
                let Some(key) = key else { return };
                self.next_producer = key;
                self.producers.push(Producer {
                    key,
                    identity,
                    admission,
                    authorization,
                    close,
                    closed: false,
                });
                let _ = reply.send(Ok(()));
            }
            Event::RegisterController {
                identity,
                authorization,
                close,
                reply,
            } => {
                let valid = same_connection(&self.connection, identity.connection_identity())
                    && identity.is_active()
                    && !self
                        .controllers
                        .iter()
                        .any(|row| row.identity.same_controller(&identity));
                let logical = if valid && self.controllers.len() < MAX_NATIVE_TRANSACTIONS {
                    self.registry.controller().ok()
                } else {
                    None
                };
                let Some(logical) = logical else {
                    let error = refused("the transaction controller admission is unavailable");
                    let _ = close.try_send(WorkerClose::Close(Some(error.clone())));
                    let _ = reply.send(Err(error));
                    return;
                };
                self.controllers.push(Controller {
                    identity,
                    logical,
                    authorization,
                    close,
                    closed: false,
                });
                let _ = reply.send(Ok(()));
            }
            Event::Posting { source, receipt } => self.posting(source, receipt),
            Event::Control { source, request } => self.control(source, request),
            Event::WorkerStopped { source, reply } => {
                match source {
                    WorkerIdentity::Controller(identity) => self.close_controller(&identity, None),
                    WorkerIdentity::Producer(identity) => self.close_producer(&identity, None),
                    WorkerIdentity::Consumer(identity) => self.close_consumer(&identity, None),
                }
                let _ = reply.send(());
            }
            Event::StopConnection { reply } => {
                self.close();
                let _ = reply.send(());
            }
        }
        self.kick();
    }

    pub(super) async fn next_operation(&mut self) -> Option<Operation> {
        if self.operations.is_empty() {
            pending().await
        } else {
            self.operations.next().await
        }
    }

    pub(super) fn accept_completion(&mut self, operation: Operation) {
        if let Some(key) = operation.key()
            && let Some(group) = self.groups.get_mut(&key)
            && group.operations > 0
        {
            group.operations -= 1;
        }
        if self.closed {
            return;
        }
        match operation {
            Operation::Declared { key, result } => match result {
                Ok(identity) => {
                    if let Some(group) = self.groups.get_mut(&key) {
                        if identity
                            .controller_identity()
                            .same_controller(&group.controller)
                            && identity.transaction_id() == &group.id
                        {
                            group.native = Some(identity);
                        } else {
                            self.fail_group(key);
                        }
                    }
                }
                Err(_) => {
                    self.abort_group(key, Some(retirements::unavailable_error()));
                    self.fail_and_close_controller(key);
                }
            },
            Operation::PostingChecked {
                key,
                producer,
                receipt,
                result,
            } => {
                self.checked_posting(key, producer, *receipt, result);
            }
            Operation::Prepared {
                key,
                producer,
                result,
            } => {
                let valid = self
                    .producers
                    .iter()
                    .find(|row| row.key == producer)
                    .is_some_and(|row| !row.closed && row.identity.is_active());
                if let Some(group) = self.groups.get_mut(&key) {
                    group.posting_busy = false;
                }
                match result {
                    Ok(posting)
                        if valid
                            && self
                                .groups
                                .get(&key)
                                .is_some_and(|group| !group.refused && !group.ending) =>
                    {
                        let exact = self
                            .producers
                            .iter()
                            .find(|row| row.key == producer)
                            .is_some_and(|row| posting.belongs_to_receiver(&row.identity));
                        if exact {
                            if let Some(group) = self.groups.get_mut(&key) {
                                group.prepared.push(NativePreparedWork::Posting(posting));
                            }
                        } else {
                            self.fail_group(key);
                        }
                    }
                    _ if self.ignore_obsolete_work(key) => {}
                    _ => self.fail_group(key),
                }
            }
            Operation::RetirementChecked {
                key,
                consumer,
                receipt,
                result,
            } => {
                self.checked_retirement(key, consumer, receipt, result);
            }
            Operation::RetirementPrepared {
                key,
                consumer,
                result,
            } => {
                self.prepared_retirement(key, consumer, result);
            }
            Operation::Ready {
                key,
                receipt,
                result,
            } => {
                let obsolete = self
                    .groups
                    .get(&key)
                    .is_none_or(|group| group.rollback || group.ending);
                if result.is_err() && !obsolete {
                    self.fail_group(key);
                }
                if let Some(group) = self.groups.get_mut(&key) {
                    group.waiting_ready = false;
                    if !obsolete {
                        group.ready = result.is_ok();
                        group.sealed = Some(receipt);
                    }
                }
            }
            Operation::Authorized { key, result } => {
                if let Some(group) = self.groups.get_mut(&key) {
                    group.handoff_busy = false;
                }
                if self.ignore_obsolete_work(key) {
                    self.reap_rows();
                    self.kick();
                    return;
                }
                match result {
                    Ok(expiry) => self.handoff(key, expiry),
                    Err(error) => {
                        self.abort_group(key, Some(error.clone()));
                        self.close_group_producers(key, error);
                    }
                }
            }
            Operation::Applied { key, result } => match result {
                Ok(completion) => {
                    let (application, resources) = completion.into_parts();
                    let completion = retirements::owner_completion(&application);
                    self.push(key, async move {
                        Operation::Finished {
                            key,
                            result: resources.finish().await,
                            completion,
                        }
                    });
                }
                Err(_) => {
                    self.abort_group(key, Some(retirements::unavailable_error()));
                    self.fail_and_close_controller(key);
                }
            },
            Operation::Finished {
                key,
                result,
                completion,
            } => {
                if result.is_err() {
                    let error = match &completion {
                        RetirementCompletion::Refused(error) => error.clone(),
                        _ => retirements::unavailable_error(),
                    };
                    self.abort_group(key, Some(error));
                    self.fail_and_close_controller(key);
                } else {
                    self.complete_retirements(key, completion);
                }
                if let Some(group) = self.groups.get_mut(&key) {
                    group.ending = true;
                }
            }
            Operation::DeclarationRefused { controller, result } => {
                if result.is_err() {
                    self.close_controller(
                        &controller,
                        Some(refused("the transaction control response failed")),
                    );
                }
            }
        }
        self.reap_rows();
        self.kick();
    }

    pub(super) fn tick(&mut self) {
        self.tick_at(Instant::now());
    }

    pub(super) fn tick_at(&mut self, now: Instant) {
        if self.closed {
            return;
        }
        self.registry.expire();
        let expired: Vec<_> = self
            .groups
            .iter()
            .filter_map(|(&key, group)| {
                let logical = self
                    .controllers
                    .iter()
                    .find(|row| row.identity.same_controller(&group.controller))?;
                let state = self.registry.state(&logical.logical, &group.id).ok();
                (now >= group.deadline
                    && matches!(
                        state,
                        Some(AtomicCommitState::Pending | AtomicCommitState::Aborted)
                    ))
                .then_some(key)
            })
            .collect();
        for key in expired {
            self.fail_group(key);
            if let Some(group) = self.groups.get(&key) {
                let identity = group.controller.clone();
                self.close_controller(
                    &identity,
                    Some(AmqpProtocolError::new(
                        ErrorCondition::Custom(Symbol::from("amqp:transaction:timeout")),
                        "the local transaction declaration deadline elapsed",
                        None,
                    )),
                );
            }
        }
        let dead_controllers: Vec<_> = self
            .controllers
            .iter()
            .filter(|row| !row.closed && !row.identity.is_active())
            .map(|row| row.identity.clone())
            .collect();
        for identity in dead_controllers {
            self.close_controller(&identity, None);
        }
        let dead_producers: Vec<_> = self
            .producers
            .iter()
            .filter(|row| !row.closed && !row.identity.is_active())
            .map(|row| row.identity.clone())
            .collect();
        for identity in dead_producers {
            self.close_producer(&identity, None);
        }
        let dead_consumers: Vec<_> = self
            .consumers
            .iter()
            .filter(|row| !row.closed && !row.identity.is_active())
            .map(|row| row.identity.clone())
            .collect();
        for identity in dead_consumers {
            self.close_consumer(&identity, None);
        }
        let faulted: Vec<_> =
            self.groups
                .iter()
                .filter_map(|(&key, group)| {
                    (!group.submitted
                        && !group.refused
                        && group.native.as_ref().is_some_and(|native| {
                            native.state() == NativeTransactionState::Faulted
                        }))
                    .then_some(key)
                })
                .collect();
        for key in faulted {
            self.fail_group(key);
        }
        self.reap_rows();
        self.kick();
    }

    pub(super) fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        // Publish pending logical aborts before any owned native future drops.
        self.registry.close();
        let keys: Vec<_> = self.groups.keys().copied().collect();
        for key in keys {
            self.complete_retirements(
                key,
                RetirementCompletion::Refused(refused("the atomic connection is closing")),
            );
        }
        for controller in &mut self.controllers {
            controller.closed = true;
            let _ = controller.close.try_send(WorkerClose::Close(None));
        }
        for producer in &mut self.producers {
            producer.closed = true;
            let _ = producer.close.try_send(WorkerClose::Close(None));
        }
        for consumer in &mut self.consumers {
            consumer.closed = true;
            let _ = consumer.close.try_send(WorkerClose::Close(None));
        }
    }

    fn control(&mut self, source: NativeControllerIdentity, request: CoordinatorRequest) {
        let controller = self
            .controllers
            .iter()
            .find(|row| !row.closed && row.identity.same_controller(&source));
        let Some(controller) = controller else { return };
        if !source.is_active() || !same_connection(&self.connection, source.connection_identity()) {
            self.close_controller(&source, None);
            return;
        }
        let logical = controller.logical.clone();
        match request {
            CoordinatorRequest::Declare(receipt) => {
                if !receipt.controller_identity().same_controller(&source) {
                    self.close_controller(
                        &source,
                        Some(refused("the declaration controller does not match")),
                    );
                    return;
                }
                if self.operations.len() >= MAX_OPERATIONS {
                    self.close_controller(
                        &source,
                        Some(refused("transaction control work is full")),
                    );
                    return;
                }
                let deadline = Instant::now().checked_add(ATOMIC_TRANSACTION_TIMEOUT);
                let declared = if self.groups.len() < MAX_NATIVE_TRANSACTIONS && deadline.is_some()
                {
                    self.registry.declare(&logical)
                } else {
                    self.refuse_declaration(
                        source,
                        receipt,
                        NativeDeclarationRefusal::ResourceLimit,
                    );
                    return;
                };
                let Ok(id) = declared else {
                    self.refuse_declaration(
                        source,
                        receipt,
                        NativeDeclarationRefusal::ResourceLimit,
                    );
                    return;
                };
                let (Some(key), Some(deadline)) = (id_key(&id), deadline) else {
                    let _ = self.registry.abort_pending(&logical, &id);
                    self.refuse_declaration(source, receipt, NativeDeclarationRefusal::Unavailable);
                    return;
                };
                self.groups
                    .insert(key, Group::new(id.clone(), source, deadline));
                self.push(key, async move {
                    Operation::Declared {
                        key,
                        result: receipt.declared(id).await,
                    }
                });
            }
            CoordinatorRequest::Discharge(receipt) => {
                let id = receipt.transaction_id().clone();
                let key = id_key(&id);
                let exact = key
                    .and_then(|key| self.groups.get(&key))
                    .is_some_and(|group| group.controller.same_controller(&source));
                if !exact {
                    if receipt.fail() && self.operations.len() < MAX_OPERATIONS {
                        // Native terminal fail=true cleanup is not an owner replay.
                        let _ = self.registry.discharge(&logical, &id, true);
                        self.operations.push(Box::pin(async move {
                            Operation::DeclarationRefused {
                                controller: source,
                                result: receipt.rollback().await,
                            }
                        }));
                    } else {
                        self.close_controller(
                            &source,
                            Some(refused("the transaction control identifier is unavailable")),
                        );
                    }
                    return;
                }
                let Some(key) = key else { return };
                if receipt.fail() {
                    let live_manifest = receipt.retirement_origins().is_some()
                        && receipt.posting_receivers().is_some();
                    if let Some(group) = self.groups.get_mut(&key) {
                        group.rollback = true;
                        group.rollback_origins = receipt.retirement_origins();
                        group.rollback_postings = receipt.posting_receivers();
                    }
                    let _ = self.registry.discharge(&logical, &id, true);
                    let error = (!live_manifest)
                        .then(|| refused("terminal rollback cannot rearm an active delivery"));
                    self.abort_group(key, error);
                }
                let Some(group) = self.groups.get_mut(&key) else {
                    return;
                };
                let duplicate = group.submitted
                    || (!receipt.fail() && (group.sealed.is_some() || group.waiting_ready))
                    || (receipt.fail() && group.sealed.as_ref().is_some_and(|prior| prior.fail()));
                if duplicate {
                    self.fail_and_close_controller(key);
                    return;
                }
                group.sealed = Some(receipt);
            }
        }
    }

    fn kick(&mut self) {
        if self.closed {
            return;
        }
        let keys: Vec<_> = self.groups.keys().copied().collect();
        for key in keys {
            if self.operations.len() >= MAX_OPERATIONS {
                break;
            }
            let rollback_ready = self.rollback_collectors_ready(key);
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };
            if group.ending || group.submitted || group.native.is_none() {
                continue;
            }
            if group.refused {
                if !group.waiting_ready
                    && !group.posting_busy
                    && (!group.rollback || group.operations == 0)
                    && rollback_ready
                    && let Some(receipt) = group.sealed.take()
                {
                    group.ending = true;
                    let receipt_manifest =
                        group.rollback_origins.is_some() && group.rollback_postings.is_some();
                    self.push(key, async move {
                        let rollback = receipt.fail();
                        let result = if rollback {
                            receipt.rollback().await
                        } else {
                            receipt.refuse_staging().await
                        };
                        let completion = if rollback && receipt_manifest {
                            RetirementCompletion::Rearmed
                        } else {
                            RetirementCompletion::Refused(refused(
                                "transaction staging was refused",
                            ))
                        };
                        Operation::Finished {
                            key,
                            result,
                            completion,
                        }
                    });
                }
                continue;
            }
            if !group.waiting_ready
                && !group.ready
                && let Some(receipt) = group.sealed.take()
            {
                group.waiting_ready = true;
                self.push(key, async move {
                    let result = receipt.wait_ready().await;
                    Operation::Ready {
                        key,
                        receipt,
                        result,
                    }
                });
            }
            if self.operations.len() >= MAX_OPERATIONS {
                break;
            }
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };
            if !group.posting_busy
                && let Some(work) = group.queued.pop_front()
            {
                group.posting_busy = true;
                self.start_work(key, work);
            }
            if self.operations.len() >= MAX_OPERATIONS {
                break;
            }
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };
            if group.ready
                && !group.posting_busy
                && group.queued.is_empty()
                && group.sealed.is_some()
                && !group.handoff_busy
            {
                group.handoff_busy = true;
                let authorizations: Vec<_> = group
                    .producers
                    .iter()
                    .filter_map(|key| {
                        self.producers
                            .iter()
                            .find(|row| row.key == *key)
                            .and_then(|row| row.authorization.clone())
                    })
                    .chain(group.consumers.iter().filter_map(|key| {
                        self.consumers
                            .iter()
                            .find(|row| row.key == *key)
                            .and_then(|row| row.authorization.clone())
                    }))
                    .collect();
                let controller_authorization = self
                    .controllers
                    .iter()
                    .find(|row| row.identity.same_controller(&group.controller))
                    .and_then(|row| row.authorization.clone());
                self.push(key, async move {
                    let result = async {
                        let mut expiry = match controller_authorization {
                            Some(authorization) => Some(
                                authorization
                                    .any_grant_claim_expiry_epoch_seconds()
                                    .await
                                    .map_err(|_| {
                                        super::super::unauthorized_error(
                                            "the controller's authorization has expired",
                                        )
                                    })?,
                            ),
                            None => None,
                        };
                        for authorization in authorizations {
                            let producer_expiry =
                                authorization.claim_expiry_epoch_seconds().await?;
                            expiry = Some(
                                expiry
                                    .map_or(producer_expiry, |expiry| expiry.min(producer_expiry)),
                            );
                        }
                        Ok(expiry)
                    }
                    .await;
                    Operation::Authorized { key, result }
                });
            }
        }
    }

    fn handoff(&mut self, key: u64, claim_expiry: Option<u64>) {
        let valid = self.groups.get(&key).is_some_and(|group| {
            group.ready
                && !group.refused
                && !group.submitted
                && !group.ending
                && !group.posting_busy
                && group.queued.is_empty()
                && group.producers.iter().all(|key| {
                    self.producers
                        .iter()
                        .any(|row| row.key == *key && !row.closed && row.identity.is_active())
                })
                && self.retirement_sources_valid(key, group)
        });
        if !valid {
            self.fail_group(key);
            return;
        }
        let Some(group) = self.groups.get(&key) else {
            return;
        };
        let Some(controller) = self
            .controllers
            .iter()
            .find(|row| row.identity.same_controller(&group.controller))
        else {
            return;
        };
        let mut logical = match self
            .registry
            .discharge(&controller.logical, &group.id, false)
        {
            Ok(AtomicTransactionDischarge::Submit(submission)) => submission,
            _ => {
                self.fail_group(key);
                return;
            }
        };
        let Some(group) = self.groups.get_mut(&key) else {
            return;
        };
        if let Some(expiry) = claim_expiry {
            logical.restrict_claim_expiry_epoch_seconds(expiry);
        }
        let Some(sealed) = group.sealed.take() else {
            logical.permit().abort();
            self.fail_group(key);
            return;
        };
        let postings = std::mem::take(&mut group.prepared);
        let native = match sealed.prepare_work(postings) {
            Ok(native) => native,
            Err(_) => {
                logical.permit().abort();
                self.fail_and_close_controller(key);
                return;
            }
        };
        group.submitted = true;
        let submission = OwnedNativeAtomicMessagingSubmission::new(native, logical);
        // Factory capture arms cancellation before this future can be dropped.
        let applied = self.broker.submit_native_atomic_messaging_owned(submission);
        self.push(key, async move {
            Operation::Applied {
                key,
                result: applied.await,
            }
        });
    }

    fn push(
        &mut self,
        key: u64,
        operation: impl std::future::Future<Output = Operation> + Send + 'static,
    ) {
        if let Some(group) = self.groups.get_mut(&key) {
            group.operations += 1;
        }
        self.operations.push(Box::pin(operation));
    }

    fn refuse_declaration(
        &mut self,
        controller: NativeControllerIdentity,
        receipt: amqp::PendingDeclareReceipt,
        reason: NativeDeclarationRefusal,
    ) {
        self.operations.push(Box::pin(async move {
            Operation::DeclarationRefused {
                controller,
                result: receipt.refuse(reason).await,
            }
        }));
    }

    fn refuse_posting(
        &mut self,
        key: u64,
        producer: u64,
        receipt: amqp::TransactionPostingReceipt,
        error: AmqpProtocolError,
    ) {
        self.fail_group(key);
        receipt.fail();
        if let Some(row) = self.producers.iter().find(|row| row.key == producer) {
            let identity = row.identity.clone();
            self.close_producer(&identity, Some(error));
        }
        if let Some(group) = self.groups.get_mut(&key) {
            group.posting_busy = false;
        }
    }

    fn fail_group(&mut self, key: u64) {
        self.abort_group(key, Some(refused("transactional queue work was refused")));
    }

    fn abort_group(&mut self, key: u64, error: Option<AmqpProtocolError>) {
        let Some(group) = self.groups.get(&key) else {
            return;
        };
        if let Some(controller) = self
            .controllers
            .iter()
            .find(|row| row.identity.same_controller(&group.controller))
        {
            let _ = self.registry.abort_pending(&controller.logical, &group.id);
        }
        // Submitted work may still be Pending. The same CAS protects the gap
        // between native owner claim and logical owner claim; Started is inert.
        let submitted = group.submitted;
        if let Some(group) = self.groups.get_mut(&key)
            && !submitted
        {
            group.refused = true;
            group.queued.clear();
            group.prepared.clear();
        }
        if let Some(error) = error {
            self.complete_retirements(key, RetirementCompletion::Refused(error));
        }
    }

    fn fail_and_close_controller(&mut self, key: u64) {
        self.fail_group(key);
        if let Some(group) = self.groups.get(&key) {
            let identity = group.controller.clone();
            self.close_controller(
                &identity,
                Some(refused("the transaction controller is closing")),
            );
        }
    }

    fn close_controller(
        &mut self,
        identity: &NativeControllerIdentity,
        error: Option<AmqpProtocolError>,
    ) {
        if let Some(controller) = self
            .controllers
            .iter_mut()
            .find(|row| row.identity.same_controller(identity))
        {
            let _ = self.registry.close_controller(&controller.logical);
            if !controller.closed {
                controller.closed = true;
                let _ = controller.close.try_send(WorkerClose::Close(error));
            }
        }
        let keys: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| group.controller.same_controller(identity))
            .map(|(&key, _)| key)
            .collect();
        for key in keys {
            self.fail_group(key);
            if let Some(group) = self.groups.get_mut(&key) {
                group.ending = true;
            }
        }
    }

    fn close_producer(
        &mut self,
        identity: &amqp::NativeReceiverIdentity,
        error: Option<AmqpProtocolError>,
    ) {
        let producer = self
            .producers
            .iter()
            .find(|row| row.identity.same_receiver(identity))
            .map(|row| row.key);
        let Some(producer) = producer else { return };
        let keys: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| {
                group.producers.contains(&producer)
                    || group.rollback_postings.as_ref().is_some_and(|origins| {
                        origins.iter().any(|source| source.same_receiver(identity))
                    })
            })
            .map(|(&key, _)| key)
            .collect();
        for key in keys {
            self.fail_group(key);
        }
        if let Some(row) = self.producers.iter_mut().find(|row| row.key == producer)
            && !row.closed
        {
            row.closed = true;
            let _ = row.close.try_send(WorkerClose::Close(error));
        }
    }

    fn close_group_producers(&mut self, key: u64, error: AmqpProtocolError) {
        let identities: Vec<_> = self
            .groups
            .get(&key)
            .into_iter()
            .flat_map(|group| &group.producers)
            .filter_map(|key| {
                self.producers
                    .iter()
                    .find(|row| row.key == *key)
                    .map(|row| row.identity.clone())
            })
            .collect();
        for identity in identities {
            self.close_producer(&identity, Some(error.clone()));
        }
    }

    fn reap_rows(&mut self) {
        self.groups
            .retain(|_, group| !(group.ending && group.operations == 0));
        self.producers.retain(|row| {
            !row.closed
                || self.groups.values().any(|group| {
                    group.producers.contains(&row.key)
                        || group.rollback_postings.as_ref().is_some_and(|origins| {
                            origins
                                .iter()
                                .any(|source| source.same_receiver(&row.identity))
                        })
                })
        });
        self.consumers.retain(|row| {
            !row.closed
                || self.groups.values().any(|group| {
                    group.consumers.contains(&row.key)
                        || group.rollback_origins.as_ref().is_some_and(|origins| {
                            origins
                                .iter()
                                .any(|(source, _)| source.same_sender(&row.identity))
                        })
                })
        });
        self.controllers.retain(|row| {
            !row.closed
                || self
                    .groups
                    .values()
                    .any(|group| group.controller.same_controller(&row.identity))
        });
    }
}

impl<B: NativeAtomicBroker> Drop for Owner<B> {
    fn drop(&mut self) {
        self.close();
    }
}

fn refused(description: &'static str) -> AmqpProtocolError {
    AmqpProtocolError::new(AmqpError::ResourceLimitExceeded, description, None)
}

fn staging_error(error: &crate::AtomicTransactionRegistryError) -> AmqpProtocolError {
    use crate::{AtomicMessagingWorkError, AtomicTransactionRegistryError};
    let condition = match error {
        AtomicTransactionRegistryError::Work(AtomicMessagingWorkError::Input(error)) => {
            crate::condition::condition_for(error)
        }
        AtomicTransactionRegistryError::UnsupportedTarget
        | AtomicTransactionRegistryError::BindingMismatch => crate::condition::NOT_ALLOWED,
        _ => crate::condition::RESOURCE_LIMIT_EXCEEDED,
    };
    AmqpProtocolError::new(
        ErrorCondition::Custom(Symbol::from(condition)),
        "transactional queue staging was refused",
        None,
    )
}
