use amqp::{
    NativeOutgoingDeliveryIdentity, NativeSenderIdentity, Outcome, PreparedRetirement,
    TransactionRetirementReceipt,
};
use domain::{CommandKind, SettlementDisposition};
use tokio::sync::{mpsc, oneshot};

use super::super::groups::{HeldDelivery, Retirement, RetirementWaiter};
use super::super::{LinkAuthorization, QueueAdmission};
use super::*;

impl<B: NativeAtomicBroker> Owner<B> {
    pub(super) fn register_consumer(
        &mut self,
        identity: NativeSenderIdentity,
        admission: QueueAdmission,
        authorization: Option<LinkAuthorization>,
        close: mpsc::Sender<WorkerClose>,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        let valid = same_connection(&self.connection, identity.connection_identity())
            && identity.is_active()
            && !admission.config.requires_session
            && admission.binding.kind() == EntityIncarnationKind::Queue
            && admission.binding.target() == admission.binding.owner()
            && !admission.binding.target().is_dead_letter_queue()
            && !admission.binding.target().is_subscription_path()
            && !self
                .consumers
                .iter()
                .any(|row| row.identity.same_sender(&identity));
        let key = self.next_consumer.checked_add(1);
        if !valid || key.is_none() || self.producers.len() + self.consumers.len() >= MAX_LINKS {
            let error = refused("the transactional consumer admission is unavailable");
            let _ = close.try_send(WorkerClose::Close(Some(error.clone())));
            let _ = reply.send(Err(error));
            return;
        }
        let Some(key) = key else { return };
        self.next_consumer = key;
        self.consumers.push(Consumer {
            key,
            identity,
            admission,
            authorization,
            close,
            closed: false,
            held: None,
            pending: None,
        });
        let _ = reply.send(Ok(()));
    }

    pub(super) fn register_held(
        &mut self,
        source: NativeSenderIdentity,
        delivery: HeldDelivery,
        reply: oneshot::Sender<Result<(), AmqpProtocolError>>,
    ) {
        let row = self
            .consumers
            .iter_mut()
            .find(|row| row.identity.same_sender(&source));
        if let Some(row) = row
            && !row.closed
            && row.pending.is_none()
            && row.held.is_none()
            && delivery.original.belongs_to_sender(&row.identity)
        {
            row.held = Some(delivery);
            let _ = reply.send(Ok(()));
            return;
        }
        let error = refused("the held queue delivery registration was refused");
        self.close_consumer(&source, Some(error.clone()));
        let _ = reply.send(Err(error));
    }

    pub(super) fn clear_held(
        &mut self,
        source: NativeSenderIdentity,
        original: NativeOutgoingDeliveryIdentity,
        reply: oneshot::Sender<()>,
    ) {
        let row = self
            .consumers
            .iter_mut()
            .find(|row| row.identity.same_sender(&source));
        if let Some(row) = row
            && !row.closed
            && row.pending.is_none()
            && row
                .held
                .as_ref()
                .is_some_and(|held| held.original.same_delivery(&original))
        {
            row.held = None;
            let _ = reply.send(());
            return;
        }
        self.close_consumer(
            &source,
            Some(refused("the held delivery origin does not match")),
        );
    }

    pub(super) fn retirement(
        &mut self,
        source: NativeSenderIdentity,
        receipt: TransactionRetirementReceipt,
        reply: oneshot::Sender<RetirementCompletion>,
    ) {
        let key = id_key(receipt.transaction_id());
        let consumer = self
            .consumers
            .iter()
            .find(|row| row.identity.same_sender(&source));
        let exact_group = key
            .and_then(|key| self.groups.get(&key))
            .is_some_and(|group| {
                group
                    .controller
                    .same_controller(receipt.controller_identity())
            });
        let rollback = key
            .and_then(|key| self.groups.get(&key))
            .is_some_and(|group| {
                group.rollback
                    && !group.ending
                    && group.rollback_origins.as_ref().is_some_and(|origins| {
                        origins.iter().any(|(owner, original)| {
                            owner.same_sender(&source)
                                && original.same_delivery(receipt.delivery_identity())
                        })
                    })
            });
        let closed_origin = rollback
            && consumer.is_some_and(|row| {
                (row.closed || !row.identity.is_active())
                    && row.held.as_ref().is_some_and(|held| {
                        held.original.same_delivery(receipt.delivery_identity())
                    })
            });
        if closed_origin {
            if let Some(group) = key.and_then(|key| self.groups.get_mut(&key))
                && !group
                    .seen_retirements
                    .iter()
                    .any(|seen| seen.same_delivery(receipt.delivery_identity()))
            {
                group
                    .seen_retirements
                    .push(receipt.delivery_identity().clone());
            }
            let _ = reply.send(RetirementCompletion::Refused(refused(
                "the retirement source is closed",
            )));
            return;
        }
        let valid = exact_group
            && consumer.is_some_and(|row| {
                !row.closed
                    && row.pending.is_none()
                    && row.held.as_ref().is_some_and(|held| {
                        held.original.same_delivery(receipt.delivery_identity())
                    })
                    && receipt.belongs_to_sender(&row.identity)
            })
            && key
                .and_then(|key| self.groups.get(&key))
                .is_some_and(|group| {
                    !group.submitted
                        && !group.ending
                        && (rollback || !group.refused)
                        && !group
                            .seen_retirements
                            .iter()
                            .any(|original| original.same_delivery(receipt.delivery_identity()))
                        && group.seen_retirements.len() < MAX_NATIVE_TRANSACTION_POSTINGS
                        && (rollback
                            || group.queued.len()
                                + group.prepared.len()
                                + usize::from(group.posting_busy)
                                < MAX_NATIVE_TRANSACTION_POSTINGS)
                });
        if !valid {
            let error = refused("transactional retirement admission was refused");
            if exact_group && let Some(key) = key {
                self.abort_group(key, Some(error.clone()));
            }
            if receipt.belongs_to_sender(&source) {
                self.close_consumer(&source, Some(error.clone()));
            }
            let _ = reply.send(RetirementCompletion::Refused(error));
            return;
        }
        let (Some(key), Some(consumer)) = (key, consumer.map(|row| row.key)) else {
            return;
        };
        let original = receipt.delivery_identity().clone();
        if let Some(row) = self.consumers.iter_mut().find(|row| row.key == consumer) {
            row.pending = Some(key);
        }
        if let Some(group) = self.groups.get_mut(&key) {
            if !group.consumers.contains(&consumer) {
                group.consumers.push(consumer);
            }
            group.seen_retirements.push(original.clone());
            group.retirements.push(RetirementWaiter {
                consumer,
                original,
                reply,
            });
            if !rollback {
                group
                    .queued
                    .push_back(QueuedWork::Retirement(Retirement { consumer, receipt }));
            }
        }
        // The aborted attempt is accounted for but never staged or provisionally accepted.
    }

    pub(super) fn start_work(&mut self, key: u64, work: QueuedWork) {
        match work {
            QueuedWork::Posting(posting) => {
                let producer = posting.producer;
                let authorization = self
                    .producers
                    .iter()
                    .find(|row| row.key == producer)
                    .and_then(|row| row.authorization.clone());
                self.push(key, async move {
                    let receipt = posting.receipt;
                    let authorized = match authorization {
                        Some(auth) => auth.ensure().await,
                        None => Ok(()),
                    };
                    let result = authorized.and_then(|()| {
                        read_ingress(receipt.message(), receipt.message_format()).map_err(|error| {
                            AmqpProtocolError::new(
                                ErrorCondition::Custom(Symbol::from(error.condition())),
                                "transactional message encoding was refused",
                                None,
                            )
                        })
                    });
                    Operation::PostingChecked {
                        key,
                        producer,
                        receipt,
                        result,
                    }
                });
            }
            QueuedWork::Retirement(retirement) => {
                let consumer = retirement.consumer;
                let authorization = self
                    .consumers
                    .iter()
                    .find(|row| row.key == consumer)
                    .and_then(|row| row.authorization.clone());
                self.push(key, async move {
                    let result = match authorization {
                        Some(auth) => auth.ensure().await,
                        None => Ok(()),
                    };
                    Operation::RetirementChecked {
                        key,
                        consumer,
                        receipt: retirement.receipt,
                        result,
                    }
                });
            }
        }
    }

    pub(super) fn checked_retirement(
        &mut self,
        key: u64,
        consumer: u64,
        receipt: TransactionRetirementReceipt,
        result: Result<(), AmqpProtocolError>,
    ) {
        if self.ignore_obsolete_work(key) {
            if let Some(group) = self.groups.get_mut(&key) {
                group.posting_busy = false;
            }
            return;
        }
        let row = self.consumers.iter().find(|row| row.key == consumer);
        let valid = row.is_some_and(|row| {
            !row.closed
                && row.pending == Some(key)
                && receipt.belongs_to_sender(&row.identity)
                && row
                    .held
                    .as_ref()
                    .is_some_and(|held| held.original.same_delivery(receipt.delivery_identity()))
        }) && self.groups.get(&key).is_some_and(|group| {
            !group.refused
                && !group.ending
                && !group.submitted
                && group
                    .controller
                    .same_controller(receipt.controller_identity())
        });
        let result = result.and_then(|()| {
            if valid {
                Ok(())
            } else {
                Err(refused("transactional retirement origin was refused"))
            }
        });
        if let Err(error) = result {
            self.refuse_retirement(key, consumer, error);
            return;
        }
        let (Some(row), Some(group)) = (row, self.groups.get(&key)) else {
            return;
        };
        let Some(held) = row.held.as_ref() else {
            return;
        };
        let Some(controller) = self
            .controllers
            .iter()
            .find(|row| row.identity.same_controller(&group.controller))
        else {
            return;
        };
        let kind = CommandKind::Settle {
            sequence: held.sequence,
            lock_token: held.token,
            disposition: SettlementDisposition::Complete,
            properties_to_modify: BTreeMap::new(),
        };
        if !matches!(receipt.outcome(), Outcome::Accepted(_)) {
            self.refuse_retirement(
                key,
                consumer,
                refused("native retirement outcome was refused"),
            );
            return;
        }
        if let Err(error) = self.registry.try_stage(
            &controller.logical,
            &group.id,
            row.admission.binding.clone(),
            kind,
        ) {
            self.refuse_retirement(key, consumer, staging_error(&error));
            return;
        }
        self.push(key, async move {
            Operation::RetirementPrepared {
                key,
                consumer,
                result: receipt.provisional_accept().await,
            }
        });
    }

    pub(super) fn prepared_retirement(
        &mut self,
        key: u64,
        consumer: u64,
        result: Result<PreparedRetirement, amqp::EngineError>,
    ) {
        if let Some(group) = self.groups.get_mut(&key) {
            group.posting_busy = false;
        }
        if self.ignore_obsolete_work(key) {
            return;
        }
        let valid = self
            .consumers
            .iter()
            .find(|row| row.key == consumer)
            .is_some_and(|row| {
                !row.closed
                    && row.pending == Some(key)
                    && row.identity.is_active()
                    && result.as_ref().is_ok_and(|retirement| {
                        retirement.belongs_to_sender(&row.identity)
                            && row.held.as_ref().is_some_and(|held| {
                                held.original.same_delivery(retirement.delivery_identity())
                            })
                    })
            })
            && self.groups.get(&key).is_some_and(|group| {
                result.as_ref().is_ok_and(|retirement| {
                    retirement
                        .controller_identity()
                        .same_controller(&group.controller)
                        && retirement.transaction_id() == &group.id
                })
            });
        match result {
            Ok(retirement) if valid => {
                if let Some(group) = self.groups.get_mut(&key) {
                    group
                        .prepared
                        .push(NativePreparedWork::Retirement(retirement));
                }
            }
            _ => self.refuse_retirement(
                key,
                consumer,
                refused("retirement provisional response failed"),
            ),
        }
    }

    fn refuse_retirement(&mut self, key: u64, consumer: u64, error: AmqpProtocolError) {
        let current = self
            .consumers
            .iter()
            .find(|row| row.key == consumer)
            .is_some_and(|row| row.pending == Some(key));
        if current {
            self.abort_group(key, Some(error));
        }
        if let Some(group) = self.groups.get_mut(&key) {
            group.posting_busy = false;
        }
    }

    pub(super) fn ignore_obsolete_work(&self, key: u64) -> bool {
        self.groups.get(&key).is_none_or(|group| {
            group.refused
                || group.ending
                || (!group.submitted
                    && group
                        .native
                        .as_ref()
                        .is_some_and(|native| native.state() == NativeTransactionState::Aborted))
        })
    }

    pub(super) fn rollback_collectors_ready(&self, key: u64) -> bool {
        let Some(group) = self.groups.get(&key) else {
            return false;
        };
        if !group.rollback {
            return true;
        }
        let Some(origins) = &group.rollback_origins else {
            // Terminal replay has no captured live manifest and cannot rearm a held worker.
            return group.retirements.is_empty();
        };
        let retirements_ready = origins.iter().all(|(source, original)| {
            group
                .seen_retirements
                .iter()
                .any(|seen| seen.same_delivery(original))
                || !source.is_active()
                || self
                    .consumers
                    .iter()
                    .find(|row| row.identity.same_sender(source))
                    .is_some_and(|row| row.closed)
        });
        let postings_ready = group.rollback_postings.as_ref().is_some_and(|origins| {
            origins.iter().all(|source| {
                let expected = origins
                    .iter()
                    .filter(|origin| origin.same_receiver(source))
                    .count();
                let seen = group
                    .seen_postings
                    .iter()
                    .filter(|origin| origin.same_receiver(source))
                    .count();
                seen >= expected
                    || !source.is_active()
                    || self
                        .producers
                        .iter()
                        .find(|row| row.identity.same_receiver(source))
                        .is_some_and(|row| row.closed)
            })
        });
        retirements_ready && postings_ready
    }

    pub(super) fn retirement_sources_valid(&self, key: u64, group: &Group) -> bool {
        group.consumers.iter().all(|consumer| {
            self.consumers.iter().any(|row| {
                row.key == *consumer
                    && !row.closed
                    && row.identity.is_active()
                    && row.pending == Some(key)
                    && row.held.is_some()
            })
        })
    }

    pub(super) fn complete_retirements(&mut self, key: u64, completion: RetirementCompletion) {
        let waiters = self
            .groups
            .get_mut(&key)
            .map(|group| std::mem::take(&mut group.retirements))
            .unwrap_or_default();
        for waiter in waiters {
            let row = self
                .consumers
                .iter_mut()
                .find(|row| row.key == waiter.consumer);
            let exact = row.as_ref().is_some_and(|row| {
                row.pending == Some(key)
                    && row
                        .held
                        .as_ref()
                        .is_some_and(|held| held.original.same_delivery(&waiter.original))
            });
            if !exact {
                let _ = waiter.reply.send(RetirementCompletion::Refused(refused(
                    "retirement completion origin was refused",
                )));
                continue;
            }
            let Some(row) = row else { continue };
            row.pending = None;
            let completion = match &completion {
                RetirementCompletion::Committed => {
                    row.held = None;
                    RetirementCompletion::Committed
                }
                RetirementCompletion::Rearmed if !row.closed && row.identity.is_active() => {
                    RetirementCompletion::Rearmed
                }
                RetirementCompletion::Refused(error) => {
                    row.closed = true;
                    let _ = row.close.try_send(WorkerClose::Close(Some(error.clone())));
                    completion.clone()
                }
                RetirementCompletion::Rearmed => {
                    row.closed = true;
                    let error = refused("the retirement source is closed");
                    let _ = row.close.try_send(WorkerClose::Close(Some(error.clone())));
                    RetirementCompletion::Refused(error)
                }
            };
            let _ = waiter.reply.send(completion);
        }
    }

    pub(super) fn close_consumer(
        &mut self,
        identity: &NativeSenderIdentity,
        error: Option<AmqpProtocolError>,
    ) {
        let Some(consumer) = self
            .consumers
            .iter()
            .find(|row| row.identity.same_sender(identity))
            .map(|row| row.key)
        else {
            return;
        };
        let keys: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| {
                group.consumers.contains(&consumer)
                    || group.rollback_origins.as_ref().is_some_and(|origins| {
                        origins
                            .iter()
                            .any(|(source, _)| source.same_sender(identity))
                    })
            })
            .map(|(&key, _)| key)
            .collect();
        for key in keys {
            self.abort_group(
                key,
                Some(
                    error
                        .clone()
                        .unwrap_or_else(|| refused("the retirement source is closing")),
                ),
            );
        }
        if let Some(row) = self.consumers.iter_mut().find(|row| row.key == consumer)
            && !row.closed
        {
            row.closed = true;
            let _ = row.close.try_send(WorkerClose::Close(error));
        }
    }
}

pub(super) fn owner_completion(
    application: &Result<domain::AtomicMessagingApplication, crate::NativeAtomicOwnerError>,
) -> RetirementCompletion {
    let Err(error) = application else {
        return RetirementCompletion::Committed;
    };
    let condition = match error {
        crate::NativeAtomicOwnerError::Refused(error) => crate::condition::condition_for(error),
        crate::NativeAtomicOwnerError::NativeClaim(_)
        | crate::NativeAtomicOwnerError::LogicalClaim(_) => crate::condition::NOT_ALLOWED,
        _ => crate::condition::INTERNAL_ERROR,
    };
    RetirementCompletion::Refused(AmqpProtocolError::new(
        ErrorCondition::Custom(Symbol::from(condition)),
        "atomic queue retirement was refused",
        None,
    ))
}

pub(super) fn unavailable_error() -> AmqpProtocolError {
    AmqpProtocolError::new(
        AmqpError::InternalError,
        "the native atomic completion is unavailable",
        None,
    )
}
