use super::*;

impl<B: NativeAtomicBroker> Owner<B> {
    pub(super) fn posting(
        &mut self,
        source: amqp::NativeReceiverIdentity,
        receipt: amqp::TransactionPostingReceipt,
    ) {
        let owns_source = receipt.belongs_to_receiver(&source);
        let key = id_key(receipt.transaction_id());
        let rollback = key
            .and_then(|key| self.groups.get(&key))
            .is_some_and(|group| {
                group.rollback
                    && !group.ending
                    && group
                        .controller
                        .same_controller(receipt.controller_identity())
                    && group.rollback_postings.as_ref().is_some_and(|origins| {
                        let expected = origins
                            .iter()
                            .filter(|origin| origin.same_receiver(&source))
                            .count();
                        let seen = group
                            .seen_postings
                            .iter()
                            .filter(|origin| origin.same_receiver(&source))
                            .count();
                        seen < expected
                    })
            });
        if rollback
            && self
                .producers
                .iter()
                .any(|row| row.identity.same_receiver(&source))
        {
            if let Some(group) = key.and_then(|key| self.groups.get_mut(&key)) {
                group.seen_postings.push(source);
            }
            // This private event came from the registered receiver worker. A true
            // seal already captured it; rollback must not stage or close its source.
            return;
        }
        let producer = self
            .producers
            .iter()
            .find(|row| !row.closed && row.identity.same_receiver(&source));
        let exact = producer.is_some_and(|row| receipt.belongs_to_receiver(&row.identity))
            && key
                .and_then(|key| self.groups.get(&key))
                .is_some_and(|group| {
                    group
                        .controller
                        .same_controller(receipt.controller_identity())
                        && !group.refused
                        && !group.ending
                        && !group.submitted
                        && group.queued.len()
                            + group.prepared.len()
                            + usize::from(group.posting_busy)
                            < MAX_NATIVE_TRANSACTION_POSTINGS
                });
        if !exact {
            if let Some(key) = key
                && self.groups.get(&key).is_some_and(|group| {
                    group
                        .controller
                        .same_controller(receipt.controller_identity())
                })
            {
                self.fail_group(key);
            }
            receipt.fail();
            if owns_source {
                self.close_producer(
                    &source,
                    Some(refused("transactional posting admission was refused")),
                );
            }
            return;
        }
        let (Some(producer), Some(key)) = (producer, key) else {
            return;
        };
        let producer = producer.key;
        if let Some(group) = self.groups.get_mut(&key) {
            if !group.producers.contains(&producer) {
                group.producers.push(producer);
            }
            group.seen_postings.push(source);
            group
                .queued
                .push_back(QueuedWork::Posting(Box::new(Posting { producer, receipt })));
        }
    }

    pub(super) fn checked_posting(
        &mut self,
        key: u64,
        producer: u64,
        receipt: amqp::TransactionPostingReceipt,
        result: Result<domain::CommandKind, AmqpProtocolError>,
    ) {
        if self.ignore_obsolete_work(key) {
            if let Some(group) = self.groups.get_mut(&key) {
                group.posting_busy = false;
            }
            return;
        }
        let group = self.groups.get(&key);
        let row = self
            .producers
            .iter()
            .find(|row| row.key == producer && !row.closed);
        let valid = group.is_some_and(|group| !group.refused && !group.ending && !group.submitted)
            && row.is_some_and(|row| receipt.belongs_to_receiver(&row.identity));
        let kind = match result {
            Ok(kind) if valid => kind,
            Ok(_) => {
                self.refuse_posting(
                    key,
                    producer,
                    receipt,
                    refused("transactional posting origin was refused"),
                );
                return;
            }
            Err(error) => {
                self.refuse_posting(key, producer, receipt, error);
                return;
            }
        };
        let (Some(group), Some(row)) = (self.groups.get(&key), row) else {
            return;
        };
        let Some(controller) = self
            .controllers
            .iter()
            .find(|row| row.identity.same_controller(&group.controller))
        else {
            return;
        };
        let staged = self.registry.try_stage(
            &controller.logical,
            &group.id,
            row.admission.binding.clone(),
            kind,
        );
        if let Err(error) = staged {
            self.refuse_posting(key, producer, receipt, staging_error(&error));
            return;
        }
        self.push(key, async move {
            Operation::Prepared {
                key,
                producer,
                result: receipt.provisional_accept().await,
            }
        });
    }
}
