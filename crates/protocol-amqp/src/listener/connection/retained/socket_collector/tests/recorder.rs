use crate::{
    Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata, NativeAtomicBroker,
    NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable,
    OwnedNativeAtomicMessagingSubmission,
};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, RuleDefinition,
    SubscriptionName,
};
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Default)]
pub(super) struct Recorder {
    pub(super) binds: Arc<AtomicUsize>,
    pub(super) submitted: Arc<AtomicUsize>,
}
impl Broker for Recorder {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.binds.fetch_add(1, Ordering::SeqCst);
        Ok(Some(crate::broker::test_admission(
            namespace,
            target,
            EntityMetadata::Queue(domain::QueueConfig::default()),
        )))
    }
    async fn submit_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        Err(BrokerRejection::Refused(domain::BrokerError::QueueNotFound))
    }
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        Err(BrokerRejection::Refused(domain::BrokerError::QueueNotFound))
    }
    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        Ok(None)
    }
    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        Ok(Vec::new())
    }
    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        Ok(Vec::new())
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        pending::<()>().await;
    }
}
impl NativeAtomicBroker for Recorder {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl std::future::Future<
        Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>,
    > + Send
    + 'static {
        let abort = submission.permit().abort_on_drop();
        self.submitted.fetch_add(1, Ordering::SeqCst);
        async move {
            let _abort = abort;
            let _submission = submission;
            Err(NativeAtomicResponseUnavailable)
        }
    }
}
