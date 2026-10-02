use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use domain::{
    AtomicMessagingApplication, CommandKind, CommandOutcome, EntityBinding, EntityPath,
    NamespaceName, RuleDefinition, SubscriptionName,
};

use crate::{
    AtomicCommitDecision, AtomicCommitPermit, AtomicTransactionSubmission, Attachment, Broker,
    BrokerRejection, EntityAdmission, EntityMetadata, NativeAtomicBroker,
    NativeAtomicBrokerCompletion, NativeAtomicOwnerError, NativeAtomicResponseUnavailable,
    NativeTransactionDecision, OwnedNativeAtomicMessagingSubmission,
};
use tokio::sync::oneshot;

type HandoffPause = (oneshot::Sender<AtomicCommitPermit>, oneshot::Receiver<()>);

#[derive(Clone, Default)]
pub(super) struct Recorder {
    pub(super) handoffs: Arc<AtomicUsize>,
    pub(super) claimed: Arc<AtomicUsize>,
    pub(super) bodies: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
    pause: Arc<Mutex<Option<HandoffPause>>>,
}

impl Recorder {
    pub(super) fn pause_handoff(
        &self,
    ) -> (oneshot::Receiver<AtomicCommitPermit>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, resumed) = oneshot::channel();
        assert!(
            self.pause
                .lock()
                .expect("handoff pause")
                .replace((entered, resumed))
                .is_none()
        );
        (observed, release)
    }
}

impl Broker for Recorder {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
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
        panic!("native owner tests never submit ordinary commands")
    }

    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("native owner tests never submit unfenced commands")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("native owner tests do not read topology")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("native owner tests do not read rules")
    }

    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("native owner tests do not read fenced rules")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("native owner tests do not receive messages")
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
        let recorder = self.clone();
        recorder.handoffs.fetch_add(1, Ordering::Relaxed);
        let pause = recorder.pause.lock().expect("handoff pause").take();
        let resume = pause.map(|(entered, resume)| {
            let _ = entered.send(submission.permit().clone());
            resume
        });
        async move {
            let _abort = abort;
            if let Some(resume) = resume {
                let _ = resume.await;
            }
            let (native, logical) = submission.into_submissions();
            let (ticket, mut work) = match logical {
                AtomicTransactionSubmission::Bound(submission) => {
                    let (_, ticket, work) = submission.into_owner_parts();
                    (ticket, work)
                }
                AtomicTransactionSubmission::Empty(submission) => submission.into_owner_parts(),
            };
            let (native_ticket, resources) = native.into_owner_parts();
            let native_claim = match native_ticket.try_claim() {
                Ok(claim) => claim,
                Err(error) => {
                    drop(ticket);
                    return Ok(NativeAtomicBrokerCompletion::from_owner_parts(
                        Err(NativeAtomicOwnerError::NativeClaim(error)),
                        resources,
                    ));
                }
            };
            let logical_claim = match ticket.try_claim() {
                Ok(claim) => claim,
                Err(error) => {
                    native_claim.abort();
                    return Ok(NativeAtomicBrokerCompletion::from_owner_parts(
                        Err(NativeAtomicOwnerError::LogicalClaim(error)),
                        resources,
                    ));
                }
            };
            work.with_commands(|commands| {
                let bodies = commands
                    .iter()
                    .flat_map(|command| match command {
                        CommandKind::SendEnvelope { body, .. } => vec![body.clone()],
                        CommandKind::SendBatch { messages } => messages
                            .iter()
                            .map(|message| message.body.clone())
                            .collect(),
                        other => panic!("unexpected native owner command: {other:?}"),
                    })
                    .collect();
                recorder.bodies.lock().expect("recorder mutex").push(bodies);
            })
            .expect("unique test work");
            recorder.claimed.fetch_add(1, Ordering::Relaxed);
            logical_claim.finish(AtomicCommitDecision::Committed);
            native_claim.finish(NativeTransactionDecision::Committed);
            Ok(NativeAtomicBrokerCompletion::from_owner_parts(
                Ok(AtomicMessagingApplication {
                    outcomes: Vec::new(),
                    enqueue_targets: Vec::new(),
                }),
                resources,
            ))
        }
    }
}
