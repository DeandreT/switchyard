use domain::{RuleDefinition, SubscriptionName};
use protocol_amqp::BrokerRejection;

use super::*;

fn rejection(error: SubmitError) -> BrokerRejection {
    match error {
        SubmitError::Propose(ProposeError::Broker(refused)) => BrokerRejection::Refused(refused),
        other => BrokerRejection::Unavailable(other.to_string()),
    }
}

impl protocol_amqp::Broker for BrokerHandle {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        BrokerHandle::bind(self, namespace, target)
            .await
            .map_err(rejection)
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        BrokerHandle::submit_fenced(self, binding, entity, kind)
            .await
            .map_err(rejection)
    }

    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        BrokerHandle::rules_fenced(self, binding, topic, subscription)
            .await
            .map_err(rejection)
    }

    async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        BrokerHandle::rules(self, namespace, topic, subscription)
            .await
            .map_err(rejection)
    }

    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        BrokerHandle::entity_metadata(self, namespace, target)
            .await
            .map_err(rejection)
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.watchers.watch(namespace, entity).wait()
    }

    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        BrokerHandle::submit(self, namespace, entity, kind)
            .await
            .map_err(rejection)
    }
}
