use domain::{
    DeleteEntityTarget, EntityBinding, EntityIncarnationKind, QueueTimeToLiveUpdate,
    SubscriptionConfig, SubscriptionConfigUpdate, SubscriptionName,
};

use super::*;
use crate::{AtomSubscriptionOwnerError, SubmitError, atom_admin::xml::subscriptions};

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    /// Creates the default ordinary subscription and returns its admitted config,
    /// without a read after the existing atomic topology mutation.
    pub fn create_atom_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
        validate_names(topic, name)?;
        subscriptions::validate_config(&config)
            .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
        let parent = self
            .bind_admin_entity(namespace, &AdminTarget::Primary(topic.clone()))?
            .ok_or(BrokerError::TopicNotFound)?;
        if !matches!(parent.metadata, protocol_amqp::EntityMetadata::Topic(_)) {
            return Err(BrokerError::EntityKindMismatch.into());
        }
        let outcome = self.propose(
            namespace,
            topic,
            CommandKind::CreateSubscription {
                name: name.clone(),
                config,
            },
        )?;
        if outcome != CommandOutcome::SubscriptionCreated {
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{outcome:?}"),
            }
            .into());
        }
        Ok(config)
    }

    /// Replaces the complete supported definition without rewriting retained
    /// state or reading the store after the original atomic config update.
    pub fn update_atom_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, AtomSubscriptionOwnerError> {
        let admission = self.atom_subscription_admission(namespace, topic, name)?;
        validate_names(topic, name)?;
        if let Some((_, current)) = &admission {
            subscriptions::validate_config(current)
                .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
        }
        subscriptions::validate_config(&config)
            .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
        let kind = CommandKind::UpdateSubscription {
            name: name.clone(),
            update: complete_update(config),
        };
        let outcome = if let Some((binding, _)) = admission {
            self.propose_fenced_with_effects(&binding, topic, kind)?
                .outcome
        } else {
            // Preserve the original missing-topology and command-clock refusal
            // path. This planner does not prove orphan runtime or rule health.
            let outcome = self.propose(namespace, topic, kind)?;
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{outcome:?}"),
            }
            .into());
        };
        if outcome != CommandOutcome::SubscriptionUpdated {
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{outcome:?}"),
            }
            .into());
        }
        Ok(config)
    }

    /// Proves complete child topology and the parent's live identity in one
    /// clock-free owner turn; the wire projection does not expose rule contents.
    pub fn get_atom_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<SubscriptionConfig>, AtomSubscriptionOwnerError> {
        let Some((_, config)) = self.atom_subscription_admission(namespace, topic, name)? else {
            return Ok(None);
        };
        validate_names(topic, name)?;
        subscriptions::validate_config(&config)
            .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
        Ok(Some(config))
    }

    /// Captures the current child identity and applies its original fenced
    /// deletion in the same turn. Purge planning remains the domain's authority.
    pub(crate) fn delete_atom_subscription_with_effects(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<CommandApplication, AtomSubscriptionOwnerError> {
        let Some((binding, config)) = self.atom_subscription_admission(namespace, topic, name)?
        else {
            // The original planner checks orphan runtime/rules as well as the
            // absent topology. This refusal path still stamps one command.
            let application = self.propose_with_effects(
                namespace,
                topic,
                CommandKind::DeleteEntity {
                    target: DeleteEntityTarget::Subscription { name: name.clone() },
                },
            )?;
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{:?}", application.outcome),
            }
            .into());
        };
        validate_names(topic, name)?;
        subscriptions::validate_config(&config)
            .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
        let application = self.propose_fenced_with_effects(
            &binding,
            topic,
            CommandKind::DeleteEntity {
                target: DeleteEntityTarget::Subscription { name: name.clone() },
            },
        )?;
        if application.outcome != CommandOutcome::SubscriptionDeleted {
            return Err(ProposeError::UnexpectedOutcome {
                outcome: format!("{:?}", application.outcome),
            }
            .into());
        }
        Ok(application)
    }

    pub(super) fn atom_subscription_admission(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &SubscriptionName,
    ) -> Result<Option<(EntityBinding, SubscriptionConfig)>, AtomSubscriptionOwnerError> {
        let target = AdminTarget::Subscription {
            topic: topic.clone(),
            name: name.clone(),
        };
        let Some(admission) = self.bind_admin_entity(namespace, &target)? else {
            return Ok(None);
        };
        let protocol_amqp::EntityMetadata::Subscription(config) = admission.metadata else {
            return Err(BrokerError::EntityKindMismatch.into());
        };
        self.machine
            .bind_entity(namespace, topic, topic, EntityIncarnationKind::Topic)?
            .ok_or(BrokerError::DanglingEntityMetadata)?;
        Ok(Some((admission.binding, config)))
    }
}

fn complete_update(config: SubscriptionConfig) -> SubscriptionConfigUpdate {
    SubscriptionConfigUpdate {
        lock_duration_millis: Some(config.lock_duration_millis),
        max_delivery_count: Some(config.max_delivery_count),
        default_time_to_live_millis: Some(match config.default_time_to_live_millis {
            Some(millis) => QueueTimeToLiveUpdate::Finite { millis },
            None => QueueTimeToLiveUpdate::Unlimited,
        }),
        // The closed profile proves the existing hidden limit, never mutates it.
        max_message_bytes: None,
        requires_session: Some(false),
        dead_lettering_on_message_expiration: Some(config.dead_lettering_on_message_expiration),
        dead_lettering_on_filter_evaluation_exceptions: Some(
            config.dead_lettering_on_filter_evaluation_exceptions,
        ),
    }
}

pub(super) fn validate_names(
    topic: &EntityPath,
    name: &SubscriptionName,
) -> Result<(), AtomSubscriptionOwnerError> {
    subscriptions::validate_name(name)
        .map_err(|_| AtomSubscriptionOwnerError::UnsupportedDefinition)?;
    if topic.as_str().encode_utf16().count() > 260
        || topic.as_str().contains('\\')
        || topic
            .as_str()
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(AtomSubscriptionOwnerError::UnsupportedDefinition);
    }
    Ok(())
}

impl From<ProposeError> for AtomSubscriptionOwnerError {
    fn from(error: ProposeError) -> Self {
        Self::Submit(SubmitError::Propose(error))
    }
}

impl From<BrokerError> for AtomSubscriptionOwnerError {
    fn from(error: BrokerError) -> Self {
        Self::from(ProposeError::Broker(error))
    }
}
