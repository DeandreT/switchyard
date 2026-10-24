//! Bounded profile-only changes; topology proof precedes even patch validation.

use storage::{StateStore, WriteBatch};

use crate::{
    BrokerError, Command, CommandOutcome, SubscriptionConfig, SubscriptionConfigUpdate,
    SubscriptionName, TopicConfigUpdate, codec, keys,
};

use super::{StateMachine, topic::SubscriptionProfile};

impl<S: StateStore> StateMachine<S> {
    pub(super) fn update_topic(
        &self,
        command: &Command,
        update: TopicConfigUpdate,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let children = self.subscription_topology(&command.namespace, &command.entity)?;
        let current = self.load_topic_config(command)?;
        let updated = update.apply(current)?;
        if updated != current {
            batch.push_put(
                keys::topic_config(&command.namespace, &command.entity),
                codec::topic::encode(&updated)?,
            );
        }
        // Only size is inherited. Independent child TTLs and recorded message
        // deadlines are not reinterpreted when the topic default changes.
        for profile in children {
            let backing = crate::QueueConfig {
                max_message_bytes: updated.max_message_bytes,
                ..profile.backing
            };
            stage_profile(command, profile, backing, batch)?;
        }
        Ok(CommandOutcome::TopicUpdated)
    }

    pub(super) fn update_subscription(
        &self,
        command: &Command,
        name: &SubscriptionName,
        update: SubscriptionConfigUpdate,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let children = self.subscription_topology(&command.namespace, &command.entity)?;
        let entity = command.entity.subscription(name)?;
        let profile = children
            .into_iter()
            .find(|profile| profile.entity == entity)
            .ok_or(BrokerError::SubscriptionNotFound)?;
        let current = SubscriptionConfig {
            lock_duration_millis: profile.backing.lock_duration_millis,
            max_delivery_count: profile.backing.max_delivery_count,
            default_time_to_live_millis: profile.backing.default_time_to_live_millis,
        };
        let updated = update.apply(current)?;
        let backing = crate::QueueConfig {
            lock_duration_millis: updated.lock_duration_millis,
            max_delivery_count: updated.max_delivery_count,
            default_time_to_live_millis: updated.default_time_to_live_millis,
            ..profile.backing
        };
        stage_profile(command, profile, backing, batch)?;
        Ok(CommandOutcome::SubscriptionUpdated)
    }
}

fn stage_profile(
    command: &Command,
    profile: SubscriptionProfile,
    backing: crate::QueueConfig,
    batch: &mut WriteBatch,
) -> Result<(), BrokerError> {
    let shadow = backing.dead_letter_shadow();
    if backing != profile.backing {
        batch.push_put(
            keys::queue_config(&command.namespace, &profile.entity),
            codec::encode(&backing)?,
        );
    }
    if shadow != profile.shadow {
        batch.push_put(
            keys::queue_config(&command.namespace, &profile.entity.dead_letter_queue()?),
            codec::encode(&shadow)?,
        );
    }
    Ok(())
}
