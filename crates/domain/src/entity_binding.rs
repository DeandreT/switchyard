use serde::{Deserialize, Serialize};

use crate::{
    BrokerError, Command, EntityPath, MAX_ENTITY_PATH_BYTES, MAX_NAMESPACE_NAME_BYTES,
    NamespaceName, SUBSCRIPTION_PATH_SEGMENT, SubscriptionName,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityIncarnationKind {
    Queue,
    Topic,
    Subscription,
}

/// A retained owner identity. Shadows share their owner's incarnation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityIncarnation {
    generation: u64,
    kind: EntityIncarnationKind,
    retired: bool,
}

impl EntityIncarnation {
    pub fn new(
        generation: u64,
        kind: EntityIncarnationKind,
        retired: bool,
    ) -> Result<Self, BrokerError> {
        let value = Self {
            generation,
            kind,
            retired,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn generation(self) -> u64 {
        self.generation
    }

    pub fn kind(self) -> EntityIncarnationKind {
        self.kind
    }

    pub fn is_retired(self) -> bool {
        self.retired
    }

    pub(crate) fn validate(self) -> Result<(), BrokerError> {
        if self.generation == 0 {
            return Err(BrokerError::DanglingEntityMetadata);
        }
        Ok(())
    }

    pub(crate) fn retire(self) -> Self {
        Self {
            retired: true,
            ..self
        }
    }
}

/// An exact physical target, distinct from the owner shared with its DLQ.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityBinding {
    namespace: NamespaceName,
    target: EntityPath,
    owner: EntityPath,
    kind: EntityIncarnationKind,
    generation: u64,
}

impl EntityBinding {
    pub fn new(
        namespace: NamespaceName,
        target: EntityPath,
        owner: EntityPath,
        kind: EntityIncarnationKind,
        generation: u64,
    ) -> Result<Self, BrokerError> {
        let binding = Self {
            namespace,
            target,
            owner,
            kind,
            generation,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }

    pub fn target(&self) -> &EntityPath {
        &self.target
    }

    pub fn owner(&self) -> &EntityPath {
        &self.owner
    }

    pub fn kind(&self) -> EntityIncarnationKind {
        self.kind
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn validate(&self) -> Result<(), BrokerError> {
        if self.generation == 0
            || self.namespace.as_str().len() > MAX_NAMESPACE_NAME_BYTES
            || self.target.as_str().len() > MAX_ENTITY_PATH_BYTES
            || self.owner.as_str().len() > MAX_ENTITY_PATH_BYTES
            || NamespaceName::new(self.namespace.as_str()).is_err()
            || EntityPath::new(self.target.as_str()).is_err()
            || EntityPath::new(self.owner.as_str()).is_err()
        {
            return Err(BrokerError::InvalidEntityBinding);
        }
        let owner_valid = match self.kind {
            EntityIncarnationKind::Queue | EntityIncarnationKind::Topic => {
                !self.owner.is_dead_letter_queue() && !self.owner.is_subscription_path()
            }
            EntityIncarnationKind::Subscription => {
                let Some((parent, name)) =
                    self.owner.as_str().rsplit_once(SUBSCRIPTION_PATH_SEGMENT)
                else {
                    return Err(BrokerError::InvalidEntityBinding);
                };
                let parent =
                    EntityPath::new(parent).map_err(|_| BrokerError::InvalidEntityBinding)?;
                SubscriptionName::validate(name).is_ok()
                    && !parent.is_dead_letter_queue()
                    && !parent.is_subscription_path()
                    && !self.owner.is_dead_letter_queue()
            }
        };
        if !owner_valid {
            return Err(BrokerError::InvalidEntityBinding);
        }
        if self.target != self.owner
            && (self.kind == EntityIncarnationKind::Topic
                || self.owner.dead_letter_queue().as_ref().ok() != Some(&self.target))
        {
            return Err(BrokerError::InvalidEntityBinding);
        }
        Ok(())
    }
}

/// A separately serialized envelope keeps legacy command encodings unchanged.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FencedCommand {
    pub binding: EntityBinding,
    pub command: Command,
}

impl FencedCommand {
    pub fn new(binding: EntityBinding, command: Command) -> Self {
        Self { binding, command }
    }
}

#[cfg(test)]
mod tests;
