use crate::{Command, EntityPath, NamespaceName};

/// The owner kind captured by a broker endpoint; a DLQ shares its owner's kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityBindingKind {
    Queue,
    Topic,
    Subscription,
}

/// Immutable authority captured from a live owner, not reconstructed from a name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityBinding {
    namespace: NamespaceName,
    target: EntityPath,
    owner: EntityPath,
    kind: EntityBindingKind,
    generation: u64,
}

impl EntityBinding {
    pub(crate) fn new(
        namespace: NamespaceName,
        target: EntityPath,
        owner: EntityPath,
        kind: EntityBindingKind,
        generation: u64,
    ) -> Self {
        Self {
            namespace,
            target,
            owner,
            kind,
            generation,
        }
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

    pub fn kind(&self) -> EntityBindingKind {
        self.kind
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Keeps retained authority separate from the existing command wire encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundCommand {
    binding: EntityBinding,
    command: Command,
}

impl BoundCommand {
    pub fn new(binding: EntityBinding, command: Command) -> Self {
        Self { binding, command }
    }

    pub fn binding(&self) -> &EntityBinding {
        &self.binding
    }

    pub fn command(&self) -> &Command {
        &self.command
    }
}
