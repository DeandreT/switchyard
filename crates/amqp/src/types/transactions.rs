use super::{Array, Binary, Outcome, Symbol, Target, Value};

/// The maximum binary transaction identifier length defined by AMQP 1.0.
pub const MAX_TRANSACTION_ID_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("transaction id is {size} bytes; maximum is {maximum}")]
pub struct TransactionIdError {
    pub size: usize,
    pub maximum: usize,
}

/// An opaque binary identifier, including the structurally valid empty value.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct TransactionId(Binary);

impl TransactionId {
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, TransactionIdError> {
        let bytes = bytes.as_ref();
        validate_id_length(bytes.len())?;
        Ok(Self(Binary::from(bytes.to_vec())))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn as_binary(&self) -> &Binary {
        &self.0
    }

    pub fn into_binary(self) -> Binary {
        self.0
    }
}

impl TryFrom<Binary> for TransactionId {
    type Error = TransactionIdError;

    fn try_from(value: Binary) -> Result<Self, Self::Error> {
        validate_id_length(value.len())?;
        Ok(Self(value))
    }
}

impl AsRef<[u8]> for TransactionId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

fn validate_id_length(size: usize) -> Result<(), TransactionIdError> {
    if size > MAX_TRANSACTION_ID_BYTES {
        Err(TransactionIdError {
            size,
            maximum: MAX_TRANSACTION_ID_BYTES,
        })
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Coordinator {
    pub capabilities: Option<Array<Symbol>>,
}

/// Attach has one target terminus: an ordinary node or a coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetTerminus {
    Target(Target),
    Coordinator(Coordinator),
}

impl TargetTerminus {
    pub fn as_target(&self) -> Option<&Target> {
        match self {
            Self::Target(target) => Some(target),
            Self::Coordinator(_) => None,
        }
    }

    pub fn as_target_mut(&mut self) -> Option<&mut Target> {
        match self {
            Self::Target(target) => Some(target),
            Self::Coordinator(_) => None,
        }
    }

    pub fn as_coordinator(&self) -> Option<&Coordinator> {
        match self {
            Self::Coordinator(coordinator) => Some(coordinator),
            Self::Target(_) => None,
        }
    }
}

impl From<Target> for TargetTerminus {
    fn from(value: Target) -> Self {
        Self::Target(value)
    }
}

impl From<Coordinator> for TargetTerminus {
    fn from(value: Coordinator) -> Self {
        Self::Coordinator(value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Declare {
    /// Preserved for policy-level refusal when distributed transactions are unsupported.
    pub global_id: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Discharge {
    pub txn_id: TransactionId,
    pub fail: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Declared {
    pub txn_id: TransactionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalState {
    pub txn_id: TransactionId,
    pub outcome: Option<Outcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionCommand {
    Declare(Declare),
    Discharge(Discharge),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_accept_zero_through_thirty_two_bytes() {
        for size in [0, 1, MAX_TRANSACTION_ID_BYTES] {
            let bytes = vec![7; size];
            let id = TransactionId::new(&bytes).expect("bounded id");
            assert_eq!(id.as_bytes(), bytes);
            assert_eq!(TransactionId::try_from(id.clone().into_binary()), Ok(id));
        }
        assert_eq!(
            TransactionId::new(vec![7; MAX_TRANSACTION_ID_BYTES + 1]),
            Err(TransactionIdError {
                size: MAX_TRANSACTION_ID_BYTES + 1,
                maximum: MAX_TRANSACTION_ID_BYTES,
            })
        );
    }

    #[test]
    fn target_variants_do_not_disguise_a_coordinator_as_a_node() {
        let mut target = TargetTerminus::from(Target::new("orders"));
        target.as_target_mut().expect("ordinary target").address = Some("other".into());
        assert_eq!(
            target
                .as_target()
                .and_then(|target| target.address.as_deref()),
            Some("other")
        );
        assert!(target.as_coordinator().is_none());
        let coordinator = TargetTerminus::from(Coordinator::default());
        assert!(coordinator.as_target().is_none());
        assert_eq!(coordinator.as_coordinator(), Some(&Coordinator::default()));
    }
}
