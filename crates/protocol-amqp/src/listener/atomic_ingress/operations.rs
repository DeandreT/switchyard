use std::{future::Future, pin::Pin};

use amqp::{
    EngineError, Error as AmqpProtocolError, NativeControllerIdentity, NativeTransactionError,
    NativeTransactionIdentity, PreparedPosting, PreparedRetirement, SealedDischargeReceipt,
    TransactionPostingReceipt, TransactionRetirementReceipt,
};
use domain::CommandKind;

use super::groups::RetirementCompletion;
use crate::{NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable};

pub(super) type OperationFuture = Pin<Box<dyn Future<Output = Operation> + Send + 'static>>;

pub(super) enum Operation {
    Declared {
        key: u64,
        result: Result<NativeTransactionIdentity, EngineError>,
    },
    PostingChecked {
        key: u64,
        producer: u64,
        receipt: TransactionPostingReceipt,
        result: Result<CommandKind, AmqpProtocolError>,
    },
    Prepared {
        key: u64,
        producer: u64,
        result: Result<PreparedPosting, EngineError>,
    },
    RetirementChecked {
        key: u64,
        consumer: u64,
        receipt: TransactionRetirementReceipt,
        result: Result<(), AmqpProtocolError>,
    },
    RetirementPrepared {
        key: u64,
        consumer: u64,
        result: Result<PreparedRetirement, EngineError>,
    },
    Ready {
        key: u64,
        receipt: SealedDischargeReceipt,
        result: Result<(), NativeTransactionError>,
    },
    Authorized {
        key: u64,
        result: Result<Option<u64>, AmqpProtocolError>,
    },
    Applied {
        key: u64,
        result: Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>,
    },
    Finished {
        key: u64,
        result: Result<(), EngineError>,
        completion: RetirementCompletion,
    },
    DeclarationRefused {
        controller: NativeControllerIdentity,
        result: Result<(), EngineError>,
    },
}

impl Operation {
    pub(super) fn key(&self) -> Option<u64> {
        match self {
            Self::Declared { key, .. }
            | Self::PostingChecked { key, .. }
            | Self::Prepared { key, .. }
            | Self::RetirementChecked { key, .. }
            | Self::RetirementPrepared { key, .. }
            | Self::Ready { key, .. }
            | Self::Authorized { key, .. }
            | Self::Applied { key, .. }
            | Self::Finished { key, .. } => Some(*key),
            Self::DeclarationRefused { .. } => None,
        }
    }
}
