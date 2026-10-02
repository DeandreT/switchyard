use std::{future::Future, pin::Pin};

use amqp::{
    EngineError, Error as AmqpProtocolError, NativeControllerIdentity, NativeTransactionError,
    NativeTransactionIdentity, PreparedPosting, SealedDischargeReceipt, TransactionPostingReceipt,
};
use domain::CommandKind;

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
    Ready {
        key: u64,
        receipt: SealedDischargeReceipt,
        result: Result<(), NativeTransactionError>,
    },
    Authorized {
        key: u64,
        result: Result<(), AmqpProtocolError>,
    },
    Applied {
        key: u64,
        result: Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>,
    },
    Finished {
        key: u64,
        result: Result<(), EngineError>,
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
            | Self::Ready { key, .. }
            | Self::Authorized { key, .. }
            | Self::Applied { key, .. }
            | Self::Finished { key, .. } => Some(*key),
            Self::DeclarationRefused { .. } => None,
        }
    }
}
