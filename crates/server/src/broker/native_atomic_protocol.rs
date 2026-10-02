use std::future::Future;

use domain::BrokerError;
use protocol_amqp::{
    NativeAtomicBroker, NativeAtomicBrokerCompletion, NativeAtomicIndeterminateCause,
    NativeAtomicOwnerError, NativeAtomicResponseUnavailable, OwnedNativeAtomicMessagingSubmission,
};

use super::{BrokerHandle, GuardedAtomicSubmitError, NativeAtomicSubmitError, ProposeError};

impl NativeAtomicBroker for BrokerHandle {
    fn submit_native_atomic_messaging_owned(
        &self,
        submission: OwnedNativeAtomicMessagingSubmission,
    ) -> impl Future<Output = Result<NativeAtomicBrokerCompletion, NativeAtomicResponseUnavailable>>
    + Send
    + 'static {
        let owner = BrokerHandle::submit_native_atomic_messaging_owned(self, submission);
        async move {
            let completion = owner.await.map_err(|_| NativeAtomicResponseUnavailable)?;
            let (application, resources) = completion.into_parts();
            Ok(NativeAtomicBrokerCompletion::from_owner_parts(
                application.map_err(owner_error),
                resources,
            ))
        }
    }
}

pub(super) fn owner_error(error: NativeAtomicSubmitError) -> NativeAtomicOwnerError {
    match error {
        NativeAtomicSubmitError::NativeClaim(error) => NativeAtomicOwnerError::NativeClaim(error),
        NativeAtomicSubmitError::Guarded(error) => match error {
            GuardedAtomicSubmitError::Permit(error) => NativeAtomicOwnerError::LogicalClaim(error),
            GuardedAtomicSubmitError::BrokerStopped => NativeAtomicOwnerError::OwnerStopped,
            GuardedAtomicSubmitError::WorkUnavailable => NativeAtomicOwnerError::Indeterminate(
                NativeAtomicIndeterminateCause::WorkUnavailable,
            ),
            GuardedAtomicSubmitError::Propose(error) => match error {
                ProposeError::Broker(BrokerError::Storage(_)) => {
                    NativeAtomicOwnerError::Indeterminate(NativeAtomicIndeterminateCause::Storage)
                }
                ProposeError::UnexpectedOutcome { .. } => NativeAtomicOwnerError::Indeterminate(
                    NativeAtomicIndeterminateCause::UnexpectedOutcome,
                ),
                ProposeError::ClockWentBackward { .. } => NativeAtomicOwnerError::ClockRegression,
                ProposeError::Broker(error) => NativeAtomicOwnerError::Refused(error),
            },
        },
    }
}
