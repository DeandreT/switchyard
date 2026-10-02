use domain::{BrokerError, SqlCompileError};
use tonic::Status;

use crate::{ProposeError, SubmitError};

pub(super) fn compilation(error: SqlCompileError) -> Status {
    match error {
        SqlCompileError::Syntax => Status::invalid_argument("invalid SQL predicate syntax"),
        SqlCompileError::Unsupported { .. } => Status::unimplemented("unsupported SQL predicate"),
        SqlCompileError::Limit { .. } => {
            Status::resource_exhausted("SQL compilation limit reached")
        }
    }
}

pub(super) fn input(error: BrokerError) -> Status {
    match error {
        BrokerError::SqlRuleCompilation(error) => compilation(error),
        BrokerError::SqlActionCompilation(error) => match error {
            SqlCompileError::Syntax => Status::invalid_argument("invalid SQL action syntax"),
            SqlCompileError::Unsupported { .. } => Status::unimplemented("unsupported SQL action"),
            SqlCompileError::Limit { .. } => {
                Status::resource_exhausted("SQL compilation limit reached")
            }
        },
        BrokerError::RuleTooLarge { .. }
        | BrokerError::RuleSetTooLarge { .. }
        | BrokerError::RuleLimitExceeded { .. } => Status::resource_exhausted("rule limit reached"),
        BrokerError::InvalidRule { .. } | BrokerError::Identifier(_) => {
            Status::invalid_argument("invalid rule")
        }
        _ => Status::internal("rule validation failed"),
    }
}

pub(super) fn stored(_: BrokerError) -> Status {
    Status::internal("invalid stored rule metadata")
}

pub(super) fn mutation(error: SubmitError) -> Status {
    match error {
        SubmitError::Propose(ProposeError::Broker(BrokerError::RuleAlreadyExists)) => {
            Status::already_exists("rule already exists")
        }
        SubmitError::Propose(ProposeError::Broker(BrokerError::RuleNotFound)) => {
            Status::not_found("rule does not exist")
        }
        SubmitError::Propose(ProposeError::Broker(
            error @ (BrokerError::RuleTooLarge { .. }
            | BrokerError::RuleSetTooLarge { .. }
            | BrokerError::RuleLimitExceeded { .. }
            | BrokerError::InvalidRule { .. }
            | BrokerError::SqlRuleCompilation(_)
            | BrokerError::SqlActionCompilation(_)),
        )) => input(error),
        other => read(other),
    }
}

pub(super) fn read(error: SubmitError) -> Status {
    match error {
        SubmitError::BrokerStopped => Status::unavailable("broker owner is unavailable"),
        SubmitError::Propose(ProposeError::ClockWentBackward { .. }) => {
            Status::unavailable("broker clock is unavailable")
        }
        SubmitError::Propose(ProposeError::Broker(
            BrokerError::QueueNotFound
            | BrokerError::TopicNotFound
            | BrokerError::SubscriptionNotFound
            | BrokerError::RuleNotFound
            | BrokerError::EntityBindingStale,
        )) => Status::not_found("rule scope or target does not exist"),
        SubmitError::Propose(ProposeError::Broker(
            BrokerError::SqlRuleCompilation(SqlCompileError::Limit { .. })
            | BrokerError::SqlActionCompilation(SqlCompileError::Limit { .. }),
        )) => Status::resource_exhausted("SQL compilation limit reached"),
        _ => Status::internal("rule operation failed"),
    }
}
