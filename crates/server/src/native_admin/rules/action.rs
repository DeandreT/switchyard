use admin_api::v1;
use domain::{BrokerError, SqlAction};
use tonic::Status;

use super::status;

pub(super) fn read(input: Option<&v1::SqlRuleAction>) -> Result<SqlAction, Status> {
    let input = input.ok_or_else(|| Status::invalid_argument("a SQL rule action is required"))?;
    let version = input
        .semantic_version
        .unwrap_or(domain::SQL_ACTION_SEMANTIC_VERSION);
    if !matches!(version, 1 | 2) {
        return Err(Status::unimplemented(
            "unsupported SQL action semantic version",
        ));
    }
    if input.expression.len() > domain::MAX_SQL_EXPRESSION_BYTES
        || input
            .expression
            .encode_utf16()
            .take(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1)
            .count()
            > domain::MAX_SQL_EXPRESSION_UTF16_UNITS
    {
        return Err(Status::resource_exhausted("SQL source limit reached"));
    }
    SqlAction::with_semantic_version(input.expression.clone(), version)
        .map_err(|error| status::input(BrokerError::SqlActionCompilation(error)))
}

pub(super) fn write(action: &SqlAction) -> v1::SqlRuleAction {
    v1::SqlRuleAction {
        expression: action.expression().to_owned(),
        semantic_version: Some(action.semantic_version()),
    }
}
