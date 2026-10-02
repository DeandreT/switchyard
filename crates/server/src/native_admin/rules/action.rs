use admin_api::v1;
use domain::{BrokerError, SqlAction};
use tonic::Status;

use super::status;

pub(super) fn read(input: Option<&v1::SqlRuleAction>) -> Result<SqlAction, Status> {
    let input = input.ok_or_else(|| Status::invalid_argument("a SQL rule action is required"))?;
    if input
        .semantic_version
        .is_some_and(|version| version != domain::SQL_ACTION_SEMANTIC_VERSION)
    {
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
    SqlAction::new(input.expression.clone())
        .map_err(|error| status::input(BrokerError::SqlActionCompilation(error)))
}

pub(super) fn write(action: &SqlAction) -> v1::SqlRuleAction {
    v1::SqlRuleAction {
        expression: action.expression().to_owned(),
        semantic_version: Some(action.semantic_version()),
    }
}
