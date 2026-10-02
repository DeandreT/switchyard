use admin_api::v1;
use prost::Message;
use serde::Serialize;

use super::super::CliError;
use super::{
    MAX_RESPONSE_BYTES, MAX_RULES,
    filter::{self, JsonFilter},
    validate_rule_name,
};

#[derive(Debug, Serialize)]
pub(super) struct RuleOutput {
    namespace: String,
    subscription_path: String,
    name: String,
    filter: JsonFilter,
    created_at_unix_millis: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ListRuleOutput {
    rules: Vec<RuleOutput>,
}

#[derive(Debug, Serialize)]
pub(super) struct MutationOutput {
    namespace: String,
    subscription_path: String,
    name: String,
    completed: bool,
}

fn invalid() -> CliError {
    CliError::Input("invalid rule response")
}

pub(super) fn rule(
    input: v1::Rule,
    namespace: &str,
    path: &str,
    name: Option<&str>,
) -> Result<RuleOutput, CliError> {
    if input.encoded_len() > MAX_RESPONSE_BYTES
        || input.namespace != namespace
        || input.subscription_path != path
        || name.is_some_and(|name| input.name != name)
    {
        return Err(invalid());
    }
    validate_rule_name(&input.name).map_err(|_| invalid())?;
    let filter = filter::from_protobuf(input.filter.ok_or_else(invalid)?).map_err(|_| invalid())?;
    Ok(RuleOutput {
        namespace: input.namespace,
        subscription_path: input.subscription_path,
        name: input.name,
        filter,
        created_at_unix_millis: input.created_at_unix_millis.to_string(),
    })
}

pub(super) fn list(
    input: v1::ListRulesResponse,
    namespace: &str,
    path: &str,
) -> Result<ListRuleOutput, CliError> {
    if input.encoded_len() > MAX_RESPONSE_BYTES
        || input.rules.len() > MAX_RULES
        || input
            .rules
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
    {
        return Err(invalid());
    }
    let rules = input
        .rules
        .into_iter()
        .map(|input| rule(input, namespace, path, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ListRuleOutput { rules })
}

pub(super) fn mutation(namespace: &str, path: &str, name: &str) -> MutationOutput {
    MutationOutput {
        namespace: namespace.to_owned(),
        subscription_path: path.to_owned(),
        name: name.to_owned(),
        completed: true,
    }
}
