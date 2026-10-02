use admin_api::v1::{self, rule_filter, rule_scalar_value};
use serde_json::{Value, json};

use super::*;

const NAMESPACE: &str = "tenant";
const PATH: &str = "Orders/subscriptions/Alpha";

fn parse(input: Value) -> Result<v1::RuleFilter, CliError> {
    filter::parse_json(&serde_json::to_vec(&input).expect("test JSON"))
}

fn scalar_input(input: Value) -> Result<v1::RuleScalarValue, CliError> {
    let json = serde_json::from_value::<scalar::JsonScalar>(input)
        .map_err(|_| CliError::Input("invalid test scalar"))?;
    scalar::into_protobuf(json)
}

fn scalar_value(value: rule_scalar_value::Value) -> v1::RuleScalarValue {
    v1::RuleScalarValue { value: Some(value) }
}

fn valid_rule(name: &str) -> v1::Rule {
    v1::Rule {
        namespace: NAMESPACE.into(),
        subscription_path: PATH.into(),
        name: name.into(),
        filter: Some(v1::RuleFilter {
            filter: Some(rule_filter::Filter::TrueFilter(v1::TrueRuleFilter {})),
        }),
        created_at_unix_millis: 1_000,
        action: None,
    }
}

#[path = "tests/actions.rs"]
mod actions;
#[path = "tests/input.rs"]
mod input;
#[path = "tests/output.rs"]
mod responses;
#[path = "tests/scalars.rs"]
mod scalars;
