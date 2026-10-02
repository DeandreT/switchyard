use std::path::PathBuf;

use admin_api::v1::{
    CreateRuleRequest, CreateRuleWithActionRequest, DeleteRuleRequest, GetRuleRequest,
    ListRulesRequest, rule_service_client::RuleServiceClient,
};
use clap::{Args, Subcommand};
use prost::Message;

use super::{
    Arguments, CliError, ConnectionSettings, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES,
    REQUEST_TIMEOUT, topology, write_output,
};

mod action;
mod filter;
mod output;
mod scalar;
#[cfg(test)]
mod tests;

const MAX_FILTER_FILE_BYTES: usize = 512 * 1024;
const MAX_ACTION_FILE_BYTES: usize = 32 * 1024;
const MAX_RULES: usize = 32;

#[derive(Debug, Subcommand)]
pub(super) enum RuleCommand {
    Create(RuleCreate),
    Get {
        topic: String,
        subscription: String,
        name: String,
    },
    List {
        topic: String,
        subscription: String,
    },
    Delete {
        topic: String,
        subscription: String,
        name: String,
    },
}

#[derive(Debug, Args)]
pub(super) struct RuleCreate {
    topic: String,
    subscription: String,
    name: String,
    #[arg(long, value_name = "PATH")]
    filter_file: PathBuf,
    #[arg(long, value_name = "PATH")]
    action_file: Option<PathBuf>,
}

enum PreparedCommand {
    Create(Box<CreateRuleRequest>),
    CreateWithAction(Box<CreateRuleWithActionRequest>),
    Get(GetRuleRequest),
    List(ListRulesRequest),
    Delete(DeleteRuleRequest),
}

fn validate_rule_name(name: &str) -> Result<(), CliError> {
    if name.trim().is_empty()
        || name.encode_utf16().take(51).count() > 50
        || name.chars().any(char::is_control)
        || name.contains(['/', '\\', '@', '?', '#', '*'])
    {
        return Err(CliError::Input("invalid rule name"));
    }
    Ok(())
}

fn validate_request_size<T: Message>(request: &T) -> Result<(), CliError> {
    if request.encoded_len() > MAX_REQUEST_BYTES {
        return Err(CliError::Input(
            "rule request exceeds its encoded-byte limit",
        ));
    }
    Ok(())
}

fn prepare(namespace: &str, command: &RuleCommand) -> Result<PreparedCommand, CliError> {
    match command {
        RuleCommand::Create(input) => {
            let path = topology::subscription_path(&input.topic, &input.subscription)?;
            validate_rule_name(&input.name)?;
            let filter = Some(filter::load(&input.filter_file)?);
            match &input.action_file {
                Some(action_file) => {
                    let request = CreateRuleWithActionRequest {
                        namespace: namespace.to_owned(),
                        subscription_path: path,
                        name: input.name.clone(),
                        filter,
                        action: Some(action::load(action_file)?),
                    };
                    validate_request_size(&request)?;
                    Ok(PreparedCommand::CreateWithAction(Box::new(request)))
                }
                None => {
                    let request = CreateRuleRequest {
                        namespace: namespace.to_owned(),
                        subscription_path: path,
                        name: input.name.clone(),
                        filter,
                    };
                    validate_request_size(&request)?;
                    Ok(PreparedCommand::Create(Box::new(request)))
                }
            }
        }
        RuleCommand::Get {
            topic,
            subscription,
            name,
        } => {
            let path = topology::subscription_path(topic, subscription)?;
            validate_rule_name(name)?;
            let request = GetRuleRequest {
                namespace: namespace.to_owned(),
                subscription_path: path,
                name: name.clone(),
                include_actions: true,
            };
            validate_request_size(&request)?;
            Ok(PreparedCommand::Get(request))
        }
        RuleCommand::List {
            topic,
            subscription,
        } => {
            let request = ListRulesRequest {
                namespace: namespace.to_owned(),
                subscription_path: topology::subscription_path(topic, subscription)?,
                include_actions: true,
            };
            validate_request_size(&request)?;
            Ok(PreparedCommand::List(request))
        }
        RuleCommand::Delete {
            topic,
            subscription,
            name,
        } => {
            let path = topology::subscription_path(topic, subscription)?;
            validate_rule_name(name)?;
            let request = DeleteRuleRequest {
                namespace: namespace.to_owned(),
                subscription_path: path,
                name: name.clone(),
            };
            validate_request_size(&request)?;
            Ok(PreparedCommand::Delete(request))
        }
    }
}

pub(super) async fn execute(arguments: &Arguments, command: &RuleCommand) -> Result<(), CliError> {
    let command = prepare(&arguments.namespace, command)?;
    let settings = ConnectionSettings::prepare(arguments)?;
    let mut client = RuleServiceClient::new(settings.connect_channel().await?)
        .max_encoding_message_size(MAX_REQUEST_BYTES)
        .max_decoding_message_size(MAX_RESPONSE_BYTES);
    let operation = async {
        match command {
            PreparedCommand::Create(input) => {
                let completed =
                    output::mutation(&input.namespace, &input.subscription_path, &input.name);
                client
                    .create_rule(settings.request(*input))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_output(&completed)
            }
            PreparedCommand::CreateWithAction(input) => {
                let completed =
                    output::mutation(&input.namespace, &input.subscription_path, &input.name);
                client
                    .create_rule_with_action(settings.request(*input))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_output(&completed)
            }
            PreparedCommand::Get(input) => {
                let namespace = input.namespace.clone();
                let path = input.subscription_path.clone();
                let name = input.name.clone();
                let response = client
                    .get_rule(settings.request(input))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?
                    .into_inner();
                write_output(&output::rule(response, &namespace, &path, Some(&name))?)
            }
            PreparedCommand::List(input) => {
                let namespace = input.namespace.clone();
                let path = input.subscription_path.clone();
                let response = client
                    .list_rules(settings.request(input))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?
                    .into_inner();
                write_output(&output::list(response, &namespace, &path)?)
            }
            PreparedCommand::Delete(input) => {
                let completed =
                    output::mutation(&input.namespace, &input.subscription_path, &input.name);
                client
                    .delete_rule(settings.request(input))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_output(&completed)
            }
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, operation)
        .await
        .map_err(|_| CliError::Timeout)?
}
