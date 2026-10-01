use admin_api::v1::{
    CreateEntityRequest, Entity, EntityKind, GetEntityRequest, ListEntitiesRequest,
    ListEntitiesResponse, SubscriptionConfiguration, TopicConfiguration, UnlimitedTimeToLive,
    subscription_configuration::DefaultTimeToLive as SubscriptionTimeToLive,
    topic_configuration::DefaultTimeToLive as TopicTimeToLive,
};
use clap::{Args, Subcommand};
use serde::Serialize;

use super::{
    Arguments, CliError, ConnectionSettings, EntityOutput, ListOutput, MAX_PAGE_TOKEN_BYTES,
    REQUEST_TIMEOUT, validate_identifier, validate_queue_path, write_output,
};

#[derive(Debug, Subcommand)]
pub(super) enum TopicCommand {
    Create(TopicMutation),
    Get {
        path: String,
    },
    List {
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        #[arg(long, default_value = "")]
        page_token: String,
    },
}

#[derive(Debug, Args)]
pub(super) struct TopicMutation {
    path: String,
    #[command(flatten)]
    configuration: TopicConfigurationArguments,
}

#[derive(Debug, Default, Args)]
struct TopicConfigurationArguments {
    #[arg(long, visible_alias = "ttl-millis", conflicts_with = "ttl_unlimited")]
    default_ttl_millis: Option<u64>,
    #[arg(
        long,
        visible_alias = "default-ttl-unlimited",
        conflicts_with = "default_ttl_millis"
    )]
    ttl_unlimited: bool,
    #[arg(long)]
    max_message_bytes: Option<u64>,
    #[arg(long, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    requires_duplicate_detection: Option<bool>,
    #[arg(long, visible_alias = "duplicate-detection-window-millis")]
    duplicate_detection_history_time_window_millis: Option<u64>,
}

impl TopicConfigurationArguments {
    fn protobuf(&self) -> TopicConfiguration {
        TopicConfiguration {
            default_time_to_live: self
                .default_ttl_millis
                .map(TopicTimeToLive::DefaultTtlMillis)
                .or_else(|| {
                    self.ttl_unlimited
                        .then_some(TopicTimeToLive::DefaultTtlUnlimited(UnlimitedTimeToLive {}))
                }),
            max_message_bytes: self.max_message_bytes,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(super) enum SubscriptionCommand {
    Create(SubscriptionMutation),
    Get {
        topic: String,
        name: String,
    },
    List {
        topic: String,
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        #[arg(long, default_value = "")]
        page_token: String,
    },
}

#[derive(Debug, Args)]
pub(super) struct SubscriptionMutation {
    topic: String,
    name: String,
    #[command(flatten)]
    configuration: SubscriptionConfigurationArguments,
}

#[derive(Debug, Default, Args)]
struct SubscriptionConfigurationArguments {
    #[arg(long)]
    lock_duration_millis: Option<u64>,
    #[arg(long)]
    max_delivery_count: Option<u32>,
    #[arg(long, visible_alias = "ttl-millis", conflicts_with = "ttl_unlimited")]
    default_ttl_millis: Option<u64>,
    #[arg(
        long,
        visible_alias = "default-ttl-unlimited",
        conflicts_with = "default_ttl_millis"
    )]
    ttl_unlimited: bool,
    #[arg(long)]
    max_message_bytes: Option<u64>,
    #[arg(long, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    requires_session: Option<bool>,
    #[arg(long, visible_alias = "dead-letter-on-expiration", num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    dead_lettering_on_message_expiration: Option<bool>,
    #[arg(long, visible_alias = "dead-letter-on-filter-exceptions", num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    dead_lettering_on_filter_evaluation_exceptions: Option<bool>,
}

impl SubscriptionConfigurationArguments {
    fn protobuf(&self) -> SubscriptionConfiguration {
        SubscriptionConfiguration {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live: self
                .default_ttl_millis
                .map(SubscriptionTimeToLive::DefaultTtlMillis)
                .or_else(|| {
                    self.ttl_unlimited
                        .then_some(SubscriptionTimeToLive::DefaultTtlUnlimited(
                            UnlimitedTimeToLive {},
                        ))
                }),
            max_message_bytes: self.max_message_bytes,
            requires_session: self.requires_session,
            dead_lettering_on_message_expiration: self.dead_lettering_on_message_expiration,
            dead_lettering_on_filter_evaluation_exceptions: self
                .dead_lettering_on_filter_evaluation_exceptions,
        }
    }
}

pub(super) async fn execute_topic(
    arguments: &Arguments,
    command: &TopicCommand,
) -> Result<(), CliError> {
    match command {
        TopicCommand::Create(input) => validate_primary_path(&input.path)?,
        TopicCommand::Get { path } => validate_primary_path(path)?,
        TopicCommand::List {
            page_size,
            page_token,
        } => validate_page(*page_size, page_token)?,
    }
    let settings = ConnectionSettings::prepare(arguments)?;
    let mut client = settings.connect().await?;
    let namespace = arguments.namespace.clone();
    let operation = async {
        match command {
            TopicCommand::Create(input) => {
                let response = client
                    .create_entity(settings.request(CreateEntityRequest {
                        namespace,
                        path: input.path.clone(),
                        kind: EntityKind::Topic as i32,
                        topic_config: Some(input.configuration.protobuf()),
                        ..CreateEntityRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_entity(response.into_inner(), EntityKind::Topic)
            }
            TopicCommand::Get { path } => {
                let response = client
                    .get_entity(settings.request(GetEntityRequest {
                        namespace,
                        path: path.clone(),
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_entity(response.into_inner(), EntityKind::Topic)
            }
            TopicCommand::List {
                page_size,
                page_token,
            } => {
                let response = client
                    .list_entities(settings.request(ListEntitiesRequest {
                        namespace,
                        kind: EntityKind::Topic as i32,
                        page_size: *page_size,
                        page_token: page_token.clone(),
                        ..ListEntitiesRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_list(response.into_inner(), EntityKind::Topic)
            }
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, operation)
        .await
        .map_err(|_| CliError::Timeout)?
}

pub(super) async fn execute_subscription(
    arguments: &Arguments,
    command: &SubscriptionCommand,
) -> Result<(), CliError> {
    let path = match command {
        SubscriptionCommand::Create(input) => subscription_path(&input.topic, &input.name)?,
        SubscriptionCommand::Get { topic, name } => subscription_path(topic, name)?,
        SubscriptionCommand::List {
            topic,
            page_size,
            page_token,
        } => {
            validate_primary_path(topic)?;
            validate_page(*page_size, page_token)?;
            String::new()
        }
    };
    let settings = ConnectionSettings::prepare(arguments)?;
    let mut client = settings.connect().await?;
    let namespace = arguments.namespace.clone();
    let operation = async {
        match command {
            SubscriptionCommand::Create(input) => {
                let response = client
                    .create_entity(settings.request(CreateEntityRequest {
                        namespace,
                        path,
                        kind: EntityKind::Subscription as i32,
                        subscription_config: Some(input.configuration.protobuf()),
                        ..CreateEntityRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_entity(response.into_inner(), EntityKind::Subscription)
            }
            SubscriptionCommand::Get { .. } => {
                let response = client
                    .get_entity(settings.request(GetEntityRequest { namespace, path }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_entity(response.into_inner(), EntityKind::Subscription)
            }
            SubscriptionCommand::List {
                topic,
                page_size,
                page_token,
            } => {
                let response = client
                    .list_entities(settings.request(ListEntitiesRequest {
                        namespace,
                        kind: EntityKind::Subscription as i32,
                        parent_topic: topic.clone(),
                        page_size: *page_size,
                        page_token: page_token.clone(),
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_list(response.into_inner(), EntityKind::Subscription)
            }
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, operation)
        .await
        .map_err(|_| CliError::Timeout)?
}

pub(super) fn write_entity(entity: Entity, kind: EntityKind) -> Result<(), CliError> {
    if entity.kind != kind as i32 {
        return Err(CliError::Input(
            "response does not match the requested entity kind",
        ));
    }
    write_output(&EntityOutput::from(entity))
}

fn write_list(response: ListEntitiesResponse, kind: EntityKind) -> Result<(), CliError> {
    if response
        .entities
        .iter()
        .any(|entity| entity.kind != kind as i32)
    {
        return Err(CliError::Input(
            "response does not match the requested entity kind",
        ));
    }
    write_output(&ListOutput::from(response))
}

fn validate_primary_path(path: &str) -> Result<(), CliError> {
    validate_queue_path(path)
}

fn subscription_path(topic: &str, name: &str) -> Result<String, CliError> {
    validate_primary_path(topic)?;
    let bytes = name.as_bytes();
    if bytes.len() > 50
        || !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(CliError::Input("invalid subscription name"));
    }
    let path = format!("{topic}/subscriptions/{name}");
    validate_identifier(
        &path,
        260 - "/$deadletterqueue".len(),
        "subscription and dead-letter paths exceed 260 bytes",
    )?;
    Ok(path)
}

fn validate_page(page_size: u32, page_token: &str) -> Result<(), CliError> {
    if page_size > 1024 {
        return Err(CliError::Input("page size cannot exceed 1024"));
    }
    if page_token.len() > MAX_PAGE_TOKEN_BYTES
        || !page_token.is_ascii()
        || page_token.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(CliError::Input("invalid entity page token"));
    }
    Ok(())
}

#[derive(Serialize)]
pub(super) struct TopicConfigurationOutput {
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: Option<u64>,
    requires_duplicate_detection: Option<bool>,
    duplicate_detection_history_time_window_millis: Option<u64>,
}

impl From<TopicConfiguration> for TopicConfigurationOutput {
    fn from(config: TopicConfiguration) -> Self {
        Self {
            default_time_to_live_millis: match config.default_time_to_live {
                Some(TopicTimeToLive::DefaultTtlMillis(millis)) => Some(millis),
                None | Some(TopicTimeToLive::DefaultTtlUnlimited(_)) => None,
            },
            max_message_bytes: config.max_message_bytes,
            requires_duplicate_detection: config.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: config
                .duplicate_detection_history_time_window_millis,
        }
    }
}

#[derive(Serialize)]
pub(super) struct SubscriptionConfigurationOutput {
    lock_duration_millis: Option<u64>,
    max_delivery_count: Option<u32>,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: Option<u64>,
    requires_session: Option<bool>,
    dead_lettering_on_message_expiration: Option<bool>,
    dead_lettering_on_filter_evaluation_exceptions: Option<bool>,
}

impl From<SubscriptionConfiguration> for SubscriptionConfigurationOutput {
    fn from(config: SubscriptionConfiguration) -> Self {
        Self {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: match config.default_time_to_live {
                Some(SubscriptionTimeToLive::DefaultTtlMillis(millis)) => Some(millis),
                None | Some(SubscriptionTimeToLive::DefaultTtlUnlimited(_)) => None,
            },
            max_message_bytes: config.max_message_bytes,
            requires_session: config.requires_session,
            dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
            dead_lettering_on_filter_evaluation_exceptions: config
                .dead_lettering_on_filter_evaluation_exceptions,
        }
    }
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
