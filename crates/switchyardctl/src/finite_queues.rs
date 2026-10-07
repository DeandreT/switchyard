use admin_api::v1::{
    CreateFiniteQueueRequest, FiniteQueue, GetFiniteQueueRequest, QueueConfiguration,
    SetFiniteQueueDefinitionRequest, finite_queue_service_client::FiniteQueueServiceClient,
};
use clap::{Args, Subcommand};
use serde::Serialize;

use super::{
    Arguments, CliError, ConfigurationArguments, ConfigurationOutput, ConnectionSettings,
    MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, REQUEST_TIMEOUT, validate_queue_path, write_output,
};

pub(super) const OPERATIONS: [&str; 3] = ["create", "get", "set-definition"];
const FULL_CONFIG_ERROR: &str =
    "finite queues require every configuration field and an explicit TTL";
const LIMIT_ERROR: &str = "finite queues require positive --reservation-limit-bytes";
const GENERATION_ERROR: &str = "finite definition requires positive --expected-generation";
const RESPONSE_ERROR: &str = "response does not match the requested finite queue";
const RESPONSE_CONFIG_ERROR: &str = "response does not contain complete finite queue configuration";

#[derive(Debug, Subcommand)]
pub(super) enum FiniteQueueCommand {
    Create(CreateArguments),
    Get { path: String },
    SetDefinition(DefinitionArguments),
}

#[derive(Debug, Args)]
pub(super) struct CreateArguments {
    path: String,
    #[command(flatten)]
    configuration: ConfigurationArguments,
    #[arg(long)]
    reservation_limit_bytes: Option<u64>,
}

#[derive(Debug, Args)]
pub(super) struct DefinitionArguments {
    path: String,
    #[command(flatten)]
    configuration: ConfigurationArguments,
    #[arg(long)]
    reservation_limit_bytes: Option<u64>,
    #[arg(long)]
    expected_generation: Option<u64>,
}

impl FiniteQueueCommand {
    fn path(&self) -> &str {
        match self {
            Self::Create(input) => &input.path,
            Self::Get { path } => path,
            Self::SetDefinition(input) => &input.path,
        }
    }
}

#[derive(Debug)]
enum Prepared {
    Create(CreateFiniteQueueRequest),
    Get(GetFiniteQueueRequest),
    SetDefinition(SetFiniteQueueDefinitionRequest),
}

impl Prepared {
    fn expected_generation(&self) -> Option<u64> {
        match self {
            Self::SetDefinition(input) => input.expected_generation,
            Self::Create(_) | Self::Get(_) => None,
        }
    }
}

fn complete_configuration(config: &QueueConfiguration) -> bool {
    config.lock_duration_millis.is_some()
        && config.max_delivery_count.is_some()
        && config.default_time_to_live.is_some()
        && config.max_message_bytes.is_some()
        && config.requires_session.is_some()
        && config.requires_duplicate_detection.is_some()
        && config
            .duplicate_detection_history_time_window_millis
            .is_some()
        && config.dead_lettering_on_message_expiration.is_some()
}

fn configuration(input: &ConfigurationArguments) -> Result<QueueConfiguration, CliError> {
    let config = input.protobuf();
    if !complete_configuration(&config) {
        return Err(CliError::Input(FULL_CONFIG_ERROR));
    }
    Ok(config)
}

fn positive(value: Option<u64>, error: &'static str) -> Result<u64, CliError> {
    value
        .filter(|value| *value != 0)
        .ok_or(CliError::Input(error))
}

fn prepare(command: &FiniteQueueCommand, namespace: &str) -> Result<Prepared, CliError> {
    validate_queue_path(command.path())?;
    match command {
        FiniteQueueCommand::Create(input) => {
            let config = configuration(&input.configuration)?;
            let limit = positive(input.reservation_limit_bytes, LIMIT_ERROR)?;
            Ok(Prepared::Create(CreateFiniteQueueRequest {
                namespace: namespace.to_owned(),
                path: input.path.clone(),
                config: Some(config),
                reservation_limit_bytes: Some(limit),
            }))
        }
        FiniteQueueCommand::Get { path } => Ok(Prepared::Get(GetFiniteQueueRequest {
            namespace: namespace.to_owned(),
            path: path.clone(),
        })),
        FiniteQueueCommand::SetDefinition(input) => {
            let config = configuration(&input.configuration)?;
            let limit = positive(input.reservation_limit_bytes, LIMIT_ERROR)?;
            let generation = positive(input.expected_generation, GENERATION_ERROR)?;
            Ok(Prepared::SetDefinition(SetFiniteQueueDefinitionRequest {
                namespace: namespace.to_owned(),
                path: input.path.clone(),
                expected_generation: Some(generation),
                config: Some(config),
                reservation_limit_bytes: Some(limit),
            }))
        }
    }
}

#[derive(Serialize)]
struct FiniteQueueOutput {
    namespace: String,
    path: String,
    generation: u64,
    config: ConfigurationOutput,
    reservation_limit_bytes: u64,
    reserved_logical_bytes: u64,
    retained_message_count: u64,
}

fn output(
    response: FiniteQueue,
    namespace: &str,
    path: &str,
    expected_generation: Option<u64>,
) -> Result<FiniteQueueOutput, CliError> {
    if response.namespace != namespace
        || response.path != path
        || response.generation == 0
        || response.reservation_limit_bytes == 0
        || expected_generation.is_some_and(|expected| response.generation != expected)
    {
        return Err(CliError::Input(RESPONSE_ERROR));
    }
    let config = response
        .config
        .ok_or(CliError::Input(RESPONSE_CONFIG_ERROR))?;
    if !complete_configuration(&config) {
        return Err(CliError::Input(RESPONSE_CONFIG_ERROR));
    }
    Ok(FiniteQueueOutput {
        namespace: response.namespace,
        path: response.path,
        generation: response.generation,
        config: config.into(),
        reservation_limit_bytes: response.reservation_limit_bytes,
        reserved_logical_bytes: response.reserved_logical_bytes,
        retained_message_count: response.retained_message_count,
    })
}

pub(super) async fn execute(
    arguments: &Arguments,
    command: &FiniteQueueCommand,
) -> Result<(), CliError> {
    let prepared = prepare(command, &arguments.namespace)?;
    let expected_generation = prepared.expected_generation();
    let settings = ConnectionSettings::prepare(arguments)?;
    let mut client = FiniteQueueServiceClient::new(settings.connect_channel().await?)
        .max_encoding_message_size(MAX_REQUEST_BYTES)
        .max_decoding_message_size(MAX_RESPONSE_BYTES);
    let operation = async {
        let response = match prepared {
            Prepared::Create(input) => client.create_finite_queue(settings.request(input)).await,
            Prepared::Get(input) => client.get_finite_queue(settings.request(input)).await,
            Prepared::SetDefinition(input) => {
                client
                    .set_finite_queue_definition(settings.request(input))
                    .await
            }
        }
        .map_err(|status| CliError::Request(status.code()))?
        .into_inner();
        write_output(&output(
            response,
            &arguments.namespace,
            command.path(),
            expected_generation,
        )?)
    };
    tokio::time::timeout(REQUEST_TIMEOUT, operation)
        .await
        .map_err(|_| CliError::Timeout)?
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::{Command, DefaultTimeToLive, UnlimitedTimeToLive};

    fn config() -> ConfigurationArguments {
        ConfigurationArguments {
            lock_duration_millis: Some(60_000),
            max_delivery_count: Some(10),
            default_ttl_millis: None,
            ttl_unlimited: true,
            max_message_bytes: Some(262_144),
            requires_session: Some(false),
            requires_duplicate_detection: Some(false),
            duplicate_detection_history_time_window_millis: Some(600_000),
            dead_lettering_on_message_expiration: Some(false),
        }
    }

    fn mutation(
        configuration: ConfigurationArguments,
        definition: bool,
        limit: Option<u64>,
        generation: Option<u64>,
    ) -> FiniteQueueCommand {
        if definition {
            FiniteQueueCommand::SetDefinition(DefinitionArguments {
                path: "orders".into(),
                configuration,
                reservation_limit_bytes: limit,
                expected_generation: generation,
            })
        } else {
            FiniteQueueCommand::Create(CreateArguments {
                path: "orders".into(),
                configuration,
                reservation_limit_bytes: limit,
            })
        }
    }

    fn omit(input: &mut ConfigurationArguments, field: usize) {
        match field {
            0 => input.lock_duration_millis = None,
            1 => input.max_delivery_count = None,
            2 => {
                input.default_ttl_millis = None;
                input.ttl_unlimited = false;
            }
            3 => input.max_message_bytes = None,
            4 => input.requires_session = None,
            5 => input.requires_duplicate_detection = None,
            6 => input.duplicate_detection_history_time_window_millis = None,
            7 => input.dead_lettering_on_message_expiration = None,
            _ => unreachable!("exact full configuration field"),
        }
    }

    fn response() -> FiniteQueue {
        FiniteQueue {
            namespace: "tenant".into(),
            path: "orders".into(),
            generation: u64::MAX,
            config: Some(config().protobuf()),
            reservation_limit_bytes: u64::MAX,
            reserved_logical_bytes: u64::MAX,
            retained_message_count: u64::MAX,
        }
    }

    #[test]
    fn complete_requests_preserve_false_unlimited_and_direct_generation() {
        let expected = config().protobuf();
        let Prepared::Create(request) =
            prepare(&mutation(config(), false, Some(8192), None), "tenant").unwrap()
        else {
            panic!("finite creation request");
        };
        assert_eq!(request.namespace, "tenant");
        assert_eq!(request.path, "orders");
        assert_eq!(request.config, Some(expected));
        assert_eq!(request.reservation_limit_bytes, Some(8192));
        let Prepared::SetDefinition(request) =
            prepare(&mutation(config(), true, Some(4096), Some(7)), "tenant").unwrap()
        else {
            panic!("finite definition request");
        };
        assert_eq!(request.expected_generation, Some(7));
        assert_eq!(request.config, Some(config().protobuf()));
        assert_eq!(request.reservation_limit_bytes, Some(4096));
        assert_eq!(config().protobuf().requires_session, Some(false));
        assert_eq!(
            config().protobuf().requires_duplicate_detection,
            Some(false)
        );
        assert_eq!(
            config().protobuf().dead_lettering_on_message_expiration,
            Some(false)
        );
        assert_eq!(
            config().protobuf().default_time_to_live,
            Some(DefaultTimeToLive::DefaultTtlUnlimited(
                UnlimitedTimeToLive {}
            ))
        );
    }

    #[test]
    fn every_missing_configuration_field_is_refused_without_defaults() {
        for definition in [false, true] {
            for field in 0..8 {
                let mut input = config();
                omit(&mut input, field);
                assert_eq!(
                    prepare(&mutation(input, definition, Some(1), Some(1)), "tenant")
                        .unwrap_err()
                        .to_string(),
                    FULL_CONFIG_ERROR
                );
            }
        }
    }

    #[test]
    fn capacity_and_generation_require_positive_full_unsigned_presence() {
        for definition in [false, true] {
            for limit in [None, Some(0)] {
                assert_eq!(
                    prepare(&mutation(config(), definition, limit, Some(1)), "tenant")
                        .unwrap_err()
                        .to_string(),
                    LIMIT_ERROR
                );
            }
        }
        for generation in [None, Some(0)] {
            assert_eq!(
                prepare(&mutation(config(), true, Some(1), generation), "tenant")
                    .unwrap_err()
                    .to_string(),
                GENERATION_ERROR
            );
        }
        let Prepared::SetDefinition(request) = prepare(
            &mutation(config(), true, Some(u64::MAX), Some(u64::MAX)),
            "tenant",
        )
        .unwrap() else {
            panic!("full unsigned definition request");
        };
        assert_eq!(request.expected_generation, Some(u64::MAX));
        assert_eq!(request.reservation_limit_bytes, Some(u64::MAX));
    }

    #[test]
    fn zero_configuration_values_are_preserved_for_owner_priority() {
        for definition in [false, true] {
            let mut input = config();
            input.lock_duration_millis = Some(0);
            input.max_delivery_count = Some(0);
            input.default_ttl_millis = Some(0);
            input.ttl_unlimited = false;
            input.max_message_bytes = Some(0);
            input.duplicate_detection_history_time_window_millis = Some(0);
            let expected = input.protobuf();
            let actual =
                match prepare(&mutation(input, definition, Some(1), Some(1)), "tenant").unwrap() {
                    Prepared::Create(input) => input.config,
                    Prepared::SetDefinition(input) => input.config,
                    Prepared::Get(_) => panic!("full mutation request"),
                };
            assert_eq!(actual, Some(expected));
            assert_eq!(
                actual.unwrap().default_time_to_live,
                Some(DefaultTimeToLive::DefaultTtlMillis(0))
            );
        }
    }

    #[test]
    fn finite_response_json_is_exact_and_incomplete_identity_is_refused() {
        let json =
            serde_json::to_value(output(response(), "tenant", "orders", Some(u64::MAX)).unwrap())
                .unwrap();
        let mut keys = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            [
                "config",
                "generation",
                "namespace",
                "path",
                "reservation_limit_bytes",
                "reserved_logical_bytes",
                "retained_message_count"
            ]
        );
        for field in [
            "generation",
            "reservation_limit_bytes",
            "reserved_logical_bytes",
            "retained_message_count",
        ] {
            assert_eq!(json[field].as_u64(), Some(u64::MAX));
        }
        assert_eq!(json["config"].as_object().unwrap().len(), 8);
        assert!(json["config"]["default_time_to_live_millis"].is_null());
        for field in [
            "requires_session",
            "requires_duplicate_detection",
            "dead_lettering_on_message_expiration",
        ] {
            assert_eq!(json["config"][field], false);
        }
        let mut zero = response();
        zero.reserved_logical_bytes = 0;
        zero.retained_message_count = 0;
        let json = serde_json::to_value(output(zero, "tenant", "orders", None).unwrap()).unwrap();
        assert_eq!(json["reserved_logical_bytes"], 0);
        assert_eq!(json["retained_message_count"], 0);
        for fault in 0..5 {
            let mut invalid = response();
            match fault {
                0 => invalid.namespace = "other".into(),
                1 => invalid.path = "other".into(),
                2 => invalid.generation = 0,
                3 => invalid.reservation_limit_bytes = 0,
                _ => invalid.generation = 1,
            }
            assert!(matches!(
                output(invalid, "tenant", "orders", Some(u64::MAX)),
                Err(CliError::Input(RESPONSE_ERROR))
            ));
        }
        let mut invalid = response();
        invalid.config = None;
        assert!(matches!(
            output(invalid, "tenant", "orders", None),
            Err(CliError::Input(RESPONSE_CONFIG_ERROR))
        ));
        for field in 0..8 {
            let mut invalid = response();
            let config = invalid.config.as_mut().unwrap();
            match field {
                0 => config.lock_duration_millis = None,
                1 => config.max_delivery_count = None,
                2 => config.default_time_to_live = None,
                3 => config.max_message_bytes = None,
                4 => config.requires_session = None,
                5 => config.requires_duplicate_detection = None,
                6 => config.duplicate_detection_history_time_window_millis = None,
                _ => config.dead_lettering_on_message_expiration = None,
            }
            assert!(matches!(
                output(invalid, "tenant", "orders", None),
                Err(CliError::Input(RESPONSE_CONFIG_ERROR))
            ));
        }
    }

    #[test]
    fn finite_command_names_and_ttl_conflicts_are_closed() {
        assert_eq!(OPERATIONS, ["create", "get", "set-definition"]);
        for operation in OPERATIONS {
            let parsed =
                Arguments::try_parse_from(["switchyardctl", "finite-queue", operation, "orders"])
                    .unwrap();
            assert!(matches!(parsed.command, Command::FiniteQueue { .. }));
        }
        for operation in ["list", "update", "delete", "set-limit"] {
            assert!(
                Arguments::try_parse_from(["switchyardctl", "finite-queue", operation, "orders"])
                    .is_err()
            );
        }
        assert!(
            Arguments::try_parse_from([
                "switchyardctl",
                "finite-queue",
                "create",
                "orders",
                "--default-ttl-millis",
                "1",
                "--ttl-unlimited",
            ])
            .is_err()
        );
    }
}
