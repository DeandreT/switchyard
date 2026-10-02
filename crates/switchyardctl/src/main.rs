#![forbid(unsafe_code)]

use std::{
    fmt,
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
    time::Duration,
};

use admin_api::{
    PROTOBUF_PACKAGE,
    v1::{
        CreateEntityRequest, DeleteEntityRequest, Entity, EntityKind, GetEntityRequest,
        ListEntitiesRequest, ListEntitiesResponse, QueueConfiguration, UnlimitedTimeToLive,
        UpdateEntityRequest, entity_service_client::EntityServiceClient,
        queue_configuration::DefaultTimeToLive,
    },
};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::{Host, Position, Url};

mod rules;
mod topology;

use rules::RuleCommand;
use topology::{
    SubscriptionCommand, SubscriptionConfigurationOutput, TopicCommand, TopicConfigurationOutput,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CA_FILE_BYTES: usize = 1024 * 1024;
const MAX_TOKEN_FILE_BYTES: usize = 16 * 1024;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_PAGE_TOKEN_BYTES: usize = 512;

#[derive(Debug, Parser)]
#[command(name = "switchyardctl", version, about = "Administer Switchyard")]
struct Arguments {
    #[arg(long, global = true, default_value = "https://127.0.0.1:9443")]
    endpoint: String,
    #[arg(long, global = true, default_value = "development")]
    namespace: String,
    #[arg(long, visible_alias = "ca-pem", global = true, value_name = "PATH")]
    ca_certificate: Option<PathBuf>,
    #[arg(long, global = true)]
    tls_server_name: Option<String>,
    #[arg(long, global = true, value_name = "PATH")]
    token_file: Option<PathBuf>,
    /// Allow unauthenticated development HTTP on loopback only.
    #[arg(long, global = true)]
    allow_insecure: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the native API contract and supported operations.
    Compatibility,
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    Subscription {
        #[command(subcommand)]
        command: SubscriptionCommand,
    },
    Rule {
        #[command(subcommand)]
        command: RuleCommand,
    },
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    Create(QueueMutation),
    Delete {
        path: String,
    },
    Get {
        path: String,
    },
    List {
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        #[arg(long, default_value = "")]
        page_token: String,
    },
    Update(QueueMutation),
}

#[derive(Debug, Args)]
struct QueueMutation {
    path: String,
    #[command(flatten)]
    configuration: ConfigurationArguments,
}

#[derive(Debug, Default, Args)]
struct ConfigurationArguments {
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
    #[arg(long, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    requires_duplicate_detection: Option<bool>,
    #[arg(long, visible_alias = "duplicate-detection-window-millis")]
    duplicate_detection_history_time_window_millis: Option<u64>,
    #[arg(long, visible_alias = "dead-letter-on-expiration", num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    dead_lettering_on_message_expiration: Option<bool>,
}

impl ConfigurationArguments {
    fn protobuf(&self) -> QueueConfiguration {
        QueueConfiguration {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live: self
                .default_ttl_millis
                .map(DefaultTimeToLive::DefaultTtlMillis)
                .or_else(|| {
                    self.ttl_unlimited
                        .then_some(DefaultTimeToLive::DefaultTtlUnlimited(
                            UnlimitedTimeToLive {},
                        ))
                }),
            max_message_bytes: self.max_message_bytes,
            requires_session: self.requires_session,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: self.dead_lettering_on_message_expiration,
        }
    }
}

#[derive(Debug)]
enum CliError {
    Input(&'static str),
    Connect,
    Request(tonic::Code),
    Timeout,
    Output,
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(message) => formatter.write_str(message),
            Self::Connect => {
                formatter.write_str("could not establish the administration connection")
            }
            Self::Request(code) => write!(formatter, "administration request failed ({code:?})"),
            Self::Timeout => formatter.write_str("administration request timed out"),
            Self::Output => formatter.write_str("could not write the JSON response"),
        }
    }
}

struct ConnectionSettings {
    endpoint: String,
    tls: bool,
    ca_pem: Option<Vec<u8>>,
    tls_server_name: Option<String>,
    token: Option<MetadataValue<Ascii>>,
}

impl ConnectionSettings {
    fn prepare(arguments: &Arguments) -> Result<Self, CliError> {
        let url = validate_endpoint(arguments)?;
        validate_identifier(&arguments.namespace, 50, "invalid namespace")?;
        let ca_pem = arguments
            .ca_certificate
            .as_deref()
            .map(|path| read_file(path, MAX_CA_FILE_BYTES, "could not read the CA file"))
            .transpose()?;
        if let Some(pem) = &ca_pem {
            validate_ca(pem)?;
        } else if url.scheme() == "https" {
            return Err(CliError::Input("HTTPS requires --ca-certificate"));
        }
        let token = arguments
            .token_file
            .as_deref()
            .map(|path| read_file(path, MAX_TOKEN_FILE_BYTES, "could not read the token file"))
            .transpose()?
            .map(|bytes| token_metadata(&bytes))
            .transpose()?;
        Ok(Self {
            tls: url.scheme() == "https",
            endpoint: url.into(),
            ca_pem,
            tls_server_name: arguments.tls_server_name.clone(),
            token,
        })
    }

    async fn connect_channel(&self) -> Result<Channel, CliError> {
        let mut endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|_| CliError::Input("invalid administration endpoint"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);
        if self.tls {
            let mut tls = ClientTlsConfig::new();
            if let Some(name) = &self.tls_server_name {
                tls = tls.domain_name(name);
            }
            if let Some(pem) = &self.ca_pem {
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|_| CliError::Input("invalid TLS configuration"))?;
        }
        tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| CliError::Timeout)?
            .map_err(|_| CliError::Connect)
    }

    async fn connect(&self) -> Result<EntityServiceClient<Channel>, CliError> {
        Ok(EntityServiceClient::new(self.connect_channel().await?)
            .max_encoding_message_size(MAX_REQUEST_BYTES)
            .max_decoding_message_size(MAX_RESPONSE_BYTES))
    }

    fn request<T>(&self, input: T) -> Request<T> {
        let mut request = Request::new(input);
        request.set_timeout(REQUEST_TIMEOUT);
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        request
    }
}

fn validate_endpoint(arguments: &Arguments) -> Result<Url, CliError> {
    if arguments.endpoint.len() > 2048 {
        return Err(CliError::Input("administration endpoint is too long"));
    }
    if arguments.endpoint.contains('@')
        || arguments.endpoint.trim() != arguments.endpoint
        || arguments
            .endpoint
            .bytes()
            .any(|byte| byte.is_ascii_control())
    {
        return Err(CliError::Input("invalid administration endpoint"));
    }
    let url = Url::parse(&arguments.endpoint)
        .map_err(|_| CliError::Input("invalid administration endpoint"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return Err(CliError::Input("endpoint must be an HTTP or HTTPS origin"));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url[..Position::BeforeHost].ends_with('@')
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(CliError::Input(
            "endpoint must not contain credentials, a query, a fragment, or a path",
        ));
    }
    if let Some(name) = &arguments.tls_server_name {
        rustls::pki_types::ServerName::try_from(name.clone())
            .map_err(|_| CliError::Input("invalid TLS server name"))?;
    }
    if url.scheme() == "http" {
        let loopback = match url.host() {
            Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(Host::Ipv4(address)) => address.is_loopback(),
            Some(Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        };
        if !arguments.allow_insecure || !loopback {
            return Err(CliError::Input(
                "development HTTP requires --allow-insecure and a loopback endpoint",
            ));
        }
        if arguments.token_file.is_some() {
            return Err(CliError::Input("shared-access tokens require HTTPS"));
        }
        if arguments.ca_certificate.is_some() || arguments.tls_server_name.is_some() {
            return Err(CliError::Input("TLS options require HTTPS"));
        }
    } else if arguments.allow_insecure {
        return Err(CliError::Input(
            "--allow-insecure is only supported for development HTTP",
        ));
    }
    Ok(url)
}

fn validate_identifier(value: &str, maximum: usize, error: &'static str) -> Result<(), CliError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(CliError::Input(error));
    }
    Ok(())
}

fn read_file(path: &Path, maximum: usize, error: &'static str) -> Result<Vec<u8>, CliError> {
    let file = open_credential_file(path, error)?;
    read_regular_file(file, maximum, error)
}

#[cfg(unix)]
fn open_credential_file(path: &Path, error: &'static str) -> Result<File, CliError> {
    use rustix::fs::{Mode, OFlags};

    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| CliError::Input(error))?;
    Ok(File::from(descriptor))
}

#[cfg(not(unix))]
fn open_credential_file(path: &Path, error: &'static str) -> Result<File, CliError> {
    if !path
        .metadata()
        .map_err(|_| CliError::Input(error))?
        .is_file()
    {
        return Err(CliError::Input("credential files must be regular files"));
    }
    File::open(path).map_err(|_| CliError::Input(error))
}

fn read_regular_file(file: File, maximum: usize, error: &'static str) -> Result<Vec<u8>, CliError> {
    if !file
        .metadata()
        .map_err(|_| CliError::Input(error))?
        .is_file()
    {
        return Err(CliError::Input("credential files must be regular files"));
    }
    read_limited(file, maximum, error)
}

fn read_limited(
    reader: impl Read,
    maximum: usize,
    error: &'static str,
) -> Result<Vec<u8>, CliError> {
    let mut bytes = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CliError::Input(error))?;
    if bytes.len() > maximum {
        return Err(CliError::Input("credential file exceeds its size limit"));
    }
    Ok(bytes)
}

fn validate_ca(pem: &[u8]) -> Result<(), CliError> {
    let mut certificates = 0;
    let mut roots = rustls::RootCertStore::empty();
    for item in rustls_pemfile::read_all(&mut BufReader::new(pem)) {
        let rustls_pemfile::Item::X509Certificate(certificate) =
            item.map_err(|_| CliError::Input("invalid CA PEM file"))?
        else {
            return Err(CliError::Input(
                "CA PEM file must contain only certificates",
            ));
        };
        roots
            .add(certificate)
            .map_err(|_| CliError::Input("invalid CA certificate"))?;
        certificates += 1;
    }
    if certificates == 0 {
        return Err(CliError::Input("CA PEM file contains no certificates"));
    }
    Ok(())
}

fn token_metadata(bytes: &[u8]) -> Result<MetadataValue<Ascii>, CliError> {
    let token = std::str::from_utf8(bytes)
        .map_err(|_| CliError::Input("invalid shared-access token file"))?
        .trim();
    if token.is_empty()
        || !token.starts_with("SharedAccessSignature ")
        || !token.is_ascii()
        || token.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(CliError::Input("invalid shared-access token file"));
    }
    let mut metadata = MetadataValue::try_from(token)
        .map_err(|_| CliError::Input("invalid shared-access token file"))?;
    metadata.set_sensitive(true);
    Ok(metadata)
}

#[derive(Serialize)]
struct EntityOutput {
    namespace: String,
    path: String,
    kind: &'static str,
    placement_group_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    used_logical_bytes: Option<u64>,
    queue_config: Option<ConfigurationOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_config: Option<TopicConfigurationOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_config: Option<SubscriptionConfigurationOutput>,
}

impl From<Entity> for EntityOutput {
    fn from(entity: Entity) -> Self {
        Self {
            namespace: entity.namespace,
            path: entity.path,
            kind: match EntityKind::try_from(entity.kind) {
                Ok(EntityKind::Queue) => "queue",
                Ok(EntityKind::Topic) => "topic",
                Ok(EntityKind::Subscription) => "subscription",
                Ok(EntityKind::Unspecified) => "unspecified",
                Err(_) => "unknown",
            },
            placement_group_id: entity.placement_group_id,
            max_size_bytes: entity.max_size_bytes,
            used_logical_bytes: entity.used_logical_bytes,
            queue_config: entity.queue_config.map(Into::into),
            topic_config: entity.topic_config.map(Into::into),
            subscription_config: entity.subscription_config.map(Into::into),
        }
    }
}

#[derive(Serialize)]
struct ConfigurationOutput {
    lock_duration_millis: Option<u64>,
    max_delivery_count: Option<u32>,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: Option<u64>,
    requires_session: Option<bool>,
    requires_duplicate_detection: Option<bool>,
    duplicate_detection_history_time_window_millis: Option<u64>,
    dead_lettering_on_message_expiration: Option<bool>,
}

impl From<QueueConfiguration> for ConfigurationOutput {
    fn from(configuration: QueueConfiguration) -> Self {
        Self {
            lock_duration_millis: configuration.lock_duration_millis,
            max_delivery_count: configuration.max_delivery_count,
            default_time_to_live_millis: configuration.default_time_to_live.and_then(
                |ttl| match ttl {
                    DefaultTimeToLive::DefaultTtlMillis(millis) => Some(millis),
                    DefaultTimeToLive::DefaultTtlUnlimited(_) => None,
                },
            ),
            max_message_bytes: configuration.max_message_bytes,
            requires_session: configuration.requires_session,
            requires_duplicate_detection: configuration.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: configuration
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: configuration
                .dead_lettering_on_message_expiration,
        }
    }
}

#[derive(Serialize)]
struct ListOutput {
    entities: Vec<EntityOutput>,
    next_page_token: String,
}

impl From<ListEntitiesResponse> for ListOutput {
    fn from(response: ListEntitiesResponse) -> Self {
        Self {
            entities: response.entities.into_iter().map(Into::into).collect(),
            next_page_token: response.next_page_token,
        }
    }
}

#[derive(Serialize)]
struct CompatibilityOutput {
    package: &'static str,
    transport: &'static str,
    version: &'static str,
    queue_operations: [&'static str; 5],
    topic_operations: [&'static str; 5],
    subscription_operations: [&'static str; 5],
    rule_operations: [&'static str; 4],
}

fn write_output(output: &impl Serialize) -> Result<(), CliError> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, output).map_err(|_| CliError::Output)?;
    io::Write::write_all(&mut stdout, b"\n").map_err(|_| CliError::Output)
}

async fn execute(arguments: Arguments) -> Result<(), CliError> {
    let command = match &arguments.command {
        Command::Compatibility => {
            return write_output(&CompatibilityOutput {
                package: PROTOBUF_PACKAGE,
                transport: "grpc",
                version: env!("CARGO_PKG_VERSION"),
                queue_operations: ["create", "get", "list", "update", "delete"],
                topic_operations: ["create", "get", "list", "update", "delete"],
                subscription_operations: ["create", "get", "list", "update", "delete"],
                rule_operations: ["create", "get", "list", "delete"],
            });
        }
        Command::Topic { command } => return topology::execute_topic(&arguments, command).await,
        Command::Subscription { command } => {
            return topology::execute_subscription(&arguments, command).await;
        }
        Command::Rule { command } => return rules::execute(&arguments, command).await,
        Command::Queue { command } => command,
    };
    validate_queue_command(command)?;
    let settings = ConnectionSettings::prepare(&arguments)?;
    let mut client = settings.connect().await?;
    let namespace = arguments.namespace;
    let operation = async {
        match command {
            QueueCommand::Create(input) => {
                let response = client
                    .create_entity(settings.request(CreateEntityRequest {
                        namespace,
                        path: input.path.clone(),
                        kind: EntityKind::Queue as i32,
                        queue_config: Some(input.configuration.protobuf()),
                        ..CreateEntityRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                topology::write_entity(response.into_inner(), EntityKind::Queue)
            }
            QueueCommand::Get { path } => {
                let response = client
                    .get_entity(settings.request(GetEntityRequest {
                        namespace,
                        path: path.clone(),
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                topology::write_entity(response.into_inner(), EntityKind::Queue)
            }
            QueueCommand::Delete { path } => {
                let response = client
                    .delete_entity(settings.request(DeleteEntityRequest {
                        namespace,
                        path: path.clone(),
                        kind: EntityKind::Queue as i32,
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                topology::write_operation(response.into_inner())
            }
            QueueCommand::List {
                page_size,
                page_token,
            } => {
                let response = client
                    .list_entities(settings.request(ListEntitiesRequest {
                        namespace,
                        page_size: *page_size,
                        page_token: page_token.clone(),
                        ..ListEntitiesRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                write_output(&ListOutput::from(response.into_inner()))
            }
            QueueCommand::Update(input) => {
                let response = client
                    .update_entity(settings.request(UpdateEntityRequest {
                        namespace,
                        path: input.path.clone(),
                        queue_config: Some(input.configuration.protobuf()),
                        ..UpdateEntityRequest::default()
                    }))
                    .await
                    .map_err(|status| CliError::Request(status.code()))?;
                topology::write_entity(response.into_inner(), EntityKind::Queue)
            }
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, operation)
        .await
        .map_err(|_| CliError::Timeout)?
}

fn validate_queue_command(command: &QueueCommand) -> Result<(), CliError> {
    match command {
        QueueCommand::Create(input) | QueueCommand::Update(input) => {
            validate_queue_path(&input.path)
        }
        QueueCommand::Get { path } | QueueCommand::Delete { path } => validate_queue_path(path),
        QueueCommand::List {
            page_size,
            page_token,
        } => {
            if *page_size > 1024 {
                return Err(CliError::Input("page size cannot exceed 1024"));
            }
            if page_token.len() > MAX_PAGE_TOKEN_BYTES
                || !page_token.is_ascii()
                || page_token.bytes().any(|byte| byte.is_ascii_control())
            {
                return Err(CliError::Input("invalid queue page token"));
            }
            Ok(())
        }
    }
}

fn validate_queue_path(path: &str) -> Result<(), CliError> {
    validate_identifier(path, 260, "invalid entity path")?;
    if path.to_ascii_lowercase().ends_with("/$deadletterqueue") {
        return Err(CliError::Input(
            "dead-letter queues cannot be administered directly",
        ));
    }
    if path
        .as_bytes()
        .windows(b"/subscriptions/".len())
        .any(|part| part.eq_ignore_ascii_case(b"/subscriptions/"))
    {
        return Err(CliError::Input(
            "subscription paths require the subscription command",
        ));
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = execute(Arguments::parse()).await {
        eprintln!("switchyardctl: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(options: &[&str]) -> Arguments {
        let mut argv = vec!["switchyardctl"];
        argv.extend_from_slice(options);
        argv.extend(["queue", "get", "orders"]);
        Arguments::try_parse_from(argv).unwrap()
    }

    fn configuration(options: &[&str]) -> QueueConfiguration {
        let mut argv = vec!["switchyardctl", "queue", "update", "orders"];
        argv.extend_from_slice(options);
        let Arguments {
            command:
                Command::Queue {
                    command: QueueCommand::Update(input),
                },
            ..
        } = Arguments::try_parse_from(argv).unwrap()
        else {
            panic!("queue update");
        };
        input.configuration.protobuf()
    }

    #[test]
    fn connection_defaults_match_the_native_listener() {
        let arguments = arguments(&[]);
        assert_eq!(arguments.endpoint, "https://127.0.0.1:9443");
        assert_eq!(arguments.namespace, "development");
        assert!(!arguments.allow_insecure);
        assert!(validate_endpoint(&arguments).is_ok());
        assert!(ConnectionSettings::prepare(&arguments).is_err());
    }

    #[test]
    fn configuration_omission_does_not_manufacture_updates() {
        assert_eq!(configuration(&[]), QueueConfiguration::default());
    }

    #[test]
    fn zero_and_false_values_remain_present() {
        let config = configuration(&[
            "--lock-duration-millis",
            "0",
            "--max-delivery-count",
            "0",
            "--default-ttl-millis",
            "0",
            "--max-message-bytes",
            "0",
            "--requires-session=false",
            "--requires-duplicate-detection=false",
            "--duplicate-detection-window-millis",
            "0",
            "--dead-lettering-on-message-expiration=false",
        ]);
        assert_eq!(config.lock_duration_millis, Some(0));
        assert_eq!(config.max_delivery_count, Some(0));
        assert_eq!(
            config.default_time_to_live,
            Some(DefaultTimeToLive::DefaultTtlMillis(0))
        );
        assert_eq!(config.max_message_bytes, Some(0));
        assert_eq!(config.requires_session, Some(false));
        assert_eq!(config.requires_duplicate_detection, Some(false));
        assert_eq!(
            config.duplicate_detection_history_time_window_millis,
            Some(0)
        );
        assert_eq!(config.dead_lettering_on_message_expiration, Some(false));
    }

    #[test]
    fn bare_boolean_flags_and_unlimited_ttl_are_explicit() {
        let config = configuration(&[
            "--requires-session",
            "--requires-duplicate-detection",
            "--dead-letter-on-expiration",
            "--ttl-unlimited",
        ]);
        assert_eq!(config.requires_session, Some(true));
        assert_eq!(config.requires_duplicate_detection, Some(true));
        assert_eq!(config.dead_lettering_on_message_expiration, Some(true));
        assert_eq!(
            config.default_time_to_live,
            Some(DefaultTimeToLive::DefaultTtlUnlimited(
                UnlimitedTimeToLive {}
            ))
        );
    }

    #[test]
    fn ttl_choices_are_mutually_exclusive() {
        assert!(
            Arguments::try_parse_from([
                "switchyardctl",
                "queue",
                "create",
                "orders",
                "--default-ttl-millis",
                "1",
                "--ttl-unlimited",
            ])
            .is_err()
        );
    }

    #[test]
    fn globals_and_list_continuations_parse_after_subcommands() {
        let arguments = Arguments::try_parse_from([
            "switchyardctl",
            "queue",
            "list",
            "--namespace",
            "tenant",
            "--page-size",
            "7",
            "--page-token",
            "v1.cursor",
            "--endpoint",
            "https://localhost:9443",
        ])
        .unwrap();
        assert_eq!(arguments.namespace, "tenant");
        let Command::Queue {
            command:
                QueueCommand::List {
                    page_size,
                    page_token,
                },
        } = arguments.command
        else {
            panic!("list");
        };
        assert_eq!(page_size, 7);
        assert_eq!(page_token, "v1.cursor");
    }

    #[test]
    fn endpoint_rejects_credentials_and_unsupported_components() {
        for endpoint in [
            "not-an-endpoint",
            "ftp://localhost",
            "https://user:secret@localhost",
            "https://user@localhost",
            "https://@localhost",
            "https://localhost/path",
            "https://localhost/%2f",
            "https://localhost?token=secret",
            "https://localhost#secret",
            "https://localhost?",
            "https://localhost#",
        ] {
            assert!(
                validate_endpoint(&arguments(&["--endpoint", endpoint])).is_err(),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn development_http_requires_opt_in_loopback_and_no_credentials() {
        assert!(validate_endpoint(&arguments(&["--endpoint", "http://localhost:9443"])).is_err());
        for host in ["localhost", "127.0.0.1", "[::1]"] {
            let endpoint = format!("http://{host}:9443");
            assert!(
                validate_endpoint(&arguments(&["--endpoint", &endpoint, "--allow-insecure"]))
                    .is_ok()
            );
        }
        for options in [
            vec!["--endpoint", "http://example.com", "--allow-insecure"],
            vec![
                "--endpoint",
                "http://127.0.0.1",
                "--allow-insecure",
                "--token-file",
                "missing",
            ],
            vec![
                "--endpoint",
                "http://localhost",
                "--allow-insecure",
                "--ca-certificate",
                "missing",
            ],
            vec![
                "--endpoint",
                "http://localhost",
                "--allow-insecure",
                "--tls-server-name",
                "localhost",
            ],
            vec!["--allow-insecure"],
        ] {
            assert!(validate_endpoint(&arguments(&options)).is_err());
        }
    }

    #[test]
    fn tls_name_is_validated_before_networking() {
        for name in ["", "not/a/name", "localhost:9443", "name\n"] {
            assert!(validate_endpoint(&arguments(&["--tls-server-name", name])).is_err());
        }
        assert!(validate_endpoint(&arguments(&["--tls-server-name", "localhost"])).is_ok());
    }

    #[test]
    fn file_reads_stop_at_the_limit_plus_one() {
        assert_eq!(
            read_limited(&b"abcd"[..], 4, "read failed").unwrap(),
            b"abcd"
        );
        assert!(read_limited(&b"abcde"[..], 4, "read failed").is_err());
        assert!(read_limited(io::repeat(0), 4, "read failed").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn fifo_credentials_and_symlinks_to_them_are_refused_without_waiting() {
        use std::{os::unix::fs::symlink, sync::mpsc, thread};

        use rustix::fs::{CWD, Mode, mkfifoat};

        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        let link = directory.path().join("fifo-link");
        symlink(&fifo, &link).unwrap();
        for path in [fifo, link] {
            let (sender, result) = mpsc::channel();
            let worker = thread::spawn(move || {
                let _ = sender.send(read_file(&path, 16, "read failed"));
            });
            let error = result
                .recv_timeout(Duration::from_secs(1))
                .expect("opening a FIFO must not wait for a writer")
                .unwrap_err();
            assert_eq!(error.to_string(), "credential files must be regular files");
            worker.join().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_credential_symlinks_remain_supported() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::write(&source, b"credential").unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&source, &link).unwrap();
        assert_eq!(read_file(&link, 16, "read failed").unwrap(), b"credential");
    }

    #[cfg(unix)]
    #[test]
    fn regular_file_validation_uses_the_opened_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::write(&source, b"original credential").unwrap();
        let opened = open_credential_file(&source, "open failed").unwrap();
        std::fs::remove_file(&source).unwrap();
        std::fs::create_dir(&source).unwrap();
        assert_eq!(
            read_regular_file(opened, 32, "read failed").unwrap(),
            b"original credential"
        );
        let opened_directory = open_credential_file(&source, "open failed").unwrap();
        assert!(read_regular_file(opened_directory, 32, "read failed").is_err());
    }

    #[test]
    fn invalid_ca_content_is_rejected_before_connect() {
        for pem in [
            &b""[..],
            &b"not a certificate"[..],
            &b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"[..],
            &b"-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"[..],
        ] {
            assert!(validate_ca(pem).is_err());
        }
    }

    #[test]
    fn tokens_are_file_only_sensitive_metadata_with_no_line_breaks() {
        let token = b"SharedAccessSignature sr=resource&sig=secret&se=123&skn=manage\r\n";
        let metadata = token_metadata(token).unwrap();
        assert!(metadata.is_sensitive());
        assert_eq!(
            metadata.to_str().unwrap(),
            std::str::from_utf8(token).unwrap().trim()
        );
        for token in [
            &b""[..],
            &b"Bearer secret"[..],
            &b"SharedAccessSignature sr=x\nsig=secret"[..],
            &b"SharedAccessSignature \xff"[..],
        ] {
            assert!(token_metadata(token).is_err());
        }
        assert!(
            Arguments::try_parse_from([
                "switchyardctl",
                "queue",
                "get",
                "orders",
                "--token",
                "secret"
            ])
            .is_err()
        );
    }

    #[test]
    fn errors_do_not_include_remote_messages_or_credentials() {
        let status = tonic::Status::unauthenticated("secret-token-from-remote");
        let displayed = CliError::Request(status.code()).to_string();
        assert!(displayed.contains("Unauthenticated"));
        assert!(!displayed.contains("secret"));
        let error = token_metadata(b"secret-token-from-file")
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("secret-token-from-file"));
    }

    #[test]
    fn output_preserves_finite_zero_false_and_unlimited_ttl() {
        let config = ConfigurationOutput::from(configuration(&[
            "--default-ttl-millis",
            "0",
            "--requires-session=false",
        ]));
        let json = serde_json::to_value(config).unwrap();
        assert_eq!(json["default_time_to_live_millis"], 0);
        assert_eq!(json["requires_session"], false);
        assert!(json["requires_duplicate_detection"].is_null());
        let config = ConfigurationOutput::from(configuration(&["--ttl-unlimited"]));
        let json = serde_json::to_value(config).unwrap();
        assert!(json["default_time_to_live_millis"].is_null());
    }

    #[test]
    fn output_lists_entities_and_continuation_without_private_metadata() {
        let output = ListOutput::from(ListEntitiesResponse {
            entities: vec![Entity {
                namespace: "tenant".into(),
                path: "orders".into(),
                kind: EntityKind::Queue as i32,
                queue_config: Some(configuration(&["--max-message-bytes", "8192"])),
                ..Entity::default()
            }],
            next_page_token: "v1.cursor".into(),
        });
        let json = serde_json::to_value(output).unwrap();
        assert_eq!(json["entities"][0]["kind"], "queue");
        assert_eq!(json["entities"][0]["path"], "orders");
        assert_eq!(
            json["entities"][0]["queue_config"]["max_message_bytes"],
            8192
        );
        assert_eq!(json["next_page_token"], "v1.cursor");
        assert!(json.get("authorization").is_none());
        assert!(json["entities"][0].get("max_size_bytes").is_none());
        assert!(json["entities"][0].get("used_logical_bytes").is_none());
    }

    #[test]
    fn reported_entity_capacity_and_usage_preserve_zero_presence() {
        let output = EntityOutput::from(Entity {
            max_size_bytes: Some(0),
            used_logical_bytes: Some(0),
            ..Entity::default()
        });
        let json = serde_json::to_value(output).unwrap();
        assert_eq!(json["max_size_bytes"], 0);
        assert_eq!(json["used_logical_bytes"], 0);
    }

    #[test]
    fn invalid_scope_and_pagination_are_local_errors() {
        for path in ["", "orders\n", "orders/$deadletterqueue"] {
            assert!(validate_queue_path(path).is_err());
        }
        assert!(validate_identifier("", 50, "namespace").is_err());
        assert!(validate_identifier(&"n".repeat(51), 50, "namespace").is_err());
        for (page_size, token) in [
            (1025, "".to_owned()),
            (1, "x".repeat(513)),
            (1, "\n".to_owned()),
        ] {
            assert!(
                validate_queue_command(&QueueCommand::List {
                    page_size,
                    page_token: token
                })
                .is_err()
            );
        }
    }
}
