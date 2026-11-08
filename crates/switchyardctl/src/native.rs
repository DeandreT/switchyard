//! Bounded TLS adapter over the existing generated entity client. Validation and
//! authorization stay on the server; optional values are not defaulted here.
use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use admin_api::wire::{self, entity_service_client::EntityServiceClient};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, Subcommand};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

const MAX_TOKEN: usize = 16 * 1024;
const MAX_CA: usize = 1024 * 1024;
const MAX_CURSOR: usize = 64 * 1024;
const MAX_RESPONSE: usize = 1024 * 1024;

#[derive(Debug, Default, Args)]
pub struct Connection {
    /// HTTPS origin. Hostname is verified against the trusted certificate.
    #[arg(long, global = true)]
    endpoint: Option<String>,
    /// Logical namespace, independent of endpoint host and JWT audience/resource.
    #[arg(long, global = true)]
    namespace: Option<String>,
    /// PEM trust anchors; machine trust is neither read nor changed.
    #[arg(long, global = true)]
    ca_file: Option<PathBuf>,
    /// One namespace-root Manage JWT in a regular file, never a token argument.
    #[arg(long, global = true)]
    token_file: Option<PathBuf>,
    /// Total network operation deadline, including all list pages.
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout_secs: u64,
    /// Maximum complete JSON output bytes; failures emit no partial catalog.
    #[arg(long, global = true, default_value_t = 4194304, value_parser = clap::value_parser!(u64).range(1024..=16777216))]
    max_output_bytes: u64,
}

#[derive(Debug, Args)]
pub struct EntityCommands {
    #[command(subcommand)]
    command: EntityCommand,
}
#[derive(Debug, Subcommand)]
enum EntityCommand {
    Create {
        path: String,
        #[command(flatten)]
        options: Options,
    },
    Get {
        path: String,
    },
    List(Paging),
}
#[derive(Debug, Args)]
pub struct SubscriptionCommands {
    #[command(subcommand)]
    command: SubscriptionCommand,
}
#[derive(Debug, Subcommand)]
enum SubscriptionCommand {
    Create {
        topic: String,
        name: String,
        #[command(flatten)]
        options: Options,
    },
    Get {
        topic: String,
        name: String,
    },
    /// List explicit subscriptions across topics in this namespace.
    List(Paging),
}

#[derive(Debug, Default, Args)]
pub struct Options {
    #[arg(long)]
    default_ttl_millis: Option<u64>,
    #[arg(long)]
    lock_duration_millis: Option<u64>,
    #[arg(long)]
    max_delivery_count: Option<u32>,
    #[arg(long)]
    max_message_bytes: Option<u64>,
    #[arg(long, action = clap::ArgAction::Set)]
    requires_session: Option<bool>,
    #[arg(long, action = clap::ArgAction::Set)]
    requires_duplicate_detection: Option<bool>,
    /// Zero retains the native API's default-history meaning.
    #[arg(long)]
    duplicate_detection_history_millis: Option<u64>,
}

#[derive(Debug, Args)]
pub struct Paging {
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=100))]
    page_size: u32,
    /// Maximum RPC pages, including empty continuing pages; exhaustion fails.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=10000))]
    max_pages: u32,
    /// Optional base64-encoded opaque native continuation bytes.
    #[arg(long)]
    continuation: Option<String>,
}

#[derive(Clone, Copy)]
pub enum Kind {
    Queue,
    Topic,
    Subscription,
}
impl Kind {
    fn wire(self) -> wire::EntityKind {
        match self {
            Self::Queue => wire::EntityKind::Queue,
            Self::Topic => wire::EntityKind::Topic,
            Self::Subscription => wire::EntityKind::Subscription,
        }
    }
    fn selection(self) -> wire::EntitySelection {
        match self {
            Self::Queue => wire::EntitySelection::Queues,
            Self::Topic => wire::EntitySelection::Topics,
            Self::Subscription => wire::EntitySelection::Subscriptions,
        }
    }
}
pub enum Operation {
    Create(Kind, Address, Options),
    Get(Kind, Address),
    List(Kind, Paging),
}
pub struct Address {
    path: String,
    topic: Option<String>,
    name: Option<String>,
}
impl Address {
    fn entity(path: String) -> Self {
        Self {
            path,
            topic: None,
            name: None,
        }
    }
    fn subscription(topic: String, name: String) -> Self {
        Self {
            path: String::new(),
            topic: Some(topic),
            name: Some(name),
        }
    }
}
impl EntityCommands {
    pub fn operation(self, kind: Kind) -> Operation {
        match self.command {
            EntityCommand::Create { path, options } => {
                Operation::Create(kind, Address::entity(path), options)
            }
            EntityCommand::Get { path } => Operation::Get(kind, Address::entity(path)),
            EntityCommand::List(paging) => Operation::List(kind, paging),
        }
    }
}
impl SubscriptionCommands {
    pub fn operation(self) -> Operation {
        match self.command {
            SubscriptionCommand::Create {
                topic,
                name,
                options,
            } => Operation::Create(
                Kind::Subscription,
                Address::subscription(topic, name),
                options,
            ),
            SubscriptionCommand::Get { topic, name } => {
                Operation::Get(Kind::Subscription, Address::subscription(topic, name))
            }
            SubscriptionCommand::List(paging) => Operation::List(Kind::Subscription, paging),
        }
    }
}

fn read_bounded(path: &Path, limit: usize, label: &str) -> Result<Vec<u8>, String> {
    // Nonblocking open prevents a substituted FIFO from hanging before the
    // regular-file check. No device/pipe input or unbounded read is supported.
    #[cfg(unix)]
    let file = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| format!("cannot open {label} file"))?;
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|_| format!("cannot open {label} file"))?;
    let metadata = file
        .metadata()
        .map_err(|_| format!("cannot inspect {label} file"))?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(format!(
            "{label} must be a regular file of at most {limit} bytes"
        ));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| format!("cannot read {label} file"))?;
    if bytes.len() > limit {
        return Err(format!("{label} file exceeds byte limit"));
    }
    Ok(bytes)
}

fn authorization(bytes: &[u8]) -> Result<MetadataValue<Ascii>, String> {
    // Permit only surrounding line endings, not arbitrary trim() whitespace.
    let token = bytes
        .strip_prefix(b"\r\n")
        .or_else(|| bytes.strip_prefix(b"\n"))
        .unwrap_or(bytes);
    let token = token
        .strip_suffix(b"\r\n")
        .or_else(|| token.strip_suffix(b"\n"))
        .unwrap_or(token);
    if token.is_empty() || !token.iter().all(|b| b.is_ascii_graphic()) {
        return Err(
            "token file must contain one credential with only an optional surrounding newline"
                .into(),
        );
    }
    let token = std::str::from_utf8(token).map_err(|_| "invalid token file".to_owned())?;
    let mut value: MetadataValue<Ascii> = format!("Bearer {token}")
        .parse()
        .map_err(|_| "invalid token file".to_owned())?;
    value.set_sensitive(true);
    Ok(value)
}

fn request<T>(body: T, authorization: &MetadataValue<Ascii>) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", authorization.clone());
    request
}

// Never format Status/transport Debug or arbitrary peer messages/metadata: a peer
// can reflect credentials. Keep the server's status classification/precedence.
fn rpc_error(status: tonic::Status) -> String {
    let hint = match status.code() {
        tonic::Code::Unauthenticated => "credential rejected; check token file and expiry",
        tonic::Code::PermissionDenied => "namespace-root Manage permission required",
        tonic::Code::AlreadyExists => "entity already exists",
        tonic::Code::NotFound => "entity or parent topic not found",
        tonic::Code::InvalidArgument => "invalid entity address, options or continuation",
        tonic::Code::Unimplemented => "requested entity option or operation is unsupported",
        tonic::Code::FailedPrecondition => "server precondition refused the operation",
        tonic::Code::DeadlineExceeded => {
            "RPC deadline exceeded; a submitted create may have completed"
        }
        tonic::Code::ResourceExhausted => "server capacity or response limit reached",
        _ => "server could not complete the operation",
    };
    format!("{}: {hint}", status.code())
}

fn entity_json(entity: wire::Entity) -> Value {
    json!({"namespace":entity.namespace,"path":entity.path,"kind":wire::EntityKind::try_from(entity.kind).map(|k| k.as_str_name()).unwrap_or("UNKNOWN"),
        "placement_group_id":entity.placement_group_id,"max_size_bytes":entity.max_size_bytes,"used_logical_bytes":entity.used_logical_bytes,
        "requires_duplicate_detection":entity.requires_duplicate_detection,"duplicate_detection_history_millis":entity.duplicate_detection_history_millis,
        "default_ttl_millis":entity.default_ttl_millis,"lock_duration_millis":entity.lock_duration_millis,"max_delivery_count":entity.max_delivery_count,
        "max_message_bytes":entity.max_message_bytes,"requires_session":entity.requires_session,"subscription_topic":entity.subscription_topic,"subscription_name":entity.subscription_name})
}

/// A complete JSON result is buffered within this byte cap; stdout is untouched
/// on any RPC, cursor, deadline or budget failure.
struct Output {
    bytes: Vec<u8>,
    limit: usize,
}
impl Output {
    fn append(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err("output byte budget exhausted; no complete result emitted".into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn entity(&mut self, entity: wire::Entity) -> Result<(), String> {
        self.append(
            &serde_json::to_vec(&entity_json(entity))
                .map_err(|_| "cannot encode entity output".to_owned())?,
        )
    }
}

pub fn run(connection: Connection, operation: Operation) -> Result<(), String> {
    let endpoint = connection
        .endpoint
        .ok_or("--endpoint is required for native commands")?;
    let namespace = connection
        .namespace
        .ok_or("--namespace is required for native commands")?;
    let ca = connection
        .ca_file
        .ok_or("--ca-file is required for native commands")?;
    let token = connection
        .token_file
        .ok_or("--token-file is required for native commands")?;
    let url = url::Url::parse(&endpoint).map_err(|_| "invalid HTTPS endpoint")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(
            "--endpoint must be an HTTPS origin without user information, path, query or fragment"
                .into(),
        );
    }
    let authorization = authorization(&read_bounded(&token, MAX_TOKEN, "token")?)?;
    let certificate = read_bounded(&ca, MAX_CA, "CA")?;
    let timeout = Duration::from_secs(connection.timeout_secs);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| "cannot start client runtime")?;
    let result = runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let channel = Endpoint::from_shared(url.to_string()).map_err(|_| "invalid HTTPS endpoint")?
                .connect_timeout(timeout).timeout(timeout)
                .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(certificate)))
                .map_err(|_| "cannot configure TLS trust; check CA file")?
                .connect().await.map_err(|_| "TLS connection failed; check endpoint, trusted CA and certificate hostname")?;
            let mut client = EntityServiceClient::new(channel).max_decoding_message_size(MAX_RESPONSE);
            let mut output = Output { bytes: Vec::new(), limit: connection.max_output_bytes as usize };
            match operation {
                Operation::Create(kind, address, options) => {
                    let body = wire::CreateEntityRequest {
                        namespace, path: address.path, kind: kind.wire() as i32,
                        subscription_topic: address.topic, subscription_name: address.name,
                        default_ttl_millis: options.default_ttl_millis, lock_duration_millis: options.lock_duration_millis,
                        max_delivery_count: options.max_delivery_count, max_message_bytes: options.max_message_bytes,
                        requires_session: options.requires_session, requires_duplicate_detection: options.requires_duplicate_detection,
                        duplicate_detection_history_millis: options.duplicate_detection_history_millis.unwrap_or(0), ..Default::default()
                    };
                    output.entity(client.create_entity(request(body, &authorization)).await.map_err(rpc_error)?.into_inner())?;
                }
                Operation::Get(kind, address) => {
                    let body = wire::GetEntityRequest { namespace, path: address.path, subscription_topic: address.topic, subscription_name: address.name };
                    let entity = client.get_entity(request(body, &authorization)).await.map_err(rpc_error)?.into_inner();
                    if entity.kind != kind.wire() as i32 { return Err("entity exists with a different kind than requested".into()); }
                    output.entity(entity)?;
                }
                Operation::List(kind, paging) => {
                    let mut continuation = match paging.continuation {
                        Some(encoded) if encoded.len() <= MAX_CURSOR * 4 / 3 + 4 => Some(STANDARD.decode(encoded).map_err(|_| "invalid base64 continuation")?),
                        Some(_) => return Err("continuation exceeds byte limit".into()), None => None,
                    };
                    let mut seen = HashSet::new();
                    if let Some(cursor) = &continuation { check_cursor(cursor)?; seen.insert(Sha256::digest(cursor)); }
                    let mut first = true;
                    output.append(b"{\"entities\":[")?;
                    for page_index in 0..paging.max_pages {
                        let body = wire::ListEntitiesRequest { namespace: namespace.clone(), page_size: paging.page_size, selection: kind.selection() as i32, continuation, ..Default::default() };
                        let page = client.list_entities(request(body, &authorization)).await.map_err(rpc_error)?.into_inner();
                        reject_textual_continuation(&page)?;
                        if page.entities.len() > paging.page_size as usize { return Err("server exceeded requested page size".into()); }
                        for entity in page.entities {
                            if !first { output.append(b",")?; } first = false; output.entity(entity)?;
                        }
                        continuation = page.continuation;
                        match &continuation {
                            None => break,
                            Some(cursor) => {
                                check_cursor(cursor)?;
                                if !seen.insert(Sha256::digest(cursor)) { return Err("continuation cycle detected; no complete result emitted".into()); }
                                if page_index + 1 == paging.max_pages { return Err("page budget exhausted; no complete result emitted".into()); }
                            }
                        }
                    }
                    output.append(b"],\"continuation\":null}")?;
                }
            }
            output.append(b"\n")?;
            Ok::<_, String>(output.bytes)
        }).await.map_err(|_| "command deadline exceeded; a submitted create may have completed".to_owned())?
    });
    // Tokio's DNS resolver can be a blocking OS call. After the operation's
    // deadline, runtime Drop must not wait indefinitely for that worker. This
    // is the CLI's final operation; process exit terminates any such worker.
    runtime.shutdown_background();
    let bytes = result?;
    std::io::stdout()
        .lock()
        .write_all(&bytes)
        .map_err(|_| "cannot write output".to_owned())
}

fn check_cursor(cursor: &[u8]) -> Result<(), String> {
    if cursor.len() > MAX_CURSOR {
        return Err("server continuation exceeds byte limit".into());
    }
    Ok(())
}

#[allow(deprecated)]
fn reject_textual_continuation(page: &wire::ListEntitiesResponse) -> Result<(), String> {
    if !page.next_page_token.is_empty() {
        return Err(
            "server returned an unsupported textual continuation; no complete result emitted"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_line_endings_are_narrow_and_metadata_debug_is_redacted() {
        for token in [
            b"public-fixture".as_slice(),
            b"public-fixture\n",
            b"\npublic-fixture\n",
            b"\r\npublic-fixture\r\n",
        ] {
            let header = authorization(token).unwrap();
            assert_eq!(header.to_str().unwrap(), "Bearer public-fixture");
            assert!(header.is_sensitive());
            assert!(!format!("{header:?}").contains("public-fixture"));
        }
        for token in [
            b"".as_slice(),
            b"\n",
            b" token",
            b"token ",
            b"token\t",
            b"one\ntwo",
            b"one\rtwo",
            b"token\n\n",
            b"\xff",
        ] {
            let error = authorization(token).unwrap_err();
            assert_eq!(
                error,
                "token file must contain one credential with only an optional surrounding newline"
            );
        }
    }

    #[test]
    fn reflected_server_messages_and_metadata_are_never_formatted() {
        let mut status = tonic::Status::unauthenticated("REFLECTED-CREDENTIAL");
        status
            .metadata_mut()
            .insert("secret", "REFLECTED-CREDENTIAL".parse().unwrap());
        let error = rpc_error(status);
        assert!(error.contains("credential rejected"));
        assert!(!error.contains("REFLECTED-CREDENTIAL"));
    }

    #[test]
    fn bounded_regular_file_reads_reject_large_files_and_directories() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input");
        std::fs::write(&path, b"1234").unwrap();
        assert_eq!(read_bounded(&path, 4, "test").unwrap(), b"1234");
        assert!(read_bounded(&path, 3, "test").is_err());
        assert!(read_bounded(directory.path(), 100, "test").is_err());
        #[cfg(unix)]
        {
            let fifo = directory.path().join("fifo");
            rustix::fs::mknodat(
                rustix::fs::CWD,
                &fifo,
                rustix::fs::FileType::Fifo,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
                0,
            )
            .unwrap();
            assert!(read_bounded(&fifo, 100, "test").is_err());
        }
    }
}
