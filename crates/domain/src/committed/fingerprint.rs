use std::io::{self, Write};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::*;

// These fields and variant order are frozen independently of CommandKind and
// QueueConfig. Changing the canonical schema requires a new entry version.
#[derive(Serialize)]
struct EntryV1<'a> {
    domain: [u8; 4],
    version: u8,
    stream: &'a [u8; 16],
    previous: Option<CommittedEntryMark>,
    entry: CommittedEntryId,
    work: WorkV1<'a>,
}

#[derive(Serialize)]
enum WorkV1<'a> {
    Blank,
    Membership {
        schema_version: u16,
        payload: &'a [u8],
    },
    CreateQueue {
        namespace: &'a str,
        entity: &'a str,
        issued_at: u64,
        config: QueueConfigV1,
    },
    Send {
        namespace: &'a str,
        entity: &'a str,
        issued_at: u64,
        message_id: &'a str,
        body: &'a [u8],
        time_to_live_millis: Option<u64>,
        session_id: Option<&'a str>,
    },
}

#[derive(Serialize)]
struct QueueConfigV1 {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: u64,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_time_window_millis: u64,
    dead_lettering_on_message_expiration: bool,
}

impl TryFrom<QueueConfig> for QueueConfigV1 {
    type Error = CommittedApplyError;

    fn try_from(config: QueueConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: config
                .max_message_bytes
                .try_into()
                .map_err(|_| CommittedApplyError::EntryEncoding)?,
            requires_session: config.requires_session,
            requires_duplicate_detection: config.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: config
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
        })
    }
}

pub(crate) fn entry_fingerprint(
    update: &CommittedCheckpointUpdate,
    work: &CommittedQueueWork,
) -> Result<[u8; 32], CommittedApplyError> {
    update.stream.validate()?;
    let work = match work {
        CommittedQueueWork::Blank => WorkV1::Blank,
        CommittedQueueWork::Membership {
            schema_version,
            payload,
        } => {
            if *schema_version == 0 {
                return Err(CommittedApplyError::InvalidMembershipSchema);
            }
            require_bound(payload.len(), "membership", MAX_COMMITTED_MEMBERSHIP_BYTES)?;
            WorkV1::Membership {
                schema_version: *schema_version,
                payload,
            }
        }
        CommittedQueueWork::Queue(queue) => {
            let command = queue.as_command();
            validate_scope(command)?;
            match &command.kind {
                CommandKind::CreateQueue { config } => WorkV1::CreateQueue {
                    namespace: command.namespace.as_str(),
                    entity: command.entity.as_str(),
                    issued_at: command.issued_at.as_millis(),
                    config: (*config).try_into()?,
                },
                CommandKind::Send {
                    message_id,
                    body,
                    time_to_live_millis,
                    session_id,
                } => {
                    require_bound(body.len(), "body", MAX_COMMITTED_BODY_BYTES)?;
                    if let Some(session) = session_id {
                        require_identifier_length(
                            session.as_str(),
                            "session id",
                            crate::MAX_SESSION_ID_BYTES,
                        )?;
                        SessionId::new(session.as_str())?;
                    }
                    WorkV1::Send {
                        namespace: command.namespace.as_str(),
                        entity: command.entity.as_str(),
                        issued_at: command.issued_at.as_millis(),
                        message_id,
                        body,
                        time_to_live_millis: *time_to_live_millis,
                        session_id: session_id.as_ref().map(SessionId::as_str),
                    }
                }
                _ => return Err(CommittedApplyError::EntryEncoding),
            }
        }
    };
    let entry = EntryV1 {
        domain: *b"SWYE",
        version: 1,
        stream: update.stream.as_bytes(),
        previous: update.expected_previous,
        entry: update.entry,
        work,
    };
    let mut sink = BoundedHash {
        hash: Sha256::new(),
        used: 0,
        exceeded: false,
    };
    if postcard::to_io(&entry, &mut sink).is_err() {
        return Err(if sink.exceeded {
            CommittedApplyError::TooLarge {
                resource: "entry",
                maximum: MAX_COMMITTED_ENTRY_BYTES,
            }
        } else {
            CommittedApplyError::EntryEncoding
        });
    }
    Ok(sink.hash.finalize().into())
}

fn validate_scope(command: &Command) -> Result<(), CommittedApplyError> {
    require_identifier_length(
        command.namespace.as_str(),
        "namespace",
        crate::MAX_NAMESPACE_NAME_BYTES,
    )?;
    require_identifier_length(
        command.entity.as_str(),
        "entity path",
        crate::MAX_ENTITY_PATH_BYTES,
    )?;
    NamespaceName::new(command.namespace.as_str())?;
    EntityPath::new(command.entity.as_str())?;
    Ok(())
}

fn require_identifier_length(
    value: &str,
    kind: &'static str,
    maximum: usize,
) -> Result<(), IdentifierError> {
    if value.len() > maximum {
        return Err(IdentifierError::TooLong { kind, maximum });
    }
    Ok(())
}

fn require_bound(
    actual: usize,
    resource: &'static str,
    maximum: usize,
) -> Result<(), CommittedApplyError> {
    if actual > maximum {
        return Err(CommittedApplyError::TooLarge { resource, maximum });
    }
    Ok(())
}

struct BoundedHash {
    hash: Sha256,
    used: usize,
    exceeded: bool,
}

impl Write for BoundedHash {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_COMMITTED_ENTRY_BYTES - self.used {
            self.exceeded = true;
            return Err(io::Error::other("committed entry byte limit"));
        }
        self.hash.update(bytes);
        self.used += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
