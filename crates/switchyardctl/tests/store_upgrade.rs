//! The explicit offline store-upgrade command, through the real binary.

use std::{
    error::Error,
    path::Path,
    process::{Command, Output},
};

use fjall::{Database, KeyspaceCreateOptions, PersistMode, Readable};
use storage::{FjallStore, StateStore, WriteBatch};
use tempfile::TempDir;

fn upgrade(directory: &Path) -> Result<Output, Box<dyn Error>> {
    Ok(Command::new(env!("CARGO_BIN_EXE_switchyardctl"))
        .args(["store", "upgrade", "--data-dir"])
        .arg(directory)
        .output()?)
}

fn stamp(directory: &Path, version: u32) -> Result<(), Box<dyn Error>> {
    let database = Database::builder(directory).open()?;
    let meta = database.keyspace("meta", KeyspaceCreateOptions::default)?;
    let mut batch = database.batch().durability(Some(PersistMode::SyncAll));
    batch.insert(&meta, b"format_version", version.to_be_bytes());
    batch.commit()?;
    Ok(())
}

fn marker(directory: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let database = Database::builder(directory).open()?;
    let meta = database.keyspace("meta", KeyspaceCreateOptions::default)?;
    let marker = database.snapshot().get(&meta, b"format_version")?;
    Ok(marker.map(|marker| marker.to_vec()).unwrap_or_default())
}

#[test]
fn upgrades_once_then_reports_already_current() -> Result<(), Box<dyn Error>> {
    let directory = TempDir::new()?;
    let store = FjallStore::open(directory.path())?;
    store.apply(WriteBatch::default().put(vec![0x0e, 1], vec![2]))?;
    drop(store);
    stamp(directory.path(), 2)?;

    let output = upgrade(directory.path())?;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?.trim(),
        format!(
            "upgraded store format 2 to 3: {}",
            directory.path().display()
        )
    );
    assert_eq!(marker(directory.path())?, 3_u32.to_be_bytes());
    let output = upgrade(directory.path())?;
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout)?.trim(),
        format!("store format is already 3: {}", directory.path().display())
    );
    let store = FjallStore::open(directory.path())?;
    assert_eq!(store.get(&[0x0e, 1])?, Some(vec![2]));
    Ok(())
}

#[test]
fn refusals_exit_nonzero_without_creating_or_changing_stores() -> Result<(), Box<dyn Error>> {
    let parent = TempDir::new()?;
    let missing = parent.path().join("absent");
    let output = upgrade(&missing)?;
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)?.starts_with("switchyardctl: store upgrade refused: ")
    );
    assert!(!missing.exists());

    let directory = TempDir::new()?;
    drop(FjallStore::open(directory.path())?);
    stamp(directory.path(), 1)?;
    let output = upgrade(directory.path())?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr)?.trim(),
        "switchyardctl: store upgrade refused: store format 1 has no explicit upgrade to format 3"
    );
    assert_eq!(marker(directory.path())?, 1_u32.to_be_bytes());
    Ok(())
}

#[test]
fn unsafe_content_refuses_without_printing_payload_bytes() -> Result<(), Box<dyn Error>> {
    use domain::{
        EntityPath, MessageEnvelope, MessageRecord, MessageRecordFormat, MessageState,
        NamespaceName, SequenceNumber, Timestamp, codec, keys,
    };
    let record = MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: String::from("m"),
        body: b"BODY-SECRET".to_vec(),
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        // A valid section followed by bytes that are not an AMQP section.
        envelope: Some(MessageEnvelope::new(
            [&[0x00, 0x53, 0x75, 0xa0, 1, 7][..], b"ENVELOPE-SECRET"].concat(),
        )),
        format: MessageRecordFormat::V1,
    };
    let key = keys::message(
        &NamespaceName::new("tenant")?,
        &EntityPath::new("orders")?,
        record.sequence,
    );
    let directory = TempDir::new()?;
    let store = FjallStore::open(directory.path())?;
    store.apply(WriteBatch::default().put(key, codec::message::encode(&record)?))?;
    drop(store);
    stamp(directory.path(), 2)?;

    let output = upgrade(directory.path())?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.starts_with("switchyardctl: store upgrade refused: legacy row 0 "));
    assert!(!stderr.contains("SECRET"), "{stderr}");
    assert!(output.stdout.is_empty());
    assert_eq!(marker(directory.path())?, 2_u32.to_be_bytes());
    Ok(())
}
