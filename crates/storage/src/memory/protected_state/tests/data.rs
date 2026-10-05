use std::{
    sync::{Arc, Barrier, mpsc},
    thread,
    time::Duration,
};

use super::{fixture::*, *};
use crate::{SnapshotCatalogRecord, protected_state::copy_state};

#[test]
fn pristine_protected_state_is_empty_and_uninitialized() -> TestResult {
    let writer = MemoryProtectedStateStore::new();
    let actual = view(&writer.reader())?;
    assert_eq!(
        actual,
        View {
            initialized: false,
            rows: vec![],
            live: None,
            fence: None,
            bytes: 0
        }
    );
    let default = MemoryProtectedStateStore::default();
    assert_eq!(view(&default.reader())?, actual);
    Ok(())
}

#[test]
fn empty_publication_initializes_with_present_empty_live_pair() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    publish(&mut writer, &[], b"", b"", b"f")?;
    assert_eq!(
        view(&writer.reader())?,
        View {
            initialized: true,
            rows: vec![],
            live: Some((vec![], vec![])),
            fence: Some(b"f".to_vec()),
            bytes: 1
        }
    );
    Ok(())
}

#[test]
fn publication_copies_complete_opaque_parts() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    publish(
        &mut writer,
        &[(b"a", b""), (b"z", b"\xff\0")],
        b"\xffm",
        b"\0a",
        b"\xfff",
    )?;
    assert_eq!(
        view(&writer.reader())?,
        View {
            initialized: true,
            rows: vec![(b"a".to_vec(), vec![]), (b"z".to_vec(), b"\xff\0".to_vec())],
            live: Some((b"\xffm".to_vec(), b"\0a".to_vec())),
            fence: Some(b"\xfff".to_vec()),
            bytes: 10
        }
    );
    Ok(())
}

#[test]
fn replacement_removes_every_old_only_row() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    publish(
        &mut writer,
        &[(b"a", b"old"), (b"b", b"gone"), (b"z", b"last")],
        b"m1",
        b"a1",
        b"f1",
    )?;
    publish(
        &mut writer,
        &[(b"a", b"new"), (b"c", b"added")],
        b"m2",
        b"a2",
        b"f2",
    )?;
    let actual = view(&writer.reader())?;
    assert_eq!(
        actual.rows,
        vec![
            (b"a".to_vec(), b"new".to_vec()),
            (b"c".to_vec(), b"added".to_vec())
        ]
    );
    assert_eq!(actual.live, Some((b"m2".to_vec(), b"a2".to_vec())));
    assert_eq!(actual.fence.as_deref(), Some(b"f2".as_slice()));
    assert_eq!(counts(&writer).1, 2);
    Ok(())
}

#[test]
fn unchanged_fence_is_known_refusal() -> TestResult {
    let mut writer = initialized();
    let reader = writer.reader();
    let before = view(&reader)?;
    let count = counts(&writer);
    let refused = publish(
        &mut writer,
        &[(b"b", b"changed")],
        b"other",
        b"other",
        b"one",
    );
    let after_count = counts(&writer);
    let after = view(&reader)?;
    assert_eq!(refused, Err(ProtectedStateError::UnchangedFence));
    assert_eq!(count, after_count);
    assert_eq!(before, after);
    Ok(())
}

#[test]
fn exact_body_noop_requires_distinct_fence() -> TestResult {
    let mut writer = initialized();
    publish(&mut writer, &[(b"a", b"old")], b"meta", b"body", b"two")?;
    let second = view(&writer.reader())?;
    assert_eq!(second.fence.as_deref(), Some(b"two".as_slice()));
    publish(&mut writer, &[(b"a", b"old")], b"meta", b"body", b"one")?;
    let third = view(&writer.reader())?;
    assert_eq!(third.rows, second.rows);
    assert_eq!(third.live, second.live);
    assert_eq!(third.fence.as_deref(), Some(b"one".as_slice()));
    assert_eq!(counts(&writer).1, 3);
    Ok(())
}

#[test]
fn owned_capture_outlives_all_originating_handles() -> TestResult {
    let actual = {
        let writer = initialized();
        let reader = writer.reader();
        let other = reader.clone();
        let state = other.capture_protected_state()?;
        drop(writer);
        drop(reader);
        drop(other);
        state
    };
    assert!(actual.is_initialized());
    assert_eq!(actual.records().entries()[0].1, b"old");
    assert_eq!(actual.live_catalog().unwrap().artifact(), b"body");
    assert_eq!(actual.fence(), Some(b"one".as_slice()));
    Ok(())
}

#[test]
fn publication_does_not_retain_input_borrows() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    {
        let key = b"local-key".to_vec();
        let value = b"local-value".to_vec();
        let metadata = b"local-metadata".to_vec();
        let artifact = b"local-artifact".to_vec();
        let fence = b"local-fence".to_vec();
        let rows = [(key.as_slice(), value.as_slice())];
        let pair = SnapshotCatalogRecord::new(&metadata, &artifact)?;
        writer.publish(ProtectedStatePublication::new(&rows, pair, &fence)?)?;
    }
    let actual = view(&writer.reader())?;
    assert_eq!(
        actual.rows,
        vec![(b"local-key".to_vec(), b"local-value".to_vec())]
    );
    assert_eq!(
        actual.live,
        Some((b"local-metadata".to_vec(), b"local-artifact".to_vec()))
    );
    assert_eq!(actual.fence.as_deref(), Some(b"local-fence".as_slice()));
    Ok(())
}

#[test]
fn cloned_readers_capture_the_same_private_origin() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    let first = writer.reader();
    let second = first.clone();
    publish(&mut writer, &[(b"k", b"v")], b"m", b"a", b"f")?;
    assert_eq!(view(&first)?, view(&second)?);
    publish(&mut writer, &[], b"", b"", b"g")?;
    assert_eq!(view(&first)?, view(&second)?);
    Ok(())
}

#[test]
fn one_locked_helper_view_blocks_publication() -> TestResult {
    let writer = initialized();
    let reader = writer.reader();
    let guard = reader.cell.read().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let task = thread::spawn(move || {
        let mut writer = writer;
        let started = started_tx.send(());
        let result = publish(&mut writer, &[(b"b", b"new")], b"new", b"new", b"two");
        let completed = done_tx.send(());
        (writer, started, result, completed)
    });
    let started = started_rx.recv_timeout(Duration::from_secs(2));
    let before_release = done_rx.try_recv();
    let copied = guard
        .data
        .check()
        .and_then(|shape| copy_state(&guard.data, shape));
    drop(guard);
    let joined = task.join();
    // This is an actual helper-held read lock, not a public phase callback.
    let (writer, sent, result, completed) = joined.unwrap();
    started?;
    sent?;
    completed?;
    result?;
    assert!(matches!(before_release, Err(mpsc::TryRecvError::Empty)));
    assert_eq!(copied?.fence(), Some(b"one".as_slice()));
    assert_eq!(
        view(&writer.reader())?.fence.as_deref(),
        Some(b"two".as_slice())
    );
    Ok(())
}

#[test]
fn concurrent_capture_observes_only_complete_old_or_new() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    publish(&mut writer, &[(b"k", b"zero")], b"zero", b"zero", b"zero")?;
    let reader = writer.reader();
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let task = thread::spawn(move || {
        other.wait();
        let mut results = Vec::new();
        for i in 1u32..=64 {
            let token = i.to_le_bytes();
            let rows = [(b"k".as_slice(), token.as_slice())];
            results.push(publish(&mut writer, &rows, &token, &token, &token));
        }
        (writer, results)
    });
    barrier.wait();
    let observed: Vec<_> = (0..128).map(|_| view(&reader)).collect();
    let joined = task.join();
    let (writer, results) = joined.unwrap();
    for result in results {
        result?;
    }
    for actual in observed {
        let actual = actual?;
        let token = &actual.rows[0].1;
        assert_eq!(actual.live.as_ref().unwrap().0, *token);
        assert_eq!(actual.live.as_ref().unwrap().1, *token);
        assert_eq!(actual.fence.as_ref().unwrap(), token);
        assert!(actual.initialized);
        assert_eq!(actual.bytes, 1 + token.len() * 4);
    }
    assert_eq!(counts(&writer).1, 65);
    Ok(())
}

#[test]
fn wrappers_and_static_errors_do_not_recurse_into_bytes() -> TestResult {
    let mut writer = MemoryProtectedStateStore::new();
    let pair = SnapshotCatalogRecord::new(b"secret-metadata", b"secret-artifact")?;
    let rows = [(b"secret-key".as_slice(), b"secret-value".as_slice())];
    let input = ProtectedStatePublication::new(&rows, pair, b"secret-fence")?;
    let input_debug = format!("{input:?}");
    writer.publish(input)?;
    let reader = writer.reader();
    let actual = reader.capture_protected_state()?;
    for debug in [
        input_debug,
        format!("{writer:?}"),
        format!("{reader:?}"),
        format!("{actual:?}"),
    ] {
        assert!(!debug.contains("secret"));
    }
    use std::error::Error;
    for error in [
        ProtectedStateError::LimitExceeded,
        ProtectedStateError::Allocation,
        ProtectedStateError::InvalidInput,
        ProtectedStateError::InvalidState,
        ProtectedStateError::UnchangedFence,
        ProtectedStateError::Poisoned,
        ProtectedStateError::PublishUnknown,
    ] {
        assert!(error.source().is_none());
        assert!(!error.to_string().contains("secret"));
    }
    assert_eq!(actual.records().entries()[0].0, b"secret-key");
    assert!(format!("{:?}", actual.records()).contains("115"));
    Ok(())
}
