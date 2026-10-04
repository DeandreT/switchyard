use std::{
    future::Future,
    io::{IoSlice, SeekFrom},
    task::Waker,
};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

mod sealed;

fn buffer_capacity<const LIMIT: usize>(buffer: &Buffer<LIMIT>) -> usize {
    match &buffer.bytes {
        Backing::Mutable(bytes) => bytes.capacity(),
        Backing::SealedImage(_) => panic!("mutable fixture acquired sealed backing"),
    }
}

#[test]
fn meets_pinned_snapshot_data_traits() {
    fn check<T: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin + 'static>(_: T) {}
    check(BoundedSnapshotData::new());
}

#[test]
fn committed_image_container_limit_fits_the_transport_buffer_without_allocation() -> TestResult {
    let position = u64::try_from(domain::MAX_COMMITTED_IMAGE_BYTES)?;
    let mut data = BoundedSnapshotData::new();
    Pin::new(&mut data).start_seek(SeekFrom::Start(position))?;
    assert_eq!(data.position(), position);
    assert!(data.is_empty());
    assert_eq!(buffer_capacity(&data.buffer), 0);
    Ok(())
}

#[tokio::test]
async fn real_limit_bounds_seek_and_write_without_large_allocation() -> TestResult {
    let mut data = BoundedSnapshotData::new();
    let limit = u64::try_from(MAX_SNAPSHOT_BYTES)?;
    assert_eq!(data.seek(SeekFrom::Start(limit)).await?, limit);
    assert!(data.is_empty());
    assert_eq!(buffer_capacity(&data.buffer), 0);

    assert_eq!(
        data.write(b"x").await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        data.seek(SeekFrom::Start(limit + 1))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(data.position(), limit);
    assert!(data.is_empty());
    assert_eq!(buffer_capacity(&data.buffer), 0);
    Ok(())
}

#[tokio::test]
async fn constructor_copies_bytes_and_starts_at_zero() -> TestResult {
    let mut source = *b"source";
    let mut data = BoundedSnapshotData::from_bytes(&source)?;
    source.fill(b'x');
    assert_eq!(data.position(), 0);
    assert_eq!(data.len(), 6);
    assert_eq!(data.as_bytes(), b"source");
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert_eq!(received, b"source");
    assert_eq!(data.position(), 6);
    assert_eq!(
        Buffer::<5>::from_bytes(b"source")
            .err()
            .expect("constructor must refuse")
            .kind(),
        io::ErrorKind::InvalidInput
    );
    Ok(())
}

#[tokio::test]
async fn exact_fill_succeeds_and_next_byte_is_refused_atomically() -> TestResult {
    let mut data = Buffer::<8>::default();
    data.write_all(b"abcdefgh").await?;
    assert_eq!(data.bytes.as_bytes(), b"abcdefgh");
    assert_eq!(data.position, 8);
    assert_eq!(
        AsyncWriteExt::write(&mut data, b"x")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(data.bytes.as_bytes(), b"abcdefgh");
    assert_eq!(data.position, 8);
    Ok(())
}

#[tokio::test]
async fn oversized_write_does_not_overwrite_a_fitting_prefix() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"abcdef")?;
    data.seek(SeekFrom::Start(3)).await?;
    let capacity = buffer_capacity(&data);
    assert_eq!(
        AsyncWriteExt::write(&mut data, b"123456")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(data.bytes.as_bytes(), b"abcdef");
    assert_eq!(data.position, 3);
    assert_eq!(buffer_capacity(&data), capacity);
    Ok(())
}

#[tokio::test]
async fn bounded_overwrite_preserves_unwritten_suffix() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"abcdef")?;
    data.seek(SeekFrom::Start(2)).await?;
    data.write_all(b"XY").await?;
    assert_eq!(data.bytes.as_bytes(), b"abXYef");
    assert_eq!(data.position, 4);
    Ok(())
}

#[tokio::test]
async fn sparse_write_materializes_only_the_bounded_gap() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"a")?;
    data.seek(SeekFrom::Start(6)).await?;
    assert_eq!(data.bytes.as_bytes(), b"a");
    data.write_all(b"z").await?;
    assert_eq!(data.bytes.as_bytes(), b"a\0\0\0\0\0z");
    assert_eq!(data.position, 7);
    Ok(())
}

#[tokio::test]
async fn empty_write_at_sparse_limit_does_not_allocate_or_extend() -> TestResult {
    let mut data = Buffer::<8>::default();
    data.seek(SeekFrom::Start(8)).await?;
    assert_eq!(AsyncWriteExt::write(&mut data, b"").await?, 0);
    assert_eq!(data.position, 8);
    assert!(data.bytes.as_bytes().is_empty());
    assert_eq!(buffer_capacity(&data), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_signed_and_absolute_seeks_preserve_state() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"abcd")?;
    data.seek(SeekFrom::Start(2)).await?;
    let capacity = buffer_capacity(&data);
    for request in [
        SeekFrom::Start(9),
        SeekFrom::Start(u64::MAX),
        SeekFrom::End(5),
        SeekFrom::End(-5),
        SeekFrom::Current(-3),
        SeekFrom::Current(i64::MIN),
        SeekFrom::Current(i64::MAX),
    ] {
        assert_eq!(
            data.seek(request).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(data.position, 2);
        assert_eq!(data.bytes.as_bytes(), b"abcd");
        assert_eq!(buffer_capacity(&data), capacity);
    }
    Ok(())
}

#[cfg(target_pointer_width = "64")]
#[tokio::test]
async fn arithmetic_overflow_is_checked_before_allocation() -> TestResult {
    let mut data = Buffer::<{ usize::MAX }>::default();
    data.seek(SeekFrom::Start(u64::MAX)).await?;
    assert_eq!(
        data.seek(SeekFrom::Current(1)).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        AsyncWriteExt::write(&mut data, b"x")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(data.position, u64::MAX);
    assert!(data.bytes.as_bytes().is_empty());
    assert_eq!(buffer_capacity(&data), 0);
    Ok(())
}

#[tokio::test]
async fn end_start_and_current_seek_match_chunked_reader_pattern() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"abcdef")?;
    assert_eq!(data.seek(SeekFrom::End(0)).await?, 6);
    assert_eq!(data.seek(SeekFrom::Start(1)).await?, 1);
    let mut chunk = [0; 2];
    data.read_exact(&mut chunk).await?;
    assert_eq!(&chunk, b"bc");
    assert_eq!(data.seek(SeekFrom::Current(-1)).await?, 2);
    assert_eq!(data.seek(SeekFrom::End(-1)).await?, 5);
    data.read_exact(&mut chunk[..1]).await?;
    assert_eq!(chunk[0], b'f');
    Ok(())
}

#[tokio::test]
async fn capacity_overflow_is_sanitized_and_preserves_the_buffer() -> TestResult {
    let mut data = Buffer::<{ usize::MAX }>::default();
    let position = u64::try_from(usize::MAX - 1)?;
    data.seek(SeekFrom::Start(position)).await?;
    // A Vec cannot reserve usize::MAX bytes, but the endpoint addition fits.
    let error = AsyncWriteExt::write(&mut data, b"x").await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!(error.to_string(), "snapshot buffer allocation failed");
    assert_eq!(data.position, position);
    assert!(data.bytes.as_bytes().is_empty());
    assert_eq!(buffer_capacity(&data), 0);
    Ok(())
}

#[tokio::test]
async fn empty_and_eof_reads_do_not_move_a_sparse_position() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"abc")?;
    assert_eq!(data.read(&mut []).await?, 0);
    assert_eq!(data.position, 0);
    data.seek(SeekFrom::Start(6)).await?;
    assert_eq!(data.read(&mut [0; 2]).await?, 0);
    assert_eq!(data.position, 6);
    assert_eq!(data.bytes.as_bytes(), b"abc");
    Ok(())
}

#[tokio::test]
async fn default_vectored_write_uses_only_first_nonempty_slice() -> TestResult {
    let mut data = Buffer::<8>::from_bytes(b"aa")?;
    data.seek(SeekFrom::End(0)).await?;
    assert!(!data.is_write_vectored());
    let slices = [IoSlice::new(b""), IoSlice::new(b"bb"), IoSlice::new(b"ccc")];
    assert_eq!(data.write_vectored(&slices).await?, 2);
    assert_eq!(data.bytes.as_bytes(), b"aabb");
    assert_eq!(data.position, 4);
    let too_large = [
        IoSlice::new(b""),
        IoSlice::new(b"xxxxxx"),
        IoSlice::new(b"y"),
    ];
    assert_eq!(
        data.write_vectored(&too_large).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(data.bytes.as_bytes(), b"aabb");
    assert_eq!(data.position, 4);
    Ok(())
}

#[tokio::test]
async fn flush_and_shutdown_preserve_in_memory_cursor_behavior() -> TestResult {
    let mut data = BoundedSnapshotData::from_bytes(b"data")?;
    data.flush().await?;
    data.shutdown().await?;
    data.seek(SeekFrom::Start(0)).await?;
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert_eq!(received, b"data");
    data.write_all(b"x").await?;
    assert_eq!(data.as_bytes(), b"datax");
    Ok(())
}

#[tokio::test]
async fn unpolled_io_futures_are_inert() -> TestResult {
    let mut data = BoundedSnapshotData::from_bytes(b"unchanged")?;
    drop(data.write_all(b"replacement"));
    drop(data.seek(SeekFrom::Start(3)));
    let mut received = [0; 3];
    drop(data.read_exact(&mut received));
    assert_eq!(data.as_bytes(), b"unchanged");
    assert_eq!(data.position(), 0);
    assert_eq!(received, [0; 3]);
    Ok(())
}

#[test]
fn polled_write_is_complete_without_hidden_work_after_waiter_drop() -> TestResult {
    let mut data = BoundedSnapshotData::new();
    let mut write = Box::pin(data.write_all(b"done"));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(write.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    drop(write);
    assert_eq!(data.as_bytes(), b"done");
    assert_eq!(data.position(), 4);
    Ok(())
}

#[test]
fn debug_and_bound_errors_do_not_include_data() -> TestResult {
    let data = BoundedSnapshotData::from_bytes(b"private-snapshot-content")?;
    let debug = format!("{data:?}");
    assert!(!debug.contains("private-snapshot-content"));
    assert!(!debug.contains("112, 114"));
    assert!(debug.contains("buffered_bytes"));
    let error = Buffer::<1>::from_bytes(b"private-snapshot-content")
        .err()
        .expect("bound must refuse");
    assert_eq!(error.to_string(), "snapshot buffer bound exceeded");
    Ok(())
}
