use std::{future::Future, io::IoSlice, task::Waker};

use domain::{DecodedCommittedImage, MAX_COMMITTED_BODY_BYTES, ValidatedCreateSendLayout17Image};

use super::*;

mod fixture;
mod reopen;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const PRIVATE_BODY: &[u8] = b"private-snapshot-message-body";

fn assert_sealed(data: &BoundedSnapshotData, pointer: usize, position: u64) {
    assert_eq!(data.as_bytes().as_ptr() as usize, pointer);
    assert_eq!(data.position(), position);
    assert!(matches!(&data.buffer.bytes, Backing::SealedImage(_)));
}

fn assert_write_refusal(error: io::Error) {
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "sealed snapshot data is read-only");
    assert!(!format!("{error:?}: {error}").contains("private-"));
}

#[test]
fn owned_constructor_keeps_the_original_allocation_and_starts_at_zero() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let length = image.len();
    let data = BoundedSnapshotData::from_image(image)?;
    assert_sealed(&data, pointer, 0);
    assert_eq!(data.len(), length);
    assert!(!data.is_empty());
    assert_eq!(&data.as_bytes()[..4], b"SWYI");
    // Moving the transport value only moves its small ownership carrier.
    let moved = data;
    assert_sealed(&moved, pointer, 0);
    assert_eq!(moved.len(), length);
    Ok(())
}

#[test]
fn constructor_adds_no_business_semantics_or_native_metadata_admission() -> TestResult {
    let image = fixture::structural_only_image()?;
    assert!(
        ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(
            image.as_bytes()
        )?)
        .is_err()
    );
    let pointer = image.as_bytes().as_ptr() as usize;
    let length = image.len();
    let data = BoundedSnapshotData::from_image(image)?;
    assert_sealed(&data, pointer, 0);
    assert_eq!(data.len(), length);
    assert!(
        ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(data.as_bytes())?)
            .is_err()
    );
    Ok(())
}

#[test]
fn actual_image_length_is_checked_against_an_independent_tiny_transport_bound() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    assert!(image.len() > 1);
    let error = Buffer::<1>::from_image(image)
        .err()
        .ok_or("tiny transport accepted a longer domain image")?;
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), "snapshot buffer bound exceeded");
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let length = image.len();
    let accepted = Buffer::<MAX_SNAPSHOT_BYTES>::from_image(image)?;
    assert_eq!(accepted.bytes.as_bytes().as_ptr() as usize, pointer);
    assert_eq!(accepted.bytes.as_bytes().len(), length);
    assert_eq!(accepted.position, 0);
    Ok(())
}

#[tokio::test]
async fn chunked_reads_reproduce_the_whole_maximum_body_frame_and_checksum() -> TestResult {
    let body = (0..MAX_COMMITTED_BODY_BYTES)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let image = fixture::image(&body)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let length = image.len();
    let mut data = BoundedSnapshotData::from_image(image)?;
    let mut received = Vec::new();
    let mut chunk = [0; 1021];
    loop {
        let read = data.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..read]);
    }
    assert_eq!(received, data.as_bytes());
    assert_eq!(received.len(), length);
    let checked =
        ValidatedCreateSendLayout17Image::validate(DecodedCommittedImage::decode(&received)?)?;
    assert_eq!(checked.stream(), fixture::stream()?);
    assert_eq!(checked.message_count(), 1);
    assert_sealed(&data, pointer, u64::try_from(length)?);
    data.seek(SeekFrom::End(-32)).await?;
    let mut checksum = [0; 32];
    data.read_exact(&mut checksum).await?;
    assert_eq!(checksum, received[length - 32..]);
    assert_sealed(&data, pointer, u64::try_from(length)?);
    Ok(())
}

#[tokio::test]
async fn empty_and_sparse_eof_reads_preserve_the_sealed_cursor_and_bytes() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    assert_eq!(data.read(&mut []).await?, 0);
    assert_sealed(&data, pointer, 0);
    let sparse = u64::try_from(data.len())? + 7;
    data.seek(SeekFrom::Start(sparse)).await?;
    let mut received = [19; 8];
    assert_eq!(data.read(&mut received).await?, 0);
    assert_eq!(received, [19; 8]);
    assert_sealed(&data, pointer, sparse);
    data.seek(SeekFrom::Start(u64::try_from(MAX_SNAPSHOT_BYTES)?))
        .await?;
    assert_eq!(data.read(&mut received).await?, 0);
    assert_sealed(&data, pointer, u64::try_from(MAX_SNAPSHOT_BYTES)?);
    Ok(())
}

#[tokio::test]
async fn rejected_absolute_signed_and_overflow_seeks_preserve_sealed_state() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    let length = data.len();
    data.seek(SeekFrom::Start(2)).await?;
    for request in [
        SeekFrom::Start(u64::try_from(MAX_SNAPSHOT_BYTES)? + 1),
        SeekFrom::Start(u64::MAX),
        SeekFrom::End(-i64::try_from(length)? - 1),
        SeekFrom::End(i64::try_from(MAX_SNAPSHOT_BYTES)?),
        SeekFrom::Current(-3),
        SeekFrom::Current(i64::MIN),
        SeekFrom::Current(i64::MAX),
    ] {
        assert_eq!(
            data.seek(request).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_sealed(&data, pointer, 2);
        assert_eq!(data.len(), length);
    }
    Ok(())
}

#[tokio::test]
async fn valid_end_start_current_seeks_match_the_shared_chunked_reader_pattern() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    let length = data.len();
    assert_eq!(data.seek(SeekFrom::End(0)).await?, u64::try_from(length)?);
    assert_eq!(data.seek(SeekFrom::Start(1)).await?, 1);
    let mut read = [0; 2];
    data.read_exact(&mut read).await?;
    assert_eq!(read, data.as_bytes()[1..3]);
    assert_eq!(data.seek(SeekFrom::Current(-1)).await?, 2);
    assert_eq!(
        data.seek(SeekFrom::End(-1)).await?,
        u64::try_from(length - 1)?
    );
    data.read_exact(&mut read[..1]).await?;
    assert_eq!(read[0], data.as_bytes()[length - 1]);
    assert_sealed(&data, pointer, u64::try_from(length)?);
    Ok(())
}

#[tokio::test]
async fn nonempty_and_empty_actual_writes_refuse_before_modifying_any_sealed_position() -> TestResult
{
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    let length = data.len();
    for position in [
        0,
        1,
        u64::try_from(length)?,
        u64::try_from(MAX_SNAPSHOT_BYTES)?,
    ] {
        data.seek(SeekFrom::Start(position)).await?;
        for bytes in [b"replacement".as_slice(), b"".as_slice()] {
            assert_write_refusal(data.write(bytes).await.unwrap_err());
            assert_sealed(&data, pointer, position);
            assert_eq!(data.len(), length);
        }
        assert_write_refusal(data.write_all(b"replacement").await.unwrap_err());
        assert_sealed(&data, pointer, position);
    }
    Ok(())
}

#[test]
fn raw_poll_write_guard_precedes_even_unrepresentable_position_checks_for_empty_input() -> TestResult
{
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = Buffer::<MAX_SNAPSHOT_BYTES>::from_image(image)?;
    // This private test corruption is unreachable through checked public seeks.
    data.position = u64::MAX;
    let mut cx = Context::from_waker(Waker::noop());
    for bytes in [b"replacement".as_slice(), b"".as_slice()] {
        let Poll::Ready(Err(error)) = Pin::new(&mut data).poll_write(&mut cx, bytes) else {
            return Err("sealed write did not immediately refuse".into());
        };
        assert_write_refusal(error);
        assert_eq!(data.position, u64::MAX);
        assert_eq!(data.bytes.as_bytes().as_ptr() as usize, pointer);
    }
    Ok(())
}

#[tokio::test]
async fn default_vectored_writes_reject_nonempty_all_empty_and_empty_vector_inputs() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    assert!(!data.is_write_vectored());
    let mixed = [
        IoSlice::new(b""),
        IoSlice::new(b"replacement"),
        IoSlice::new(b"tail"),
    ];
    assert_write_refusal(data.write_vectored(&mixed).await.unwrap_err());
    let empty = [IoSlice::new(b""), IoSlice::new(b"")];
    assert_write_refusal(data.write_vectored(&empty).await.unwrap_err());
    assert_write_refusal(data.write_vectored(&[]).await.unwrap_err());
    assert_sealed(&data, pointer, 0);
    Ok(())
}

#[tokio::test]
async fn flush_shutdown_and_tokio_empty_write_all_are_inert_without_unsealing() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    data.seek(SeekFrom::Start(3)).await?;
    data.write_all(&[]).await?;
    data.flush().await?;
    data.shutdown().await?;
    assert_sealed(&data, pointer, 3);
    assert_write_refusal(data.write(b"").await.unwrap_err());
    assert_write_refusal(data.write(b"replacement").await.unwrap_err());
    data.seek(SeekFrom::Start(0)).await?;
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert_eq!(received, data.as_bytes());
    assert_sealed(&data, pointer, u64::try_from(data.len())?);
    Ok(())
}

#[tokio::test]
async fn unpolled_sealed_io_futures_leave_source_and_destination_untouched() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    drop(data.write_all(b"replacement"));
    drop(data.seek(SeekFrom::Start(3)));
    let mut received = [29; 3];
    drop(data.read_exact(&mut received));
    drop(data.flush());
    drop(data.shutdown());
    assert_sealed(&data, pointer, 0);
    assert_eq!(received, [29; 3]);
    Ok(())
}

#[test]
fn polled_write_refusal_has_no_hidden_work_after_the_waiter_is_dropped() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let pointer = image.as_bytes().as_ptr() as usize;
    let mut data = BoundedSnapshotData::from_image(image)?;
    let mut future = Box::pin(data.write_all(b"replacement"));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Err(error)) = future.as_mut().poll(&mut cx) else {
        return Err("polled sealed write did not refuse synchronously".into());
    };
    assert_write_refusal(error);
    drop(future);
    assert_sealed(&data, pointer, 0);
    Ok(())
}

#[test]
fn sealed_data_and_owned_reader_future_meet_pinned_send_unpin_static_traits() -> TestResult {
    fn data_traits<T: AsyncRead + AsyncWrite + AsyncSeek + Send + Unpin + 'static>(_: &T) {}
    fn future_traits<F: Future<Output = io::Result<Vec<u8>>> + Send + 'static>(_: F) {}
    let data = BoundedSnapshotData::from_image(fixture::image(PRIVATE_BODY)?)?;
    data_traits(&data);
    future_traits(async move {
        let mut data = data;
        let mut received = Vec::new();
        data.read_to_end(&mut received).await?;
        Ok(received)
    });
    Ok(())
}

#[test]
fn sealed_debug_and_bound_write_errors_do_not_expose_frame_or_membership_bytes() -> TestResult {
    let image = fixture::image(PRIVATE_BODY)?;
    let data = BoundedSnapshotData::from_image(image)?;
    let diagnostic = format!("{data:?}");
    assert!(!diagnostic.contains("private-"));
    assert!(!diagnostic.contains("112, 114"));
    assert!(diagnostic.contains("buffered_bytes"));
    assert!(diagnostic.contains("position"));
    let image = fixture::image(PRIVATE_BODY)?;
    let error = Buffer::<1>::from_image(image)
        .err()
        .ok_or("tiny sealed limit was not enforced")?;
    assert_eq!(error.to_string(), "snapshot buffer bound exceeded");
    assert!(!format!("{error:?}: {error}").contains("private-"));
    Ok(())
}
