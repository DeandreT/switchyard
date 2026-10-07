use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

use super::super::captured_image_fixture as bootstrap_fixture;
#[path = "fixture.rs"]
mod fixture;
#[path = "framing.rs"]
mod framing;
#[path = "mismatches.rs"]
mod mismatches;
#[path = "pairs.rs"]
mod pairs;

macro_rules! cases {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            use super::*;
            $(#[test]
            fn $case() -> TestResult {
                pairs::$case(storage::MemoryReplicaStore::new())
            })+
        }
        mod durable {
            use super::*;
            $(#[test]
            fn $case() -> TestResult {
                let directory = testkit::DurableProvider::temporary()?;
                pairs::$case(storage::FjallReplicaStore::open(directory.path())?)
            })+
        }
    };
}

cases!(
    native_pair_preserves_exact_capture_and_projects_only_captured_checkpoint,
    initial_pair_has_no_log_or_membership,
    retained_older_pair_survives_later_applied_progress,
);

#[test]
fn wrappers_and_all_errors_are_source_private() -> TestResult {
    let source = bootstrap_fixture::selected(false)?;
    assert_eq!(source.request().expected_checkpoint(), &source.checkpoint);
    let encoded = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let pair = DecodedNativeSnapshotPair::decode(encoded.as_bytes(), source.image.as_bytes())?;
    for text in [format!("{encoded:?}"), format!("{pair:?}")] {
        for secret in [
            "PRIVATE",
            "tenant",
            "orders",
            "node-7",
            "sha256",
            "fingerprint",
        ] {
            assert!(!text.contains(secret));
        }
    }
    for error in [
        NativeSnapshotMetadataError::LimitExceeded,
        NativeSnapshotMetadataError::Allocation,
        NativeSnapshotMetadataError::UnsupportedFormat,
        NativeSnapshotMetadataError::InvalidMetadata,
        NativeSnapshotMetadataError::InvalidImage,
        NativeSnapshotMetadataError::IncompatibleCheckpoint,
        NativeSnapshotMetadataError::ImageMismatch,
    ] {
        let text = format!("{error:?}: {error}");
        for secret in ["PRIVATE", "tenant", "orders", "node-7", "/tmp/"] {
            assert!(!text.contains(secret));
        }
    }
    Ok(())
}

#[test]
fn current_native_metadata_and_pairs_refuse_valid_historical_role1_images() -> TestResult {
    let source = bootstrap_fixture::initial()?;
    // Frozen historical data and metadata are constructed only in this fixture.
    let historical = domain::EncodedCommittedImage::encode(
        domain::CommittedImageRole::CreateSendV1,
        source.checkpoint.stream(),
        &source.snapshot,
    )?;
    assert!(
        domain::ValidatedCreateSendImage::validate(domain::DecodedCommittedImage::decode(
            historical.as_bytes()
        )?)
        .is_ok()
    );
    assert_eq!(
        EncodedNativeSnapshotMetadata::encode(historical.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidImage)
    );
    let wire = codec::MetadataV1::from_image(&source.checkpoint, historical.as_bytes())?;
    let metadata = codec::encode(&wire)?;
    assert_eq!(
        DecodedNativeSnapshotPair::decode(&metadata, historical.as_bytes()).err(),
        Some(NativeSnapshotMetadataError::InvalidImage)
    );
    let current = EncodedNativeSnapshotMetadata::encode(source.image.as_bytes())?;
    let pair = DecodedNativeSnapshotPair::decode(current.as_bytes(), source.image.as_bytes())?;
    assert_eq!(
        pair.image().role(),
        domain::CommittedImageRole::CreateSendLayout17V1
    );
    Ok(())
}
