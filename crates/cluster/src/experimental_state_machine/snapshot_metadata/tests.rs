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
