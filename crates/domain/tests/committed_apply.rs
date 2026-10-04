use std::error::Error;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(
                #[test]
                fn $case() -> super::TestResult {
                    super::$case(storage::MemoryReplicaStore::new())
                }
            )+
        }
        mod durable {
            $(
                #[test]
                fn $case() -> super::TestResult {
                    let directory = testkit::DurableProvider::temporary()?;
                    super::$case(storage::FjallReplicaStore::open(directory.path())?)
                }
            )+
        }
    };
}

#[path = "committed_apply/atomicity.rs"]
mod atomicity;
#[path = "committed_apply/corruption.rs"]
mod corruption;
#[path = "committed_apply/crash.rs"]
mod crash;
#[path = "committed_apply/fixture.rs"]
mod fixture;
#[path = "committed_apply/image.rs"]
mod image;
#[path = "committed_apply/lifecycle.rs"]
mod lifecycle;
#[path = "committed_apply/limits.rs"]
mod limits;
#[path = "committed_apply/origins.rs"]
mod origins;
#[path = "committed_apply/replay.rs"]
mod replay;
#[path = "committed_apply/validated_image.rs"]
mod validated_image;
