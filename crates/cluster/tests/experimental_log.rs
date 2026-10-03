use std::{error::Error, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(30);

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    tokio::time::timeout(crate::DEADLINE, super::$case(storage::MemoryReplicaStore::new())).await??;
                    Ok(())
                }
            )+
        }
        mod durable {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    let directory = testkit::DurableProvider::temporary()?;
                    tokio::time::timeout(crate::DEADLINE, super::$case(storage::FjallReplicaStore::open(directory.path())?)).await??;
                    Ok(())
                }
            )+
        }
    };
}

#[path = "experimental_log/admission.rs"]
mod admission;
#[path = "experimental_log/append.rs"]
mod append;
#[path = "experimental_log/callback.rs"]
mod callback;
#[path = "experimental_log/corruption.rs"]
mod corruption;
#[path = "experimental_log/crash.rs"]
mod crash;
#[path = "experimental_log/fixture.rs"]
mod fixture;
#[path = "experimental_log/lifecycle.rs"]
mod lifecycle;
#[path = "experimental_log/limits.rs"]
mod limits;
#[path = "experimental_log/ranges.rs"]
mod ranges;
