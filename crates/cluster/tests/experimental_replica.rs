use std::{error::Error, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(30);

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    tokio::time::timeout(crate::DEADLINE, super::$case(storage::MemoryReplicaStore::new(), storage::MemoryReplicaStore::new())).await??;
                    Ok(())
                }
            )+
        }
        mod durable {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    let directory = testkit::DurableProvider::temporary()?;
                    let log = storage::FjallReplicaStore::open(directory.path().join("log"))?;
                    let state = storage::FjallReplicaStore::open(directory.path().join("state"))?;
                    tokio::time::timeout(crate::DEADLINE, super::$case(log, state)).await??;
                    Ok(())
                }
            )+
        }
    };
}

#[path = "experimental_replica/cleanup.rs"]
mod cleanup;
#[path = "experimental_replica/fixture.rs"]
mod fixture;
#[path = "experimental_replica/lifecycle.rs"]
mod lifecycle;
#[path = "experimental_replica/pairing.rs"]
mod pairing;
