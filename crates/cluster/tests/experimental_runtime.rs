use std::{error::Error, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(45);

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    let stores = std::array::from_fn(|_| (
                        storage::MemoryReplicaStore::new(), storage::MemoryReplicaStore::new(),
                    ));
                    tokio::time::timeout(crate::DEADLINE, super::$case(stores)).await??;
                    Ok(())
                }
            )+
        }
        mod durable {
            $(
                #[tokio::test]
                async fn $case() -> crate::TestResult {
                    let directory = testkit::DurableProvider::temporary()?;
                    let stores = crate::fixture::durable_stores(directory.path())?;
                    tokio::time::timeout(crate::DEADLINE, super::$case(stores)).await??;
                    Ok(())
                }
            )+
        }
    };
}

#[path = "experimental_runtime/client.rs"]
mod client;
#[path = "experimental_runtime/failover.rs"]
mod failover;
#[path = "experimental_runtime/fixture.rs"]
mod fixture;
#[path = "experimental_runtime/quorum.rs"]
mod quorum;
#[path = "experimental_runtime/recovery.rs"]
mod recovery;
#[path = "experimental_runtime/rejoin.rs"]
mod rejoin;
#[path = "experimental_runtime/rejoin_lifecycle.rs"]
mod rejoin_lifecycle;
