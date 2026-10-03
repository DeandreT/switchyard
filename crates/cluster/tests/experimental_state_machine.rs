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

#[path = "experimental_state_machine/admission.rs"]
mod admission;
#[path = "experimental_state_machine/bounds.rs"]
mod bounds;
#[path = "experimental_state_machine/corruption.rs"]
mod corruption;
#[path = "experimental_state_machine/crash.rs"]
mod crash;
#[path = "experimental_state_machine/fixture.rs"]
mod fixture;
#[path = "experimental_state_machine/lifecycle.rs"]
mod lifecycle;
#[path = "experimental_state_machine/physical.rs"]
mod physical;
