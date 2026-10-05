use super::{classify::*, records::*, wire::*, *};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

mod codec_cases;
mod disposition_cases;
mod fixture;
mod policy_cases;
