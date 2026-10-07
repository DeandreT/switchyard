use super::*;

const MARKER_PREFIX: &str = "official .NET capacity ingress ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CapacityStage {
    Seed,
    Quota,
    Abandon,
    Complete,
    Retry,
    SizeSeed,
    BrokerSize,
    NegotiatedSize,
    Small,
    DrainSize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CapacityConstructor {
    Named,
    Connection,
}

impl CapacityConstructor {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Named => "named",
            Self::Connection => "connection",
        }
    }
}

impl CapacityStage {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::Quota => "quota",
            Self::Abandon => "abandon",
            Self::Complete => "complete",
            Self::Retry => "retry",
            Self::SizeSeed => "size-seed",
            Self::BrokerSize => "broker-size",
            Self::NegotiatedSize => "negotiated-size",
            Self::Small => "small",
            Self::DrainSize => "drain-size",
        }
    }

    fn marker(self, constructor: CapacityConstructor) -> String {
        format!(
            "{MARKER_PREFIX}{} {} passed",
            self.name(),
            constructor.name()
        )
    }

    fn completed(self, constructor: CapacityConstructor, stdout: &str) -> bool {
        if stdout.len() > MAX_OUTPUT_BYTES {
            return false;
        }
        let expected = self.marker(constructor);
        let mut found = false;
        for raw in stdout.split_inclusive('\n') {
            let Some(line) = raw.strip_suffix('\n') else {
                if raw.starts_with(MARKER_PREFIX) {
                    return false;
                }
                continue;
            };
            let line = line.strip_suffix('\r').unwrap_or(line);
            if !line.starts_with(MARKER_PREFIX) {
                continue;
            }
            if found || line != expected {
                return false;
            }
            found = true;
        }
        found
    }
}

pub(crate) async fn build_capacity_client(sdk_version: &str) -> TestResult<tempfile::TempDir> {
    let artifacts = super::build_client(sdk_version).await?;
    eprintln!("capacity-sdk artifacts directory={:?}", artifacts.path());
    Ok(artifacts)
}

fn command(
    dll: &Path,
    stage: CapacityStage,
    constructor: CapacityConstructor,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", ca_directory)
        .arg(dll)
        .arg("capacity-ingress")
        .arg(stage.name())
        .arg(HOST)
        .arg(endpoint)
        .arg(queue)
        .arg(constructor.name())
        .arg(RULE)
        .arg(KEY);
    command
}

pub(crate) async fn run_capacity_client(
    dll: &Path,
    stage: CapacityStage,
    constructor: CapacityConstructor,
    endpoint: &str,
    queue: &str,
    ca_file: &Path,
    ca_directory: &Path,
) -> TestResult<Output> {
    let output = super::run(
        command(
            dll,
            stage,
            constructor,
            endpoint,
            queue,
            ca_file,
            ca_directory,
        ),
        "official .NET capacity ingress client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await?;
    if !output.status.success() || !stage.completed(constructor, &output.stdout) {
        return Err(io::Error::other(
            "official .NET capacity ingress child completed without its exact marker",
        )
        .into());
    }
    super::atom_administration::verify_capacity_loaded_assemblies(dll, &output.stdout)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGES: [(CapacityStage, &str); 10] = [
        (CapacityStage::Seed, "seed"),
        (CapacityStage::Quota, "quota"),
        (CapacityStage::Abandon, "abandon"),
        (CapacityStage::Complete, "complete"),
        (CapacityStage::Retry, "retry"),
        (CapacityStage::SizeSeed, "size-seed"),
        (CapacityStage::BrokerSize, "broker-size"),
        (CapacityStage::NegotiatedSize, "negotiated-size"),
        (CapacityStage::Small, "small"),
        (CapacityStage::DrainSize, "drain-size"),
    ];
    const CONSTRUCTORS: [(CapacityConstructor, &str); 2] = [
        (CapacityConstructor::Named, "named"),
        (CapacityConstructor::Connection, "connection"),
    ];

    #[test]
    fn stages_and_constructors_require_unique_completed_exact_markers() {
        for (stage, stage_name) in STAGES {
            assert_eq!(stage.name(), stage_name);
            for (constructor, constructor_name) in CONSTRUCTORS {
                assert_eq!(constructor.name(), constructor_name);
                let marker = format!("{MARKER_PREFIX}{stage_name} {constructor_name} passed");
                assert_eq!(stage.marker(constructor), marker);
                assert!(stage.completed(constructor, &format!("{marker}\n")));
                assert!(stage.completed(constructor, &format!("diagnostic\r\n{marker}\r\n")));
                for invalid in [
                    String::new(),
                    marker.clone(),
                    format!("{marker}\r"),
                    format!("prefix {marker}\n"),
                    format!("{marker} suffix\n"),
                    format!("{}\n", &marker[..marker.len() - 1]),
                    format!("{marker}\n{marker}\n"),
                    format!("{MARKER_PREFIX}unknown {constructor_name} passed\n"),
                    format!("{MARKER_PREFIX}{stage_name} unknown passed\n"),
                    format!("{marker}\n{MARKER_PREFIX}"),
                    format!("{marker}\n{MARKER_PREFIX}unknown named passed\n"),
                ] {
                    assert!(!stage.completed(constructor, &invalid));
                }
                for (other_stage, _) in STAGES {
                    if other_stage != stage {
                        assert!(!stage.completed(
                            constructor,
                            &format!("{}\n", other_stage.marker(constructor)),
                        ));
                    }
                }
                for (other_constructor, _) in CONSTRUCTORS {
                    if other_constructor != constructor {
                        assert!(!stage.completed(
                            constructor,
                            &format!("{}\n", stage.marker(other_constructor)),
                        ));
                    }
                }
            }
        }
    }

    #[test]
    fn completed_marker_output_is_bounded() {
        let stage = CapacityStage::Seed;
        let constructor = CapacityConstructor::Named;
        let marker = stage.marker(constructor);
        assert!(!stage.completed(
            constructor,
            &format!("{}\n{marker}\n", "x".repeat(MAX_OUTPUT_BYTES)),
        ));
    }

    #[test]
    fn every_stage_constructor_has_exact_arguments_and_child_only_trust() {
        use std::{collections::BTreeMap, ffi::OsStr};

        for (stage, stage_name) in STAGES {
            for (constructor, constructor_name) in CONSTRUCTORS {
                let command = command(
                    Path::new("client.dll"),
                    stage,
                    constructor,
                    "wss://localhost:12345/",
                    "capacity-queue",
                    Path::new("private-ca.pem"),
                    Path::new("empty-ca-directory"),
                );
                let command = command.as_std();
                assert_eq!(command.get_program(), "dotnet");
                assert_eq!(
                    command
                        .get_args()
                        .map(|arg| arg.to_str().unwrap())
                        .collect::<Vec<_>>(),
                    [
                        "client.dll",
                        "capacity-ingress",
                        stage_name,
                        HOST,
                        "wss://localhost:12345/",
                        "capacity-queue",
                        constructor_name,
                        RULE,
                        KEY,
                    ]
                );
                let environment: BTreeMap<_, _> = command.get_envs().collect();
                assert_eq!(
                    environment,
                    BTreeMap::from([
                        (OsStr::new("DOTNET_PROCESSOR_COUNT"), Some(OsStr::new("2"))),
                        (
                            OsStr::new("SSL_CERT_FILE"),
                            Some(OsStr::new("private-ca.pem"))
                        ),
                        (
                            OsStr::new("SSL_CERT_DIR"),
                            Some(OsStr::new("empty-ca-directory")),
                        ),
                    ]),
                );
            }
        }
    }
}
