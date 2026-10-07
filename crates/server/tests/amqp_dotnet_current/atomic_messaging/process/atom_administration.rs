use super::*;
use sha2::{Digest, Sha256};
use std::io::Read;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AtomScenario {
    Empty,
    Create,
    Update,
    Noop,
    Refusals,
    Quota,
    Retention,
    Delete,
    Paging,
    Denied,
    TlsRefused,
    SubscriptionsEmpty,
    SubscriptionsCreate,
    SubscriptionsInspect,
    SubscriptionsUpdate,
    SubscriptionsRefusals,
    SubscriptionsDelete,
    SubscriptionsRecreate,
    SubscriptionsDenied,
    SubscriptionsTlsRefused,
    RulesEmpty,
    RulesCreate,
    RulesInspect,
    RulesRefusals,
    RulesDelete,
    RulesSql,
    RulesRecreate,
    RulesOpaque,
    RulesDenied,
    RulesTlsRefused,
}

impl AtomScenario {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Create => "create",
            Self::Update => "update",
            Self::Noop => "noop",
            Self::Refusals => "refusals",
            Self::Quota => "quota",
            Self::Retention => "retention",
            Self::Delete => "delete",
            Self::Paging => "paging",
            Self::Denied => "denied",
            Self::TlsRefused => "tls-refused",
            Self::SubscriptionsEmpty => "subscriptions-empty",
            Self::SubscriptionsCreate => "subscriptions-create",
            Self::SubscriptionsInspect => "subscriptions-inspect",
            Self::SubscriptionsUpdate => "subscriptions-update",
            Self::SubscriptionsRefusals => "subscriptions-refusals",
            Self::SubscriptionsDelete => "subscriptions-delete",
            Self::SubscriptionsRecreate => "subscriptions-recreate",
            Self::SubscriptionsDenied => "subscriptions-denied",
            Self::SubscriptionsTlsRefused => "subscriptions-tls-refused",
            Self::RulesEmpty => "rules-empty",
            Self::RulesCreate => "rules-create",
            Self::RulesInspect => "rules-inspect",
            Self::RulesRefusals => "rules-refusals",
            Self::RulesDelete => "rules-delete",
            Self::RulesSql => "rules-sql",
            Self::RulesRecreate => "rules-recreate",
            Self::RulesOpaque => "rules-opaque",
            Self::RulesDenied => "rules-denied",
            Self::RulesTlsRefused => "rules-tls-refused",
        }
    }

    fn marker(self) -> String {
        format!(
            "official .NET Atom administration {} named-key/connection-string passed",
            self.name()
        )
    }

    fn completed(self, stdout: &str) -> bool {
        let expected = self.marker();
        stdout.split_inclusive('\n').any(|line| {
            let Some(line) = line.strip_suffix('\n') else {
                return false;
            };
            line.strip_suffix('\r').unwrap_or(line) == expected
        })
    }
}

pub(crate) async fn build_atom_client(sdk_version: &str) -> TestResult<tempfile::TempDir> {
    let artifacts = super::build_client(sdk_version).await?;
    eprintln!(
        "atom-sdk artifacts sdk={sdk_version} directory={:?}",
        artifacts.path()
    );
    Ok(artifacts)
}

fn command(
    dll: &Path,
    scenario: AtomScenario,
    endpoint: &str,
    ca_file: &Path,
    rule: &str,
    key: &str,
) -> Command {
    let mut command = Command::new("dotnet");
    command
        .env("DOTNET_PROCESSOR_COUNT", "2")
        .arg(dll)
        .arg("atom-administration")
        .arg(scenario.name())
        .arg(endpoint)
        .arg(ca_file)
        .arg(rule)
        .arg(key);
    command
}

pub(crate) async fn run_atom_client(
    dll: &Path,
    scenario: AtomScenario,
    endpoint: &str,
    ca_file: &Path,
    rule: &str,
    key: &str,
) -> TestResult<Output> {
    let output = super::run(
        command(dll, scenario, endpoint, ca_file, rule, key),
        "official .NET Atom administration client",
        RUN_DEADLINE,
        MAX_OUTPUT_BYTES,
    )
    .await?;
    if !output.status.success() || !scenario.completed(&output.stdout) {
        return Err(io::Error::other(format!(
            "official .NET Atom administration {} completed without its exact marker ({})\nstdout:\n{}\nstderr:\n{}",
            scenario.name(), output.status, output.stdout, output.stderr,
        )).into());
    }
    let evidence = verify_loaded_assemblies(dll, &output.stdout)?;
    for (name, loaded) in [
        ("Azure.Messaging.ServiceBus", evidence.service_bus),
        ("Azure.Core", evidence.core),
    ] {
        let [major, minor, build, revision] = loaded.version;
        eprintln!(
            "atom-sdk loaded scenario={} assembly={name} version={major}.{minor}.{build}.{revision} sha256={}",
            scenario.name(),
            hash_text(&loaded.sha256)
        );
    }
    Ok(output)
}

const MAX_ASSEMBLY_BYTES: u64 = 16 * 1024 * 1024;
const LOADED_PREFIX: &str = "Atom SDK loaded ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LoadedAssembly {
    version: [u16; 4],
    sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LoadedAssemblies {
    service_bus: LoadedAssembly,
    core: LoadedAssembly,
}

fn assembly_version(value: &str) -> Option<[u16; 4]> {
    let mut parts = value.split('.');
    let mut version = [0; 4];
    for component in &mut version {
        let part = parts.next()?;
        if part.is_empty()
            || !part.bytes().all(|byte| byte.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return None;
        }
        *component = part.parse().ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(version)
}

fn assembly_hash(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let mut hash = [0; 32];
    for (output, pair) in hash.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *output = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    Some(hash)
}

fn loaded_assemblies(stdout: &str) -> Option<LoadedAssemblies> {
    if stdout.len() > MAX_OUTPUT_BYTES {
        return None;
    }
    let mut service_bus = None;
    let mut core = None;
    for raw in stdout.split_inclusive('\n') {
        let Some(line) = raw.strip_suffix('\n') else {
            if raw.starts_with(LOADED_PREFIX) {
                return None;
            }
            continue;
        };
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(fields) = line.strip_prefix(LOADED_PREFIX) else {
            continue;
        };
        if line.len() > 192 {
            return None;
        }
        let (name, fields) = fields.strip_prefix("assembly=")?.split_once(" version=")?;
        let (version, hash) = fields.split_once(" sha256=")?;
        let evidence = LoadedAssembly {
            version: assembly_version(version)?,
            sha256: assembly_hash(hash)?,
        };
        let slot = match name {
            "Azure.Messaging.ServiceBus" => &mut service_bus,
            "Azure.Core" => &mut core,
            _ => return None,
        };
        if slot.replace(evidence).is_some() {
            return None;
        }
    }
    Some(LoadedAssemblies {
        service_bus: service_bus?,
        core: core?,
    })
}

fn hash_owned_assembly(path: &Path) -> TestResult<[u8; 32]> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > MAX_ASSEMBLY_BYTES
    {
        return Err(io::Error::other("owned SDK assembly file is missing or unsupported").into());
    }
    let mut source = std::fs::File::open(path)?.take(MAX_ASSEMBLY_BYTES + 1);
    let mut hash = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > MAX_ASSEMBLY_BYTES {
            return Err(io::Error::other("owned SDK assembly exceeded its read bound").into());
        }
        hash.update(&buffer[..count]);
    }
    if total != metadata.len() {
        return Err(io::Error::other("owned SDK assembly length changed").into());
    }
    Ok(hash.finalize().into())
}

fn verify_loaded_assemblies(dll: &Path, stdout: &str) -> TestResult<LoadedAssemblies> {
    let evidence = loaded_assemblies(stdout)
        .ok_or_else(|| io::Error::other("loaded SDK assembly evidence is missing or malformed"))?;
    let directory = dll
        .parent()
        .ok_or_else(|| io::Error::other("owned SDK output directory is missing"))?;
    for (name, loaded) in [
        ("Azure.Messaging.ServiceBus", evidence.service_bus),
        ("Azure.Core", evidence.core),
    ] {
        if hash_owned_assembly(&directory.join(format!("{name}.dll")))? != loaded.sha256 {
            return Err(io::Error::other(
                "loaded SDK assembly file fingerprint mismatched the owned output",
            )
            .into());
        }
    }
    Ok(evidence)
}

pub(super) fn verify_capacity_loaded_assemblies(dll: &Path, stdout: &str) -> TestResult {
    let evidence = verify_loaded_assemblies(dll, stdout)?;
    for (name, loaded) in [
        ("Azure.Messaging.ServiceBus", evidence.service_bus),
        ("Azure.Core", evidence.core),
    ] {
        let [major, minor, build, revision] = loaded.version;
        eprintln!(
            "capacity-sdk loaded assembly={name} version={major}.{minor}.{build}.{revision} sha256={}",
            hash_text(&loaded.sha256)
        );
    }
    Ok(())
}

fn hash_text(hash: &[u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, version: &str, hash: &str) -> String {
        format!("Atom SDK loaded assembly={name} version={version} sha256={hash}\n")
    }

    #[test]
    fn loaded_records_require_both_exact_names_without_duplicates() {
        let hash = "AB".repeat(32);
        let service = record("Azure.Messaging.ServiceBus", "7.21.0.0", &hash);
        let core = record("Azure.Core", "1.0.0.0", &hash);
        let expected = LoadedAssemblies {
            service_bus: LoadedAssembly {
                version: [7, 21, 0, 0],
                sha256: [0xAB; 32],
            },
            core: LoadedAssembly {
                version: [1, 0, 0, 0],
                sha256: [0xAB; 32],
            },
        };
        assert_eq!(
            loaded_assemblies(&format!("diagnostic\n{service}{core}")),
            Some(expected)
        );
        assert_eq!(
            loaded_assemblies(&format!("{core}{service}").replace('\n', "\r\n")),
            Some(expected)
        );
        for invalid in [
            String::new(),
            service.clone(),
            core.clone(),
            format!("{service}{core}{core}"),
            format!("{service}{service}{core}"),
            format!("{service}{}", record("Unknown", "1.0.0.0", &hash)),
            format!("prefix {service}{core}"),
            format!("{service}{core}Atom SDK loaded unknown=yes\n"),
            format!("{service}{}", core.trim_end()),
            format!("{service}{}\r", core.trim_end()),
            format!("{service}{} extra=yes\n", core.trim_end()),
            "x".repeat(MAX_OUTPUT_BYTES + 1),
        ] {
            assert!(loaded_assemblies(&invalid).is_none());
        }
    }

    #[test]
    fn loaded_version_and_hash_fields_are_closed_and_bounded() {
        let hash = "AF".repeat(32);
        let service = record("Azure.Messaging.ServiceBus", "7.21.0.0", &hash);
        for version in [
            "",
            "1.2.3",
            "1.2.3.4.5",
            "-1.2.3.4",
            "+1.2.3.4",
            "01.2.3.4",
            "65536.0.0.0",
            "1.2.3.x",
        ] {
            assert!(
                loaded_assemblies(&format!(
                    "{service}{}",
                    record("Azure.Core", version, &hash)
                ))
                .is_none()
            );
        }
        assert_eq!(
            assembly_version("65535.65535.65535.65535"),
            Some([65535; 4])
        );
        assert_eq!(assembly_version("0.0.0.0"), Some([0; 4]));
        for hash in [
            "AF".repeat(31),
            "AF".repeat(33),
            "af".repeat(32),
            "AG".repeat(32),
            "A ".repeat(32),
        ] {
            assert!(
                loaded_assemblies(&format!(
                    "{service}{}",
                    record("Azure.Core", "1.0.0.0", &hash)
                ))
                .is_none()
            );
        }
    }

    #[test]
    fn loaded_fingerprints_must_match_the_two_owned_output_files() -> TestResult {
        let directory = tempfile::TempDir::new()?;
        let dll = directory.path().join("client.dll");
        let service = directory.path().join("Azure.Messaging.ServiceBus.dll");
        let core = directory.path().join("Azure.Core.dll");
        std::fs::write(&service, b"owned fixture service bytes")?;
        std::fs::write(&core, b"owned fixture core bytes")?;
        let stdout = format!(
            "{}{}",
            record(
                "Azure.Messaging.ServiceBus",
                "7.21.0.0",
                &hash_text(&hash_owned_assembly(&service)?)
            ),
            record(
                "Azure.Core",
                "1.0.0.0",
                &hash_text(&hash_owned_assembly(&core)?)
            )
        );
        assert!(verify_loaded_assemblies(&dll, &stdout).is_ok());
        std::fs::write(&core, b"changed fixture core bytes")?;
        assert!(verify_loaded_assemblies(&dll, &stdout).is_err());
        std::fs::remove_file(&core)?;
        assert!(verify_loaded_assemblies(&dll, &stdout).is_err());
        std::os::unix::fs::symlink(&service, &core)?;
        assert!(verify_loaded_assemblies(&dll, &stdout).is_err());
        std::fs::remove_file(&core)?;
        std::fs::write(&core, b"")?;
        assert!(verify_loaded_assemblies(&dll, &stdout).is_err());
        Ok(())
    }

    #[test]
    fn all_scenarios_require_a_completed_exact_marker() {
        let scenarios = [
            AtomScenario::Empty,
            AtomScenario::Create,
            AtomScenario::Update,
            AtomScenario::Noop,
            AtomScenario::Refusals,
            AtomScenario::Quota,
            AtomScenario::Retention,
            AtomScenario::Delete,
            AtomScenario::Paging,
            AtomScenario::Denied,
            AtomScenario::TlsRefused,
            AtomScenario::SubscriptionsEmpty,
            AtomScenario::SubscriptionsCreate,
            AtomScenario::SubscriptionsInspect,
            AtomScenario::SubscriptionsUpdate,
            AtomScenario::SubscriptionsRefusals,
            AtomScenario::SubscriptionsDelete,
            AtomScenario::SubscriptionsRecreate,
            AtomScenario::SubscriptionsDenied,
            AtomScenario::SubscriptionsTlsRefused,
            AtomScenario::RulesEmpty,
            AtomScenario::RulesCreate,
            AtomScenario::RulesInspect,
            AtomScenario::RulesRefusals,
            AtomScenario::RulesDelete,
            AtomScenario::RulesSql,
            AtomScenario::RulesRecreate,
            AtomScenario::RulesOpaque,
            AtomScenario::RulesDenied,
            AtomScenario::RulesTlsRefused,
        ];
        for scenario in scenarios {
            let marker = scenario.marker();
            assert!(scenario.completed(&format!("{marker}\n")));
            assert!(scenario.completed(&format!("diagnostic\r\n{marker}\r\n")));
            for invalid in [
                String::new(),
                marker.clone(),
                format!("{marker}\r"),
                format!("prefix {marker}\n"),
                format!("{marker} suffix\n"),
                format!("{}\n", &marker[..marker.len() - 1]),
            ] {
                assert!(!scenario.completed(&invalid));
            }
            assert!(!scenario.completed(
                "official .NET Atom administration unknown named-key/connection-string passed\n"
            ));
        }
        assert!(!AtomScenario::Create.completed(&AtomScenario::Update.marker()));
        assert!(!AtomScenario::Create.completed(&format!("{}\n", AtomScenario::Update.marker())));
    }

    #[test]
    fn command_has_six_closed_arguments_and_no_global_trust_environment() {
        let command = command(
            Path::new("client.dll"),
            AtomScenario::Retention,
            "https://localhost:12345/",
            Path::new("private-ca.pem"),
            "manage",
            "fixture-key",
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
                "atom-administration",
                "retention",
                "https://localhost:12345/",
                "private-ca.pem",
                "manage",
                "fixture-key"
            ]
        );
        assert_eq!(
            command.get_envs().collect::<Vec<_>>(),
            [(
                std::ffi::OsStr::new("DOTNET_PROCESSOR_COUNT"),
                Some(std::ffi::OsStr::new("2"))
            )]
        );
    }
}
