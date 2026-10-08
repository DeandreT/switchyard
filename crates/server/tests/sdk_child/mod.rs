//! Linux-only controlled-child custody and fixed SDK runtime-asset selection.
//!
//! Group descendants must not deliberately escape their group. Reaping is for the
//! original immediate child only; synchronous Drop can wait on kernel I/O.
//! Project/bin/obj and NuGet paths are invocation-owned and pin-isolated.

pub mod child;
pub mod identity;
pub mod pins;

use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tempfile::TempDir;

const BUILD_DEADLINE: Duration = Duration::from_secs(300);
const RUN_DEADLINE: Duration = Duration::from_secs(180);
const STREAM_BYTES: usize = 1024 * 1024;
static INVOCATIONS: AtomicU64 = AtomicU64::new(0);

pub struct SdkRun {
    root: TempDir,
    project: PathBuf,
    dll: PathBuf,
    inputs: pins::PinInputs,
    packages: Vec<pins::ResolvedAsset>,
    artifacts: Vec<identity::Artifact>,
}

pub struct VerifiedRun {
    pub output: Output,
    pub records: Vec<identity::Record>,
    pub packages: Vec<pins::ResolvedAsset>,
}

impl SdkRun {
    pub fn prepare(source: &Path, project_name: &str, pin: pins::SdkPin) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "selected SDK child custody gates require Linux",
            ));
        }
        let conformance = source
            .parent()
            .ok_or_else(|| io::Error::other("missing conformance parent"))?;
        let pin_directory = conformance.join("pins").join(pin.selector());
        let inputs = pins::PinInputs::read(pin, &source.join(project_name), &pin_directory)?;
        let root = tempfile::tempdir()?;
        let project_dir = root.path().join("project");
        let shared_dir = root.path().join("shared");
        fs::create_dir(&project_dir)?;
        fs::create_dir(&shared_dir)?;
        for name in [
            "packages",
            "http-cache",
            "scratch",
            "plugins-cache",
            "cli-home",
        ] {
            fs::create_dir(root.path().join(name))?;
        }
        fs::copy(
            conformance.join("pins/NuGet.Config"),
            root.path().join("NuGet.Config"),
        )?;
        for name in ["SdkPin.props", "packages.lock.json"] {
            fs::copy(pin_directory.join(name), project_dir.join(name))?;
        }
        for name in [project_name, "Program.cs", "TopicConformance.cs"] {
            fs::copy(source.join(name), project_dir.join(name))?;
        }
        let shared_source = source
            .parent()
            .ok_or_else(|| io::Error::other("missing shared parent"))?
            .join("shared");
        for name in [
            "CaseInsensitiveIdentityConformance.cs",
            "RuleConformance.cs",
            "ScheduledTopicConformance.cs",
            "SdkCustody.cs",
        ] {
            fs::copy(shared_source.join(name), shared_dir.join(name))?;
        }
        let stem = Path::new(project_name)
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("missing project name"))?;
        let dll = project_dir
            .join("bin/Release/net10.0")
            .join(format!("{stem}.dll"));
        Ok(Self {
            root,
            project: project_dir.join(project_name),
            dll,
            inputs,
            packages: Vec::new(),
            artifacts: Vec::new(),
        })
    }

    pub async fn build(&mut self) -> io::Result<()> {
        self.artifacts.clear();
        self.packages.clear();
        let started = Instant::now();
        let mut restore = self.dotnet_command();
        restore
            .arg("restore")
            .arg(&self.project)
            .arg("--locked-mode")
            .arg("--configfile")
            .arg(self.root.path().join("NuGet.Config"))
            .arg("--disable-parallel")
            .arg("--force")
            .arg("--nologo")
            .arg("--tl:off")
            .arg("--maxcpucount:2")
            .arg("/nodeReuse:false");
        let output = child::run(
            restore,
            child::Limits {
                deadline: BUILD_DEADLINE,
                stream_bytes: STREAM_BYTES,
            },
        )
        .await?;
        require_success("locked restore", &output)?;
        let remaining = BUILD_DEADLINE
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "SDK restore exhausted the build deadline",
                )
            })?;
        let mut command = self.dotnet_command();
        command
            .arg("build")
            .arg(&self.project)
            .arg("--configuration")
            .arg("Release")
            .arg("--no-restore")
            .arg("--maxcpucount:2")
            .arg("--disable-build-servers")
            .arg("/nodeReuse:false")
            .arg("--nologo")
            .arg("--tl:off")
            .env("DOTNET_PROCESSOR_COUNT", "2");
        let output = child::run(
            command,
            child::Limits {
                deadline: remaining,
                stream_bytes: STREAM_BYTES,
            },
        )
        .await?;
        require_success("build", &output)?;
        let directory = self.dll.parent().expect("DLL has a parent");
        self.packages = self.verify_package_assets()?;
        self.artifacts = [
            identity::Artifact::read("entry", &self.dll)?,
            identity::Artifact::read(
                "service_bus",
                &directory.join("Azure.Messaging.ServiceBus.dll"),
            )?,
            identity::Artifact::read("core", &directory.join("Azure.Core.dll"))?,
        ]
        .into();
        Ok(())
    }

    pub async fn run(&self, args: &[OsString], env: &[(&str, &OsStr)]) -> io::Result<VerifiedRun> {
        if self.artifacts.is_empty() {
            return Err(io::Error::other("SDK project must be built before running"));
        }
        if self.verify_package_assets()? != self.packages {
            return Err(io::Error::other(
                "SDK package identities changed before launch",
            ));
        }
        let mut digest = Sha256::new();
        digest.update(self.root.path().as_os_str().as_encoded_bytes());
        digest.update(std::process::id().to_le_bytes());
        digest.update(INVOCATIONS.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        let nonce = format!("{:x}", digest.finalize());
        let mut command = self.dotnet_command();
        command.arg(&self.dll).args(args);
        for (name, value) in env {
            command.env(name, value);
        }
        command
            .env("SWITCHYARD_SDK_NONCE", &nonce)
            .env("DOTNET_PROCESSOR_COUNT", "2");
        let output = child::run(
            command,
            child::Limits {
                deadline: RUN_DEADLINE,
                stream_bytes: STREAM_BYTES,
            },
        )
        .await?;
        require_success("run", &output)?;
        let records = identity::verify(&output.stdout, &nonce, &self.artifacts)?;
        if self.verify_package_assets()? != self.packages {
            return Err(io::Error::other(
                "SDK package identities changed during the run",
            ));
        }
        Ok(VerifiedRun {
            output,
            records,
            packages: self.packages.clone(),
        })
    }

    pub fn project_directory(&self) -> &Path {
        self.project.parent().expect("project has a parent")
    }

    pub fn verify_package_assets(&self) -> io::Result<Vec<pins::ResolvedAsset>> {
        let directory = self.project_directory();
        self.inputs
            .verify_lock(&directory.join("packages.lock.json"))?;
        self.inputs.verify_assets(
            &directory.join("obj/project.assets.json"),
            &self.project,
            &self.root.path().join("packages"),
            self.dll.parent().expect("DLL has a parent"),
        )
    }

    fn dotnet_command(&self) -> Command {
        let mut command = Command::new("dotnet");
        command
            .current_dir(self.root.path())
            .env("NUGET_PACKAGES", self.root.path().join("packages"))
            .env("NUGET_HTTP_CACHE_PATH", self.root.path().join("http-cache"))
            .env("NUGET_SCRATCH", self.root.path().join("scratch"))
            .env(
                "NUGET_PLUGINS_CACHE_PATH",
                self.root.path().join("plugins-cache"),
            )
            .env("DOTNET_CLI_HOME", self.root.path().join("cli-home"))
            .env("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
            .env("DOTNET_PROCESSOR_COUNT", "2");
        command
    }
}

fn require_success(phase: &str, output: &Output) -> io::Result<()> {
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "SDK {phase} failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}
