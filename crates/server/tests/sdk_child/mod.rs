//! Linux-only controlled-child custody, not a sandbox or exact NuGet pin proof.
//!
//! Group descendants must not deliberately escape their group. Reaping is for the
//! original immediate child only; synchronous Drop can wait on kernel I/O.
//! Project/bin/obj are fresh; the existing global NuGet package cache is reused.

pub mod child;
pub mod identity;

use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
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
    artifacts: Vec<identity::Artifact>,
}

pub struct VerifiedRun {
    pub output: Output,
    pub records: Vec<identity::Record>,
}

impl SdkRun {
    pub fn prepare(source: &Path, project_name: &str) -> io::Result<Self> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "selected SDK child custody gates require Linux",
            ));
        }
        let root = tempfile::tempdir()?;
        let project_dir = root.path().join("project");
        let shared_dir = root.path().join("shared");
        fs::create_dir(&project_dir)?;
        fs::create_dir(&shared_dir)?;
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
            artifacts: Vec::new(),
        })
    }

    pub async fn build(&mut self) -> io::Result<()> {
        let mut command = Command::new("dotnet");
        command
            .arg("build")
            .arg(&self.project)
            .arg("--configuration")
            .arg("Release")
            .arg("--maxcpucount:2")
            .arg("--disable-build-servers")
            .arg("/nodeReuse:false")
            .arg("--nologo")
            .arg("--tl:off")
            .env("DOTNET_PROCESSOR_COUNT", "2");
        let output = child::run(
            command,
            child::Limits {
                deadline: BUILD_DEADLINE,
                stream_bytes: STREAM_BYTES,
            },
        )
        .await?;
        require_success("build", &output)?;
        let directory = self.dll.parent().expect("DLL has a parent");
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
        let mut digest = Sha256::new();
        digest.update(self.root.path().as_os_str().as_encoded_bytes());
        digest.update(std::process::id().to_le_bytes());
        digest.update(INVOCATIONS.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        let nonce = format!("{:x}", digest.finalize());
        let mut command = Command::new("dotnet");
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
        Ok(VerifiedRun { output, records })
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
