use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PREFIX: &str = "SWITCHYARD_SDK_CUSTODY ";
pub const RECORD_BYTES: usize = 16 * 1024;
pub const ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssemblyIdentity {
    pub role: String,
    pub full_name: String,
    pub informational_version: String,
    pub location: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub schema: u32,
    pub kind: String,
    pub nonce: String,
    pub assemblies: Vec<AssemblyIdentity>,
}

pub struct Artifact {
    pub role: &'static str,
    pub simple_name: String,
    pub path: PathBuf,
    pub sha256: String,
}

impl Artifact {
    pub fn read(role: &'static str, path: &Path) -> io::Result<Self> {
        let path = path.canonicalize()?;
        let simple_name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("artifact has no UTF-8 simple name"))?
            .to_owned();
        let sha256 = hash_file(&path)?;
        Ok(Self {
            role,
            simple_name,
            path,
            sha256,
        })
    }
}

pub fn hash_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() > ARTIFACT_BYTES {
        return Err(invalid("SDK assembly exceeded the file byte ceiling"));
    }
    let mut total = 0_u64;
    let mut buffer = [0_u8; 8192];
    let mut digest = Sha256::new();
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > ARTIFACT_BYTES {
            return Err(invalid("SDK assembly grew beyond the file byte ceiling"));
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn verify(stdout: &[u8], nonce: &str, artifacts: &[Artifact]) -> io::Result<Vec<Record>> {
    if artifacts.len() != 3 || !hex_digest(nonce) {
        return Err(invalid("invalid custody invocation inputs"));
    }
    let text = std::str::from_utf8(stdout).map_err(|_| invalid("child stdout is not UTF-8"))?;
    let mut records: Vec<Record> = Vec::with_capacity(2);
    for framed in text.split_inclusive('\n') {
        let terminated = framed.ends_with('\n');
        let line = framed.strip_suffix('\n').unwrap_or(framed);
        let line = line.strip_suffix('\r').unwrap_or(line);
        if !terminated
            && (line.starts_with(PREFIX) || (!line.is_empty() && PREFIX.starts_with(line)))
        {
            return Err(invalid("unterminated or truncated SDK custody record"));
        }
        let Some(body) = line.strip_prefix(PREFIX) else {
            if line.starts_with("SWITCHYARD_SDK_CUSTODY") {
                return Err(invalid("malformed SDK custody marker"));
            }
            continue;
        };
        if line.len() > RECORD_BYTES || records.len() == 2 {
            return Err(invalid("duplicate or oversized SDK custody record"));
        }
        let record: Record =
            serde_json::from_str(body).map_err(|_| invalid("malformed SDK custody record"))?;
        let kind = if records.is_empty() {
            "start"
        } else {
            "complete"
        };
        if record.schema != 1
            || record.kind != kind
            || record.nonce != nonce
            || record.assemblies.len() != artifacts.len()
        {
            return Err(invalid("SDK custody record does not match its invocation"));
        }
        for (identity, artifact) in record.assemblies.iter().zip(artifacts) {
            if identity.role != artifact.role
                || identity.full_name.len() > 1024
                || identity.full_name.split(',').next() != Some(artifact.simple_name.as_str())
                || !identity.full_name.contains(',')
                || identity.informational_version.is_empty()
                || identity.informational_version.len() > 1024
                || identity.location.is_empty()
                || identity.location.len() > 4096
                || !hex_digest(&identity.sha256)
                || identity.sha256 != artifact.sha256
                || Path::new(&identity.location).canonicalize()? != artifact.path
            {
                return Err(invalid(
                    "loaded SDK assembly does not match the launched artifact",
                ));
            }
        }
        if let Some(start) = records.first()
            && record.assemblies != start.assemblies
        {
            return Err(invalid("SDK custody identities changed before completion"));
        }
        records.push(record);
    }
    if records.len() != 2 {
        return Err(invalid(
            "child omitted start or post-disposal completion custody",
        ));
    }
    for artifact in artifacts {
        if hash_file(&artifact.path)? != artifact.sha256 {
            return Err(invalid("launched SDK artifact changed during the run"));
        }
    }
    Ok(records)
}

fn hex_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
