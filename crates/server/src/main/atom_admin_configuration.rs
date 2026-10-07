use std::{fs, io::Read, net::SocketAddr, path::Path};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use server::{AtomAdminListener, BrokerHandle, StartupError};

use super::{Arguments, LoadedTls};

pub(super) const MAX_ATOM_ADMIN_KEY_BYTES: usize = 8 * 1024;

pub(super) struct PreparedAtomAdmin {
    pub(super) address: SocketAddr,
    pub(super) scope: ResourceScope,
    pub(super) policy: SharedAccessPolicy,
}

impl PreparedAtomAdmin {
    pub(super) fn listener(
        &self,
        broker: BrokerHandle,
        namespace: domain::NamespaceName,
        tls: &LoadedTls,
    ) -> Result<AtomAdminListener, StartupError> {
        AtomAdminListener::new(
            broker,
            namespace,
            self.policy.clone(),
            self.scope.clone(),
            tls.amqp.clone(),
        )
        .map_err(|_| StartupError::AtomAdminConfiguration)
    }
}

pub(super) fn load_atom_admin_configuration(
    arguments: &Arguments,
    tls: bool,
) -> Result<Option<PreparedAtomAdmin>, StartupError> {
    let Some(address) = arguments.atom_admin_listen else {
        if arguments.atom_admin_audience_host.is_some()
            || arguments.atom_admin_key_name.is_some()
            || arguments.atom_admin_key_file.is_some()
        {
            return Err(StartupError::AtomAdminRequiresListener);
        }
        return Ok(None);
    };
    let (host, name, path) = match (
        arguments.atom_admin_audience_host.as_deref(),
        arguments.atom_admin_key_name.as_deref(),
        arguments.atom_admin_key_file.as_deref(),
    ) {
        (Some(host), Some(name), Some(path)) => (host, name, path),
        _ => return Err(StartupError::IncompleteAtomAdminConfiguration),
    };
    if !tls {
        return Err(StartupError::AtomAdminRequiresTls);
    }
    let scope =
        ResourceScope::namespace(host).map_err(|_| StartupError::AtomAdminInvalidAudience)?;
    if name.is_empty() {
        return Err(StartupError::AtomAdminInvalidKeyName);
    }
    let bytes = read_atom_admin_key_regular_file(open_atom_admin_key_file(path)?)?;
    let key = std::str::from_utf8(&bytes).map_err(|_| StartupError::AtomAdminKeyNotUtf8)?;
    let key = SharedAccessKey::new(key.trim_end_matches(['\r', '\n']))
        .map_err(|_| StartupError::AtomAdminInvalidKey)?;
    let rule = SharedAccessRule::new(name, scope.clone(), key, None, PermissionSet::MANAGE)
        .map_err(|_| StartupError::AtomAdminConfiguration)?;
    let policy =
        SharedAccessPolicy::new([rule]).map_err(|_| StartupError::AtomAdminConfiguration)?;
    Ok(Some(PreparedAtomAdmin {
        address,
        scope,
        policy,
    }))
}

#[cfg(unix)]
pub(super) fn open_atom_admin_key_file(path: &Path) -> Result<fs::File, StartupError> {
    use rustix::fs::{Mode, OFlags};

    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| StartupError::ReadAtomAdminKey)?;
    Ok(fs::File::from(descriptor))
}

#[cfg(not(unix))]
pub(super) fn open_atom_admin_key_file(path: &Path) -> Result<fs::File, StartupError> {
    // This precheck is not a universal non-Unix special-file race guarantee.
    if !path
        .metadata()
        .map_err(|_| StartupError::ReadAtomAdminKey)?
        .is_file()
    {
        return Err(StartupError::AtomAdminKeyNotRegularFile);
    }
    fs::File::open(path).map_err(|_| StartupError::ReadAtomAdminKey)
}

pub(super) fn read_atom_admin_key_regular_file(file: fs::File) -> Result<Vec<u8>, StartupError> {
    let metadata = file
        .metadata()
        .map_err(|_| StartupError::ReadAtomAdminKey)?;
    if !metadata.is_file() {
        return Err(StartupError::AtomAdminKeyNotRegularFile);
    }
    if metadata.len() > MAX_ATOM_ADMIN_KEY_BYTES as u64 {
        return Err(StartupError::AtomAdminKeyTooLarge);
    }
    read_atom_admin_key_bytes(file)
}

pub(super) fn read_atom_admin_key_bytes(reader: impl Read) -> Result<Vec<u8>, StartupError> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_ATOM_ADMIN_KEY_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| StartupError::ReadAtomAdminKey)?;
    if bytes.len() > MAX_ATOM_ADMIN_KEY_BYTES {
        return Err(StartupError::AtomAdminKeyTooLarge);
    }
    Ok(bytes)
}
