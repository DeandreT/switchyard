#![forbid(unsafe_code)]

mod policy;
mod sas;

use serde::{Deserialize, Serialize};

pub use crate::{
    policy::{
        PermissionSet, PolicyError, ResourceScope, ResourceScopeError, SharedAccessKey,
        SharedAccessPolicy, SharedAccessRule,
    },
    sas::{AccessGrant, SasError},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Send,
    Listen,
    Manage,
    Audit,
    Cluster,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Sender,
    Receiver,
    NamespaceAdministrator,
    Auditor,
    ClusterAdministrator,
}

impl Role {
    pub const fn allows(self, permission: Permission) -> bool {
        match self {
            Self::Sender => matches!(permission, Permission::Send),
            Self::Receiver => matches!(permission, Permission::Listen),
            Self::NamespaceAdministrator => {
                matches!(
                    permission,
                    Permission::Send | Permission::Listen | Permission::Manage | Permission::Audit
                )
            }
            Self::Auditor => matches!(permission, Permission::Audit),
            Self::ClusterAdministrator => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_cannot_manage_entities() {
        assert!(Role::Sender.allows(Permission::Send));
        assert!(!Role::Sender.allows(Permission::Manage));
    }
}
