#![forbid(unsafe_code)]

use domain::{EntityPath, NamespaceName, PlacementGroupId};
use serde::{Deserialize, Serialize};

pub const PROTOBUF_PACKAGE: &str = "switchyard.admin.v1";
pub const ADMIN_TLS_PORT: u16 = 9443;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Queue,
    Topic,
    Subscription,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityDefinition {
    pub namespace: NamespaceName,
    pub path: EntityPath,
    pub kind: EntityKind,
    pub placement_group_id: PlacementGroupId,
    pub max_size_bytes: u64,
    pub requires_session: bool,
}
