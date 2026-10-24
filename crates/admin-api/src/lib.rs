#![forbid(unsafe_code)]

mod service;

use domain::{EntityPath, NamespaceName, PlacementGroupId};
use serde::{Deserialize, Serialize};

pub use service::{
    CreateEntityRequest, EntityAddress, EntityBackend, EntityOptions, EntityPage, EntityProfile,
    EntityService, EntityTarget, GetEntityRequest, ListEntitiesRequest, ServiceError,
    UnsupportedFeature,
};

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
    pub profile: EntityProfile,
    /// Unavailable on the local broker, not an inferred placement.
    pub placement_group_id: Option<PlacementGroupId>,
    /// Entity capacity, not the profile's per-message limit. Unavailable locally.
    pub max_size_bytes: Option<u64>,
    /// Runtime usage is unavailable locally, including for an empty entity.
    pub used_logical_bytes: Option<u64>,
}
