//! Explicit finite-queue RPCs exercise one serialized owner on both stores.

use std::{error::Error, future::Future, pin::Pin, time::Duration};

use admin_api::v1::{
    CreateEntityRequest, CreateFiniteQueueRequest, EntityKind, FiniteQueue, GetEntityRequest,
    GetFiniteQueueRequest, QueueConfiguration, SetFiniteQueueDefinitionRequest,
    UnlimitedTimeToLive, entity_service_server::EntityService,
    finite_queue_service_server::FiniteQueueService, queue_configuration::DefaultTimeToLive,
};
use domain::{
    CommandKind, CommandOutcome, DeleteEntityTarget, EntityPath, NamespaceName, QueueConfig,
    StateMachine, Timestamp, TopicConfig, codec, keys,
};
use server::{Broker, Clock, LocalProposer, ManualClock, NativeAdminService};
use storage::{Mutation, StateStore, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
const TEST_DEADLINE: Duration = Duration::from_secs(60);

#[path = "native_finite_queue_admin/cases.rs"]
mod cases;
#[path = "native_finite_queue_admin/fixture.rs"]
mod fixture;
#[path = "native_finite_queue_admin/transport.rs"]
mod transport;

macro_rules! paired {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            macro_rules! case {
                ($name:ident, $owner:path) => {
                    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                    async fn $name() -> TestResult {
                        $owner($provider).await
                    }
                };
            }
            case!(
                full_definition_responses_and_clock_free_get,
                cases::full_definition_responses_and_clock_free_get
            );
            case!(
                strict_presence_and_positive_identity_refuse_before_owner,
                cases::strict_presence_and_positive_identity_refuse_before_owner
            );
            case!(
                noop_and_exact_config_limit_batches_have_no_postcommit_read,
                cases::noop_and_exact_config_limit_batches_have_no_postcommit_read
            );
            case!(
                immutable_numeric_capacity_priority_rolls_back_both_settings,
                cases::immutable_numeric_capacity_priority_rolls_back_both_settings
            );
            case!(
                explicit_incarnation_refuses_stale_before_host_clock,
                cases::explicit_incarnation_refuses_stale_before_host_clock
            );
            case!(
                future_size_and_ttl_preserve_retained_records_and_usage,
                cases::future_size_and_ttl_preserve_retained_records_and_usage
            );
            case!(
                unsupported_and_corrupt_profiles_are_never_repaired,
                cases::unsupported_and_corrupt_profiles_are_never_repaired
            );
            case!(
                preapply_failure_reopen_and_retry_are_atomic,
                cases::preapply_failure_reopen_and_retry_are_atomic
            );
            case!(
                authorization_precedes_conversion_and_owner_work,
                cases::authorization_precedes_conversion_and_owner_work
            );
            case!(
                legacy_entity_service_contract_remains_unchanged,
                cases::legacy_entity_service_contract_remains_unchanged
            );
            case!(
                stopped_owner_is_unavailable_without_effects,
                cases::stopped_owner_is_unavailable_without_effects
            );
            case!(
                legacy_and_finite_clones_share_nonblocking_admission,
                cases::legacy_and_finite_clones_share_nonblocking_admission
            );
            case!(
                private_ca_name_checked_finite_rpc_roundtrip_and_tls_refusals,
                transport::private_ca_name_checked_finite_rpc_roundtrip_and_tls_refusals
            );
            case!(
                actual_tls_sas_scope_denials_preserve_owner_and_health,
                transport::actual_tls_sas_scope_denials_preserve_owner_and_health
            );
            case!(
                actual_tls_offline_jwt_manage_and_denials_keep_sas_independent,
                transport::actual_tls_offline_jwt_manage_and_denials_keep_sas_independent
            );
            case!(
                old_service_registration_has_no_finite_method_fallback,
                transport::old_service_registration_has_no_finite_method_fallback
            );
        }
    };
}

paired!(memory, testkit::MemoryProvider::new());
paired!(durable, testkit::DurableProvider::temporary()?);
