//! Rule administration uses bounded owner reads and exact subscription fences.

use std::{
    collections::BTreeMap,
    error::Error,
    future::{Future, poll_fn},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use admin_api::v1::{
    CorrelationProperty, CorrelationRuleFilter, CreateRuleRequest, DeleteRuleRequest,
    FalseRuleFilter, GetRuleRequest, ListRulesRequest, Rule, RuleFilter, RuleNullValue,
    RuleScalarValue, SqlRuleFilter, TrueRuleFilter, rule_filter, rule_scalar_value,
    rule_service_server::RuleService,
};
use domain::{
    CommandKind, CorrelationFilter, DeleteEntityTarget, EntityPath, MessageValue, NamespaceName,
    RuleDefinition, RuleName, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, codec, keys,
};
use server::{Broker, Clock, LocalProposer, ManualClock, NativeAdminService};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
const PATH: &str = "Orders/subscriptions/Alpha";

#[path = "native_rule_admin/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "native_rule_admin/atomicity.rs"]
mod atomicity;
#[path = "native_rule_admin/authorization.rs"]
mod authorization;
#[path = "native_rule_admin/failures.rs"]
mod failures;
#[path = "native_rule_admin/lifecycle.rs"]
mod lifecycle;
#[path = "native_rule_admin/scalars.rs"]
mod scalars;

macro_rules! for_each_backend {
    ($($case:path => $name:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $name() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 24, $case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $name() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 24, $case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    super::lifecycle::crud_default_sorting_and_clock_free_reopen => crud_default_sorting_and_clock_free_reopen,
    super::lifecycle::literal_names_paths_and_empty_sets_are_isolated => literal_names_paths_and_empty_sets_are_isolated,
    super::scalars::all_scalar_constructors_round_trip_without_coercion => all_scalar_constructors_round_trip_without_coercion,
    super::failures::invalid_filters_and_scalar_widths_never_reach_owner => invalid_filters_and_scalar_widths_never_reach_owner,
    super::failures::rule_count_condition_and_encoded_byte_limits_are_atomic => rule_count_condition_and_encoded_byte_limits_are_atomic,
    super::authorization::manage_scope_precedes_store_and_sql_validation => manage_scope_precedes_store_and_sql_validation,
    super::atomicity::failed_mutations_retry_and_reopen_without_partial_rules => failed_mutations_retry_and_reopen_without_partial_rules,
    super::atomicity::corrupt_sets_refuse_without_partial_responses_or_repairs => corrupt_sets_refuse_without_partial_responses_or_repairs,
    super::atomicity::captured_subscription_generation_fences_inflight_rpcs => captured_subscription_generation_fences_inflight_rpcs,
}
