//! Rule administration traverses verified TLS and shares the native HTTP/2 edge.

use std::{error::Error, time::Duration};

use admin_api::v1::{
    CorrelationProperty, CorrelationRuleFilter, CreateEntityRequest, CreateRuleRequest,
    CreateRuleWithActionRequest, DeleteRuleRequest, EntityKind, FalseRuleFilter, GetEntityRequest,
    GetRuleRequest, ListRulesRequest, Rule, RuleFilter, RuleNullValue, RuleScalarValue,
    SqlRuleAction, SqlRuleFilter, TrueRuleFilter, entity_service_client::EntityServiceClient,
    rule_filter::Filter, rule_scalar_value::Value as Scalar,
    rule_service_client::RuleServiceClient,
};
use domain::NamespaceName;
use prost::Message as ProstMessage;
use storage::StateStore;
use testkit::StoreProvider;
use tokio::time::timeout;
use tonic::{Code, Request};

#[path = "native_rule_admin_transport/action_routing.rs"]
mod action_routing;
#[path = "native_rule_admin_transport/action_security.rs"]
mod action_security;
#[path = "native_rule_admin_transport/actions.rs"]
mod actions;
#[path = "native_rule_admin_transport/crud.rs"]
mod crud;
#[path = "native_rule_admin_transport/fixture.rs"]
mod fixture;
#[path = "native_rule_admin_transport/routing.rs"]
mod routing;
#[path = "native_rule_admin_transport/security.rs"]
mod security;

use fixture::{Node, create, delete, get, list, request, sas};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-rule-transport-secret";
const DEADLINE: Duration = Duration::from_secs(8);
const CHILD: &str = "Orders/subscriptions/Alpha";

macro_rules! for_each_backend {
    ($($case:ident => $module:ident::$function:ident,)+) => {
        mod memory {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 10,
                    super::$module::$function(::testkit::MemoryProvider::new())).await?
            })+
        }
        mod durable {
            $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 10,
                    super::$module::$function(::testkit::DurableProvider::temporary()?)).await?
            })+
        }
    };
}

for_each_backend! {
    typed_rules_round_trip_over_verified_tls => crud::round_trip,
    rule_requests_require_exact_manage_scope => security::exact_scope,
    oversized_requests_and_bounded_responses_leave_services_healthy => security::limits,
    native_rules_drive_ordinary_amqp_fanout_and_survive_reopen => routing::round_trip,
    action_creation_and_opt_in_reads_preserve_exact_metadata => actions::round_trip,
    action_requests_reject_invalid_input_before_mutation => actions::refusals,
    action_requests_require_exact_manage_scope => action_security::exact_scope,
    native_actions_drive_three_independent_amqp_copies_and_survive_reopen => action_routing::round_trip,
}
