//! Rule commands traverse the real native listener without AMQP or XML administration.

use std::{
    error::Error,
    io,
    process::{Output, Stdio},
    time::Duration,
};

use serde_json::{Value, json};
use storage::StateStore;
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::TcpListener,
    process::Command,
    time::timeout,
};

#[path = "native_rule_commands/crud.rs"]
mod crud;
#[path = "native_rule_commands/fixture.rs"]
mod fixture;
#[path = "native_rule_commands/input.rs"]
mod input;
#[path = "native_rule_commands/security.rs"]
mod security;

use fixture::{Node, run};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-rule-cli-test-secret";
const DEADLINE: Duration = Duration::from_secs(20);
const CHILD: &str = "Orders/subscriptions/Alpha";

fn failed(output: &Output) -> String {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(!stderr.contains(KEY));
    stderr
}

macro_rules! cases {
    ($($name:ident => $module:ident::$function:ident,)+) => {
        $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $name() -> TestResult {
            timeout(DEADLINE * 12, $module::$function()).await?
        })+
    };
}

cases! {
    typed_rule_commands_round_trip_complete_filters_and_clock_free_reads => crud::round_trip,
    rule_commands_inherit_verified_tls_and_exact_child_manage_scope => security::exact_scope,
    invalid_rule_files_and_arguments_do_not_open_a_connection => input::invalid,
    oversized_protobuf_rule_request_is_refused_before_connect => input::request_limit,
    sql_compilation_and_versions_remain_server_decisions => crud::server_statuses,
}
