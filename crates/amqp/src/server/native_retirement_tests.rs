use super::*;
use crate::{
    Body, Coordinator, Declare, Discharge, Source, Target, TransactionCommand, TransactionId,
    TransactionalState,
};
use serde_amqp::{Value, primitives::Array};

#[path = "native_retirement_tests/authority.rs"]
mod authority;
#[path = "native_retirement_tests/cancellation.rs"]
mod cancellation;
#[path = "native_retirement_tests/fixture.rs"]
mod fixture;
#[path = "native_retirement_tests/flush_gate.rs"]
mod flush_gate;
#[path = "native_retirement_tests/lifecycle.rs"]
mod lifecycle;
#[path = "native_retirement_tests/lifetime.rs"]
mod lifetime;
#[path = "native_retirement_tests/ordering.rs"]
mod ordering;
#[path = "native_retirement_tests/refusals.rs"]
mod refusals;

use fixture::*;
