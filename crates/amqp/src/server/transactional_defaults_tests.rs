use super::*;
use crate::{
    Body, Coordinator, Declare, Source, Target, TargetTerminus, TransactionCommand, TransactionId,
};

mod actor;
mod coordinator;
mod fixture;
mod negotiation;

use fixture::{CHANNEL, Fixture, HANDLE, Policy, bounded, coordinator_request, request};
