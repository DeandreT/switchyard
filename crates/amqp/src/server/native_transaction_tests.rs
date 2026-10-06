use super::*;
use crate::{
    Coordinator, Declare, Discharge, Source, Target, TransactionCommand, TransactionId,
    TransactionalState, Value,
};

#[path = "native_transaction_tests/fixture.rs"]
mod fixture;

use fixture::*;

#[path = "native_transaction_tests/lifecycle.rs"]
mod lifecycle;

#[path = "native_transaction_tests/claims.rs"]
mod claims;

#[path = "native_transaction_tests/faults.rs"]
mod faults;

#[path = "native_transaction_tests/refusals.rs"]
mod refusals;

#[path = "native_transaction_tests/receiver_provenance.rs"]
mod receiver_provenance;

#[path = "native_transaction_tests/formats.rs"]
mod formats;

#[path = "native_transaction_tests/error_close.rs"]
mod error_close;

#[tokio::test]
async fn default_connection_refuses_coordinator_without_publishing_approval() {
    let mut fixture = Fixture::new(false).await;
    let mut session = fixture.session(CONTROL_CHANNEL).await;
    fixture
        .peer
        .attach(CONTROL_CHANNEL, coordinator_attach(CONTROL_HANDLE))
        .await;
    let end = fixture.peer.end(CONTROL_CHANNEL).await;
    assert_eq!(
        end.error
            .expect("explicit refusal")
            .condition
            .as_symbol()
            .as_str(),
        "amqp:not-implemented"
    );
    assert!(
        bounded(
            "no default coordinator approval",
            session.next_incoming_attach()
        )
        .await
        .is_none()
    );
    fixture
        .peer
        .send(
            CONTROL_CHANNEL,
            Performative::End(End::default()),
            Vec::new(),
        )
        .await;
    let mut healthy = fixture.session(HEALTHY_CHANNEL).await;
    fixture
        .peer
        .attach(HEALTHY_CHANNEL, ordinary_attach(POST_HANDLE))
        .await;
    let incoming = bounded(
        "ordinary approval on sibling",
        healthy.next_incoming_attach(),
    )
    .await
    .expect("ordinary request");
    assert_eq!(
        incoming
            .target
            .as_ref()
            .and_then(|target| target.as_target())
            .and_then(|target| target.address.as_deref()),
        Some("queue")
    );
    fixture.connection.shutdown().await;
}

#[path = "native_transaction_tests/correlation.rs"]
mod correlation;
