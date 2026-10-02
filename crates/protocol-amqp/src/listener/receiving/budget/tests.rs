use std::collections::BTreeMap;

use domain::{
    AnnotationKey, DeadLetterInfo, DeadLetterReason, DeliveryLock, MessageBody, MessageDescriptor,
    MessageEnvelope, MessageIdentifier, MessageRecord, MessageState, MessageStatus, MessageValue,
    SequenceNumber, SessionId, Timestamp,
};

use super::*;

fn delivery() -> Delivery {
    Delivery {
        sequence: SequenceNumber::new(7),
        message_id: "message".to_owned(),
        body: vec![1, 2, 3],
        enqueued_at: Timestamp::from_millis(1_000),
        expires_at: Some(Timestamp::from_millis(11_000)),
        time_to_live_millis: Some(10_000),
        envelope: None,
        delivery_count: 2,
        status: MessageStatus::Active,
        scheduled_enqueue_time: Some(Timestamp::from_millis(900)),
        lock: Some(DeliveryLock {
            token: LockToken::new(11),
            locked_until: Timestamp::from_millis(21_000),
        }),
        session_id: None,
        dead_letter: None,
    }
}

fn record(delivery: &Delivery) -> MessageRecord {
    MessageRecord {
        sequence: delivery.sequence,
        message_id: delivery.message_id.clone(),
        body: delivery.body.clone(),
        enqueued_at: delivery.enqueued_at,
        expires_at: delivery.expires_at,
        envelope: delivery.envelope.clone(),
        delivery_count: delivery.delivery_count,
        state: MessageState::Ready,
        session_id: delivery.session_id.clone(),
        dead_letter: delivery.dead_letter.clone(),
        scheduled_enqueue_time: delivery.scheduled_enqueue_time,
    }
}

#[test]
fn content_cap_is_inclusive_and_failed_admission_preserves_existing_leases() {
    let budget = ContentBudget::default();
    let first = budget
        .try_acquire(MAX_RECEIVING_CONTENT_BYTES - 1)
        .expect("fits");
    let last = budget.try_acquire(1).expect("exact cap fits");
    assert!(budget.is_full());
    assert!(budget.try_acquire(1).is_none());
    assert!(budget.try_acquire(usize::MAX).is_none());
    assert_eq!(
        budget.0.load(Ordering::Acquire),
        MAX_RECEIVING_CONTENT_BYTES
    );
    drop(last);
    assert!(!budget.is_full());
    assert!(budget.try_acquire(2).is_none());
    drop(first);
    assert_eq!(budget.0.load(Ordering::Acquire), 0);
}

#[test]
fn cancelling_one_lease_does_not_refund_another_delivery() {
    let budget = ContentBudget::default();
    let first = budget.try_acquire(100).expect("fits");
    let second = budget.try_acquire(200).expect("fits");
    drop(second);
    assert_eq!(budget.0.load(Ordering::Acquire), 100);
    drop(first);
    assert_eq!(budget.0.load(Ordering::Acquire), 0);
}

#[test]
fn aggregate_pressure_defers_a_within_cap_delivery_until_existing_work_refunds() {
    let budget = ContentBudget::default();
    let held = budget
        .try_acquire(MAX_RECEIVING_CONTENT_BYTES - 100)
        .expect("fits");
    let candidate = projected_delivery_bytes(&delivery()).expect("individual delivery fits");
    assert!(candidate > 100);
    assert!(budget.try_acquire(candidate).is_none());
    assert_eq!(
        budget.0.load(Ordering::Acquire),
        MAX_RECEIVING_CONTENT_BYTES - 100
    );
    drop(held);
    let parked = budget
        .try_acquire(candidate)
        .expect("the parked delivery now fits");
    assert_eq!(budget.0.load(Ordering::Acquire), candidate);
    drop(parked);
    assert_eq!(budget.0.load(Ordering::Acquire), 0);
}

#[test]
fn unpolled_owned_work_refunds_its_content_lease() {
    let budget = ContentBudget::default();
    let lease = budget
        .try_acquire(MAX_RECEIVING_CONTENT_BYTES)
        .expect("fits");
    let future = async move {
        std::future::pending::<()>().await;
        drop(lease);
    };
    assert!(budget.is_full());
    drop(future);
    assert_eq!(budget.0.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn content_remains_charged_through_outcome_settlement_and_final_ack_waits() {
    let budget = ContentBudget::default();
    let lease = budget
        .try_acquire(MAX_RECEIVING_CONTENT_BYTES)
        .expect("fits");
    let (outcome, waiting_outcome) = tokio::sync::oneshot::channel::<()>();
    let (settlement, waiting_settlement) = tokio::sync::oneshot::channel::<()>();
    let (ack, waiting_ack) = tokio::sync::oneshot::channel::<()>();
    let future = async move {
        let _ = waiting_outcome.await;
        let _ = waiting_settlement.await;
        let _ = waiting_ack.await;
        drop(lease);
    };
    tokio::pin!(future);
    use futures_util::FutureExt;
    assert!(future.as_mut().now_or_never().is_none());
    outcome.send(()).expect("outcome receiver is retained");
    assert!(future.as_mut().now_or_never().is_none());
    assert!(budget.is_full());
    settlement
        .send(())
        .expect("settlement receiver is retained");
    assert!(future.as_mut().now_or_never().is_none());
    assert!(budget.is_full());
    ack.send(()).expect("ACK receiver is retained");
    future.await;
    assert_eq!(budget.0.load(Ordering::Acquire), 0);
}

#[test]
fn legacy_projection_refuses_one_byte_over_without_mutating_the_acquired_delivery() {
    let mut delivery = delivery();
    let overhead = delivery.delivery_size_upper_bound() as usize - delivery.body.len();
    delivery.body = vec![4; MAX_RECEIVING_CONTENT_BYTES - overhead];
    assert_eq!(
        projected_delivery_bytes(&delivery).expect("exact cap"),
        MAX_RECEIVING_CONTENT_BYTES
    );
    delivery.body.push(5);
    let before = delivery.body.as_ptr();
    let error = projected_delivery_bytes(&delivery).expect_err("one byte over is refused");
    let ReceiveExit::Refused(error) = error else {
        panic!("the refusal has a wire condition");
    };
    assert_eq!(
        error.condition,
        ErrorCondition::Amqp(AmqpError::ResourceLimitExceeded)
    );
    assert_eq!(error.description.as_deref(), Some(CONTENT_DESCRIPTION));
    assert_eq!(delivery.body.as_ptr(), before);
    assert_eq!(delivery.body.last(), Some(&5));
}

#[test]
fn record_and_delivery_share_the_projection_and_do_not_double_charge_compatibility_body() {
    let mut delivery = delivery();
    delivery.envelope = Some(Box::new(MessageEnvelope {
        body: MessageBody::Data(vec![vec![8, 9]]),
        ..MessageEnvelope::default()
    }));
    delivery.body = vec![0; MAX_RECEIVING_CONTENT_BYTES + 1];
    let projected = projected_delivery_bytes(&delivery).expect("typed content is authoritative");
    assert_eq!(
        record(&delivery).delivery_size_upper_bound(),
        projected as u64
    );
    assert!(projected < 1_024);
    let encoded = amqp::encode_message(&crate::write_delivery(&delivery)).expect("valid message");
    assert!(encoded.len() <= projected);
}

#[test]
fn projection_covers_typed_body_footer_session_and_canonical_broker_overlays() {
    let mut delivery = delivery();
    delivery.session_id = Some(SessionId::new("session").expect("valid session"));
    delivery.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::Application("reason".repeat(20)),
        description: "description".repeat(20),
        dead_lettered_at: Timestamp::from_millis(500),
    });
    let envelope = MessageEnvelope {
        properties: domain::MessageProperties {
            message_id: Some(MessageIdentifier::Uuid([3; 16])),
            reply_to_group_id: Some("reply-session".to_owned()),
            subject: Some("subject".to_owned()),
            ..domain::MessageProperties::default()
        },
        application_properties: BTreeMap::from([
            (
                "DeadLetterReason".to_owned(),
                MessageValue::String("old".repeat(40)),
            ),
            (
                "DeadLetterErrorDescription".to_owned(),
                MessageValue::String("old".to_owned()),
            ),
            ("audit".to_owned(), MessageValue::Binary(vec![0; 300])),
        ]),
        message_annotations: BTreeMap::from([
            (
                AnnotationKey::Symbol("x-opt-sequence-number".to_owned()),
                MessageValue::Long(99),
            ),
            (
                AnnotationKey::Symbol("custom".to_owned()),
                MessageValue::String("value".to_owned()),
            ),
        ]),
        footer: BTreeMap::from([(
            AnnotationKey::Symbol("footer".to_owned()),
            MessageValue::Described {
                descriptor: MessageDescriptor::Code(0x42),
                value: Box::new(MessageValue::Map(vec![(
                    MessageValue::String("key".to_owned()),
                    MessageValue::Array(vec![MessageValue::Long(1), MessageValue::Long(2)]),
                )])),
            },
        )]),
        body: MessageBody::Sequence(vec![vec![MessageValue::List(vec![
            MessageValue::Null,
            MessageValue::Binary(vec![5; 500]),
        ])]]),
        ..MessageEnvelope::default()
    };
    envelope.validate().expect("valid rich content");
    delivery.envelope = Some(Box::new(envelope));
    let before = delivery.clone();
    let projected = projected_delivery_bytes(&delivery).expect("within cap");
    assert_eq!(delivery, before);
    assert_eq!(
        record(&delivery).delivery_size_upper_bound(),
        projected as u64
    );
    let encoded = amqp::encode_message(&crate::write_delivery(&delivery)).expect("valid message");
    assert!(encoded.len() <= projected);
    let legacy = Delivery {
        envelope: None,
        ..delivery
    };
    let encoded =
        amqp::encode_message(&crate::write_delivery(&legacy)).expect("valid legacy message");
    assert!(encoded.len() as u64 <= legacy.delivery_size_upper_bound());
}
