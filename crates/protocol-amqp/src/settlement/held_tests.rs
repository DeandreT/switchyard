use super::*;

use amqp::{Accepted, Error, ErrorCondition, Modified, Rejected, Released, Symbol};
use domain::{SessionHold, SessionId};

fn hold() -> SessionHold {
    SessionHold::new(
        SessionId::new("original").expect("session"),
        LockToken::new(19),
    )
}

#[test]
fn held_mapping_keeps_each_legacy_disposition_and_property_update() {
    let mut fields = Fields::new();
    fields.insert(Symbol::from("status"), Value::String("updated".to_owned()));
    let modified = Modified {
        undeliverable_here: Some(true),
        message_annotations: Some(fields),
        ..Modified::default()
    };
    let mut info = Fields::new();
    info.insert(
        Symbol::from("DeadLetterReason"),
        Value::String("reason".to_owned()),
    );
    let error = Error {
        condition: ErrorCondition::Custom(Symbol::from(DEAD_LETTER_CONDITION)),
        description: Some("description".to_owned()),
        info: Some(info),
    };
    for outcome in [
        Outcome::Accepted(Accepted),
        Outcome::Released(Released),
        Outcome::Modified(modified),
        Outcome::Rejected(Rejected { error: Some(error) }),
    ] {
        let legacy = settlement_command(SequenceNumber::new(7), LockToken::new(9), outcome.clone())
            .expect("legacy mapping");
        let CommandKind::Settle {
            disposition,
            properties_to_modify,
            ..
        } = legacy
        else {
            panic!("legacy shape")
        };
        let held = held_settlement_command(
            SequenceNumber::new(7),
            LockToken::new(9),
            Some(hold()),
            outcome,
        )
        .expect("held mapping");
        assert_eq!(
            held,
            CommandKind::SettleHeld {
                sequence: SequenceNumber::new(7),
                lock_token: LockToken::new(9),
                session: Some(hold()),
                disposition,
                properties_to_modify,
            }
        );
    }
}

#[test]
fn held_mapping_keeps_none_explicit_without_widening_the_legacy_mapper() {
    let held = held_settlement_command(
        SequenceNumber::new(7),
        LockToken::new(9),
        None,
        Outcome::Accepted(Accepted),
    )
    .expect("held mapping");
    assert!(matches!(
        held,
        CommandKind::SettleHeld { session: None, .. }
    ));
    let legacy = settlement_command(
        SequenceNumber::new(7),
        LockToken::new(9),
        Outcome::Accepted(Accepted),
    )
    .expect("legacy mapping");
    assert!(matches!(legacy, CommandKind::Settle { .. }));
}

#[test]
fn held_mapping_retains_exact_invalid_field_refusal() {
    let outcome = Outcome::Declared(amqp::Declared {
        txn_id: amqp::TransactionId::new([1]).expect("transaction"),
    });
    assert_eq!(
        held_settlement_command(
            SequenceNumber::new(7),
            LockToken::new(9),
            Some(hold()),
            outcome.clone()
        ),
        settlement_command(SequenceNumber::new(7), LockToken::new(9), outcome)
    );
}
