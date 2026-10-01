use super::*;

fn id() -> TransactionId {
    TransactionId::new([1, 2, 3]).expect("bounded id")
}

fn binary_id() -> Value {
    Value::Binary(id().into_binary())
}

fn named(name: &str, fields: Vec<Value>) -> Value {
    Value::Described(Box::new(Described {
        descriptor: Descriptor::Name(Symbol::from(name)),
        value: Value::List(fields),
    }))
}

fn wire_value(value: &Value) -> io::Result<Value> {
    let bytes = encode_value(value)?;
    let (decoded, consumed) = decode_value(&bytes)?;
    assert_eq!(consumed, bytes.len());
    Ok(decoded)
}

fn attach(target: TargetTerminus) -> Attach {
    Attach {
        name: "transaction-codec".into(),
        handle: 0,
        role: Role::Sender,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: None,
        target: Some(target),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: Some(0),
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn round_trip(performative: Performative) -> io::Result<()> {
    let frame = Frame::Amqp {
        channel: 7,
        performative: Some(performative),
        payload: vec![],
    };
    assert_eq!(decode_frame(&encode_frame(&frame)?)?, frame);
    Ok(())
}

#[test]
fn coordinator_is_a_distinct_target_on_attach() -> io::Result<()> {
    let coordinator = Coordinator {
        capabilities: Some(Array::from(vec![
            Symbol::from("amqp:local-transactions"),
            Symbol::from("amqp:multi-ssns-per-txn"),
        ])),
    };
    round_trip(Performative::Attach(Box::new(attach(coordinator.into()))))?;
    let ordinary = Target::new("orders");
    assert_eq!(
        target_terminus_to_value(&ordinary.clone().into())?,
        target_to_value(&ordinary)
    );
    round_trip(Performative::Attach(Box::new(attach(ordinary.into()))))
}

#[test]
fn scalar_and_array_coordinator_capabilities_are_supported() -> io::Result<()> {
    let symbol = Symbol::from("amqp:local-transactions");
    for capabilities in [
        Value::Symbol(symbol.clone()),
        Value::Array(Array::from(vec![Value::Symbol(symbol.clone())])),
    ] {
        let value = wire_value(&described(COORDINATOR, Value::List(vec![capabilities])))?;
        let coordinator = target_terminus_from_value(value)?;
        assert_eq!(
            coordinator
                .as_coordinator()
                .and_then(|value| value.capabilities.as_ref())
                .map(|values| values.as_slice()),
            Some([symbol.clone()].as_slice())
        );
    }
    Ok(())
}

#[test]
fn absent_and_empty_capabilities_encode_as_null() -> io::Result<()> {
    for capabilities in [None, Some(Array::from(Vec::<Symbol>::new()))] {
        let value = target_terminus_to_value(&Coordinator { capabilities }.into())?;
        assert_eq!(encode_value(&value)?, [0x00, 0x53, 0x30, 0x45]);
        assert_eq!(
            target_terminus_from_value(value)?,
            Coordinator::default().into()
        );
    }
    // An empty generic array loses its original constructor. This explicitly
    // pins the local normalization policy for both symbol and non-symbol tags.
    for constructor in [0xa3, 0x70] {
        let bytes = [
            0x00,
            0x53,
            0x30,
            0xc0,
            0x05,
            0x01,
            0xe0,
            0x02,
            0x00,
            constructor,
        ];
        let (value, used) = decode_value(&bytes)?;
        assert_eq!(used, bytes.len());
        assert_eq!(
            target_terminus_from_value(value)?,
            Coordinator::default().into()
        );
    }
    Ok(())
}

#[test]
fn non_symbol_capabilities_and_non_list_termini_are_refused() {
    for capabilities in [
        Value::String("amqp:local-transactions".into()),
        Value::List(vec![Value::Symbol(Symbol::from("amqp:local-transactions"))]),
        Value::Array(Array::from(vec![Value::Uint(1)])),
        Value::Array(Array::from(vec![
            Value::Symbol(Symbol::from("amqp:local-transactions")),
            Value::Uint(1),
        ])),
        Value::Symbol(Symbol::from("non-ASCII-\u{e9}")),
    ] {
        assert!(
            target_terminus_from_value(described(COORDINATOR, Value::List(vec![capabilities])))
                .is_err()
        );
    }
    assert!(target_terminus_from_value(described(COORDINATOR, Value::Null)).is_err());
    assert!(target_terminus_from_value(described(SOURCE, Value::List(vec![]))).is_err());
    assert!(
        target_terminus_to_value(
            &Coordinator {
                capabilities: Some(Array::from(vec![Symbol::from("\u{e9}")]))
            }
            .into()
        )
        .is_err()
    );
}

#[test]
fn capability_count_and_raw_byte_work_are_bounded_before_encoding() {
    let maximum = crate::value_codec::MAX_VALUE_ELEMENTS;
    let capabilities = Array::from(vec![Symbol::from(""); maximum]);
    let value = Coordinator {
        capabilities: Some(capabilities),
    };
    assert!(target_terminus_to_value(&value.into()).is_ok());
    let value = Coordinator {
        capabilities: Some(Array::from(vec![Symbol::from(""); maximum + 1])),
    };
    assert!(
        target_terminus_to_value(&value.into())
            .expect_err("too many capabilities")
            .to_string()
            .contains("value limit")
    );
    let maximum = crate::value_codec::MAX_EXPANDED_VALUE_BYTES;
    let symbol = Symbol::from("x".repeat(maximum));
    let value = Coordinator {
        capabilities: Some(Array::from(vec![symbol.clone()])),
    };
    assert!(target_terminus_to_value(&value.into()).is_ok());
    let value = Coordinator {
        capabilities: Some(Array::from(vec![symbol, Symbol::from("x")])),
    };
    assert!(
        target_terminus_to_value(&value.into())
            .expect_err("too many raw symbol bytes")
            .to_string()
            .contains("byte limit")
    );
    let value = described(
        COORDINATOR,
        Value::List(vec![Value::Array(Array::from(vec![
            Value::Symbol(
                Symbol::from("")
            );
            crate::value_codec::MAX_VALUE_ELEMENTS
                + 1
        ]))]),
    );
    assert!(target_terminus_from_value(value).is_err());
}

#[test]
fn declare_and_discharge_use_amqp_value_bodies() -> io::Result<()> {
    let commands = [
        TransactionCommand::Declare(Declare::default()),
        TransactionCommand::Discharge(Discharge {
            txn_id: id(),
            fail: None,
        }),
        TransactionCommand::Discharge(Discharge {
            txn_id: id(),
            fail: Some(false),
        }),
        TransactionCommand::Discharge(Discharge {
            txn_id: id(),
            fail: Some(true),
        }),
    ];
    for command in commands {
        let message = Message {
            body: Body::Value(Value::from(command.clone())),
            ..Message::default()
        };
        let Body::Value(decoded) = decode_message(&encode_message(&message)?)?.body else {
            panic!("one AMQP value section expected");
        };
        assert_eq!(TransactionCommand::try_from(decoded)?, command);
    }
    assert_eq!(
        encode_value(&Value::from(
            TransactionCommand::Declare(Declare::default())
        ))?,
        [0x00, 0x53, 0x31, 0x45]
    );
    Ok(())
}

#[test]
fn declared_and_transactional_states_round_trip_on_wire_fields() -> io::Result<()> {
    for state in [
        DeliveryState::Declared(Declared { txn_id: id() }),
        DeliveryState::Transactional(TransactionalState {
            txn_id: id(),
            outcome: None,
        }),
        DeliveryState::Transactional(TransactionalState {
            txn_id: id(),
            outcome: Some(Outcome::Accepted(Accepted)),
        }),
    ] {
        round_trip(Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(0),
            delivery_tag: Some(Binary::from(vec![1])),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: Some(state.clone()),
            resume: false,
            aborted: false,
            batchable: false,
        }))?;
        round_trip(Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 0,
            last: None,
            settled: false,
            state: Some(state.clone()),
            batchable: false,
        }))?;
        let mut request = attach(Target::new("orders").into());
        request.unsettled = Some(OrderedMap::from_iter([(
            Binary::from(vec![1]),
            Some(state),
        )]));
        round_trip(Performative::Attach(Box::new(request)))?;
    }
    Ok(())
}

#[test]
fn every_outcome_including_declared_is_structurally_valid_in_transactional_state() -> io::Result<()>
{
    for outcome in [
        Outcome::Accepted(Accepted),
        Outcome::Released(Released),
        Outcome::Rejected(Rejected {
            error: Some(Error::new(AmqpError::NotAllowed, "rejected", None)),
        }),
        Outcome::Modified(Modified {
            delivery_failed: Some(true),
            undeliverable_here: Some(false),
            message_annotations: None,
        }),
        Outcome::Declared(Declared { txn_id: id() }),
    ] {
        let state = DeliveryState::Transactional(TransactionalState {
            txn_id: id(),
            outcome: Some(outcome),
        });
        assert_eq!(
            delivery_state_from_value(wire_value(&delivery_state_to_value(&state)?)?)?,
            state
        );
    }
    Ok(())
}

#[test]
fn transactional_state_is_nonterminal_and_not_an_outcome() {
    let state = DeliveryState::Transactional(TransactionalState {
        txn_id: id(),
        outcome: Some(Outcome::Accepted(Accepted)),
    });
    assert!(!state.is_terminal());
    assert_eq!(Outcome::try_from(state.clone()), Err(state));
    let declared = Declared { txn_id: id() };
    let state = DeliveryState::Declared(declared.clone());
    assert!(state.is_terminal());
    assert_eq!(
        Outcome::try_from(state.clone()),
        Ok(Outcome::Declared(declared.clone()))
    );
    assert_eq!(DeliveryState::from(Outcome::Declared(declared)), state);
    assert!(
        !DeliveryState::Received {
            section_number: 0,
            section_offset: 0
        }
        .is_terminal()
    );
}

#[test]
fn received_and_nested_transactional_states_cannot_be_provisional_outcomes() {
    for outcome in [
        described(RECEIVED, Value::List(vec![Value::Uint(0), Value::Ulong(0)])),
        described(TRANSACTIONAL_STATE, Value::List(vec![binary_id()])),
        Value::Bool(true),
        described(ACCEPTED, Value::Null),
    ] {
        let value = described(TRANSACTIONAL_STATE, Value::List(vec![binary_id(), outcome]));
        assert!(delivery_state_from_value(value).is_err());
    }
}

#[test]
fn required_ids_reject_missing_null_wrong_types_and_oversize() {
    for fields in [
        vec![],
        vec![Value::Null],
        vec![Value::String("id".into())],
        vec![Value::Uint(1)],
        vec![Value::Binary(Binary::from(vec![1; 33]))],
    ] {
        for descriptor in [DISCHARGE, DECLARED, TRANSACTIONAL_STATE] {
            let value = described(descriptor, Value::List(fields.clone()));
            let result = if descriptor == DISCHARGE {
                TransactionCommand::try_from(value).map(|_| ())
            } else {
                delivery_state_from_value(value).map(|_| ())
            };
            assert_eq!(
                result.expect_err("invalid id").kind(),
                io::ErrorKind::InvalidData
            );
        }
    }
}

#[test]
fn oversized_borrowed_id_is_refused_with_its_exact_length() {
    let length = 1024 * 1024;
    let fields = vec![Value::Binary(Binary::from(vec![1; length]))];
    let error = declared_from_fields(&fields).expect_err("oversized borrowed id");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<TransactionIdError>()),
        Some(&TransactionIdError {
            size: length,
            maximum: MAX_TRANSACTION_ID_BYTES
        })
    );
    let value = described(DISCHARGE, Value::List(fields));
    let error = TransactionCommand::try_from(value).expect_err("oversized command id");
    assert_eq!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<TransactionIdError>())
            .map(|error| error.size),
        Some(length)
    );
}

#[test]
fn required_binary_ids_accept_zero_and_thirty_two_octets() -> io::Result<()> {
    for length in [0, 32] {
        let id = TransactionId::new(vec![7; length]).expect("valid id");
        let command = TransactionCommand::Discharge(Discharge {
            txn_id: id.clone(),
            fail: None,
        });
        assert_eq!(
            TransactionCommand::try_from(wire_value(&Value::from(command.clone()))?)?,
            command
        );
        let declared = DeliveryState::Declared(Declared { txn_id: id.clone() });
        assert_eq!(
            delivery_state_from_value(wire_value(&delivery_state_to_value(&declared)?)?)?,
            declared
        );
        let state = DeliveryState::Transactional(TransactionalState {
            txn_id: id,
            outcome: None,
        });
        assert_eq!(
            delivery_state_from_value(wire_value(&delivery_state_to_value(&state)?)?)?,
            state
        );
    }
    Ok(())
}

#[test]
fn discharge_fail_requires_boolean_and_command_body_requires_list() {
    for value in [
        Value::Int(0),
        Value::String("false".into()),
        Value::Binary(Binary::from(vec![])),
    ] {
        assert!(
            TransactionCommand::try_from(described(
                DISCHARGE,
                Value::List(vec![binary_id(), value])
            ))
            .is_err()
        );
    }
    for descriptor in [DECLARE, DISCHARGE] {
        assert!(TransactionCommand::try_from(described(descriptor, Value::Null)).is_err());
    }
    assert!(TransactionCommand::try_from(Value::Null).is_err());
    assert!(TransactionCommand::try_from(described(ACCEPTED, Value::List(vec![]))).is_err());
}

#[test]
fn all_five_symbolic_descriptor_aliases_are_supported() -> io::Result<()> {
    assert_eq!(
        target_terminus_from_value(wire_value(&named("amqp:coordinator:list", vec![]))?)?,
        Coordinator::default().into()
    );
    assert_eq!(
        TransactionCommand::try_from(wire_value(&named("amqp:declare:list", vec![]))?)?,
        TransactionCommand::Declare(Declare::default())
    );
    assert_eq!(
        TransactionCommand::try_from(wire_value(&named(
            "amqp:discharge:list",
            vec![binary_id(), Value::Bool(true)]
        ))?)?,
        TransactionCommand::Discharge(Discharge {
            txn_id: id(),
            fail: Some(true)
        })
    );
    assert_eq!(
        delivery_state_from_value(wire_value(&named("amqp:declared:list", vec![binary_id()]))?)?,
        DeliveryState::Declared(Declared { txn_id: id() })
    );
    let value = named(
        "amqp:transactional-state:list",
        vec![binary_id(), named("amqp:declared:list", vec![binary_id()])],
    );
    assert_eq!(
        delivery_state_from_value(wire_value(&value)?)?,
        DeliveryState::Transactional(TransactionalState {
            txn_id: id(),
            outcome: Some(Outcome::Declared(Declared { txn_id: id() }))
        })
    );
    Ok(())
}

#[test]
fn global_id_content_is_preserved_for_later_policy() -> io::Result<()> {
    let global_id = Value::Described(Box::new(Described {
        descriptor: Descriptor::Name(Symbol::from("example:global-id:list")),
        value: Value::List(vec![
            Value::Binary(Binary::from(vec![8, 9])),
            Value::String("global".into()),
        ]),
    }));
    let command = TransactionCommand::Declare(Declare {
        global_id: Some(global_id),
    });
    assert_eq!(
        TransactionCommand::try_from(wire_value(&Value::from(command.clone()))?)?,
        command
    );
    assert_eq!(
        TransactionCommand::try_from(described(DECLARE, Value::List(vec![Value::Null])))?,
        TransactionCommand::Declare(Declare::default())
    );
    Ok(())
}

#[test]
fn source_default_outcome_accepts_declared_but_not_transactional_state() -> io::Result<()> {
    let mut source = Source::new("orders");
    source.default_outcome = Some(DeliveryState::Declared(Declared { txn_id: id() }));
    assert_eq!(
        source_from_value(wire_value(&source_to_value(&source)?)?)?,
        source
    );
    source.default_outcome = Some(DeliveryState::Transactional(TransactionalState {
        txn_id: id(),
        outcome: Some(Outcome::Accepted(Accepted)),
    }));
    assert!(source_to_value(&source).is_err());
    let value = described(
        SOURCE,
        Value::List(vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            delivery_state_to_value(source.default_outcome.as_ref().expect("state"))?,
        ]),
    );
    assert!(source_from_value(value).is_err());
    Ok(())
}

#[test]
fn control_bodies_use_existing_message_allocation_and_depth_limits() -> io::Result<()> {
    let command = TransactionCommand::Declare(Declare {
        global_id: Some(Value::String("x".repeat(32))),
    });
    let message = Message {
        body: Body::Value(Value::from(command)),
        ..Message::default()
    };
    let bytes = encode_message(&message)?;
    assert_eq!(decode_message(&bytes)?, message);
    let mut budget = MessageDecodeBudget::new(16, 4)?;
    assert!(decode_message_with_budget(&bytes, &mut budget).is_err());
    let mut deep = Value::Null;
    for _ in 0..crate::value_codec::MAX_VALUE_NESTING {
        deep = Value::List(vec![deep]);
    }
    let message = Message {
        body: Body::Value(Value::from(TransactionCommand::Declare(Declare {
            global_id: Some(deep),
        }))),
        ..Message::default()
    };
    assert!(encode_message(&message).is_err());
    Ok(())
}
