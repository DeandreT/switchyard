use super::*;

fn round_trip(performative: Performative) {
    let frame = Frame::Amqp {
        channel: 3,
        performative: Some(performative),
        payload: b"payload".to_vec(),
    };
    let encoded = encode_frame(&frame).expect("frame encodes");
    assert_eq!(decode_frame(&encoded).expect("frame decodes"), frame);
}

#[test]
fn transport_performatives_round_trip() {
    round_trip(Performative::Open(Open::new("container")));
    round_trip(Performative::Begin(Begin::default()));
    round_trip(Performative::Attach(Box::new(Attach {
        name: String::from("orders"),
        handle: 7,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Unsettled,
        rcv_settle_mode: ReceiverSettleMode::Second,
        source: Some(Source::new("orders")),
        target: Some(Target::new("client")),
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: Some(262_144),
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    })));
    round_trip(Performative::Transfer(Transfer {
        handle: 7,
        delivery_id: Some(11),
        delivery_tag: Some(Binary::from(vec![9; 16])),
        message_format: Some(0),
        settled: Some(false),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }));
    round_trip(Performative::Disposition(Disposition {
        role: Role::Receiver,
        first: 11,
        last: None,
        settled: false,
        state: Some(DeliveryState::Accepted(Accepted)),
        batchable: false,
    }));
}

#[test]
fn sasl_performatives_round_trip() {
    let frame = Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
        mechanisms: vec![Symbol::from("ANONYMOUS"), Symbol::from("PLAIN")],
    }));
    let encoded = encode_frame(&frame).expect("frame encodes");
    assert_eq!(decode_frame(&encoded).expect("frame decodes"), frame);
}
