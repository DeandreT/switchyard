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

/// Odd map count: one key with no value.
const ODD_MAP8: [u8; 5] = [0xc1, 0x03, 0x01, 0x54, 0x01];
const ODD_MAP32: [u8; 11] = [0xd1, 0, 0, 0, 6, 0, 0, 0, 1, 0x54, 0x01];

fn assert_refused(name: &str, bytes: &[u8]) -> String {
    let error = encoded_value_len(bytes, ValueChecks::Received).expect_err(name);
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}");
    error.to_string()
}

fn assert_accepted(name: &str, bytes: &[u8]) {
    assert_eq!(
        encoded_value_len(bytes, ValueChecks::Received).expect(name),
        bytes.len(),
        "{name}"
    );
    serde_amqp::from_slice::<Value>(bytes).expect(name);
}

fn list8(values: &[&[u8]]) -> Vec<u8> {
    let body = values.concat();
    let mut bytes = vec![0xc0, body.len() as u8 + 1, values.len() as u8];
    bytes.extend(body);
    bytes
}

/// `depth` list32 values, each holding the next, around an empty list.
fn nested_lists(depth: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + 9 * depth);
    for level in 1..=depth {
        let inner = 1 + 9 * (depth - level);
        bytes.push(0xd0);
        bytes.extend_from_slice(&(inner as u32 + 4).to_be_bytes());
        bytes.extend_from_slice(&1_u32.to_be_bytes());
    }
    bytes.push(0x45);
    bytes
}

#[test]
fn odd_map_counts_are_refused_wherever_they_nest() {
    let in_map = [&[0xc1, 0x09, 0x02, 0xa1, 0x01, b'k'][..], &ODD_MAP8].concat();
    for (name, bytes) in [
        ("map8", ODD_MAP8.to_vec()),
        ("map32", ODD_MAP32.to_vec()),
        ("in list", list8(&[&ODD_MAP8, &[0x54, 0x07]])),
        ("in map", in_map),
        ("described", [&[0x00, 0x53, 0x77][..], &ODD_MAP32].concat()),
        ("count without entries", vec![0xc1, 0x01, 0x01]),
    ] {
        assert!(
            assert_refused(name, &bytes).contains("odd element count"),
            "{name}"
        );
    }
}

#[test]
fn odd_map_sections_and_performatives_are_decode_errors() {
    for section in [0x71, 0x72, 0x74, 0x78] {
        for map in [&ODD_MAP8[..], &ODD_MAP32] {
            let encoded = [&[0x00, 0x53, section][..], map].concat();
            let error = decode_message(&encoded).expect_err("odd map section is refused");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }
    // The issue's application-properties section after a valid data section.
    let mut encoded = encode_message(&Message::data(b"body".to_vec())).expect("message encodes");
    encoded.splice(0..0, [0x00, 0x53, 0x74, 0xc1, 0x03, 0x01, 0x54, 0x01]);
    assert!(decode_message(&encoded).is_err());

    // open with properties as its only field after the required container id.
    let mut fields: Vec<&[u8]> = vec![&[0xa1, 0x01, b'c']];
    fields.extend([&[0x40][..]; 8]);
    fields.push(&ODD_MAP8);
    let open = list8(&fields);
    let body = [&[0x00, 0x53, 0x10][..], &open].concat();
    let mut frame = ((8 + body.len()) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&[2, 0, 0, 0]);
    frame.extend(body);
    let error = decode_frame(&frame).expect_err("odd map performative is refused");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn arrays_of_compound_or_described_elements_are_refused() {
    for (name, bytes) in [
        (
            "maps",
            vec![0xe0, 0x08, 0x01, 0xc1, 0x05, 0x02, 0x54, 0x01, 0x54, 0x02],
        ),
        (
            "map32s",
            vec![
                0xe0, 0x0e, 0x01, 0xd1, 0, 0, 0, 8, 0, 0, 0, 2, 0x54, 0x01, 0x54, 0x02,
            ],
        ),
        (
            "lists",
            vec![
                0xe0, 0x0a, 0x02, 0xc0, 0x03, 0x01, 0x50, 0x01, 0x03, 0x01, 0x50, 0x02,
            ],
        ),
        (
            "list32s",
            vec![0xe0, 0x0c, 0x01, 0xd0, 0, 0, 0, 6, 0, 0, 0, 1, 0x50, 0x01],
        ),
        (
            "arrays",
            vec![
                0xe0, 0x0a, 0x02, 0xe0, 0x03, 0x01, 0x50, 0x01, 0x03, 0x01, 0x50, 0x02,
            ],
        ),
        (
            "array32s",
            vec![0xe0, 0x0c, 0x01, 0xf0, 0, 0, 0, 6, 0, 0, 0, 1, 0x50, 0x01],
        ),
        (
            "described",
            vec![
                0xe0, 0x09, 0x02, 0x00, 0x53, 0x10, 0xa1, 0x01, b'a', 0x01, b'b',
            ],
        ),
    ] {
        assert!(
            assert_refused(name, &bytes).contains("arrays of compound"),
            "{name}"
        );
    }
}

#[test]
fn compound_sizes_must_hold_their_counts_and_contents() {
    for (name, bytes) in [
        ("list8 without count", vec![0xc0, 0x00, 0x00]),
        ("map8 without count", vec![0xc1, 0x00, 0x00]),
        ("list32 without count", vec![0xd0, 0, 0, 0, 0, 0, 0, 0, 0]),
        ("array8 without count", vec![0xe0, 0x00, 0x01]),
        ("array8 without constructor", vec![0xe0, 0x01, 0x01, 0x50]),
        (
            "list8 count beyond size",
            vec![0xc0, 0x02, 0x02, 0x50, 0x01],
        ),
        (
            "array8 short elements",
            vec![0xe0, 0x04, 0x03, 0x50, 0x01, 0x02],
        ),
        ("truncated", vec![0xb1, 0, 0, 0, 9, b'x']),
        ("unknown code", vec![0xff]),
        ("empty", Vec::new()),
    ] {
        assert_refused(name, &bytes);
    }
    for (name, bytes) in [
        ("list8 padding", vec![0xc0, 0x04, 0x01, 0x50, 0x01, 0x40]),
        ("array8 padding", vec![0xe0, 0x04, 0x01, 0x50, 0x01, 0x40]),
    ] {
        assert!(
            assert_refused(name, &bytes).contains("does not match"),
            "{name}"
        );
    }
    let empty_array_with_constructor = [0xe0, 0x02, 0x00, 0xa3];
    assert!(
        assert_refused(
            "empty array constructor before a value",
            &list8(&[&empty_array_with_constructor, &[0x52, 0x05]]),
        )
        .contains("must be the last value")
    );
}

#[test]
fn shapes_serde_amqp_decodes_are_still_accepted() {
    let symbols = Value::Array(Array::from(vec![
        Value::Symbol(Symbol::from("com.microsoft:session-filter")),
        Value::Symbol(Symbol::from("x")),
    ]));
    let mut map = OrderedMap::new();
    map.insert(
        Value::Symbol(Symbol::from("k")),
        Value::List(vec![Value::Int(1)]),
    );
    map.insert(Value::Int(2), Value::Map(OrderedMap::new()));
    for (name, bytes) in [
        ("symbol array", serde_amqp::to_vec(&symbols).unwrap()),
        ("nested map", serde_amqp::to_vec(&Value::Map(map)).unwrap()),
        (
            "uint array",
            vec![0xe0, 0x0a, 0x02, 0x70, 0, 0, 0, 1, 0, 0, 0, 2],
        ),
        (
            "str8 array",
            vec![0xe0, 0x06, 0x02, 0xa1, 0x01, b'a', 0x01, b'b'],
        ),
        ("list0 array", vec![0xe0, 0x02, 0x02, 0x45]),
        ("empty array", vec![0xe0, 0x01, 0x00]),
        ("empty array32", vec![0xf0, 0, 0, 0, 4, 0, 0, 0, 0]),
        (
            "trailing empty array constructor",
            vec![0xe0, 0x02, 0x00, 0xa3],
        ),
        (
            "last empty array constructor in a list",
            list8(&[&[0x52, 0x05], &[0xe0, 0x02, 0x00, 0xa3]]),
        ),
        (
            "list in list",
            list8(&[&list8(&[&[0x50, 0x01]]), &[0x50, 0x02]]),
        ),
        (
            "uuid array",
            [&[0xe0, 0x22, 0x02, 0x98][..], &[0x11; 16], &[0x22; 16]].concat(),
        ),
        (
            "long array",
            [
                &[0xe0, 0x12, 0x02, 0x81][..],
                &[0, 0, 0, 0, 0, 0, 0, 1],
                &[0xff; 8],
            ]
            .concat(),
        ),
        (
            "timestamp array",
            [
                &[0xe0, 0x0a, 0x01, 0x83][..],
                &1_700_000_000_000_i64.to_be_bytes(),
            ]
            .concat(),
        ),
        (
            "sym32 array",
            vec![0xe0, 0x0c, 0x02, 0xb3, 0, 0, 0, 1, b'a', 0, 0, 0, 1, b'b'],
        ),
        ("empty list8", vec![0xc0, 0x01, 0x00]),
        ("empty map8", vec![0xc1, 0x01, 0x00]),
        ("nested limit", nested_lists(MAX_NESTED_VALUES)),
    ] {
        assert_accepted(name, &bytes);
    }
}

#[test]
fn deep_nesting_is_refused_without_recursion() {
    for (name, bytes) in [
        ("over limit", nested_lists(MAX_NESTED_VALUES + 1)),
        ("frame-sized", nested_lists(400_000)),
        ("described chain", vec![0x00; 1 << 20]),
    ] {
        for checks in [ValueChecks::Received, ValueChecks::Stored] {
            let error = encoded_value_len(&bytes, checks).expect_err(name);
            assert!(error.to_string().contains("nests too deeply"), "{name}");
        }
    }
}

/// A properties section whose list8 counts one message id but declares one
/// padding byte more, as the earlier length-only measurement accepted.
const PADDED_PROPERTIES: [u8; 10] = [0x00, 0x53, 0x73, 0xc0, 0x05, 0x01, 0xa1, 0x01, b'x', 0x40];

#[test]
fn stored_bytes_skip_only_the_exactness_rules() {
    for (name, bytes) in [
        ("padded section", PADDED_PROPERTIES.to_vec()),
        (
            "padded nested list",
            list8(&[&[0xc0, 0x03, 0x01, 0x50, 0x01, 0x40]]),
        ),
        (
            "empty array constructor mid-list",
            list8(&[&[0xe0, 0x02, 0x00, 0xa3], &[0x52, 0x05]]),
        ),
        (
            "array of one list",
            vec![0xe0, 0x06, 0x01, 0xc0, 0x03, 0x01, 0x50, 0x01],
        ),
    ] {
        assert_refused(name, &bytes);
        assert_eq!(
            encoded_value_len(&bytes, ValueChecks::Stored).expect(name),
            bytes.len(),
            "{name}"
        );
    }
    for (name, bytes) in [
        ("odd map", ODD_MAP8.to_vec()),
        (
            "array of maps",
            vec![0xe0, 0x08, 0x01, 0xc1, 0x05, 0x02, 0x54, 0x01, 0x54, 0x02],
        ),
        ("list8 without count", vec![0xc0, 0x00, 0x00]),
        ("array8 without constructor", vec![0xe0, 0x01, 0x01, 0x50]),
        (
            "contents beyond size",
            vec![0xc0, 0x02, 0x02, 0x50, 0x01, 0x50, 0x02],
        ),
    ] {
        let error = encoded_value_len(&bytes, ValueChecks::Stored).expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}");
    }
}

#[test]
fn a_padded_stored_message_decodes_but_is_refused_on_receipt() {
    let mut encoded = PADDED_PROPERTIES.to_vec();
    encoded.extend(encode_message(&Message::data(b"body".to_vec())).expect("body encodes"));
    assert!(decode_message(&encoded).is_err());
    let message = decode_stored_message(&encoded).expect("stored message decodes");
    assert_eq!(
        message
            .properties
            .and_then(|properties| properties.message_id),
        Some(MessageId::String(String::from("x")))
    );
}

#[test]
fn a_standalone_value_must_be_complete_and_structurally_sound() {
    let encoded = serde_amqp::to_vec(&Value::String(String::from("kept"))).expect("value encodes");
    for decode in [decode_value, decode_stored_value] {
        assert_eq!(
            decode(&encoded).expect("one complete value decodes"),
            Value::String(String::from("kept"))
        );
        assert_eq!(decode(&[0x40]).expect("null decodes"), Value::Null);
        let mut trailing = encoded.clone();
        trailing.push(0x40);
        assert!(decode(&trailing).is_err(), "trailing bytes are refused");
        assert!(decode(&ODD_MAP8).is_err(), "an odd map is refused");
        assert!(decode(&[]).is_err(), "no value is refused");
    }
    // A padded list is a stored shape only.
    let padded = [0xc0, 0x04, 0x01, 0x54, 0x01, 0x40];
    assert!(decode_value(&padded).is_err());
    assert_eq!(
        decode_stored_value(&padded).expect("a padded stored value decodes"),
        Value::List(vec![Value::Int(1)])
    );
}
