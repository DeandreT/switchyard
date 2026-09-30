use super::*;
use crate::value_codec::{MAX_EXPANDED_VALUE_BYTES, MAX_VALUE_ELEMENTS, MAX_VALUE_NESTING};

fn section(descriptor: u8, value: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x00, 0x53, descriptor];
    bytes.extend_from_slice(value);
    bytes
}

fn counted(code: u8, count: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![code];
    let size = u32::try_from(payload.len()).expect("bounded fixture") + 4;
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn null_array(count: usize) -> Vec<u8> {
    section(
        0x77,
        &counted(
            0xf0,
            u32::try_from(count).expect("bounded fixture"),
            &[0x40],
        ),
    )
}

fn data(length: usize) -> Vec<u8> {
    let mut binary = vec![0xb0];
    binary.extend_from_slice(
        &u32::try_from(length)
            .expect("bounded fixture")
            .to_be_bytes(),
    );
    binary.resize(binary.len() + length, 7);
    section(0x75, &binary)
}

fn named_null_array(name: &str, count: u32) -> Vec<u8> {
    let mut constructor = vec![0x00, 0xa3];
    constructor.push(u8::try_from(name.len()).expect("short descriptor"));
    constructor.extend_from_slice(name.as_bytes());
    constructor.push(0x40);
    section(0x77, &counted(0xf0, count, &constructor))
}

#[test]
fn public_budget_construction_never_enlarges_the_hard_limits() {
    let defaults = MessageDecodeBudget::default();
    assert_eq!(defaults.remaining_values(), MAX_VALUE_ELEMENTS);
    assert_eq!(defaults.remaining_copied_bytes(), MAX_EXPANDED_VALUE_BYTES);
    assert_eq!(
        MessageDecodeBudget::new(MAX_VALUE_ELEMENTS, MAX_EXPANDED_VALUE_BYTES)
            .expect("exact hard limits"),
        defaults
    );
    for (nodes, bytes) in [
        (MAX_VALUE_ELEMENTS + 1, 0),
        (0, MAX_EXPANDED_VALUE_BYTES + 1),
        (usize::MAX, usize::MAX),
    ] {
        assert_eq!(
            MessageDecodeBudget::new(nodes, bytes)
                .expect_err("hard limits cannot grow")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    let mut empty = MessageDecodeBudget::new(0, 0).expect("zero budget");
    assert_eq!(
        decode_message_with_budget(&[], &mut empty).expect("empty semantic message"),
        Message::default()
    );
    assert!(decode_message_with_budget(&section(0x77, &[0x40]), &mut empty).is_err());
    assert_eq!(empty.remaining_values(), 0);
    assert_eq!(empty.remaining_copied_bytes(), 0);
}

#[test]
fn independent_compact_arrays_share_the_hard_node_budget() {
    let count = MAX_VALUE_ELEMENTS / 2 - 2;
    let encoded = null_array(count);
    assert!(encoded.len() < 16, "large arrays have zero-width elements");
    let mut budget = MessageDecodeBudget::default();
    for remaining in [MAX_VALUE_ELEMENTS / 2, 0] {
        let message = decode_message_with_budget(&encoded, &mut budget).expect("fitting array");
        let Body::Value(Value::Array(values)) = message.body else {
            panic!("array message");
        };
        assert_eq!(values.len(), count);
        assert_eq!(budget.remaining_values(), remaining);
        assert_eq!(budget.remaining_copied_bytes(), MAX_EXPANDED_VALUE_BYTES);
    }
    let error = decode_message_with_budget(&section(0x77, &[0x40]), &mut budget)
        .expect_err("a later message cannot replenish exhausted nodes");
    assert!(error.to_string().contains("element limit"));

    let mut exact = MessageDecodeBudget::new(6, 0).expect("bounded budget");
    decode_message_with_budget(&null_array(2), &mut exact).expect("four nodes");
    decode_message_with_budget(&section(0x77, &[0x40]), &mut exact).expect("two nodes");
    assert_eq!(exact.remaining_values(), 0);
    assert!(decode_message_with_budget(&null_array(1), &mut exact).is_err());
}

#[test]
fn independent_binary_messages_share_the_hard_copied_byte_budget() {
    let encoded = data(MAX_EXPANDED_VALUE_BYTES / 2);
    let mut budget = MessageDecodeBudget::default();
    for remaining in [MAX_EXPANDED_VALUE_BYTES / 2, 0] {
        let message = decode_message_with_budget(&encoded, &mut budget).expect("fitting binary");
        let Body::Data(sections) = message.body else {
            panic!("data message");
        };
        assert_eq!(sections[0].len(), MAX_EXPANDED_VALUE_BYTES / 2);
        assert_eq!(budget.remaining_copied_bytes(), remaining);
    }
    assert_eq!(budget.remaining_values(), MAX_VALUE_ELEMENTS - 4);
    let error = decode_message_with_budget(&data(1), &mut budget)
        .expect_err("a later message cannot replenish copied bytes");
    assert!(error.to_string().contains("expanded value byte limit"));
    assert_eq!(budget.remaining_values(), MAX_VALUE_ELEMENTS - 6);
    assert_eq!(budget.remaining_copied_bytes(), 0);
}

#[test]
fn named_array_constructors_and_every_clone_share_the_budget() {
    let encoded = named_null_array("abcdefgh", 2);
    let mut budget = MessageDecodeBudget::new(12, 48).expect("two fitting arrays");
    for remaining in [24, 0] {
        let message =
            decode_message_with_budget(&encoded, &mut budget).expect("fitting descriptors");
        let Body::Value(Value::Array(values)) = message.body else {
            panic!("described array message");
        };
        assert_eq!(values.len(), 2);
        for value in values.iter() {
            let Value::Described(value) = value else {
                panic!("described element");
            };
            assert_eq!(value.descriptor, Descriptor::Name("abcdefgh".into()));
            assert_eq!(value.value, Value::Null);
        }
        assert_eq!(budget.remaining_copied_bytes(), remaining);
    }
    assert_eq!(budget.remaining_values(), 0);

    let mut short = MessageDecodeBudget::new(12, 47).expect("one byte short");
    decode_message_with_budget(&encoded, &mut short).expect("first array");
    let error = decode_message_with_budget(&encoded, &mut short)
        .expect_err("shared names cannot reset the byte budget");
    assert!(error.to_string().contains("expanded value byte limit"));
    assert_eq!(short.remaining_copied_bytes(), 15);
    assert_eq!(
        short.remaining_values(),
        4,
        "refusal precedes element clones"
    );
}

#[test]
fn scalar_copies_share_a_budget_across_types_and_messages() {
    let mut budget = MessageDecodeBudget::new(6, 5).expect("three scalars");
    let fixtures = [
        section(0x77, &[0xa1, 2, b'a', b'b']),
        section(0x75, &[0xa0, 2, 0, 255]),
        section(0x77, &[0xa3, 1, b'x']),
    ];
    for fixture in &fixtures {
        decode_message_with_budget(fixture, &mut budget).expect("fitting scalar copy");
    }
    assert_eq!(budget.remaining_values(), 0);
    assert_eq!(budget.remaining_copied_bytes(), 0);
}

#[test]
fn failure_preserves_previous_and_current_message_charges() {
    let mut budget = MessageDecodeBudget::new(10, 10).expect("bounded budget");
    decode_message_with_budget(&data(2), &mut budget).expect("first message");
    let malformed = section(0x77, &[0xa1, 3, b'a']);
    let error = decode_message_with_budget(&malformed, &mut budget)
        .expect_err("string length exceeds its input");
    assert!(error.to_string().contains("truncated"));
    assert_eq!(budget.remaining_values(), 6);
    assert_eq!(budget.remaining_copied_bytes(), 5);
    decode_message_with_budget(&data(5), &mut budget).expect("remaining bytes are still usable");
    assert_eq!(budget.remaining_values(), 4);
    assert_eq!(budget.remaining_copied_bytes(), 0);
    assert!(decode_message_with_budget(&data(1), &mut budget).is_err());
    assert_eq!(budget.remaining_values(), 2);
    assert_eq!(budget.remaining_copied_bytes(), 0);

    let mut budget = MessageDecodeBudget::new(10, 10).expect("bounded budget");
    let repeated = [data(2), section(0x73, &[0x45])].concat();
    assert!(decode_message_with_budget(&repeated, &mut budget).is_err());
    assert_eq!(
        budget.remaining_values(),
        6,
        "out-of-order sections still consumed parser nodes"
    );
    assert_eq!(budget.remaining_copied_bytes(), 8);
}

#[test]
fn ordinary_wrapper_retains_fresh_hard_caps_and_exact_boundaries() {
    let exact_nodes = null_array(MAX_VALUE_ELEMENTS - 2);
    for _ in 0..2 {
        assert!(
            decode_message(&exact_nodes).is_ok(),
            "ordinary calls use fresh budgets"
        );
    }
    assert!(decode_message(&null_array(MAX_VALUE_ELEMENTS - 1)).is_err());
    let exact_bytes = data(MAX_EXPANDED_VALUE_BYTES);
    for _ in 0..2 {
        assert!(decode_message(&exact_bytes).is_ok());
    }
    assert!(decode_message(&data(MAX_EXPANDED_VALUE_BYTES + 1)).is_err());
}

#[test]
fn depth_limits_apply_per_value_without_resetting_cumulative_nodes() {
    let mut value = vec![0x40];
    for _ in 0..MAX_VALUE_NESTING - 1 {
        value = counted(0xd0, 1, &value);
    }
    let exact = section(0x77, &value);
    let mut budget = MessageDecodeBudget::new(512, 0).expect("bounded budget");
    decode_message_with_budget(&exact, &mut budget).expect("exact nesting boundary");
    let after_first = budget.remaining_values();
    decode_message_with_budget(&exact, &mut budget)
        .expect("depth is not accumulated across messages");
    assert_eq!(budget.remaining_values(), 2 * after_first - 512);
    let too_deep = section(0x77, &counted(0xd0, 1, &value));
    let error = decode_message_with_budget(&too_deep, &mut budget)
        .expect_err("one more nesting level is refused");
    assert!(error.to_string().contains("nesting limit"));
    assert!(budget.remaining_values() < 2 * after_first - 512);
    decode_message_with_budget(&section(0x77, &[0x40]), &mut budget)
        .expect("another message keeps its own depth while sharing residual nodes");
}
