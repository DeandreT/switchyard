use message_content::{
    Array, Body, Constructor, Content, Descriptor, EncodedContent, Error, Limits, MAX_DEPTH,
    PrimitiveConstructor as C, Text, Value, Width, decode_content, decode_value, encode_content,
    encode_value, validate_value,
};

fn limits() -> Limits {
    Limits {
        max_bytes: 100_000,
        max_depth: 128,
        max_nodes: 10_000,
        max_work: 200_000,
    }
}

#[test]
fn exact_and_one_over_decode_caps_for_bytes_depth_nodes_and_work() {
    // Five bytes + three nodes = eight work units; root plus one child level.
    let wire = [0xc0, 3, 2, 0x40, 0x41];
    let exact = Limits {
        max_bytes: 5,
        max_depth: 2,
        max_nodes: 3,
        max_work: 8,
    };
    assert!(decode_value(&wire, exact).is_ok());
    for (limited, error) in [
        (
            Limits {
                max_bytes: 4,
                ..exact
            },
            Error::ByteLimit,
        ),
        (
            Limits {
                max_depth: 1,
                ..exact
            },
            Error::DepthLimit,
        ),
        (
            Limits {
                max_nodes: 2,
                ..exact
            },
            Error::NodeLimit,
        ),
        (
            Limits {
                max_work: 7,
                ..exact
            },
            Error::WorkLimit,
        ),
    ] {
        assert_eq!(decode_value(&wire, limited), Err(error));
    }
    let null = Limits {
        max_bytes: 1,
        max_depth: 1,
        max_nodes: 1,
        max_work: 2,
    };
    assert_eq!(decode_value(&[0x40], null), Ok(Value::Null));
    for (limited, error) in [
        (
            Limits {
                max_depth: 0,
                ..null
            },
            Error::DepthLimit,
        ),
        (
            Limits {
                max_nodes: 0,
                ..null
            },
            Error::NodeLimit,
        ),
        (
            Limits {
                max_work: 1,
                ..null
            },
            Error::WorkLimit,
        ),
    ] {
        assert_eq!(decode_value(&[0x40], limited), Err(error));
    }
}

#[test]
fn empty_content_needs_no_nodes_and_standalone_value_always_needs_one() {
    let zero = Limits {
        max_bytes: 0,
        max_depth: 0,
        max_nodes: 0,
        max_work: 0,
    };
    assert_eq!(decode_content(&[], zero), Ok(Content::default()));
    assert_eq!(encode_content(&Content::default(), zero), Ok(vec![]));
    assert_eq!(decode_value(&[], zero), Err(Error::DepthLimit));
    assert_eq!(encode_value(&Value::Null, zero), Err(Error::DepthLimit));
}

#[test]
fn every_unknown_format_code_is_rejected_even_as_an_empty_array_constructor() {
    // 0x40 is an empty-only compatibility sentinel, not an element constructor.
    let allowed = [
        0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x60, 0x61,
        0x70, 0x71, 0x72, 0x73, 0x74, 0x80, 0x81, 0x82, 0x83, 0x84, 0x94, 0x98, 0xa0, 0xa1, 0xa3,
        0xb0, 0xb1, 0xb3, 0xc0, 0xc1, 0xd0, 0xd1, 0xe0, 0xf0,
    ];
    for code in 1..=255_u8 {
        let result = decode_value(&[0xe0, 2, 0, code], limits());
        if allowed.contains(&code) {
            assert!(result.is_ok(), "constructor {code:02x}");
        } else {
            assert_eq!(
                result,
                Err(Error::InvalidConstructor),
                "constructor {code:02x}"
            );
        }
    }
}

#[test]
fn exact_and_one_over_encoder_and_validation_caps() {
    let value = Value::List(vec![Value::Null, Value::Null]);
    // list32 header 9 + 2 null bytes, with 3 node visits.
    let exact = Limits {
        max_bytes: 11,
        max_depth: 2,
        max_nodes: 3,
        max_work: 14,
    };
    assert_eq!(encode_value(&value, exact).unwrap().len(), 11);
    assert_eq!(validate_value(&value, exact), Ok(()));
    for (limited, error) in [
        (
            Limits {
                max_bytes: 10,
                ..exact
            },
            Error::ByteLimit,
        ),
        (
            Limits {
                max_depth: 1,
                ..exact
            },
            Error::DepthLimit,
        ),
        (
            Limits {
                max_nodes: 2,
                ..exact
            },
            Error::NodeLimit,
        ),
        (
            Limits {
                max_work: 13,
                ..exact
            },
            Error::WorkLimit,
        ),
    ] {
        assert_eq!(encode_value(&value, limited), Err(error));
        assert_eq!(validate_value(&value, limited), Err(error));
    }
}

#[test]
fn budgets_are_shared_across_sections_and_compound_siblings() {
    let wire = [0, 0x53, 0x76, 0x45, 0, 0x53, 0x76, 0x45];
    // Each section is described + descriptor + list, and 4 input bytes.
    let exact = Limits {
        max_bytes: 8,
        max_depth: 2,
        max_nodes: 6,
        max_work: 14,
    };
    assert!(decode_content(&wire, exact).is_ok());
    assert_eq!(
        decode_content(
            &wire,
            Limits {
                max_nodes: 5,
                ..exact
            }
        ),
        Err(Error::NodeLimit)
    );
    assert_eq!(
        decode_content(
            &wire,
            Limits {
                max_work: 13,
                ..exact
            }
        ),
        Err(Error::WorkLimit)
    );
    let content = Content {
        body: Body::Sequence(vec![vec![], vec![]]),
        ..Content::default()
    };
    let encoded = Limits {
        max_bytes: 24,
        max_depth: 2,
        max_nodes: 6,
        max_work: 30,
    };
    assert_eq!(encode_content(&content, encoded).unwrap().len(), 24);
    assert_eq!(
        encode_content(
            &content,
            Limits {
                max_nodes: 5,
                ..encoded
            }
        ),
        Err(Error::NodeLimit)
    );
    assert_eq!(
        encode_content(
            &content,
            Limits {
                max_work: 29,
                ..encoded
            }
        ),
        Err(Error::WorkLimit)
    );
    let siblings = [0xc0, 11, 2, 0xc0, 3, 2, 0x40, 0x40, 0xc0, 3, 2, 0x40, 0x40];
    assert_eq!(
        decode_value(
            &siblings,
            Limits {
                max_nodes: 6,
                ..limits()
            }
        ),
        Err(Error::NodeLimit)
    );
    assert!(
        decode_value(
            &siblings,
            Limits {
                max_nodes: 7,
                ..limits()
            }
        )
        .is_ok()
    );
}

#[test]
fn zero_width_array_elements_spend_global_nodes_and_work() {
    let wire = [0xe0, 2, 3, 0x41];
    // Array, constructor, three implicit true nodes; four bytes + five nodes.
    let exact = Limits {
        max_bytes: 4,
        max_depth: 2,
        max_nodes: 5,
        max_work: 9,
    };
    assert!(decode_value(&wire, exact).is_ok());
    assert_eq!(
        decode_value(
            &wire,
            Limits {
                max_nodes: 4,
                ..exact
            }
        ),
        Err(Error::NodeLimit)
    );
    assert_eq!(
        decode_value(
            &wire,
            Limits {
                max_work: 8,
                ..exact
            }
        ),
        Err(Error::WorkLimit)
    );
    let malicious = [0xf0, 0, 0, 0, 5, 255, 255, 255, 255, 0x41];
    assert_eq!(decode_value(&malicious, limits()), Err(Error::NodeLimit));
    let empty = [0xe0, 2, 0, 0x41];
    assert_eq!(
        decode_value(
            &empty,
            Limits {
                max_nodes: 1,
                ..limits()
            }
        ),
        Err(Error::NodeLimit)
    );
    let value = decode_value(&wire, limits()).unwrap();
    // Semantic encoder emits array32, still counting all implicit elements.
    let encoded = Limits {
        max_bytes: 10,
        max_depth: 2,
        max_nodes: 5,
        max_work: 15,
    };
    assert!(encode_value(&value, encoded).is_ok());
    assert_eq!(
        validate_value(
            &value,
            Limits {
                max_nodes: 4,
                ..encoded
            }
        ),
        Err(Error::NodeLimit)
    );
    assert_eq!(
        validate_value(
            &value,
            Limits {
                max_work: 14,
                ..encoded
            }
        ),
        Err(Error::WorkLimit)
    );
}

fn assert_exact_and_one_under(exact: Limits, check: impl Fn(Limits) -> Result<(), Error>) {
    assert_eq!(check(exact), Ok(()));
    for (limited, error) in [
        (
            Limits {
                max_bytes: exact.max_bytes - 1,
                ..exact
            },
            Error::ByteLimit,
        ),
        (
            Limits {
                max_depth: exact.max_depth - 1,
                ..exact
            },
            Error::DepthLimit,
        ),
        (
            Limits {
                max_nodes: exact.max_nodes - 1,
                ..exact
            },
            Error::NodeLimit,
        ),
        (
            Limits {
                max_work: exact.max_work - 1,
                ..exact
            },
            Error::WorkLimit,
        ),
    ] {
        assert_eq!(check(limited), Err(error));
    }
}

#[test]
fn empty_array_compatibility_markers_obey_all_value_budgets() {
    for (wire, constructor, nodes, depth) in [
        (vec![0xe0, 1, 0], Constructor::Absent, 1, 1),
        (vec![0xe0, 2, 0, 0x40], Constructor::Null, 2, 2),
        (
            vec![0xf0, 0, 0, 0, 4, 0, 0, 0, 0],
            Constructor::Absent,
            1,
            1,
        ),
        (
            vec![0xf0, 0, 0, 0, 5, 0, 0, 0, 0, 0x40],
            Constructor::Null,
            2,
            2,
        ),
    ] {
        let value = Value::Array(Array {
            constructor,
            elements: vec![],
        });
        // Absent constructors have no node/bytes; null sentinels spend both.
        let exact = Limits {
            max_bytes: wire.len(),
            max_depth: depth,
            max_nodes: nodes,
            max_work: wire.len() + nodes,
        };
        assert_eq!(decode_value(&wire, exact), Ok(value.clone()));
        assert_exact_and_one_under(exact, |caps| decode_value(&wire, caps).map(|_| ()));
        // The semantic encoder chooses array32 without changing the marker.
        let encoded_bytes = 8 + nodes;
        let exact = Limits {
            max_bytes: encoded_bytes,
            max_work: encoded_bytes + nodes,
            ..exact
        };
        assert_eq!(encode_value(&value, exact).unwrap().len(), encoded_bytes);
        assert_exact_and_one_under(exact, |caps| encode_value(&value, caps).map(|_| ()));
        assert_exact_and_one_under(exact, |caps| validate_value(&value, caps));
    }
}

#[test]
fn nested_empty_array_property_budgets_remain_global() {
    let wire = [
        0, 0x53, 0x74, 0xc1, 14, 2, 0xa1, 1, b'p', 0xc0, 8, 2, 0xe0, 1, 0, 0xe0, 2, 0, 0x40,
    ];
    // Described section + descriptor + map + key + list + two arrays + sentinel.
    let exact = Limits {
        max_bytes: 19,
        max_depth: 5,
        max_nodes: 8,
        max_work: 27,
    };
    let content = decode_content(&wire, exact).unwrap();
    assert_exact_and_one_under(exact, |caps| decode_content(&wire, caps).map(|_| ()));
    assert_exact_and_one_under(exact, |caps| {
        EncodedContent::from_bytes(&wire, caps).map(|_| ())
    });
    let retained = EncodedContent::from_bytes(&wire, exact).unwrap();
    assert_exact_and_one_under(exact, |caps| retained.view(caps).map(|_| ()));
    let encoded = Limits {
        max_bytes: 43,
        max_work: 51,
        ..exact
    };
    assert_eq!(encode_content(&content, encoded).unwrap().len(), 43);
    assert_exact_and_one_under(encoded, |caps| encode_content(&content, caps).map(|_| ()));
}

#[test]
fn descriptor_chains_are_bounded_even_with_unlimited_caller_depth() {
    let chain = |n: usize| {
        let mut bytes = Vec::new();
        for _ in 0..n {
            bytes.extend([0, 0x53, 1]);
        }
        bytes.push(0x40);
        bytes
    };
    assert!(decode_value(&chain(MAX_DEPTH - 1), limits()).is_ok());
    let unlimited_depth = Limits {
        max_depth: usize::MAX,
        ..limits()
    };
    assert_eq!(
        decode_value(&chain(MAX_DEPTH), unlimited_depth),
        Err(Error::DepthLimit)
    );
    let error = decode_value(&chain(2_000), unlimited_depth).unwrap_err();
    assert_eq!(error.to_string(), "AMQP nesting limit exceeded");
    let mut value = Value::Null;
    for _ in 0..512 {
        value = Value::Described {
            descriptor: Descriptor::ULong(1),
            value: Box::new(value),
        };
    }
    assert_eq!(
        validate_value(&value, unlimited_depth),
        Err(Error::DepthLimit)
    );
    assert_eq!(
        encode_value(&value, unlimited_depth),
        Err(Error::DepthLimit)
    );
    // Teardown the deliberately over-deep caller-owned value iteratively too.
    while let Value::Described { value: inner, .. } = value {
        value = *inner;
    }
}

#[test]
fn array_constructor_descriptors_and_per_element_traversal_are_bounded() {
    let mut constructor = Constructor::Primitive(C::UInt0);
    for _ in 0..3 {
        constructor = Constructor::Described {
            descriptor: Descriptor::ULong(1),
            constructor: Box::new(constructor),
        };
    }
    let value = Value::Array(Array {
        constructor,
        elements: vec![Value::UInt(0); 4],
    });
    let wire = encode_value(&value, limits()).unwrap();
    // Array + (3 described constructors + 3 descriptors + primitive) + 4 values.
    let exact = Limits {
        max_bytes: wire.len(),
        max_depth: 5,
        max_nodes: 12,
        max_work: wire.len() + 12 + 12,
    };
    assert!(decode_value(&wire, exact).is_ok());
    assert!(validate_value(&value, exact).is_ok());
    assert_eq!(
        decode_value(
            &wire,
            Limits {
                max_work: exact.max_work - 1,
                ..exact
            }
        ),
        Err(Error::WorkLimit)
    );
    assert_eq!(
        validate_value(
            &value,
            Limits {
                max_depth: 4,
                ..exact
            }
        ),
        Err(Error::DepthLimit)
    );
    let mut constructor = Constructor::Primitive(C::True);
    for _ in 0..512 {
        constructor = Constructor::Described {
            descriptor: Descriptor::ULong(1),
            constructor: Box::new(constructor),
        };
    }
    let value = Value::Array(Array {
        constructor,
        elements: vec![],
    });
    assert_eq!(
        validate_value(
            &value,
            Limits {
                max_depth: usize::MAX,
                ..limits()
            }
        ),
        Err(Error::DepthLimit)
    );
    let mut wire = vec![0xf0, 0, 0, 0, 0, 0, 0, 0, 0];
    for _ in 0..512 {
        wire.extend([0, 0x53, 1]);
    }
    wire.push(0x41);
    let size = u32::try_from(wire.len() - 5).unwrap();
    wire[1..5].copy_from_slice(&size.to_be_bytes());
    assert_eq!(
        decode_value(
            &wire,
            Limits {
                max_depth: usize::MAX,
                ..limits()
            }
        ),
        Err(Error::DepthLimit)
    );
}

#[test]
fn malformed_lengths_counts_and_constructors_refuse_before_count_sized_allocation() {
    let fixtures = [
        (vec![0xb0, 255, 255, 255, 255], Error::Truncated),
        (vec![0xd0, 255, 255, 255, 255, 0, 0, 0, 0], Error::Truncated),
        (vec![0xd0, 0, 0, 0, 4, 255, 255, 255, 255], Error::NodeLimit),
        (vec![0xd1, 0, 0, 0, 4, 255, 255, 255, 254], Error::NodeLimit),
        (vec![0xc0, 1, 1], Error::InvalidLength),
        (vec![0xc0, 2, 0, 0x40], Error::InvalidLength),
        (vec![0xc0, 0], Error::Truncated),
        (vec![0xc1, 2, 1, 0x40], Error::OddMapCount),
        (vec![0xe0, 1, 1], Error::Truncated),
        (vec![0xe0, 2, 1, 0x40], Error::InvalidConstructor),
        (vec![0xf0, 0, 0, 0, 4, 0, 0, 0, 1], Error::Truncated),
        (
            vec![0xf0, 0, 0, 0, 5, 0, 0, 0, 1, 0x40],
            Error::InvalidConstructor,
        ),
        (vec![0xe0, 3, 0, 0x40, 0x40], Error::InvalidConstructor),
        (vec![0xe0, 4, 0, 0, 0x53, 1], Error::Truncated),
        (
            vec![0xe0, 5, 0, 0, 0x53, 1, 0x40],
            Error::InvalidConstructor,
        ),
        (vec![0xc0, 5, 2, 0xe0, 1, 1, 0x41], Error::Truncated),
        (vec![0xe0, 4, 1, 0xe0, 1, 1], Error::Truncated),
        (vec![0xe0, 2, 0, 0xff], Error::InvalidConstructor),
        (vec![0xe0, 2, 1, 0x70], Error::Truncated),
        (vec![0xe0, 3, 0, 0x41, 0x40], Error::InvalidLength),
        (vec![0, 0x40, 0x40], Error::InvalidDescriptor),
        (vec![0, 0, 0x53, 1, 0x40], Error::InvalidDescriptor),
        (vec![0x56, 2], Error::InvalidValue),
        (vec![0x73, 0, 0, 0xd8, 0], Error::InvalidValue),
        (vec![0x73, 0, 0x11, 0, 0], Error::InvalidValue),
        (vec![0xa1, 1, 255], Error::InvalidUtf8),
        (vec![0xa3, 1, 255], Error::InvalidUtf8),
        (vec![0x40, 0x40], Error::TrailingBytes),
    ];
    for (wire, error) in fixtures {
        assert_eq!(
            decode_value(&wire, limits()),
            Err(error),
            "fixture {wire:02x?}"
        );
    }
    // Even when the node budget is huge, a declared standalone count cannot exceed
    // the bytes remaining in its container. No capacity is reserved from that count.
    assert_eq!(
        decode_value(
            &[0xd0, 0, 0, 0, 4, 255, 255, 255, 255],
            Limits {
                max_nodes: usize::MAX,
                max_work: usize::MAX,
                ..limits()
            }
        ),
        Err(Error::InvalidLength)
    );
    let valid = [0xc0, 7, 2, 0xa1, 2, b'o', b'k', 0x52, 7];
    assert!(decode_value(&valid, limits()).is_ok());
    for n in 0..valid.len() {
        assert!(decode_value(&valid[..n], limits()).is_err());
    }
}

#[test]
fn empty_array_markers_cannot_construct_elements_or_described_types() {
    for marker in [Constructor::Absent, Constructor::Null] {
        for elements in [vec![Value::Null], vec![Value::Bool(true)]] {
            let value = Value::Array(Array {
                constructor: marker.clone(),
                elements,
            });
            assert_eq!(
                validate_value(&value, limits()),
                Err(Error::InvalidConstructor)
            );
            assert_eq!(
                encode_value(&value, limits()),
                Err(Error::InvalidConstructor)
            );
        }
        for elements in [vec![], vec![Value::Null]] {
            let value = Value::Array(Array {
                constructor: Constructor::Described {
                    descriptor: Descriptor::ULong(1),
                    constructor: Box::new(marker.clone()),
                },
                elements,
            });
            assert_eq!(
                validate_value(&value, limits()),
                Err(Error::InvalidConstructor)
            );
            assert_eq!(
                encode_value(&value, limits()),
                Err(Error::InvalidConstructor)
            );
        }
    }
}

#[test]
fn public_array_type_mismatches_and_bad_short_widths_fail_validation_and_encoding() {
    for (code, value) in [
        (C::True, Value::Bool(false)),
        (C::UInt0, Value::UInt(1)),
        (C::SmallInt, Value::Int(128)),
        (C::SmallUInt, Value::UInt(256)),
        (C::List0, Value::List(vec![Value::Null])),
        (C::UByte, Value::Null),
        (
            C::String8,
            Value::String(Text {
                text: "x".into(),
                width: Width::Four,
            }),
        ),
    ] {
        let value = Value::Array(Array {
            constructor: Constructor::Primitive(code),
            elements: vec![value],
        });
        assert_eq!(validate_value(&value, limits()), Err(Error::InvalidValue));
        assert_eq!(encode_value(&value, limits()), Err(Error::InvalidValue));
    }
    let value = Value::String(Text {
        text: "x".repeat(256),
        width: Width::One,
    });
    assert_eq!(validate_value(&value, limits()), Err(Error::InvalidLength));
    assert_eq!(encode_value(&value, limits()), Err(Error::InvalidLength));
    // Nested list8 payload cannot silently wrap its length.
    let value = Value::Array(Array {
        constructor: Constructor::Primitive(C::List8),
        elements: vec![Value::List(vec![Value::Null; 255])],
    });
    assert_eq!(validate_value(&value, limits()), Err(Error::InvalidLength));
}
