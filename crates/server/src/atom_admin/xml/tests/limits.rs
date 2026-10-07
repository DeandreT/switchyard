use super::*;

#[test]
fn body_limit_includes_bom_and_refuses_before_utf8_parsing() {
    let mut exact = document("").into_bytes();
    exact.resize(MAX_BODY_BYTES, b' ');
    assert_eq!(decode_definition(&exact), Ok(default_definition()));
    exact.push(b' ');
    assert_eq!(
        decode_definition(&exact),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    let mut bom = vec![0xEF, 0xBB, 0xBF];
    bom.extend_from_slice(document("").as_bytes());
    bom.resize(MAX_BODY_BYTES, b' ');
    assert_eq!(decode_definition(&bom), Ok(default_definition()));
    bom.push(0xFF);
    assert_eq!(
        decode_definition(&bom),
        Err(AtomXmlError::WorkLimitExceeded)
    );
}

#[test]
fn event_limit_counts_references_and_final_eof_before_next_read() {
    let references = "&#48;".repeat(MAX_EVENTS - 10) + "&#49;";
    assert_eq!(
        parse(&property("MaxDeliveryCount", &references))
            .unwrap()
            .config
            .max_delivery_count,
        1
    );
    let over = references + "&#48;";
    assert_eq!(
        parse(&property("MaxDeliveryCount", &over)),
        Err(AtomXmlError::WorkLimitExceeded)
    );
}

#[test]
fn attribute_limit_counts_declarations_and_namespace_limit_counts_active_scopes() {
    let declarations = |count: usize| {
        (0..count)
            .map(|index| format!(" xmlns:p{index}=\"{ATOM_NS}\""))
            .collect::<String>()
    };
    let exact = document("").replacen(
        "<entry ",
        &format!("<entry{} ", declarations(MAX_ATTRIBUTES - 1)),
        1,
    );
    assert_eq!(
        decode_definition(exact.as_bytes()),
        Ok(default_definition())
    );
    let over = document("").replacen(
        "<entry ",
        &format!("<entry{} ", declarations(MAX_ATTRIBUTES)),
        1,
    );
    assert_eq!(
        decode_definition(over.as_bytes()),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    let scopes = document("")
        .replacen("<entry ", &format!("<entry{} ", declarations(31)), 1)
        .replacen("<content ", &format!("<content{} ", declarations(31)), 1);
    // 32 root bindings, 31 content bindings, and one description default = 64.
    assert_eq!(
        decode_definition(scopes.as_bytes()),
        Ok(default_definition())
    );
    let over = scopes.replacen(
        "<QueueDescription ",
        &format!("<QueueDescription xmlns:extra=\"{ATOM_NS}\" "),
        1,
    );
    assert_eq!(
        decode_definition(over.as_bytes()),
        Err(AtomXmlError::WorkLimitExceeded)
    );
}

#[test]
fn isolated_work_counters_hold_exact_bound_and_reject_next_without_mutation() {
    let mut budget = Budget::default();
    for _ in 0..MAX_EVENTS {
        assert_eq!(budget.event(), Ok(()));
    }
    assert_eq!(budget.event(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.events, MAX_EVENTS);
    for _ in 0..MAX_PROPERTIES {
        assert_eq!(budget.property(), Ok(()));
    }
    assert_eq!(budget.property(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.properties, MAX_PROPERTIES);
    assert_eq!(budget.decoded(MAX_BODY_BYTES), Ok(()));
    assert_eq!(budget.decoded(1), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.decoded, MAX_BODY_BYTES);
    assert_eq!(Budget::depth(MAX_DEPTH), Ok(()));
    assert_eq!(
        Budget::depth(MAX_DEPTH + 1),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(Budget::attributes(MAX_ATTRIBUTES), Ok(()));
    assert_eq!(
        Budget::attributes(MAX_ATTRIBUTES + 1),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    let mut scratch = MAX_BODY_BYTES;
    assert_eq!(
        bounded_add(&mut scratch, 1, MAX_BODY_BYTES),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(scratch, MAX_BODY_BYTES);
    assert_eq!(
        bounded_add(&mut scratch, usize::MAX, MAX_BODY_BYTES),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(scratch, MAX_BODY_BYTES);
}
