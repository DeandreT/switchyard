//! Validate the exact compatibility preflight separately from legacy handler
//! read-set assertions. Never discard arbitrary profile reads or tag-13 scans.

use domain::{
    EntityPath, NamespaceName, capacity_fence, keys,
    retained_accounting::{CapacityOwner, keys::usage},
};
use storage::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    Get(Key),
    Scan(Key, Key, usize),
    Apply,
    Snapshot,
}

pub fn handler_trace(
    gets: &mut Vec<Key>,
    scans: &[(Key, Key, usize)],
    events: &[Event],
) -> Vec<usize> {
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                Event::Get(key) => Some(key.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        *gets
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                Event::Scan(prefix, start, limit) => Some((prefix.clone(), start.clone(), *limit)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        scans
    );
    // Validate actual interleaved operation order, not merely independent get
    // and scan lists. Bound point-only checks must next enter owner authority.
    for (index, event) in events.iter().enumerate() {
        let Event::Get(key) = event else { continue };
        if key.first() != Some(&0x12) {
            continue;
        }
        let (namespace, parent) = keys::entity_scope_parts(key).unwrap();
        let owner = CapacityOwner::new(
            NamespaceName::new(namespace).unwrap(),
            EntityPath::new(parent).unwrap(),
            1,
        )
        .unwrap();
        assert!(index >= 2);
        assert_eq!(
            events[index - 2],
            Event::Get(keys::queue_config(owner.namespace(), owner.parent()))
        );
        assert_eq!(
            events[index - 1],
            Event::Get(keys::queue_config(owner.namespace(), owner.shadow()))
        );
        match events.get(index + 1) {
            Some(Event::Scan(prefix, _, _))
                if prefix.first() == Some(&capacity_fence::DATA_TAG) =>
            {
                let expected = probes(&owner)
                    .into_iter()
                    .map(|(prefix, start, limit)| Event::Scan(prefix, start, limit))
                    .collect::<Vec<_>>();
                assert_eq!(events.get(index + 1..index + 25), Some(expected.as_slice()));
            }
            Some(Event::Get(head)) => assert_eq!(
                *head,
                keys::entity_metadata(owner.namespace(), owner.parent()),
                "point-only preflight must enter retained owner validation"
            ),
            None => {} // An injected point-read failure can end preparation.
            other => panic!("unexpected operation after capacity point preflight: {other:?}"),
        }
    }
    let mut owners = Vec::new();
    let mut index = 0;
    while index < gets.len() {
        if gets[index].first() != Some(&0x12) {
            index += 1;
            continue;
        }
        let (namespace, parent) =
            keys::entity_scope_parts(&gets[index]).expect("canonical usage scope");
        let owner = CapacityOwner::new(
            NamespaceName::new(namespace).unwrap(),
            EntityPath::new(parent).unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(gets[index], usage(&owner));
        assert!(index >= 2, "usage probe needs its exact profile pair");
        assert_eq!(
            gets[index - 2],
            keys::queue_config(owner.namespace(), owner.parent())
        );
        assert_eq!(
            gets[index - 1],
            keys::queue_config(owner.namespace(), owner.shadow())
        );
        gets.drain(index - 2..=index);
        index -= 2;
        owners.push(owner);
    }
    let mut removed = Vec::new();
    let mut index = 0;
    while index < scans.len() {
        let (prefix, _, _) = &scans[index];
        if prefix.first() != Some(&capacity_fence::DATA_TAG) || prefix.len() == 1 {
            index += 1;
            continue;
        }
        let matches = owners.iter().any(|owner| {
            let expected = probes(owner);
            scans.get(index..index + 24) == Some(expected.as_slice())
        });
        assert!(
            matches,
            "only an exact selected-owner 24-prefix one-row preflight may be separated"
        );
        removed.extend(index..index + 24);
        index += 24;
    }
    removed
}

fn probes(owner: &CapacityOwner) -> Vec<(Key, Key, usize)> {
    let mut result = Vec::new();
    for endpoint in [owner.parent(), owner.shadow()] {
        for tag in 2..=13 {
            let prefix = [
                vec![capacity_fence::DATA_TAG, tag],
                keys::queue_config(owner.namespace(), endpoint)[1..].to_vec(),
            ]
            .concat();
            result.push((prefix.clone(), prefix, 1));
        }
    }
    result
}

#[test]
fn exact_receipt_rejects_operation_order_keys_and_limits_before_separating() {
    let owner = CapacityOwner::new(
        NamespaceName::new("n").unwrap(),
        EntityPath::new("q").unwrap(),
        1,
    )
    .unwrap();
    let gets = vec![
        keys::queue_config(owner.namespace(), owner.parent()),
        keys::queue_config(owner.namespace(), owner.shadow()),
        usage(&owner),
        keys::clock(),
    ];
    let scans = probes(&owner);
    let mut events = gets[..3]
        .iter()
        .cloned()
        .map(Event::Get)
        .collect::<Vec<_>>();
    events.extend(
        scans
            .iter()
            .cloned()
            .map(|(prefix, start, limit)| Event::Scan(prefix, start, limit)),
    );
    events.push(Event::Get(keys::clock()));
    let mut remaining = gets.clone();
    assert_eq!(
        handler_trace(&mut remaining, &scans, &events),
        (0..24).collect::<Vec<_>>()
    );
    assert_eq!(remaining, vec![keys::clock()]);
    for case in 0..5 {
        let mut invalid = events.clone();
        match case {
            0 => invalid.swap(3, 4),
            1 => {
                if let Event::Scan(_, _, limit) = &mut invalid[3] {
                    *limit = 2
                }
            }
            2 => {
                if let Event::Scan(_, start, _) = &mut invalid[3] {
                    start.push(0)
                }
            }
            3 => {
                invalid.insert(4, Event::Snapshot);
            }
            _ => invalid.swap(0, 1),
        }
        let mut raw_gets = invalid
            .iter()
            .filter_map(|event| {
                if let Event::Get(key) = event {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let raw_scans = invalid
            .iter()
            .filter_map(|event| {
                if let Event::Scan(prefix, start, limit) = event {
                    Some((prefix.clone(), start.clone(), *limit))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler_trace(
                &mut raw_gets,
                &raw_scans,
                &invalid
            )))
            .is_err()
        );
    }
}
