use super::*;
use crate::experimental_runtime::network::tests::{blank, initial, routes};

fn append(
    entries: Vec<crate::LogEntry>,
    previous: Option<crate::LogId>,
) -> AppendEntriesRequest<LogTypes> {
    AppendEntriesRequest {
        vote: openraft::Vote::new_committed(1, 7),
        prev_log_id: previous,
        entries,
        leader_commit: Some(crate::LogId::new(
            openraft::CommittedLeaderId::new(1, 7),
            100,
        )),
    }
}

#[test]
fn whole_append_preserves_valid_short_chunks_with_later_leader_commit() {
    let routes = routes();
    assert!(validate_append(&routes, 7, &append(vec![initial(), blank(1)], None)).is_ok());
    assert!(validate_append(&routes, 7, &append(vec![blank(2)], Some(blank(1).log_id))).is_ok());
    assert_eq!(
        validate_append(&routes, 7, &append(Vec::new(), Some(blank(1).log_id))).unwrap(),
        SCALAR_RPC_BYTES
    );
}

#[test]
fn whole_append_rejects_gap_backward_future_term_and_wrong_initial() {
    let routes = routes();
    for request in [
        append(vec![initial(), blank(2)], None),
        append(vec![blank(0)], None),
        append(vec![initial(), blank(1), blank(1)], None),
        append(
            vec![blank(1)],
            Some(crate::LogId::new(
                openraft::CommittedLeaderId::new(1, 7),
                u64::MAX,
            )),
        ),
    ] {
        assert!(validate_append(&routes, 7, &request).is_err());
    }
    let mut future = blank(1);
    future.log_id = crate::LogId::new(openraft::CommittedLeaderId::new(2, 7), 1);
    assert!(validate_append(&routes, 7, &append(vec![initial(), future], None)).is_err());
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[test]
fn whole_append_rejects_foreign_membership_and_source_without_charge() {
    let routes = routes();
    let mut entry = initial();
    entry.payload = openraft::EntryPayload::Membership(openraft::Membership::new(
        vec![[7, 8].into_iter().collect()],
        std::collections::BTreeMap::from([
            (7, BasicNode::new("foreign")),
            (8, BasicNode::new("foreign")),
        ]),
    ));
    assert!(validate_append(&routes, 7, &append(vec![entry], None)).is_err());
    assert!(validate_append(&routes, 8, &append(vec![initial()], None)).is_err());
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[test]
fn oversized_count_has_positive_fixed_hint_without_accepting_prefix() {
    let routes = routes();
    let entries = (1..=MAX_APPEND_ENTRIES as u64 + 1).map(blank).collect();
    let cause = validate_append(&routes, 7, &append(entries, Some(initial().log_id))).unwrap_err();
    assert_eq!(cause, AppendPreflightError::PayloadTooLarge);
    assert!(
        matches!(cause.into_rpc_error(), RPCError::PayloadTooLarge(hint) if hint.entries_hint() == 15)
    );
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[tokio::test]
async fn unavailable_and_snapshot_errors_are_static_and_do_not_admit() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let mut factory = source.factory();
    let mut client = factory
        .new_client(8, &BasicNode::new("credential-like-private-label"))
        .await;
    let error = client
        .vote(
            VoteRequest::new(openraft::Vote::new(1, 7), None),
            RPCOption::new(std::time::Duration::from_millis(1)),
        )
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("credential-like-private-label"));
    assert!(error.to_string().contains("unavailable"));
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
    let snapshot = InstallSnapshotRequest::<LogTypes> {
        vote: openraft::Vote::new_committed(1, 7),
        meta: openraft::SnapshotMeta::default(),
        offset: 0,
        data: Vec::new(),
        done: true,
    };
    assert!(
        client
            .install_snapshot(
                snapshot,
                RPCOption::new(std::time::Duration::from_millis(1))
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[tokio::test]
async fn hard_ttl_discards_waiter_without_releasing_accepted_packet() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let routes = routes();
        let source = routes.begin_node(7).unwrap();
        let mut target = routes.begin_node(8).unwrap();
        let (_endpoint, receiver) = target.test_parts();
        let mut factory = source.factory();
        let mut client = factory
            .new_client(8, &BasicNode::new(super::super::registry::stable_label(8)))
            .await;
        let result = client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 7), None),
                RPCOption::new(std::time::Duration::from_millis(1)),
            )
            .await;
        assert!(matches!(result, Err(RPCError::Timeout(_))));
        assert_eq!(
            routes.workload().unwrap(),
            super::super::RpcWorkload {
                accepted_jobs: 1,
                encoded_bytes: SCALAR_RPC_BYTES
            }
        );
        receiver.try_recv().unwrap().refuse(RpcCause::Unavailable);
        assert_eq!(
            routes.workload().unwrap(),
            super::super::RpcWorkload::default()
        );
    })
    .await
    .unwrap();
}

fn sending(index: u64, bytes: usize) -> crate::LogEntry {
    crate::LogEntry {
        log_id: blank(index).log_id,
        payload: openraft::EntryPayload::Normal(crate::QueueLogCommand::send(
            domain::NamespaceName::new("tenant").unwrap(),
            domain::EntityPath::new("orders").unwrap(),
            domain::Timestamp::from_millis(42),
            domain::CommittedSend {
                message_id: format!("message-{index}"),
                body: vec![7; bytes],
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    }
}

#[test]
fn byte_limit_is_independent_of_count_and_fixed_fifteen_fits() {
    let routes = routes();
    let mut entries = (1..=15)
        .map(|index| sending(index, crate::MAX_LOG_BODY_BYTES))
        .collect::<Vec<_>>();
    assert!(
        validate_append(&routes, 7, &append(entries.clone(), Some(initial().log_id))).unwrap()
            <= MAX_APPEND_BYTES
    );
    entries.push(sending(16, crate::MAX_LOG_BODY_BYTES));
    assert!(matches!(
        validate_append(&routes, 7, &append(entries, Some(initial().log_id))),
        Err(AppendPreflightError::PayloadTooLarge)
    ));
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[tokio::test]
async fn malformed_late_entry_refuses_whole_rpc_before_queue_or_charge() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let mut target = routes.begin_node(8).unwrap();
    let (_endpoint, receiver) = target.test_parts();
    let mut factory = source.factory();
    let mut client = factory
        .new_client(8, &BasicNode::new(super::super::registry::stable_label(8)))
        .await;
    let result = client
        .append_entries(
            append(
                vec![
                    initial(),
                    blank(1),
                    sending(2, crate::MAX_LOG_BODY_BYTES + 1),
                ],
                None,
            ),
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await;
    assert!(matches!(result, Err(RPCError::Unreachable(_))));
    assert!(matches!(
        receiver.try_recv(),
        Err(flume::TryRecvError::Empty)
    ));
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}
