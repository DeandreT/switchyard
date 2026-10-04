use cluster::{ExperimentalRaftCluster, ReplicaRuntimeError};

use super::{
    TestResult,
    fixture::{self, Backend, Evidence, Seed},
};

async fn refusal<W: storage::CommittedStore>(
    stores: [cluster::ExperimentalReplicaStores; 3],
    evidence: &[Evidence<W>],
    create: bool,
    expected: ReplicaRuntimeError,
) -> TestResult {
    let result = if create {
        ExperimentalRaftCluster::create(stores).await
    } else {
        ExperimentalRaftCluster::open(stores).await
    };
    match result {
        Err(error) => assert_eq!(error, expected),
        Ok(cluster) => {
            cluster.shutdown().await?;
            return Err("startup unexpectedly accepted invalid storage".into());
        }
    }
    for row in evidence {
        row.unchanged()?;
        assert_eq!(row.log.full_scans(), 0);
    }
    fixture::retired(evidence).await?;
    Ok(())
}

pub(super) async fn empty_open_refuses_without_mutation<B: Backend>(backend: &B) -> TestResult {
    let (stores, evidence) = fixture::prepare(backend, "empty", Seed::default()).await?;
    refusal(
        stores,
        &evidence,
        false,
        ReplicaRuntimeError::NotInitialized,
    )
    .await
}

pub(super) async fn identity_and_stream_mismatch_refuse_without_mutation<B: Backend>(
    backend: &B,
) -> TestResult {
    let duplicate = Seed {
        ids: [7, 7, 9],
        ..Seed::default()
    };
    let mut foreign = Seed::default();
    foreign.streams[1] = fixture::other_stream();
    for (name, seed) in [("duplicate", duplicate), ("stream", foreign)] {
        let (stores, evidence) = fixture::prepare(backend, name, seed).await?;
        refusal(
            stores,
            &evidence,
            true,
            ReplicaRuntimeError::ProfileMismatch,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn create_requires_no_prior_vote_or_history<B: Backend>(
    backend: &B,
) -> TestResult {
    let mut voted = Seed::default();
    voted.votes[1] = Some(openraft::Vote::new(1, 8));
    let mut nonempty = Seed::default();
    nonempty.histories[1] = vec![fixture::initial()];
    for (name, seed) in [("voted", voted), ("nonempty", nonempty)] {
        let (stores, evidence) = fixture::prepare(backend, name, seed).await?;
        refusal(stores, &evidence, true, ReplicaRuntimeError::NotPristine).await?;
    }
    Ok(())
}

pub(super) async fn all_retained_memberships_must_match_fixed_routes<B: Backend>(
    backend: &B,
) -> TestResult {
    let wrong = openraft::Membership::new(
        vec![std::collections::BTreeSet::from(fixture::IDS)],
        fixture::IDS
            .into_iter()
            .map(|id| (id, openraft::BasicNode::new(format!("foreign-label-{id}"))))
            .collect::<std::collections::BTreeMap<_, _>>(),
    );
    let mut initial = Seed::default();
    initial.histories[1] = vec![cluster::LogEntry {
        log_id: cluster::LogId::default(),
        payload: openraft::EntryPayload::Membership(wrong.clone()),
    }];
    let mut suffix = Seed::initialized();
    suffix.histories[1].push(cluster::LogEntry {
        log_id: fixture::blank(2).log_id,
        payload: openraft::EntryPayload::Membership(wrong),
    });
    for (name, seed) in [("wrong-initial", initial), ("wrong-unapplied", suffix)] {
        let (stores, evidence) = fixture::prepare(backend, name, seed).await?;
        refusal(
            stores,
            &evidence,
            false,
            ReplicaRuntimeError::MembershipMismatch,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn finite_startup_headroom_refuses_before_engine_start<B: Backend>(
    backend: &B,
) -> TestResult {
    let mut seed = Seed::default();
    seed.histories[0] = std::iter::once(fixture::initial())
        .chain((1..cluster::MAX_RETAINED_ENTRIES - 1).map(fixture::blank))
        .collect();
    seed.votes[0] = Some(openraft::Vote::new_committed(1, 7));
    let (stores, evidence) = fixture::prepare(backend, "headroom", seed).await?;
    refusal(stores, &evidence, false, ReplicaRuntimeError::Headroom).await
}
