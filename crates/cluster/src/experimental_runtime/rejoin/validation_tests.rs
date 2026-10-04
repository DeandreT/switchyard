use std::{collections::BTreeMap, sync::Arc, time::Duration};

use domain::{CommittedSend, CommittedStreamId, EntityPath, NamespaceName, QueueConfig, Timestamp};
use openraft::{
    BasicNode, CommittedLeaderId, EntryPayload, Membership,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use storage::{CommittedStore, MemoryReplicaStore, StateStore, StoreSnapshot};

use crate::experimental_runtime::{Error, continuity::RetirementEvidence, network::stable_label};
use crate::{
    ExperimentalLogStore, ExperimentalReplicaStores, ExperimentalStateMachine, LogEntry, LogId,
    LogProfile, LogVote, QueueLogCommand,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Reader = <MemoryReplicaStore as CommittedStore>::Reader;
const DEADLINE: Duration = Duration::from_secs(5);
const NODE: u64 = 8;

fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([73; 16])?)
}

fn members() -> BTreeMap<u64, BasicNode> {
    [7, 8, 9]
        .into_iter()
        .map(|id| (id, BasicNode::new(stable_label(id))))
        .collect()
}

fn id(index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(1, 7), index)
}

fn initial() -> LogEntry {
    LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(Membership::new(
            vec![[7, 8, 9].into_iter().collect()],
            members(),
        )),
    }
}

fn create() -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(1),
        payload: EntryPayload::Normal(QueueLogCommand::create_queue(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(1),
            QueueConfig::default(),
        )),
    })
}

fn send(body: &[u8]) -> TestResult<LogEntry> {
    Ok(LogEntry {
        log_id: id(2),
        payload: EntryPayload::Normal(QueueLogCommand::send(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(2),
            CommittedSend {
                message_id: "original".into(),
                body: body.to_vec(),
                time_to_live_millis: None,
                session_id: None,
            },
        )),
    })
}

struct Fixture {
    stores: ExperimentalReplicaStores,
    log: Reader,
    state: Reader,
}

impl Fixture {
    async fn new(
        entries: Vec<LogEntry>,
        applied: usize,
        vote: Option<LogVote>,
    ) -> TestResult<Self> {
        let log_writer = MemoryReplicaStore::new();
        let state_writer = MemoryReplicaStore::new();
        let log_reader = log_writer.reader();
        let state_reader = state_writer.reader();
        let mut log = ExperimentalLogStore::create(log_writer, LogProfile::new(NODE, stream()?)?)?;
        let mut state = ExperimentalStateMachine::create(state_writer, stream()?)?;
        if !entries.is_empty() {
            for chunk in entries.chunks(crate::MAX_APPEND_ENTRIES) {
                log.blocking_append(chunk.to_vec()).await?;
            }
        }
        if let Some(vote) = vote {
            log.save_vote(&vote).await?;
        }
        if applied != 0 {
            state.apply(entries[..applied].to_vec()).await?;
        }
        Ok(Self {
            stores: ExperimentalReplicaStores::prepare(NODE, log, state).await?,
            log: log_reader,
            state: state_reader,
        })
    }

    // These units test the borrowed comparison. Only orchestration obtains an
    // authoritative floor after the actual old node and BOTH owners join.
    async fn comparison_floor(&mut self) -> TestResult<Arc<RetirementEvidence>> {
        let (log, checkpoint) = self.stores.continuity_snapshot().await?;
        Ok(RetirementEvidence::checked(
            NODE,
            stream()?,
            log,
            checkpoint,
        )?)
    }

    fn snapshot(&self) -> TestResult<(StoreSnapshot, StoreSnapshot)> {
        Ok((self.log.snapshot()?, self.state.snapshot()?))
    }

    async fn validate(&mut self, floor: &Arc<RetirementEvidence>) -> Result<(), Error> {
        super::validate(
            &mut self.stores,
            NODE,
            stream().expect("test stream"),
            &members(),
            floor,
        )
        .await
    }

    async fn shutdown(self) -> TestResult {
        self.stores.shutdown().await?;
        Ok(())
    }
}

#[tokio::test]
async fn an_exact_empty_floor_needs_no_local_membership_and_does_not_write() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut fixture = Fixture::new(Vec::new(), 0, None).await?;
        let floor = fixture.comparison_floor().await?;
        let before = fixture.snapshot()?;
        fixture.validate(&floor).await?;
        assert_eq!(fixture.snapshot()?, before);
        assert!(!fixture.stores.replication_config().enable_tick);
        fixture.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn an_exact_applied_prefix_and_unapplied_tail_pass_without_mutation() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let entries = vec![initial(), create()?, send(b"unapplied")?];
        let mut fixture = Fixture::new(entries, 2, Some(LogVote::new_committed(1, 7))).await?;
        let floor = fixture.comparison_floor().await?;
        let before = fixture.snapshot()?;
        fixture.validate(&floor).await?;
        assert_eq!(fixture.snapshot()?, before);
        fixture.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn a_changed_vote_is_refused_even_when_the_tail_vote_minimum_still_passes() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let entries = vec![initial(), create()?];
        let mut old = Fixture::new(entries.clone(), 2, Some(LogVote::new_committed(3, 7))).await?;
        let floor = old.comparison_floor().await?;
        old.shutdown().await?;
        let mut candidate = Fixture::new(entries, 2, Some(LogVote::new_committed(2, 7))).await?;
        let before = candidate.snapshot()?;
        assert_eq!(candidate.validate(&floor).await, Err(Error::InvalidHistory));
        assert_eq!(candidate.snapshot()?, before);
        candidate.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn truncating_an_unapplied_suffix_cannot_satisfy_the_retired_floor() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut old = Fixture::new(
            vec![initial(), create()?, send(b"unapplied")?],
            2,
            Some(LogVote::new_committed(1, 7)),
        )
        .await?;
        let floor = old.comparison_floor().await?;
        old.shutdown().await?;
        let mut candidate = Fixture::new(
            vec![initial(), create()?],
            2,
            Some(LogVote::new_committed(1, 7)),
        )
        .await?;
        let before = candidate.snapshot()?;
        assert_eq!(candidate.validate(&floor).await, Err(Error::InvalidHistory));
        assert_eq!(candidate.snapshot()?, before);
        candidate.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn changing_unapplied_content_at_the_same_full_id_is_refused() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut old = Fixture::new(
            vec![initial(), create()?, send(b"original")?],
            2,
            Some(LogVote::new_committed(1, 7)),
        )
        .await?;
        let floor = old.comparison_floor().await?;
        old.shutdown().await?;
        let mut candidate = Fixture::new(
            vec![initial(), create()?, send(b"replacement")?],
            2,
            Some(LogVote::new_committed(1, 7)),
        )
        .await?;
        let before = candidate.snapshot()?;
        assert_eq!(candidate.validate(&floor).await, Err(Error::InvalidHistory));
        assert_eq!(candidate.snapshot()?, before);
        candidate.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn an_earlier_valid_checkpoint_is_not_the_retired_checkpoint() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let entries = vec![initial(), create()?, send(b"unapplied")?];
        let mut old = Fixture::new(entries.clone(), 2, Some(LogVote::new_committed(1, 7))).await?;
        let floor = old.comparison_floor().await?;
        old.shutdown().await?;
        let mut candidate = Fixture::new(entries, 1, Some(LogVote::new_committed(1, 7))).await?;
        let before = candidate.snapshot()?;
        assert_eq!(candidate.validate(&floor).await, Err(Error::InvalidHistory));
        assert_eq!(candidate.snapshot()?, before);
        candidate.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn a_supplied_identity_mismatch_is_refused_before_mutation() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut fixture = Fixture::new(Vec::new(), 0, None).await?;
        let floor = fixture.comparison_floor().await?;
        let before = fixture.snapshot()?;
        assert_eq!(
            super::validate(&mut fixture.stores, 9, stream()?, &members(), &floor).await,
            Err(Error::ProfileMismatch),
        );
        assert_eq!(
            super::validate(
                &mut fixture.stores,
                NODE,
                CommittedStreamId::new([74; 16])?,
                &members(),
                &floor,
            )
            .await,
            Err(Error::ProfileMismatch),
        );
        assert_eq!(fixture.snapshot()?, before);
        fixture.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn retained_membership_must_match_the_existing_fixed_routes() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut fixture = Fixture::new(
            vec![initial(), create()?],
            2,
            Some(LogVote::new_committed(1, 7)),
        )
        .await?;
        let floor = fixture.comparison_floor().await?;
        let before = fixture.snapshot()?;
        let mut wrong_members = members();
        wrong_members.insert(7, BasicNode::new("different-route"));
        assert_eq!(
            super::validate(&mut fixture.stores, NODE, stream()?, &wrong_members, &floor).await,
            Err(Error::MembershipMismatch),
        );
        assert_eq!(fixture.snapshot()?, before);
        fixture.shutdown().await
    })
    .await?
}

#[tokio::test]
async fn candidate_headroom_remains_required_with_an_exact_floor() -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        let mut entries = vec![initial()];
        entries.extend((1..crate::MAX_RETAINED_ENTRIES - 1).map(|index| LogEntry {
            log_id: id(index),
            payload: EntryPayload::Blank,
        }));
        let mut fixture = Fixture::new(entries, 1, Some(LogVote::new_committed(1, 7))).await?;
        let floor = fixture.comparison_floor().await?;
        let before = fixture.snapshot()?;
        assert_eq!(fixture.validate(&floor).await, Err(Error::Headroom));
        assert_eq!(fixture.snapshot()?, before);
        fixture.shutdown().await
    })
    .await?
}
