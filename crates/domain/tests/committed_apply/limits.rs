use domain::{
    CommittedApplyError, CommittedQueueWork, CommittedStateMachine, MAX_COMMITTED_BODY_BYTES,
    MAX_COMMITTED_CHECKPOINT_BYTES, MAX_COMMITTED_MEMBERSHIP_BYTES, QueueConfig,
};
use storage::{CommittedStore, StateStore};

use super::{TestResult, fixture::*};

fn public_bounds_refuse_before_business_reads_and_preserve_progress<W: CommittedStore>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let mut machine = CommittedStateMachine::create(writer, stream()?)?;
    let key = checkpoint_key(&control.reader())?;
    apply(&mut machine, 0, &create(1, QueueConfig::default())?)?;
    let request = update(&machine, 1)?;
    let before = control.reader().snapshot()?;
    let counts = control.counts();
    assert!(matches!(
        machine.apply_committed(
            &request,
            &send(2, "large", &vec![7; MAX_COMMITTED_BODY_BYTES + 1])?
        ),
        Err(CommittedApplyError::TooLarge {
            maximum: MAX_COMMITTED_BODY_BYTES,
            ..
        })
    ));
    let after = control.counts();
    assert_eq!(after.gets, counts.gets + 1);
    assert_eq!(after.scans, counts.scans);
    assert_eq!(after.commits, counts.commits);
    assert_eq!(control.reader().snapshot()?, before);
    assert!(matches!(
        machine.apply_committed(
            &request,
            &send(2, &"x".repeat(4 * 1024), &vec![7; MAX_COMMITTED_BODY_BYTES])?
        ),
        Err(CommittedApplyError::TooLarge { .. })
    ));
    assert_eq!(control.reader().snapshot()?, before);
    machine.apply_committed(
        &request,
        &send(2, "exact", &vec![7; MAX_COMMITTED_BODY_BYTES])?,
    )?;
    assert_eq!(
        record(&machine.reader(), 1)?
            .ok_or("missing exact-bound message")?
            .body
            .len(),
        MAX_COMMITTED_BODY_BYTES
    );

    let request = update(&machine, 2)?;
    let before = control.reader().snapshot()?;
    assert_eq!(
        machine.apply_committed(
            &request,
            &CommittedQueueWork::Membership {
                schema_version: 0,
                payload: Vec::new()
            }
        ),
        Err(CommittedApplyError::InvalidMembershipSchema)
    );
    assert!(matches!(
        machine.apply_committed(
            &request,
            &CommittedQueueWork::Membership {
                schema_version: 1,
                payload: vec![1; MAX_COMMITTED_MEMBERSHIP_BYTES + 1]
            }
        ),
        Err(CommittedApplyError::TooLarge {
            maximum: MAX_COMMITTED_MEMBERSHIP_BYTES,
            ..
        })
    ));
    assert_eq!(control.reader().snapshot()?, before);
    machine.apply_committed(
        &request,
        &CommittedQueueWork::Membership {
            schema_version: 1,
            payload: vec![1; MAX_COMMITTED_MEMBERSHIP_BYTES],
        },
    )?;
    assert_eq!(
        machine
            .checkpoint()?
            .membership()
            .ok_or("missing exact-bound membership")?
            .payload
            .len(),
        MAX_COMMITTED_MEMBERSHIP_BYTES
    );
    assert!(
        control
            .reader()
            .get(&key)?
            .ok_or("missing checkpoint")?
            .len()
            <= MAX_COMMITTED_CHECKPOINT_BYTES
    );
    Ok(())
}

for_each_backend!(public_bounds_refuse_before_business_reads_and_preserve_progress,);
