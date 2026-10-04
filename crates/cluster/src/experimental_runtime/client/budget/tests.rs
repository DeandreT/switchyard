use std::sync::Arc;

use super::*;

#[test]
fn count_limit_includes_every_held_lease_and_refunds_exactly() {
    let admission = Arc::new(Admission::default());
    let leases: Vec<_> = (0..MAX_CLIENT_JOBS)
        .map(|_| admission.acquire(7).unwrap())
        .collect();
    assert_eq!(
        admission.workload(),
        ClientWorkload {
            accepted_jobs: 16,
            encoded_bytes: 112
        }
    );
    assert!(matches!(
        admission.acquire(1),
        Err(QueueWriteError::KnownRejected(
            QueueWriteRejection::Capacity
        ))
    ));
    drop(leases);
    assert_eq!(admission.workload(), ClientWorkload::default());
    drop(admission.acquire(9).unwrap());
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[test]
fn independent_byte_limit_and_overflow_refuse_without_partial_admission() {
    let admission = Arc::new(Admission::default());
    let lease = admission.acquire(MAX_CLIENT_BYTES).unwrap();
    for bytes in [1, usize::MAX] {
        assert!(matches!(
            admission.acquire(bytes),
            Err(QueueWriteError::KnownRejected(
                QueueWriteRejection::Capacity
            ))
        ));
    }
    assert_eq!(
        admission.workload(),
        ClientWorkload {
            accepted_jobs: 1,
            encoded_bytes: MAX_CLIENT_BYTES
        }
    );
    drop(lease);
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[tokio::test]
async fn close_wakes_the_owner_but_does_not_refund_accepted_lease() {
    let admission = Arc::new(Admission::default());
    let lease = admission.acquire(13).unwrap();
    admission.close();
    tokio::time::timeout(std::time::Duration::from_secs(1), admission.closed())
        .await
        .unwrap();
    assert!(matches!(
        admission.acquire(1),
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    ));
    assert_eq!(
        admission.workload(),
        ClientWorkload {
            accepted_jobs: 1,
            encoded_bytes: 13
        }
    );
    drop(lease);
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[test]
fn poisoning_refuses_new_admission_but_cleanup_refunds_existing_lease() {
    let admission = Arc::new(Admission::default());
    let lease = admission.acquire(19).unwrap();
    let poisoned = Arc::clone(&admission);
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned.state.lock().unwrap();
            panic!("test-only admission poison");
        })
        .join()
        .is_err()
    );
    assert!(admission.is_closed());
    assert!(matches!(
        admission.acquire(1),
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    ));
    drop(lease);
    assert_eq!(admission.workload(), ClientWorkload::default());
}
