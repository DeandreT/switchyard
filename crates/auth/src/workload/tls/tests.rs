use super::*;

use crate::workload::tests::support::*;

#[test]
fn lease_interval_monotonic_cap_and_clones_are_sticky_and_not_renewable() {
    fn peer() -> VerifiedWorkloadPeer {
        VerifiedWorkloadPeer(Arc::new(PeerLease {
            endpoint: Arc::new(()),
            principal: PRINCIPAL.into(),
            binding: Binding {
                scope: ResourceScope::parse(RESOURCE).unwrap(),
                permissions: crate::PermissionSet::SEND,
            },
            interval: Interval {
                from: 100,
                until: 200,
            },
            deadline: Instant::now() + Duration::from_secs(10),
            expired: AtomicBool::new(false),
        }))
    }
    let valid = peer();
    assert!(
        valid
            .check_at(100, valid.0.deadline - Duration::from_nanos(1))
            .is_ok()
    );
    for (wall, monotonic_expired) in [(99, false), (200, false), (150, true)] {
        let p = peer();
        let clone = p.clone();
        let instant = if monotonic_expired {
            p.0.deadline
        } else {
            Instant::now()
        };
        assert_eq!(p.check_at(wall, instant), Err(WorkloadError::NotValid));
        assert_eq!(
            clone.check_at(150, Instant::now()),
            Err(WorkloadError::NotValid),
            "rollback or clone cannot resurrect"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn idle_expiry_future_wakes_at_fixed_deadline_without_a_spawned_watcher() {
    let fixture = Fixture::new();
    let policy = policy(&fixture.config());
    let now = now();
    let p = VerifiedWorkloadPeer(Arc::new(PeerLease {
        endpoint: Arc::new(()),
        principal: PRINCIPAL.into(),
        binding: policy.0.bindings[PRINCIPAL].clone(),
        interval: Interval {
            from: now - 1,
            until: now + 3600,
        },
        deadline: Instant::now() + Duration::from_secs(3),
        expired: AtomicBool::new(false),
    }));
    let began = Instant::now();
    p.wait_until_expired().await;
    assert_eq!(Instant::now() - began, Duration::from_secs(3));
    assert_eq!(p.ensure_live(), Err(WorkloadError::NotValid));
}

#[test]
fn preparation_disables_all_resumption_and_refuses_invalid_settings() {
    let fixture = Fixture::new();
    let endpoint = fixture.endpoint();
    assert!(!endpoint.config.session_storage.can_cache());
    assert!(!endpoint.config.ticketer.enabled());
    assert_eq!(endpoint.config.send_tls13_tickets, 0);
    assert_eq!(endpoint.config.max_tls13_tickets, 0);
    assert_eq!(endpoint.config.max_early_data_size, 0);
    assert!(!endpoint.config.send_half_rtt_data);
    for (alpn, timeout) in [
        (vec![], Duration::ZERO),
        (vec![], Duration::from_secs(61)),
        (vec![vec![]], Duration::from_secs(1)),
        (vec![vec![0; 256]], Duration::from_secs(1)),
        (vec![vec![1]; 9], Duration::from_secs(1)),
    ] {
        assert!(
            PreparedWorkloadTls::new(
                policy(&fixture.config()),
                fixture.server.cert.pem().as_bytes(),
                fixture.server.key.serialize_pem().as_bytes(),
                alpn,
                timeout
            )
            .is_err()
        );
    }
    assert!(
        PreparedWorkloadTls::new(
            policy(&fixture.config()),
            fixture.server.cert.pem().as_bytes(),
            fixture.client.key.serialize_pem().as_bytes(),
            vec![],
            Duration::from_secs(1)
        )
        .is_err()
    );
}

#[test]
fn chain_size_and_count_are_checked_before_certificate_inspection() {
    let fixture = Fixture::new();
    let policy = policy(&fixture.config());
    let leaf = fixture.client.cert.der();
    assert!(matches!(
        inspect_chain(&policy, leaf, &vec![leaf.clone(); 8], now()),
        Err(WorkloadError::TooLarge)
    ));
    assert!(matches!(
        inspect_chain(
            &policy,
            leaf,
            &[CertificateDer::from(vec![0; MAX_CHAIN_BYTES])],
            now()
        ),
        Err(WorkloadError::TooLarge)
    ));
}
