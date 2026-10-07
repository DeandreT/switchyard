use std::path::Path;

use domain::{BrokerError, CommandKind, EntityPath, NamespaceName, QueueCapacityStatus};
use server::{ProposeError, SubmitError};
use testkit::StoreProvider;

use super::{
    KEY, TestResult,
    fixture::{Effects, Fixture},
    postconditions::{self, Oracle, SUFFIXES},
    process::{self, AtomScenario},
};

async fn child<P: StoreProvider>(
    fixture: &Fixture<P>,
    dll: &Path,
    scenario: AtomScenario,
) -> TestResult {
    process::run_atom_client(
        dll,
        scenario,
        &fixture.endpoint,
        &fixture.ca_file,
        "manage",
        KEY,
    )
    .await?;
    Ok(())
}

fn unchanged<P: StoreProvider>(
    fixture: &Fixture<P>,
    before: &storage::StoreSnapshot,
    effects: Effects,
) -> TestResult {
    assert_eq!(
        fixture.snapshot()?,
        *before,
        "refused or unchanged SDK request mutated committed state"
    );
    assert_eq!(
        fixture.effects().applies,
        effects.applies,
        "refused or unchanged SDK request applied a batch"
    );
    Ok(())
}

async fn stage<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
    scenario: AtomScenario,
    commits: usize,
    clocks: usize,
) -> TestResult {
    let before = fixture.snapshot()?;
    let effects = fixture.effects();
    child(fixture, dll, scenario).await?;
    assert_eq!(
        fixture.effects().applies - effects.applies,
        commits,
        "wrong SDK stage commit count: {scenario:?}"
    );
    assert_eq!(
        fixture.effects().clock - effects.clock,
        clocks,
        "wrong host-clock count: {scenario:?}"
    );
    if commits == 0 {
        unchanged(fixture, &before, effects)?;
    }
    oracle.advance(&fixture.namespace, scenario)?;
    oracle.compare(&fixture.snapshot()?)?;
    Ok(())
}

pub(super) async fn crud<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
) -> TestResult {
    // Establish the same-listener healthy control before interpreting TLS refusals.
    let healthy_effects = fixture.effects();
    stage(fixture, oracle, dll, AtomScenario::Empty, 0, 0).await?;
    assert!(
        fixture.effects().reads > healthy_effects.reads,
        "healthy TLS control never reached the owner"
    );
    let initial = fixture.snapshot()?;
    let effects = fixture.effects();
    for (endpoint, ca) in [
        (&fixture.endpoint, &fixture.wrong_ca_file),
        (&fixture.wrong_name_endpoint, &fixture.ca_file),
    ] {
        process::run_atom_client(dll, AtomScenario::TlsRefused, endpoint, ca, "manage", KEY)
            .await?;
        assert_eq!(
            fixture.effects(),
            effects,
            "TLS failure reached substantive owner work"
        );
        assert_eq!(fixture.snapshot()?, initial);
    }
    process::run_atom_client(
        dll,
        AtomScenario::Denied,
        &fixture.endpoint,
        &fixture.ca_file,
        "send",
        KEY,
    )
    .await?;
    assert_eq!(
        fixture.effects(),
        effects,
        "SEND-only grant reached substantive owner work"
    );
    assert_eq!(fixture.snapshot()?, initial);
    stage(fixture, oracle, dll, AtomScenario::Create, 4, 4).await?;
    stage(fixture, oracle, dll, AtomScenario::Update, 4, 4).await?;
    stage(fixture, oracle, dll, AtomScenario::Noop, 0, 4).await?;
    // Duplicate creation uses the existing creation path; unsupported XML is rejected before admission.
    stage(fixture, oracle, dll, AtomScenario::Refusals, 0, 2).await?;

    postconditions::seed_quota(&fixture.handle(), &fixture.namespace)?;
    postconditions::seed_quota(&oracle.handle(), &fixture.namespace)?;
    oracle.compare(&fixture.snapshot()?)?;
    for suffix in SUFFIXES {
        let view = fixture
            .handle()
            .get_atom_finite_queue_blocking(
                fixture.namespace.clone(),
                EntityPath::new(format!("sdk-atom-quota-{suffix}"))?,
            )?
            .expect("real quota seed");
        let QueueCapacityStatus::FiniteV1 {
            reserved_bytes,
            message_count,
            ..
        } = view.capacity
        else {
            panic!("quota seed lost finite mode");
        };
        assert_eq!(message_count, 5);
        assert!(reserved_bytes > postconditions::mib(1).bytes());
    }
    stage(fixture, oracle, dll, AtomScenario::Quota, 0, 2).await?;

    postconditions::seed_retention(&fixture.handle(), &fixture.namespace)?;
    postconditions::seed_retention(&oracle.handle(), &fixture.namespace)?;
    oracle.compare(&fixture.snapshot()?)?;
    let retained_before = fixture.snapshot()?;
    let retained_effects = fixture.effects();
    stage(fixture, oracle, dll, AtomScenario::Retention, 2, 2).await?;
    postconditions::retained_update_only(
        &fixture.namespace,
        &retained_before,
        &fixture.snapshot()?,
        &fixture.batches_since(retained_effects),
    )?;
    for suffix in SUFFIXES {
        let before = fixture.snapshot()?;
        let effects = fixture.effects();
        let error = fixture
            .handle()
            .submit_blocking(
                fixture.namespace.clone(),
                EntityPath::new(format!("sdk-atom-retention-{suffix}"))?,
                CommandKind::Send {
                    message_id: format!("sdk-future-too-large-{suffix}"),
                    body: vec![0x66; 5 * 1024],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )
            .expect_err("future message must use the newly lowered maximum");
        assert!(matches!(
            error,
            SubmitError::Propose(ProposeError::Broker(BrokerError::MessageTooLarge { .. }))
        ));
        unchanged(fixture, &before, effects)?;
    }
    oracle.compare(&fixture.snapshot()?)?;
    stage(fixture, oracle, dll, AtomScenario::Delete, 6, 6).await?;
    postconditions::check_retained(fixture.machine().store(), &fixture.namespace)?;
    Ok(())
}

pub(super) async fn paging<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
) -> TestResult {
    let page =
        fixture
            .handle()
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 0, 100)?;
    assert!(
        page.is_empty(),
        "paging must begin in its own empty namespace"
    );
    let other_namespace = NamespaceName::new("tenant")?;
    let before = fixture.snapshot()?;
    let effects = fixture.effects();
    stage(fixture, oracle, dll, AtomScenario::Paging, 202, 202).await?;
    assert!(
        fixture
            .handle()
            .atom_finite_queues_page_blocking(fixture.namespace.clone(), 0, 100)?
            .is_empty()
    );
    let unaffected = |snapshot: &storage::StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| {
                domain::keys::entity_scope_parts(key)
                    .is_some_and(|(namespace, _)| namespace == other_namespace.as_str())
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        unaffected(&before),
        unaffected(&fixture.snapshot()?),
        "paging changed the CRUD namespace"
    );
    assert_eq!(fixture.batches_since(effects).len(), 202);
    Ok(())
}
