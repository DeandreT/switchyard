use std::path::Path;

use storage::StoreSnapshot;
use testkit::StoreProvider;

use super::{
    super::{
        KEY,
        fixture::Fixture,
        postconditions::Oracle,
        process::{self, AtomScenario},
    },
    TestResult, state,
};

async fn stage<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
    scenario: AtomScenario,
    applies: usize,
    clocks: usize,
) -> TestResult {
    let before = fixture.snapshot()?;
    let effects = fixture.effects();
    process::run_atom_client(
        dll,
        scenario,
        &fixture.endpoint,
        &fixture.ca_file,
        "manage",
        KEY,
    )
    .await?;
    assert_eq!(
        fixture.effects().applies - effects.applies,
        applies,
        "wrong subscription SDK apply count: {scenario:?}"
    );
    assert_eq!(
        fixture.effects().clock - effects.clock,
        clocks,
        "wrong subscription SDK clock count: {scenario:?}"
    );
    assert_eq!(fixture.batches_since(effects).len(), applies);
    if applies == 0 {
        assert_eq!(
            fixture.snapshot()?,
            before,
            "unchanged subscription stage mutated state"
        );
    }
    state::advance(&oracle.handle(), &fixture.namespace, scenario)?;
    oracle.compare(&fixture.snapshot()?)?;
    Ok(())
}

async fn refused_before_owner<P: StoreProvider>(
    fixture: &Fixture<P>,
    dll: &Path,
    before: &StoreSnapshot,
) -> TestResult {
    let effects = fixture.effects();
    for (endpoint, ca) in [
        (&fixture.endpoint, &fixture.wrong_ca_file),
        (&fixture.wrong_name_endpoint, &fixture.ca_file),
    ] {
        process::run_atom_client(
            dll,
            AtomScenario::SubscriptionsTlsRefused,
            endpoint,
            ca,
            "manage",
            KEY,
        )
        .await?;
        assert_eq!(
            fixture.effects(),
            effects,
            "subscription TLS failure reached owner"
        );
        assert_eq!(fixture.snapshot()?, *before);
    }
    process::run_atom_client(
        dll,
        AtomScenario::SubscriptionsDenied,
        &fixture.endpoint,
        &fixture.ca_file,
        "send",
        KEY,
    )
    .await?;
    assert_eq!(
        fixture.effects(),
        effects,
        "subscription SEND-only grant reached owner"
    );
    assert_eq!(fixture.snapshot()?, *before);
    Ok(())
}

pub(super) async fn run<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
) -> TestResult {
    state::seed_parent(&fixture.handle(), &fixture.namespace)?;
    state::seed_parent(&oracle.handle(), &fixture.namespace)?;
    state::check_parent(fixture.machine().store(), &fixture.namespace, false)?;
    oracle.compare(&fixture.snapshot()?)?;

    let healthy = fixture.effects();
    stage(fixture, oracle, dll, AtomScenario::SubscriptionsEmpty, 0, 2).await?;
    assert!(
        fixture.effects().reads > healthy.reads,
        "healthy subscription control missed owner"
    );
    refused_before_owner(fixture, dll, &fixture.snapshot()?).await?;
    let healthy = fixture.effects();
    stage(fixture, oracle, dll, AtomScenario::SubscriptionsEmpty, 0, 2).await?;
    assert!(
        fixture.effects().reads > healthy.reads,
        "post-refusal healthy control missed owner"
    );

    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsCreate,
        4,
        4,
    )
    .await?;
    state::check_created(fixture.machine().store(), &fixture.namespace)?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsInspect,
        0,
        0,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsRefusals,
        0,
        2,
    )
    .await?;

    state::seed_retention(&fixture.handle(), &fixture.namespace)?;
    state::seed_retention(&oracle.handle(), &fixture.namespace)?;
    state::check_retained(fixture.machine().store(), &fixture.namespace)?;
    oracle.compare(&fixture.snapshot()?)?;
    // Get returns only the static definition, never reconstructed rule or runtime state.
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsInspect,
        0,
        0,
    )
    .await?;
    state::check_retained(fixture.machine().store(), &fixture.namespace)?;
    let before_update = fixture.snapshot()?;
    let update_effects = fixture.effects();
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsUpdate,
        4,
        8,
    )
    .await?;
    state::check_updated(
        fixture.machine().store(),
        &fixture.namespace,
        &before_update,
        &fixture.batches_since(update_effects),
    )?;
    let retained = fixture.snapshot()?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsDelete,
        4,
        8,
    )
    .await?;
    state::check_deleted(fixture.machine().store(), &fixture.namespace, &retained)?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::SubscriptionsRecreate,
        2,
        2,
    )
    .await?;
    state::check_final(fixture.machine().store(), &fixture.namespace)?;
    state::unaffected(&fixture.namespace, &retained, &fixture.snapshot()?)?;
    Ok(())
}
