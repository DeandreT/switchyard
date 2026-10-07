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
    TestResult,
    state::{self, OwnedRules},
};

async fn stage<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
    scenario: AtomScenario,
    applies: usize,
    clocks: usize,
    expected_rules: OwnedRules,
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
        "wrong rule SDK apply count: {scenario:?}"
    );
    assert_eq!(
        fixture.effects().clock - effects.clock,
        clocks,
        "wrong rule SDK clock count: {scenario:?}"
    );
    let after = fixture.snapshot()?;
    state::check_batches(
        &fixture.namespace,
        scenario,
        &before,
        &after,
        &fixture.batches_since(effects),
    )?;
    state::check(
        fixture.machine().store(),
        &fixture.namespace,
        expected_rules,
    )?;
    state::advance(&oracle.handle(), &fixture.namespace, scenario)?;
    oracle.compare(&after)?;
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
            AtomScenario::RulesTlsRefused,
            endpoint,
            ca,
            "manage",
            KEY,
        )
        .await?;
        assert_eq!(fixture.effects(), effects, "rule TLS failure reached owner");
        assert_eq!(fixture.snapshot()?, *before);
    }
    process::run_atom_client(
        dll,
        AtomScenario::RulesDenied,
        &fixture.endpoint,
        &fixture.ca_file,
        "send",
        KEY,
    )
    .await?;
    assert_eq!(
        fixture.effects(),
        effects,
        "rule SEND-only grant reached owner"
    );
    assert_eq!(fixture.snapshot()?, *before);
    Ok(())
}

pub(super) async fn run<P: StoreProvider>(
    fixture: &Fixture<P>,
    oracle: &Oracle,
    dll: &Path,
) -> TestResult {
    state::seed(&fixture.handle(), &fixture.namespace)?;
    state::seed(&oracle.handle(), &fixture.namespace)?;
    state::check(
        fixture.machine().store(),
        &fixture.namespace,
        OwnedRules::Empty,
    )?;
    oracle.compare(&fixture.snapshot()?)?;

    let healthy = fixture.effects();
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesEmpty,
        0,
        2,
        OwnedRules::Empty,
    )
    .await?;
    assert!(
        fixture.effects().reads > healthy.reads,
        "healthy rule control missed owner"
    );
    refused_before_owner(fixture, dll, &fixture.snapshot()?).await?;
    let healthy = fixture.effects();
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesEmpty,
        0,
        2,
        OwnedRules::Empty,
    )
    .await?;
    assert!(
        fixture.effects().reads > healthy.reads,
        "post-refusal healthy rule control missed owner"
    );

    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesCreate,
        4,
        4,
        OwnedRules::Two,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesInspect,
        0,
        0,
        OwnedRules::Two,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesRefusals,
        0,
        2,
        OwnedRules::Two,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesInspect,
        0,
        0,
        OwnedRules::Two,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesOpaque,
        4,
        4,
        OwnedRules::Two,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesDelete,
        4,
        8,
        OwnedRules::Empty,
    )
    .await?;
    stage(
        fixture,
        oracle,
        dll,
        AtomScenario::RulesRecreate,
        2,
        2,
        OwnedRules::DefaultOnly,
    )
    .await
}
