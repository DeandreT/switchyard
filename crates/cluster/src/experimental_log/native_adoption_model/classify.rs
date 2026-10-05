use crate::experimental_state_machine::NativeCheckpointSummary;

use super::{records::*, wire::*, *};

pub(super) struct BorrowedPairObservation<'a> {
    pub(super) business: &'a [u8],
    pub(super) state: &'a [Row<'a>],
    pub(super) log: &'a [Row<'a>],
}

pub(super) struct TrustedModelInputs<'a> {
    pub(super) seed: [u8; 32],
    pub(super) selected: ImageIdentity<'a>,
    pub(super) native: Blob<'a, MAX_NATIVE>,
    pub(super) tail: TailChoice,
    pub(super) final_entries: EntryManifest,
    /// Independently supplied exact seed-row material, not a reopened cache.
    pub(super) seed_rows: &'a [Row<'a>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RoleIdentity {
    Old,
    Selected,
    Neither,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StageFact {
    Absent,
    ExactSelected,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Disposition {
    PendingOldPair,
    PendingFinalization,
    RetainedFinalized,
    OrderingInconsistent,
    Halt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PolicyRefusal {
    Trust,
    InitialSelected,
    InitialOld,
    Vote,
    Catalog,
    Tail,
    Fence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ModelClassification {
    pub(super) state: RoleIdentity,
    pub(super) log: RoleIdentity,
    pub(super) stage: StageFact,
    pub(super) disposition: Disposition,
    pub(super) refusal: Option<PolicyRefusal>,
}

struct Controls<'a> {
    rows: &'a [Row<'a>],
    role: Role,
}

impl<'a> Controls<'a> {
    fn capture(rows: &'a [Row<'a>], role: Role) -> Result<Self> {
        let max_rows = if role == Role::State { 7 } else { 261 };
        if rows.len() > max_rows {
            return Err(ModelCodecError::Limit);
        }
        let mut seen = 0u16;
        for row in rows {
            if role == Role::Log && entries::is_entry(*row) {
                if row.key.len() != 9 || row.value.len() > crate::MAX_LOG_ENTRY_BYTES {
                    return Err(ModelCodecError::Limit);
                }
                continue;
            }
            if row.key.len() != 2 || row.key[0] != 0x20 {
                return Err(ModelCodecError::Observation);
            }
            let tag = row.key[1];
            let cap = match (role, tag) {
                (_, 1) | (Role::State, 2 | 3) | (Role::Log, 2 | 5) => MAX_CONTROL,
                (Role::State, 4 | 6) => MAX_NATIVE,
                (Role::State, 5 | 7) => MAX_IMAGE,
                (Role::Log, 3) => MAX_BASELINE,
                (Role::Log, 4) => MAX_INTENT,
                _ => return Err(ModelCodecError::Observation),
            };
            let bit = 1u16 << tag;
            if seen & bit != 0 {
                return Err(ModelCodecError::Observation);
            }
            seen |= bit;
            if row.value.len() > cap {
                return Err(ModelCodecError::Limit);
            }
        }
        if role == Role::Log {
            entries::manifest(rows)?;
        }
        Ok(Self { rows, role })
    }

    fn get(&self, tag: u8) -> Option<&'a [u8]> {
        self.rows
            .iter()
            .find(|row| row.key == [0x20, tag])
            .map(|row| row.value)
    }

    fn need(&self, tag: u8) -> Result<&'a [u8]> {
        self.get(tag).ok_or(ModelCodecError::Observation)
    }

    fn borrowed_shape(&self) -> Result<()> {
        header(self.role, self.need(1)?)?;
        match self.role {
            Role::State => {
                if let Some(bytes) = self.get(2) {
                    state_fence(bytes)?;
                }
                if let Some(bytes) = self.get(3) {
                    stage_binding(bytes)?;
                }
            }
            Role::Log => {
                progress(self.need(2)?)?;
                baseline(self.need(3)?)?;
                decode_intent(self.need(4)?)?;
                if let Some(bytes) = self.get(5) {
                    log_fence(bytes)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn classify(
    bytes: &[u8],
    observation: BorrowedPairObservation<'_>,
    trust: TrustedModelInputs<'_>,
) -> Result<ModelClassification> {
    if observation.business.len() > MAX_IMAGE {
        return Err(ModelCodecError::Limit);
    }
    let state = Controls::capture(observation.state, Role::State)?;
    let log = Controls::capture(observation.log, Role::Log)?;
    let intent = decode_intent(bytes)?;
    state.borrowed_shape()?;
    log.borrowed_shape()?;
    trust.selected.fields()?;
    native_fields(trust.native.0)?;
    if trust.seed_rows.len() > crate::MAX_RETAINED_ENTRIES as usize {
        return Err(ModelCodecError::Limit);
    }
    if trust.seed_rows.iter().any(|row| !entries::is_entry(*row)) {
        return Err(ModelCodecError::Observation);
    }
    let seed_entries = entries::manifest(trust.seed_rows)?;
    // Only now invoke existing native/image semantic recovery or allocate copies.
    validate_observed(&state, &log, observation.business)?;
    let old_progress = progress(intent.old_progress.0)?;
    let old_baseline = baseline(intent.old_baseline.0)?;
    let old_summary = NativeCheckpointSummary::decode(old_baseline.metadata.0)
        .map_err(|_| ModelCodecError::Fields)?;
    if old_summary.stream.as_bytes() != &intent.binding.stream || old_summary.artifact_bytes < 60 {
        return Err(ModelCodecError::Fields);
    }
    let final_baseline = baseline(intent.final_baseline.0)?;
    identity::summary(final_baseline.metadata.0, intent.selected)?;
    let selected_summary = identity::summary(intent.selected_metadata.0, intent.selected)?;
    if final_baseline.metadata != intent.selected_metadata
        || !identity::native_matches(
            intent.selected_metadata.0,
            intent.selected,
            intent.selected_native.0,
        )?
    {
        return Err(ModelCodecError::Fields);
    }
    if let Some(catalog) = intent.old_catalog {
        identity::summary(catalog.metadata.0, catalog.image)?;
    }
    let expected_state = state_recipe(intent.selected_recipe.0)?.bind(digest(bytes));
    let expected_log = log_recipe(intent.final_recipe.0)?.bind(digest(bytes));
    if expected_state.selection != identity::selected_selection(&intent)?
        || expected_log.controls != identity::final_controls(&intent)?
        || expected_log.entries != intent.final_entries.digest
    {
        return Err(ModelCodecError::Fields);
    }
    let expected_header = header(Role::Log, intent.old_header.0)?;
    let binding_ok = header(Role::State, state.need(1)?)? == expected_header
        && header(Role::Log, log.need(1)?)? == expected_header
        && log.need(4)? == bytes;
    let state_identity = if !binding_ok {
        RoleIdentity::Neither
    } else {
        state_identity(&state, observation.business, &intent, expected_state)?
    };
    let log_identity = if !binding_ok {
        RoleIdentity::Neither
    } else {
        log_identity(&log, &intent, expected_log)?
    };
    let stage = stage_fact(&state, &intent, digest(bytes))?;
    let refusal = policy(
        &intent,
        &trust,
        seed_entries,
        old_progress,
        &old_summary,
        &selected_summary,
    )?;
    let disposition = match (state_identity, log_identity, stage, refusal) {
        (_, _, StageFact::Invalid, _) | (_, _, _, Some(_)) => Disposition::Halt,
        (RoleIdentity::Old, RoleIdentity::Old, _, None) => Disposition::PendingOldPair,
        (RoleIdentity::Selected, RoleIdentity::Old, StageFact::ExactSelected, None) => {
            Disposition::PendingFinalization
        }
        (RoleIdentity::Selected, RoleIdentity::Selected, StageFact::ExactSelected, None) => {
            Disposition::RetainedFinalized
        }
        (RoleIdentity::Old, RoleIdentity::Selected, _, None)
        | (RoleIdentity::Selected, _, StageFact::Absent, None) => Disposition::OrderingInconsistent,
        (_, RoleIdentity::Selected, StageFact::Absent, None) => Disposition::OrderingInconsistent,
        _ => Disposition::Halt,
    };
    Ok(ModelClassification {
        state: state_identity,
        log: log_identity,
        stage,
        disposition,
        refusal,
    })
}

fn validate_observed(state: &Controls<'_>, log: &Controls<'_>, business: &[u8]) -> Result<()> {
    let state_header = header(Role::State, state.need(1)?)?;
    let metadata = crate::EncodedNativeSnapshotMetadata::encode(business)
        .map_err(|_| ModelCodecError::Observation)?;
    let body = NativeCheckpointSummary::decode(metadata.as_bytes())
        .map_err(|_| ModelCodecError::Observation)?;
    if body.stream.as_bytes() != &state_header.binding.stream {
        return Err(ModelCodecError::Observation);
    }
    match (state.get(6), state.get(7)) {
        (None, None) => {}
        (Some(metadata), Some(artifact)) => {
            let catalog = crate::DecodedNativeSnapshotPair::decode(metadata, artifact)
                .map_err(|_| ModelCodecError::Observation)?;
            if catalog.checkpoint().stream().as_bytes() != &state_header.binding.stream {
                return Err(ModelCodecError::Observation);
            }
        }
        _ => return Err(ModelCodecError::Observation),
    }
    let log_header = header(Role::Log, log.need(1)?)?;
    let baseline = baseline(log.need(3)?)?;
    let summary = NativeCheckpointSummary::decode(baseline.metadata.0)
        .map_err(|_| ModelCodecError::Observation)?;
    if baseline.binding != log_header.binding
        || summary.stream.as_bytes() != &log_header.binding.stream
        || summary.artifact_bytes < 60
    {
        return Err(ModelCodecError::Observation);
    }
    entries::validate(log.rows, progress(log.need(2)?)?, &summary)?;
    Ok(())
}

fn catalog_matches(state: &Controls<'_>, expected: Option<CatalogIdentity<'_>>) -> Result<bool> {
    match (state.get(6), state.get(7), expected) {
        (None, None, None) => Ok(true),
        (Some(metadata), Some(artifact), Some(expected)) => {
            crate::DecodedNativeSnapshotPair::decode(metadata, artifact)
                .map_err(|_| ModelCodecError::Observation)?;
            Ok(metadata == expected.metadata.0
                && identity::image_matches(artifact, expected.image)?)
        }
        (Some(metadata), Some(artifact), None) => {
            crate::DecodedNativeSnapshotPair::decode(metadata, artifact)
                .map_err(|_| ModelCodecError::Observation)?;
            Ok(false)
        }
        (None, None, Some(_)) => Ok(false),
        _ => Err(ModelCodecError::Observation),
    }
}

fn state_identity(
    state: &Controls<'_>,
    business: &[u8],
    intent: &ModelIntent<'_>,
    expected: StateFence,
) -> Result<RoleIdentity> {
    let Some(fence) = state.get(2) else {
        return Ok(RoleIdentity::Neither);
    };
    let actual = state_fence(fence)?;
    if actual == expected
        && identity::image_matches(business, intent.selected)?
        && catalog_matches(
            state,
            Some(CatalogIdentity {
                image: intent.selected,
                metadata: intent.selected_metadata,
            }),
        )?
    {
        return Ok(RoleIdentity::Selected);
    }
    if intent.old_fence.is_some_and(|old| old.0 == fence)
        && actual.selection == identity::old_selection(intent)?
        && identity::image_matches(business, intent.old)?
        && catalog_matches(state, intent.old_catalog)?
    {
        return Ok(RoleIdentity::Old);
    }
    // Even a neither image must be semantically valid, not a digest-only blob.
    crate::EncodedNativeSnapshotMetadata::encode(business)
        .map_err(|_| ModelCodecError::Observation)?;
    Ok(RoleIdentity::Neither)
}

fn log_identity(
    log: &Controls<'_>,
    intent: &ModelIntent<'_>,
    expected: LogFence,
) -> Result<RoleIdentity> {
    let inventory = entries::manifest(log.rows)?;
    let actual_progress = progress(log.need(2)?)?;
    let actual_baseline = baseline(log.need(3)?)?;
    if actual_baseline.binding != intent.binding {
        return Ok(RoleIdentity::Neither);
    }
    let summary = NativeCheckpointSummary::decode(actual_baseline.metadata.0)
        .map_err(|_| ModelCodecError::Observation)?;
    entries::validate(log.rows, actual_progress, &summary)?;
    match log.get(5) {
        None if log.need(2)? == intent.old_progress.0
            && log.need(3)? == intent.old_baseline.0
            && inventory == intent.old_entries =>
        {
            Ok(RoleIdentity::Old)
        }
        Some(bytes)
            if log_fence(bytes)? == expected
                && log.need(2)? == intent.final_progress.0
                && log.need(3)? == intent.final_baseline.0
                && inventory == intent.final_entries =>
        {
            Ok(RoleIdentity::Selected)
        }
        _ => Ok(RoleIdentity::Neither),
    }
}

fn stage_fact(state: &Controls<'_>, intent: &ModelIntent<'_>, hash: [u8; 32]) -> Result<StageFact> {
    match (state.get(3), state.get(4), state.get(5)) {
        (None, None, None) => Ok(StageFact::Absent),
        (Some(binding), Some(metadata), Some(artifact)) => {
            let binding = stage_binding(binding)?;
            if binding.binding != intent.binding
                || binding.intent != hash
                || binding.selection != identity::selected_selection(intent)?
                || metadata != intent.selected_metadata.0
            {
                return Ok(StageFact::Invalid);
            }
            match identity::image_matches(artifact, intent.selected) {
                Ok(true) => Ok(StageFact::ExactSelected),
                Ok(false) | Err(_) => Ok(StageFact::Invalid),
            }
        }
        _ => Ok(StageFact::Invalid),
    }
}

fn policy(
    intent: &ModelIntent<'_>,
    trust: &TrustedModelInputs<'_>,
    seed_entries: EntryManifest,
    old_progress: Progress,
    old: &NativeCheckpointSummary,
    selected: &NativeCheckpointSummary,
) -> Result<Option<PolicyRefusal>> {
    if intent.seed != trust.seed
        || intent.seed != identity::seed_manifest(intent)?
        || intent.selected != trust.selected
        || intent.selected_native != trust.native
        || intent.tail != trust.tail
        || intent.final_entries != trust.final_entries
        || intent.old_entries != seed_entries
    {
        return Ok(Some(PolicyRefusal::Trust));
    }
    if selected.last.is_none() || selected.membership.is_none() {
        return Ok(Some(PolicyRefusal::InitialSelected));
    }
    let fields = intent.old.fields()?;
    if fields.last.is_none()
        || fields.membership.is_none()
        || old.last.is_none()
        || old.membership.is_none()
    {
        return Ok(Some(PolicyRefusal::InitialOld));
    }
    entries::validate(trust.seed_rows, old_progress, old)?;
    let final_progress = progress(intent.final_progress.0)?;
    if old_progress.vote != final_progress.vote
        || !entries::vote_sufficient(
            old_progress.vote,
            old.last.ok_or(ModelCodecError::Fields)?.id,
            trust.seed_rows,
        )?
        || !entries::vote_sufficient(
            old_progress.vote,
            selected.last.ok_or(ModelCodecError::Fields)?.id,
            trust.seed_rows,
        )?
    {
        return Ok(Some(PolicyRefusal::Vote));
    }
    let catalog_ok = match intent.old_catalog {
        None => identity::summary(baseline(intent.old_baseline.0)?.metadata.0, intent.old).is_ok(),
        Some(catalog) if catalog.image == intent.old => {
            entries::reaches_old(trust.seed_rows, old, fields)?
        }
        Some(catalog) => {
            let cat_fields = catalog.image.fields()?;
            cat_fields != fields
                && catalog.metadata.0 == baseline(intent.old_baseline.0)?.metadata.0
                && cat_fields.last.map(|mark| mark.id.index) < fields.last.map(|mark| mark.id.index)
                && entries::reaches_old(trust.seed_rows, old, fields)?
        }
    };
    if !catalog_ok {
        return Ok(Some(PolicyRefusal::Catalog));
    }
    let Some(old_fence) = intent.old_fence else {
        return Ok(Some(PolicyRefusal::Fence));
    };
    if state_fence(old_fence.0)?.selection != identity::old_selection(intent)? {
        return Ok(Some(PolicyRefusal::Fence));
    }
    let tail_ok = match intent.tail {
        TailChoice::ExactResetEmpty => {
            intent.final_entries == entries::manifest(&[])? && final_progress.present.is_none()
        }
        TailChoice::ExactContinuation => {
            let selected = selected.last.ok_or(ModelCodecError::Fields)?.id;
            let low = old_progress.purged.ok_or(ModelCodecError::Fields)?;
            let high = old_progress.present.unwrap_or(low);
            let anchor = if selected.index == low.index {
                selected == low
            } else {
                trust
                    .seed_rows
                    .iter()
                    .find(|row| row.key == entries::key(selected.index))
                    .map(|row| {
                        super::super::codec::decode_entry(row.value)
                            .map(|entry| committed_id(entry.log_id) == selected)
                    })
                    .transpose()
                    .map_err(|_| ModelCodecError::Observation)?
                    .unwrap_or(false)
            };
            anchor
                && selected.index >= low.index
                && selected.index <= high.index
                && match entries::suffix_manifest(trust.seed_rows, selected) {
                    Ok((manifest, present)) => {
                        manifest == intent.final_entries && present == final_progress.present
                    }
                    Err(_) => false,
                }
        }
    };
    if !tail_ok {
        return Ok(Some(PolicyRefusal::Tail));
    }
    if final_progress.entries != intent.final_entries.count
        || final_progress.bytes != intent.final_entries.bytes
        || final_progress.purged != selected.last.map(|mark| mark.id)
    {
        return Ok(Some(PolicyRefusal::Tail));
    }
    Ok(None)
}
