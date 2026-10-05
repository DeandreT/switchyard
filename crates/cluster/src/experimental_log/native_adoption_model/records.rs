use serde::{Deserialize, Serialize};

use super::{wire::*, *};

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Header {
    pub(super) binding: Binding,
    pub(super) seed: [u8; 32],
    pub(super) limits: u8,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Baseline<'a> {
    pub(super) binding: Binding,
    pub(super) ordinal: u64,
    #[serde(borrow)]
    pub(super) metadata: Blob<'a, MAX_NATIVE>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Phase {
    Ready,
    Selected,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct StateFence {
    pub(super) binding: Binding,
    pub(super) phase: Phase,
    pub(super) serial: u64,
    pub(super) seed: [u8; 32],
    pub(super) intent: Option<[u8; 32]>,
    pub(super) selection: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct LogFence {
    pub(super) binding: Binding,
    pub(super) serial: u64,
    pub(super) intent: [u8; 32],
    pub(super) controls: [u8; 32],
    pub(super) entries: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct StageBinding {
    pub(super) binding: Binding,
    pub(super) serial: u64,
    pub(super) intent: [u8; 32],
    pub(super) selection: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct StateRecipe {
    pub(super) binding: Binding,
    pub(super) phase: Phase,
    pub(super) serial: u64,
    pub(super) seed: [u8; 32],
    pub(super) selection: [u8; 32],
}

impl StateRecipe {
    pub(super) fn bind(self, intent: [u8; 32]) -> StateFence {
        StateFence {
            binding: self.binding,
            phase: self.phase,
            serial: self.serial,
            seed: self.seed,
            intent: Some(intent),
            selection: self.selection,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct LogRecipe {
    pub(super) binding: Binding,
    pub(super) serial: u64,
    pub(super) controls: [u8; 32],
    pub(super) entries: [u8; 32],
}

impl LogRecipe {
    pub(super) fn bind(self, intent: [u8; 32]) -> LogFence {
        LogFence {
            binding: self.binding,
            serial: self.serial,
            intent,
            controls: self.controls,
            entries: self.entries,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct CatalogIdentity<'a> {
    #[serde(borrow)]
    pub(super) image: ImageIdentity<'a>,
    #[serde(borrow)]
    pub(super) metadata: Blob<'a, MAX_NATIVE>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum OldJournalPhase {
    SeedReady,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ModelIntent<'a> {
    pub(super) binding: Binding,
    pub(super) serial: u64,
    pub(super) seed: [u8; 32],
    #[serde(borrow)]
    pub(super) old: ImageIdentity<'a>,
    #[serde(borrow)]
    pub(super) old_catalog: Option<CatalogIdentity<'a>>,
    #[serde(borrow)]
    pub(super) selected: ImageIdentity<'a>,
    #[serde(borrow)]
    pub(super) selected_metadata: Blob<'a, MAX_NATIVE>,
    #[serde(borrow)]
    pub(super) selected_native: Blob<'a, MAX_NATIVE>,
    #[serde(borrow)]
    pub(super) old_header: Blob<'a, MAX_CONTROL>,
    #[serde(borrow)]
    pub(super) old_fence: Option<Blob<'a, MAX_CONTROL>>,
    #[serde(borrow)]
    pub(super) old_progress: Blob<'a, MAX_CONTROL>,
    #[serde(borrow)]
    pub(super) old_baseline: Blob<'a, MAX_BASELINE>,
    pub(super) old_entries: EntryManifest,
    pub(super) old_phase: OldJournalPhase,
    pub(super) tail: TailChoice,
    #[serde(borrow)]
    pub(super) final_progress: Blob<'a, MAX_CONTROL>,
    #[serde(borrow)]
    pub(super) final_baseline: Blob<'a, MAX_BASELINE>,
    pub(super) final_entries: EntryManifest,
    #[serde(borrow)]
    pub(super) selected_recipe: Blob<'a, MAX_CONTROL>,
    #[serde(borrow)]
    pub(super) final_recipe: Blob<'a, MAX_CONTROL>,
}

pub(super) fn header(role: Role, bytes: &[u8]) -> Result<Header> {
    let value: Header = frame::decode(Kind::Header, role, bytes, MAX_CONTROL)?;
    value.binding.check()?;
    if value.limits != 1 {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn progress(bytes: &[u8]) -> Result<Progress> {
    let value: Progress = frame::decode(Kind::Progress, Role::Log, bytes, MAX_CONTROL)?;
    if value.entries > crate::MAX_RETAINED_ENTRIES || value.bytes > crate::MAX_RETAINED_BYTES {
        return Err(ModelCodecError::Fields);
    }
    let valid = match value.present {
        None => value.entries == 0 && value.bytes == 0,
        Some(last) if value.entries > 0 && value.bytes > 0 => {
            let count = match value.purged {
                Some(first)
                    if first.index < last.index
                        && native_id(first).leader_id <= native_id(last).leader_id =>
                {
                    last.index.checked_sub(first.index)
                }
                None => last.index.checked_add(1),
                _ => None,
            };
            count == Some(value.entries)
        }
        _ => false,
    };
    if valid {
        Ok(value)
    } else {
        Err(ModelCodecError::Fields)
    }
}

pub(super) fn baseline(bytes: &[u8]) -> Result<Baseline<'_>> {
    let value: Baseline<'_> = frame::decode(Kind::Baseline, Role::Log, bytes, MAX_BASELINE)?;
    value.binding.check()?;
    if value.metadata.0.is_empty() {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn state_fence(bytes: &[u8]) -> Result<StateFence> {
    let value: StateFence = frame::decode(Kind::StateFence, Role::State, bytes, MAX_CONTROL)?;
    value.binding.check()?;
    let valid = match value.phase {
        Phase::Ready => value.serial == 0 && value.intent.is_none(),
        Phase::Selected => value.serial == 1 && value.intent.is_some(),
    };
    if valid {
        Ok(value)
    } else {
        Err(ModelCodecError::Fields)
    }
}

pub(super) fn log_fence(bytes: &[u8]) -> Result<LogFence> {
    let value: LogFence = frame::decode(Kind::LogFence, Role::Log, bytes, MAX_CONTROL)?;
    value.binding.check()?;
    if value.serial != 1 {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn stage_binding(bytes: &[u8]) -> Result<StageBinding> {
    let value: StageBinding = frame::decode(Kind::StageBinding, Role::State, bytes, MAX_CONTROL)?;
    value.binding.check()?;
    if value.serial != 1 {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn state_recipe(bytes: &[u8]) -> Result<StateRecipe> {
    let value: StateRecipe = frame::decode_payload(bytes, MAX_CONTROL)?;
    value.binding.check()?;
    if value.phase != Phase::Selected || value.serial != 1 {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn log_recipe(bytes: &[u8]) -> Result<LogRecipe> {
    let value: LogRecipe = frame::decode_payload(bytes, MAX_CONTROL)?;
    value.binding.check()?;
    if value.serial != 1 {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn native_fields(bytes: &[u8]) -> Result<NativeFields<'_>> {
    let value: NativeFields<'_> = frame::decode_payload(bytes, MAX_NATIVE)?;
    let valid = match value.member_source {
        None => value.member_schema == 0 && value.membership.0.is_empty(),
        Some(_) => {
            value.member_schema == super::super::MEMBERSHIP_SCHEMA_VERSION
                && !value.membership.0.is_empty()
        }
    };
    if !valid {
        return Err(ModelCodecError::Fields);
    }
    Ok(value)
}

pub(super) fn decode_intent(bytes: &[u8]) -> Result<ModelIntent<'_>> {
    let value: ModelIntent<'_> = frame::decode(Kind::Intent, Role::Log, bytes, MAX_INTENT)?;
    value.check_shape()?;
    Ok(value)
}

pub(super) fn encode_intent(value: &ModelIntent<'_>) -> Result<Vec<u8>> {
    value.check_shape()?;
    frame::encode(Kind::Intent, Role::Log, value, MAX_INTENT)
}

impl ModelIntent<'_> {
    pub(super) fn check_shape(&self) -> Result<()> {
        self.binding.check()?;
        if self.serial != 1
            || self.old.fields()?.stream != self.binding.stream
            || self.selected.fields()?.stream != self.binding.stream
        {
            return Err(ModelCodecError::Fields);
        }
        if let Some(catalog) = self.old_catalog
            && catalog.image.fields()?.stream != self.binding.stream
        {
            return Err(ModelCodecError::Fields);
        }
        native_fields(self.selected_native.0)?;
        let header = header(Role::Log, self.old_header.0)?;
        if header.binding != self.binding || header.seed != self.seed {
            return Err(ModelCodecError::Fields);
        }
        if let Some(bytes) = self.old_fence {
            let fence = state_fence(bytes.0)?;
            if fence.binding != self.binding
                || fence.phase != Phase::Ready
                || fence.seed != self.seed
            {
                return Err(ModelCodecError::Fields);
            }
        }
        progress(self.old_progress.0)?;
        progress(self.final_progress.0)?;
        for (bytes, ordinal) in [(self.old_baseline.0, 0), (self.final_baseline.0, 1)] {
            let baseline = baseline(bytes)?;
            if baseline.binding != self.binding || baseline.ordinal != ordinal {
                return Err(ModelCodecError::Fields);
            }
        }
        let state = state_recipe(self.selected_recipe.0)?;
        let log = log_recipe(self.final_recipe.0)?;
        if state.binding != self.binding || state.seed != self.seed || log.binding != self.binding {
            return Err(ModelCodecError::Fields);
        }
        for manifest in [self.old_entries, self.final_entries] {
            if manifest.count > crate::MAX_RETAINED_ENTRIES
                || manifest.bytes > crate::MAX_RETAINED_BYTES
            {
                return Err(ModelCodecError::Fields);
            }
        }
        Ok(())
    }
}

pub(super) fn native_id(id: domain::CommittedEntryId) -> crate::LogId {
    crate::LogId::new(
        openraft::CommittedLeaderId::new(id.term, id.node_id),
        id.index,
    )
}

pub(super) fn committed_id(id: crate::LogId) -> domain::CommittedEntryId {
    domain::CommittedEntryId {
        term: id.leader_id.term,
        node_id: id.leader_id.node_id,
        index: id.index,
    }
}
