use crate::{BrokerError, MessageEnvelope};

use super::{SqlActionProgram, literals::ActionValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlActionError {
    TypeMismatch,
    UnsupportedTargetType,
    NumericOverflow,
}

impl SqlActionError {
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::TypeMismatch => "TypeMismatch",
            Self::UnsupportedTargetType => "UnsupportedTargetType",
            Self::NumericOverflow => "NumericOverflow",
        }
    }
}

#[derive(Debug)]
pub(crate) struct CheckedSqlAction {
    overrides: Vec<(usize, Option<ActionValue>)>,
    error: Option<SqlActionError>,
    property_count: usize,
    measurements: (usize, usize, usize),
    peak: (usize, usize, usize),
}

impl CheckedSqlAction {
    pub(crate) const fn error(&self) -> Option<SqlActionError> {
        self.error
    }
    pub(crate) const fn measurements(&self) -> (usize, usize, usize) {
        self.measurements
    }
    pub(crate) const fn peak(&self) -> (usize, usize, usize) {
        self.peak
    }
    pub(crate) const fn has_properties(&self) -> bool {
        self.property_count != 0
    }

    fn override_value(&self, program: &SqlActionProgram, key: &str) -> Option<Option<ActionValue>> {
        self.overrides
            .iter()
            .find_map(|(index, value)| (program.targets[*index] == key).then_some(*value))
    }

    pub(crate) fn property_cost(
        &self,
        program: &SqlActionProgram,
        envelope: Option<&MessageEnvelope>,
        key: &str,
    ) -> (usize, usize, usize) {
        match self.override_value(program, key) {
            Some(Some(value)) => (
                5_usize
                    .saturating_add(key.len())
                    .saturating_add(value.content_size(program)),
                1,
                1,
            ),
            Some(None) => (0, 0, 0),
            None => envelope
                .and_then(|envelope| envelope.application_properties.get(key))
                .map_or((0, 0, 0), |value| {
                    (
                        5_usize
                            .saturating_add(key.len())
                            .saturating_add(value.content_size()),
                        MessageEnvelope::action_value_items(value),
                        1,
                    )
                }),
        }
    }
}

impl SqlActionProgram {
    /// The plan retains indices and scalar constructors, never input payloads.
    pub(crate) fn check(
        &self,
        message_id: &str,
        body_bytes: usize,
        envelope: Option<&MessageEnvelope>,
    ) -> Result<CheckedSqlAction, BrokerError> {
        let (baseline, original_properties, mut property_count) =
            MessageEnvelope::action_measurement_baseline(message_id, body_bytes, envelope)?;
        let original_count = property_count;
        let mut property_bytes =
            original_properties.saturating_sub(if property_count == 0 { 0 } else { 19 });
        let mut checked = CheckedSqlAction {
            overrides: Vec::with_capacity(self.targets.len()),
            error: None,
            property_count,
            measurements: baseline,
            peak: (0, 0, 0),
        };
        for (index, (target, literal)) in self.targets.iter().zip(&self.values).enumerate() {
            let old = checked.property_cost(self, envelope, target);
            let value = if let Some(literal) = literal {
                let current = checked.override_value(self, target);
                let original = if current.is_none() {
                    envelope.and_then(|envelope| envelope.application_properties.get(target))
                } else {
                    None
                };
                match literal.convert(index, current.flatten(), original) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        checked.overrides.clear();
                        checked.error = Some(error);
                        checked.property_count = original_count;
                        checked.measurements = baseline;
                        return Ok(checked);
                    }
                }
            } else {
                None
            };
            if let Some(entry) = checked
                .overrides
                .iter_mut()
                .find(|(prior, _)| self.targets[*prior] == *target)
            {
                *entry = (index, value);
            } else {
                checked.overrides.push((index, value));
            }
            let new = checked.property_cost(self, envelope, target);
            property_bytes = property_bytes.saturating_sub(old.0).saturating_add(new.0);
            property_count = property_count.saturating_sub(old.2).saturating_add(new.2);
            checked.property_count = property_count;
            let properties =
                property_bytes.saturating_add(if property_count == 0 { 0 } else { 19 });
            checked.measurements = (
                baseline
                    .0
                    .saturating_sub(original_properties)
                    .saturating_add(properties),
                checked
                    .measurements
                    .1
                    .saturating_sub(old.1)
                    .saturating_add(new.1),
                baseline
                    .2
                    .saturating_sub(original_properties)
                    .saturating_add(properties),
            );
            if self.semantic_version == 2 {
                MessageEnvelope::require_action_measurement(checked.measurements)?;
                checked.peak.0 = checked.peak.0.max(checked.measurements.0);
                checked.peak.1 = checked.peak.1.max(checked.measurements.1);
                checked.peak.2 = checked.peak.2.max(checked.measurements.2);
            }
        }
        Ok(checked)
    }

    pub(crate) fn literal_bytes(&self) -> usize {
        self.values
            .iter()
            .flatten()
            .fold(0_usize, |bytes, value| bytes.saturating_add(value.bytes()))
    }

    pub(crate) fn apply_checked(&self, checked: &CheckedSqlAction, envelope: &mut MessageEnvelope) {
        for (index, value) in &checked.overrides {
            let target = &self.targets[*index];
            match value {
                Some(value) => {
                    envelope
                        .application_properties
                        .insert(target.clone(), value.materialize(self));
                }
                None => {
                    envelope.application_properties.remove(target);
                }
            }
        }
    }
}
