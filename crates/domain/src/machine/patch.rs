//! Copy-local content rewrites carried by abandon, defer and dead-letter.

use crate::{
    BrokerError, CONTENT_LIMITS, MessageContentError, MessageEnvelope, MessageRecord,
    MessageRecordFormat, OverlayProperty, PROPERTY_PATCH_HEADROOM_BYTES, QueueConfig, content,
};

use super::deferred::replace_envelope;

/// How a disposition rewrites the locked copy before it moves.
#[derive(Clone, Copy)]
pub(super) enum ContentRewrite<'a> {
    /// Legacy V1 intent: an optional whole replacement envelope.
    Replace(Option<&'a MessageEnvelope>),
    /// V4 intent: a sparse application-property patch over the stored copy.
    Patch(&'a [OverlayProperty]),
}

impl ContentRewrite<'_> {
    /// Rewrites the held record in place. Callers stage nothing before this,
    /// so a refusal leaves the stored copy, its lock and delivery count as
    /// they were.
    pub(super) fn apply(
        self,
        config: &QueueConfig,
        record: &mut MessageRecord,
    ) -> Result<(), BrokerError> {
        match self {
            Self::Replace(replacement) => replace_envelope(config, record, replacement),
            Self::Patch(patch) => patch_properties(config, record, patch),
        }
    }
}

/// Applies a patch to this copy only: siblings are separate records. Original
/// bytes never change; the overlay coalesces by name and the filter projection
/// is re-derived from the effective properties. A V1 record becomes V2, with
/// its envelope as the original and its redundant body dropped.
pub(super) fn patch_properties(
    config: &QueueConfig,
    record: &mut MessageRecord,
    patch: &[OverlayProperty],
) -> Result<(), BrokerError> {
    // Nothing to apply: the copy keeps its content and record format.
    if patch.is_empty() {
        return Ok(());
    }
    let Some(envelope) = record.envelope.as_ref() else {
        return Err(MessageContentError::PatchWithoutAmqpContent.into());
    };
    let overlay = content::patch_overlay(envelope.overlay(), patch)?;
    let removals = envelope
        .removals()
        .iter()
        .filter(|name| !patch.iter().any(|p| &p.name == *name))
        .cloned()
        .collect::<Vec<_>>();
    let is_v4 = matches!(record.format, MessageRecordFormat::V4 { .. });
    let effective_bytes = if is_v4 {
        content::effective_copy_len(envelope.as_bytes(), &overlay, &removals)
    } else {
        content::effective_len(envelope.as_bytes(), &overlay)
    };
    // The content-safety ceiling caps the allowance for entities with large
    // limits, so every copy a patch leaves can still be rendered and read.
    let maximum_bytes = config
        .max_message_bytes
        .saturating_add(PROPERTY_PATCH_HEADROOM_BYTES)
        .min(CONTENT_LIMITS.max_bytes);
    if effective_bytes > maximum_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: effective_bytes,
            maximum_bytes,
        });
    }
    // Keep historical V4 replay's legacy re-derivation for V1/V2, and derive
    // V3 IDs in Azure text form. Both parse original typed bytes; neither
    // guesses a type from a retained projection string.
    let projection = if is_v4 {
        content::effective_projection_with_removals(
            envelope.as_bytes(),
            &overlay,
            &removals,
            record.format.uses_text_ids(),
        )?
    } else {
        content::effective_projection(envelope.as_bytes(), &overlay, record.format.uses_text_ids())?
    };
    let original = record
        .envelope
        .take()
        .expect("the envelope was read above")
        .into_bytes();
    record.envelope = Some(
        MessageEnvelope::new(original)
            .with_filter_properties(projection)
            .with_overlay(overlay)
            .with_removals(removals),
    );
    record.body.clear();
    if !is_v4 && record.format != MessageRecordFormat::V3 {
        record.format = MessageRecordFormat::V2;
    }
    Ok(())
}

/// Applies an already atomic kernel result to a private copy. Even an empty
/// result is fenced V4: old readers must not infer action-copy semantics.
pub(super) fn apply_action(
    config: &QueueConfig,
    record: &mut MessageRecord,
    patch: crate::SqlActionPatch,
) -> Result<(), BrokerError> {
    let envelope = record
        .envelope
        .as_ref()
        .ok_or(MessageContentError::PatchWithoutAmqpContent)?;
    let mut overlay = envelope.overlay().to_vec();
    let mut removals = envelope.removals().to_vec();
    for entry in patch.into_entries() {
        match entry {
            crate::SqlActionProperty::Remove { name } => {
                overlay.retain(|p| p.name != name);
                if !removals.contains(&name) {
                    removals.push(name);
                }
            }
            crate::SqlActionProperty::Set { name, value } => {
                removals.retain(|n| n != &name);
                let value = content::encode_correlation_value(&value)?;
                if let Some(property) = overlay.iter_mut().find(|p| p.name == name) {
                    property.value = value;
                } else {
                    overlay.push(OverlayProperty { name, value });
                }
            }
        }
    }
    let effective_bytes = content::effective_copy_len(envelope.as_bytes(), &overlay, &removals);
    let maximum_bytes = config
        .max_message_bytes
        .saturating_add(PROPERTY_PATCH_HEADROOM_BYTES)
        .min(CONTENT_LIMITS.max_bytes);
    if effective_bytes > maximum_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: effective_bytes,
            maximum_bytes,
        });
    }
    let text_ids = record.format.uses_text_ids();
    let projection = content::effective_projection_with_removals(
        envelope.as_bytes(),
        &overlay,
        &removals,
        text_ids,
    )?;
    record.envelope = Some(
        MessageEnvelope::new(envelope.as_bytes().to_vec())
            .with_overlay(overlay)
            .with_removals(removals)
            .with_filter_properties(projection),
    );
    record.body.clear();
    record.format = MessageRecordFormat::V4 { text_ids };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_copy_headroom_never_raises_the_native_sixteen_mib_ceiling() {
        // Exactly 16 MiB of valid original AMQP Data content.
        let size = CONTENT_LIMITS.max_bytes;
        let mut bytes = vec![0x00, 0x53, 0x75, 0xb0];
        bytes.extend(u32::try_from(size - 8).unwrap().to_be_bytes());
        bytes.resize(size, 0);
        let input = crate::MessageContent::AmqpTextIds(bytes).derive().unwrap();
        let mut record = MessageRecord {
            sequence: crate::SequenceNumber::new(1),
            message_id: input.message_id,
            body: Vec::new(),
            enqueued_at: crate::Timestamp::from_millis(1),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 0,
            state: crate::MessageState::Ready,
            session_id: None,
            dead_letter: None,
            envelope: input.envelope,
            format: MessageRecordFormat::V3,
        };
        let program = crate::SqlActionProgram::compile("SET user.added = TRUE").unwrap();
        let patch = program
            .evaluate(
                crate::SqlMessageContext::default(),
                &mut crate::SqlEvaluationBudget::default(),
            )
            .unwrap();
        let before = record.clone();
        assert_eq!(
            apply_action(
                &QueueConfig {
                    max_message_bytes: usize::MAX,
                    ..QueueConfig::default()
                },
                &mut record,
                patch
            ),
            Err(BrokerError::MessageTooLarge {
                body_bytes: size + 11,
                maximum_bytes: size
            })
        );
        assert_eq!(record, before, "size refusal exposes no partial changes");
    }
}
