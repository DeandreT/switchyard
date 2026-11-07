//! Copy-local content rewrites carried by abandon, defer and dead-letter.

use crate::{
    BrokerError, MessageContentError, MessageEnvelope, MessageRecord, MessageRecordFormat,
    OverlayProperty, PROPERTY_PATCH_HEADROOM_BYTES, QueueConfig, content,
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
fn patch_properties(
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
    let effective_bytes = content::effective_len(envelope.as_bytes(), &overlay);
    let maximum_bytes = config
        .max_message_bytes
        .saturating_add(PROPERTY_PATCH_HEADROOM_BYTES);
    if effective_bytes > maximum_bytes {
        return Err(BrokerError::MessageTooLarge {
            body_bytes: effective_bytes,
            maximum_bytes,
        });
    }
    let projection = content::effective_projection(envelope.as_bytes(), &overlay)?;
    let original = record
        .envelope
        .take()
        .expect("the envelope was read above")
        .into_bytes();
    record.envelope = Some(
        MessageEnvelope::new(original)
            .with_filter_properties(projection)
            .with_overlay(overlay),
    );
    record.body.clear();
    record.format = MessageRecordFormat::V2;
    Ok(())
}
