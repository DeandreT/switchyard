use super::*;

pub(super) fn message_record(
    scope: EnqueueScope<'_>,
    config: &QueueConfig,
    message: MessageInput<'_>,
    sequence: SequenceNumber,
    scheduled_enqueue_time: Option<Timestamp>,
) -> MessageRecord {
    let time_to_live_millis = effective_time_to_live_millis(config, message.time_to_live_millis);
    let future = scheduled_enqueue_time.filter(|enqueue_at| *enqueue_at > scope.issued_at);
    MessageRecord {
        sequence,
        message_id: message.message_id.to_owned(),
        body: message.body.to_vec(),
        enqueued_at: scope.issued_at,
        expires_at: if future.is_some() {
            None
        } else {
            time_to_live_millis.map(|millis| scope.issued_at.saturating_add_millis(millis))
        },
        delivery_count: 0,
        state: future.map_or(MessageState::Ready, |enqueue_at| MessageState::Scheduled {
            enqueue_at,
            time_to_live_millis,
        }),
        session_id: message.session_id.cloned(),
        dead_letter: None,
        scheduled_enqueue_time,
        envelope: message.envelope.cloned().map(Box::new),
    }
}
