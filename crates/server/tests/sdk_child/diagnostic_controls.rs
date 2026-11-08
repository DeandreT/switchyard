//! Explicit delay probes, never enabled by an ordinary qualification run.

use domain::{CommandKind, MessageContent};
use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static UNTIL: AtomicU64 = AtomicU64::new(0);

pub fn timer_lag() -> io::Result<Option<u64>> {
    std::env::var("SWITCHYARD_SDK_TEST_TIMER_LAG_MS")
        .ok()
        .map(|text| {
            let lag: u64 = text.parse().map_err(io::Error::other)?;
            if !(1..=5000).contains(&lag) {
                return Err(io::Error::other("diagnostic timer lag must be 1..=5000 ms"));
            }
            Ok(lag)
        })
        .transpose()
}

pub fn scheduled(kind: &CommandKind) {
    let Some(lag) = timer_lag().expect("validated diagnostic lag") else {
        return;
    };
    let contents: &[MessageContent] = match kind {
        CommandKind::SendContent { content } => std::slice::from_ref(content),
        CommandKind::SendContentBatch { messages } => messages,
        _ => return,
    };
    for content in contents {
        let Some(due) = scheduled_due(content) else {
            continue;
        };
        let until = due.checked_add(lag).unwrap();
        UNTIL.store(until, Ordering::SeqCst);
        super::diagnostics::emit(
            "CONTROL timer-lag-armed NOT-QUALIFICATION",
            format!("requested_due={due} resume_at={until} lag_ms={lag}"),
        );
    }
}

fn scheduled_due(content: &MessageContent) -> Option<u64> {
    let (MessageContent::Amqp(bytes) | MessageContent::AmqpTextIds(bytes)) = content else {
        return None;
    };
    let message = amqp::decode_message(bytes).expect("diagnostic fixture message");
    if message
        .properties
        .as_ref()
        .and_then(|p| p.message_id.as_ref())
        != Some(&amqp::MessageId::String("topic-scheduled-single".into()))
    {
        return None;
    }
    let due = message
        .message_annotations
        .as_ref()
        .and_then(|a| a.get(&amqp::AnnotationKey::from("x-opt-scheduled-enqueue-time")));
    let Some(amqp::Value::Timestamp(due)) = due else {
        panic!("diagnostic scheduled topic timestamp");
    };
    Some(u64::try_from(due.milliseconds()).unwrap())
}

pub fn timer_held() -> bool {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    u128::from(UNTIL.load(Ordering::SeqCst)) > wall
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduled_bytes(id: &str) -> Vec<u8> {
        let mut annotations = amqp::MessageAnnotations::default();
        annotations.insert(
            amqp::AnnotationKey::from("x-opt-scheduled-enqueue-time"),
            amqp::Value::Timestamp(serde_amqp::primitives::Timestamp::from_milliseconds(1_729)),
        );
        amqp::encode_message(&amqp::Message {
            properties: Some(amqp::Properties {
                message_id: Some(amqp::MessageId::String(id.into())),
                ..amqp::Properties::default()
            }),
            message_annotations: Some(annotations),
            ..amqp::Message::default()
        })
        .unwrap()
    }

    #[test]
    fn timer_probe_selects_both_legacy_and_current_amqp_projection_variants() {
        let bytes = scheduled_bytes("topic-scheduled-single");
        assert_eq!(
            scheduled_due(&MessageContent::Amqp(bytes.clone())),
            Some(1_729)
        );
        assert_eq!(
            scheduled_due(&MessageContent::AmqpTextIds(bytes)),
            Some(1_729)
        );
        for content in [
            MessageContent::Amqp(scheduled_bytes("unrelated")),
            MessageContent::AmqpTextIds(scheduled_bytes("unrelated")),
            MessageContent::Raw(domain::RawMessage::default()),
        ] {
            assert_eq!(scheduled_due(&content), None);
        }
    }
}
