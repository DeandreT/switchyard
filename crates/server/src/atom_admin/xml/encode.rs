use std::io::{self, Write};

use domain::{QueueCapacityStatus, QueueCapacityView};
use quick_xml::{
    Writer,
    events::{BytesEnd, BytesStart, BytesText, Event},
};

use super::{
    ATOM_NS, AtomXmlError, KIB, MAX_FEED_ENTRIES, MAX_REPLY_BYTES, MIB, SERVICE_BUS_NS, duration,
    validate_view,
};

#[derive(Default)]
struct Reply(Vec<u8>);

impl Write for Reply {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_REPLY_BYTES)
            .ok_or_else(|| io::Error::other("XML reply work limit"))?;
        self.0.reserve(next - self.0.len());
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn event(writer: &mut Writer<Reply>, event: Event<'_>) -> Result<(), AtomXmlError> {
    writer
        .write_event(event)
        .map_err(|_| AtomXmlError::ReplyLimitExceeded)
}

fn open(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), AtomXmlError> {
    event(writer, Event::Start(BytesStart::new(name)))
}

fn close(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), AtomXmlError> {
    event(writer, Event::End(BytesEnd::new(name)))
}

fn scalar(writer: &mut Writer<Reply>, name: &'static str, text: &str) -> Result<(), AtomXmlError> {
    open(writer, name)?;
    event(writer, Event::Text(BytesText::new(text)))?;
    close(writer, name)
}

fn preflight(
    total: &mut usize,
    text_bytes: usize,
    static_bytes: usize,
) -> Result<(), AtomXmlError> {
    let next = text_bytes
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(static_bytes))
        .and_then(|bytes| total.checked_add(bytes))
        .filter(|bytes| *bytes <= MAX_REPLY_BYTES)
        .ok_or(AtomXmlError::ReplyLimitExceeded)?;
    *total = next;
    Ok(())
}

fn entry_preflight(view: &QueueCapacityView, total: &mut usize) -> Result<(), AtomXmlError> {
    validate_view(view)?;
    // 4 KiB dominates all static markup and bounded numeric/duration values.
    // The only caller text is the validated title, budgeted before escaping.
    preflight(total, view.binding.target().as_str().len(), 4_096)
}

fn write_entry(writer: &mut Writer<Reply>, view: &QueueCapacityView) -> Result<(), AtomXmlError> {
    let QueueCapacityStatus::FiniteV1 { limit, .. } = view.capacity else {
        return Err(AtomXmlError::UnsupportedDefinition);
    };
    let config = view.config;
    let mut entry = BytesStart::new("entry");
    entry.push_attribute(("xmlns", ATOM_NS));
    event(writer, Event::Start(entry))?;
    scalar(writer, "title", view.binding.target().as_str())?;
    let mut content = BytesStart::new("content");
    content.push_attribute(("type", "application/xml"));
    event(writer, Event::Start(content))?;
    let mut description = BytesStart::new("QueueDescription");
    description.push_attribute(("xmlns", SERVICE_BUS_NS));
    event(writer, Event::Start(description))?;
    scalar(
        writer,
        "LockDuration",
        &duration::format(config.lock_duration_millis),
    )?;
    scalar(
        writer,
        "MaxSizeInMegabytes",
        &(limit.bytes() / MIB).to_string(),
    )?;
    scalar(writer, "RequiresDuplicateDetection", "false")?;
    scalar(writer, "RequiresSession", "false")?;
    if let Some(ttl) = config.default_time_to_live_millis {
        scalar(writer, "DefaultMessageTimeToLive", &duration::format(ttl))?;
    }
    scalar(
        writer,
        "DeadLetteringOnMessageExpiration",
        if config.dead_lettering_on_message_expiration {
            "true"
        } else {
            "false"
        },
    )?;
    scalar(
        writer,
        "DuplicateDetectionHistoryTimeWindow",
        &duration::format(config.duplicate_detection_history_time_window_millis),
    )?;
    scalar(
        writer,
        "MaxDeliveryCount",
        &config.max_delivery_count.to_string(),
    )?;
    scalar(writer, "EnableBatchedOperations", "true")?;
    scalar(writer, "Status", "Active")?;
    open(writer, "AuthorizationRules")?;
    close(writer, "AuthorizationRules")?;
    scalar(writer, "IsAnonymousAccessible", "false")?;
    scalar(writer, "SupportOrdering", "false")?;
    scalar(writer, "EnablePartitioning", "false")?;
    scalar(writer, "EnableExpress", "false")?;
    scalar(
        writer,
        "MaxMessageSizeInKilobytes",
        &(config.max_message_bytes / KIB).to_string(),
    )?;
    close(writer, "QueueDescription")?;
    close(writer, "content")?;
    close(writer, "entry")
}

pub(crate) fn encode_entry(view: &QueueCapacityView) -> Result<Vec<u8>, AtomXmlError> {
    entry_preflight(view, &mut 0)?;
    let mut writer = Writer::new(Reply::default());
    write_entry(&mut writer, view)?;
    Ok(writer.into_inner().0)
}

pub(crate) fn encode_feed(views: &[QueueCapacityView]) -> Result<Vec<u8>, AtomXmlError> {
    if views.len() > MAX_FEED_ENTRIES {
        return Err(AtomXmlError::ReplyLimitExceeded);
    }
    let mut budget = 64;
    for view in views {
        entry_preflight(view, &mut budget)?;
    }
    let mut writer = Writer::new(Reply::default());
    let mut feed = BytesStart::new("feed");
    feed.push_attribute(("xmlns", ATOM_NS));
    event(&mut writer, Event::Start(feed))?;
    for view in views {
        write_entry(&mut writer, view)?;
    }
    close(&mut writer, "feed")?;
    Ok(writer.into_inner().0)
}

pub(crate) fn encode_error(error: AtomXmlError) -> Result<Vec<u8>, AtomXmlError> {
    let mut budget = 0;
    preflight(&mut budget, error.code().len() + error.detail().len(), 64)?;
    let mut writer = Writer::new(Reply::default());
    open(&mut writer, "Error")?;
    scalar(&mut writer, "Code", error.code())?;
    scalar(&mut writer, "Detail", error.detail())?;
    close(&mut writer, "Error")?;
    Ok(writer.into_inner().0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_sink_rejects_next_byte_without_retaining_partial_chunk() {
        let mut sink = Reply::default();
        assert_eq!(
            sink.write(&vec![b'x'; MAX_REPLY_BYTES]).unwrap(),
            MAX_REPLY_BYTES
        );
        let before = sink.0.clone();
        assert!(sink.write(b"x").is_err());
        assert_eq!(sink.0, before);
        assert!(sink.write(&[b'x'; 2]).is_err());
        assert_eq!(sink.0, before);
    }

    #[test]
    fn pre_escape_budget_is_checked_and_unchanged_on_refusal() {
        let mut budget = MAX_REPLY_BYTES - 6;
        assert_eq!(preflight(&mut budget, 1, 0), Ok(()));
        assert_eq!(budget, MAX_REPLY_BYTES);
        assert_eq!(
            preflight(&mut budget, 1, 0),
            Err(AtomXmlError::ReplyLimitExceeded)
        );
        assert_eq!(budget, MAX_REPLY_BYTES);
        assert_eq!(
            preflight(&mut 0, usize::MAX, 0),
            Err(AtomXmlError::ReplyLimitExceeded)
        );
    }
}
