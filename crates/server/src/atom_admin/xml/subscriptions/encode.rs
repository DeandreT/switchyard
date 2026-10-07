use std::io::{self, Write};

use domain::{SubscriptionConfig, SubscriptionName};
use quick_xml::{
    Writer,
    events::{BytesEnd, BytesStart, BytesText, Event},
};

use super::super::{ATOM_NS, MAX_REPLY_BYTES, SERVICE_BUS_NS, duration};
use super::{SubscriptionXmlError, validate_config, validate_name};

#[derive(Default)]
struct Reply(Vec<u8>);

impl Write for Reply {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_REPLY_BYTES)
            .ok_or_else(|| io::Error::other("subscription XML reply work limit"))?;
        self.0.reserve(next - self.0.len());
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn event(writer: &mut Writer<Reply>, event: Event<'_>) -> Result<(), SubscriptionXmlError> {
    writer
        .write_event(event)
        .map_err(|_| SubscriptionXmlError::ReplyLimitExceeded)
}

fn open(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), SubscriptionXmlError> {
    event(writer, Event::Start(BytesStart::new(name)))
}

fn close(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), SubscriptionXmlError> {
    event(writer, Event::End(BytesEnd::new(name)))
}

fn scalar(
    writer: &mut Writer<Reply>,
    name: &'static str,
    text: &str,
) -> Result<(), SubscriptionXmlError> {
    open(writer, name)?;
    event(writer, Event::Text(BytesText::new(text)))?;
    close(writer, name)
}

fn preflight(
    total: &mut usize,
    text_bytes: usize,
    static_bytes: usize,
) -> Result<(), SubscriptionXmlError> {
    let next = text_bytes
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(static_bytes))
        .and_then(|bytes| total.checked_add(bytes))
        .filter(|bytes| *bytes <= MAX_REPLY_BYTES)
        .ok_or(SubscriptionXmlError::ReplyLimitExceeded)?;
    *total = next;
    Ok(())
}

pub(crate) fn encode_entry(
    name: &SubscriptionName,
    config: &SubscriptionConfig,
) -> Result<Vec<u8>, SubscriptionXmlError> {
    validate_name(name)?;
    validate_config(config)?;
    preflight(&mut 0, name.as_str().len(), 4_096)?;
    let mut writer = Writer::new(Reply::default());
    let mut entry = BytesStart::new("entry");
    entry.push_attribute(("xmlns", ATOM_NS));
    event(&mut writer, Event::Start(entry))?;
    scalar(&mut writer, "title", name.as_str())?;
    let mut content = BytesStart::new("content");
    content.push_attribute(("type", "application/xml"));
    event(&mut writer, Event::Start(content))?;
    let mut description = BytesStart::new("SubscriptionDescription");
    description.push_attribute(("xmlns", SERVICE_BUS_NS));
    event(&mut writer, Event::Start(description))?;
    scalar(
        &mut writer,
        "LockDuration",
        &duration::format(config.lock_duration_millis),
    )?;
    scalar(&mut writer, "RequiresSession", "false")?;
    if let Some(ttl) = config.default_time_to_live_millis {
        scalar(
            &mut writer,
            "DefaultMessageTimeToLive",
            &duration::format(ttl),
        )?;
    }
    scalar(
        &mut writer,
        "DeadLetteringOnMessageExpiration",
        if config.dead_lettering_on_message_expiration {
            "true"
        } else {
            "false"
        },
    )?;
    scalar(
        &mut writer,
        "DeadLetteringOnFilterEvaluationExceptions",
        if config.dead_lettering_on_filter_evaluation_exceptions {
            "true"
        } else {
            "false"
        },
    )?;
    scalar(
        &mut writer,
        "MaxDeliveryCount",
        &config.max_delivery_count.to_string(),
    )?;
    scalar(&mut writer, "EnableBatchedOperations", "true")?;
    scalar(&mut writer, "Status", "Active")?;
    close(&mut writer, "SubscriptionDescription")?;
    close(&mut writer, "content")?;
    close(&mut writer, "entry")?;
    Ok(writer.into_inner().0)
}

pub(crate) fn encode_error(error: SubscriptionXmlError) -> Result<Vec<u8>, SubscriptionXmlError> {
    preflight(&mut 0, error.code().len() + error.detail().len(), 64)?;
    let mut writer = Writer::new(Reply::default());
    open(&mut writer, "Error")?;
    scalar(&mut writer, "Code", error.code())?;
    scalar(&mut writer, "Detail", error.detail())?;
    close(&mut writer, "Error")?;
    Ok(writer.into_inner().0)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
