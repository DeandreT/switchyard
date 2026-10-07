use std::io::{self, Write};

use quick_xml::{
    Writer,
    events::{BytesEnd, BytesStart, BytesText, Event},
};

use super::super::{ATOM_NS, MAX_FEED_ENTRIES, MAX_REPLY_BYTES, SERVICE_BUS_NS, XSI_NS, lexical};
use super::{RuleXmlError, correlation, validate_definition};
use crate::AtomRuleDefinition;

const ACTION_MARKUP_BYTES: usize = 256;

#[derive(Default)]
struct Reply(Vec<u8>);

impl Write for Reply {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_REPLY_BYTES)
            .ok_or_else(|| io::Error::other("rule XML reply work limit"))?;
        self.0.reserve(next - self.0.len());
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn event(writer: &mut Writer<Reply>, event: Event<'_>) -> Result<(), RuleXmlError> {
    writer
        .write_event(event)
        .map_err(|_| RuleXmlError::ReplyLimitExceeded)
}

fn open(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), RuleXmlError> {
    event(writer, Event::Start(BytesStart::new(name)))
}

fn close(writer: &mut Writer<Reply>, name: &'static str) -> Result<(), RuleXmlError> {
    event(writer, Event::End(BytesEnd::new(name)))
}

fn scalar(writer: &mut Writer<Reply>, name: &'static str, text: &str) -> Result<(), RuleXmlError> {
    open(writer, name)?;
    event(writer, Event::Text(BytesText::new(text)))?;
    close(writer, name)
}

fn preflight(
    total: &mut usize,
    text_bytes: usize,
    static_bytes: usize,
) -> Result<(), RuleXmlError> {
    let next = text_bytes
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(static_bytes))
        .and_then(|bytes| total.checked_add(bytes))
        .filter(|bytes| *bytes <= MAX_REPLY_BYTES)
        .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    *total = next;
    Ok(())
}

fn definition_budget(
    total: &mut usize,
    definition: &AtomRuleDefinition,
) -> Result<(), RuleXmlError> {
    validate_definition(definition)?;
    let (filter_bytes, markup_bytes) = match &definition.filter {
        domain::RuleFilter::Sql(filter) => (filter.expression().len(), 0),
        domain::RuleFilter::Correlation(filter) => correlation::budget(filter)?,
        _ => (0, 0),
    };
    let text_bytes = definition
        .name
        .as_str()
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(filter_bytes))
        .and_then(|bytes| {
            bytes.checked_add(
                definition
                    .action
                    .as_ref()
                    .map_or(0, |action| action.expression().len()),
            )
        })
        .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    let static_bytes = 1_024_usize
        .checked_add(markup_bytes)
        .and_then(|bytes| {
            bytes.checked_add(if definition.action.is_some() {
                ACTION_MARKUP_BYTES
            } else {
                0
            })
        })
        .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    preflight(total, text_bytes, static_bytes)
}

fn entry(writer: &mut Writer<Reply>, definition: &AtomRuleDefinition) -> Result<(), RuleXmlError> {
    let mut start = BytesStart::new("entry");
    start.push_attribute(("xmlns", ATOM_NS));
    event(writer, Event::Start(start))?;
    scalar(writer, "title", definition.name.as_str())?;
    let mut content = BytesStart::new("content");
    content.push_attribute(("type", "application/xml"));
    event(writer, Event::Start(content))?;
    let mut description = BytesStart::new("RuleDescription");
    description.push_attribute(("xmlns", SERVICE_BUS_NS));
    event(writer, Event::Start(description))?;
    let (kind, expression) = match &definition.filter {
        domain::RuleFilter::True => ("TrueFilter", Some("1=1")),
        domain::RuleFilter::False => ("FalseFilter", Some("1=0")),
        domain::RuleFilter::Sql(filter) => ("SqlFilter", Some(filter.expression())),
        domain::RuleFilter::Correlation(_) => ("CorrelationFilter", None),
    };
    let mut filter = BytesStart::new("Filter");
    filter.push_attribute(("xmlns:i", XSI_NS));
    filter.push_attribute(("i:type", kind));
    event(writer, Event::Start(filter))?;
    if let domain::RuleFilter::Correlation(filter) = &definition.filter {
        for (name, value) in correlation::fields(filter) {
            if let Some(value) = value {
                scalar(writer, name, value)?;
            }
        }
        open(writer, "Properties")?;
        for (key, value) in &filter.properties {
            open(writer, "KeyValueOfstringanyType")?;
            scalar(writer, "Key", key)?;
            let kind = correlation::kind(value)?;
            let text = correlation::text(value)?;
            let mut start = BytesStart::new("Value");
            let value_type = format!("l28:{}", kind.name());
            start.push_attribute(("xmlns:l28", correlation::XSD_NS));
            start.push_attribute(("i:type", value_type.as_str()));
            event(writer, Event::Start(start))?;
            event(writer, Event::Text(BytesText::new(&text)))?;
            close(writer, "Value")?;
            close(writer, "KeyValueOfstringanyType")?;
        }
        close(writer, "Properties")?;
    } else {
        scalar(
            writer,
            "SqlExpression",
            expression.ok_or(RuleXmlError::InvalidDefinition)?,
        )?;
        open(writer, "Parameters")?;
        close(writer, "Parameters")?;
    }
    close(writer, "Filter")?;
    if let Some(action) = &definition.action {
        let mut start = BytesStart::new("Action");
        start.push_attribute(("xmlns:i", XSI_NS));
        start.push_attribute(("i:type", "SqlRuleAction"));
        event(writer, Event::Start(start))?;
        scalar(writer, "SqlExpression", action.expression())?;
        open(writer, "Parameters")?;
        close(writer, "Parameters")?;
        close(writer, "Action")?;
    }
    scalar(writer, "Name", definition.name.as_str())?;
    close(writer, "RuleDescription")?;
    close(writer, "content")?;
    close(writer, "entry")
}

pub(crate) fn encode_entry(definition: &AtomRuleDefinition) -> Result<Vec<u8>, RuleXmlError> {
    definition_budget(&mut 0, definition)?;
    let mut writer = Writer::new(Reply::default());
    entry(&mut writer, definition)?;
    Ok(writer.into_inner().0)
}

pub(crate) fn encode_feed(definitions: &[AtomRuleDefinition]) -> Result<Vec<u8>, RuleXmlError> {
    if definitions.len() > MAX_FEED_ENTRIES {
        return Err(RuleXmlError::ReplyLimitExceeded);
    }
    let mut total = 128;
    for definition in definitions {
        definition_budget(&mut total, definition)?;
    }
    let mut writer = Writer::new(Reply::default());
    let mut feed = BytesStart::new("feed");
    feed.push_attribute(("xmlns", ATOM_NS));
    event(&mut writer, Event::Start(feed))?;
    for definition in definitions {
        entry(&mut writer, definition)?;
    }
    close(&mut writer, "feed")?;
    Ok(writer.into_inner().0)
}

pub(crate) fn encode_error(code: &str, detail: &str) -> Result<Vec<u8>, RuleXmlError> {
    if !lexical::legal_chars(code) || !lexical::legal_chars(detail) {
        return Err(RuleXmlError::ReplyLimitExceeded);
    }
    let text_bytes = code
        .len()
        .checked_add(detail.len())
        .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    preflight(&mut 0, text_bytes, 64)?;
    let mut writer = Writer::new(Reply::default());
    open(&mut writer, "Error")?;
    scalar(&mut writer, "Code", code)?;
    scalar(&mut writer, "Detail", detail)?;
    close(&mut writer, "Error")?;
    Ok(writer.into_inner().0)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
