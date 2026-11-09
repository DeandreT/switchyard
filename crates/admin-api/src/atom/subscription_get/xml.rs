//! Write-only XML with a pre-allocation byte guard; no raw markup or input parser.
use super::Failure;
use domain::SubscriptionConfig;
use quick_xml::{
    Writer,
    events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event},
};
use std::io::{self, Write};

pub(super) const XML_BYTES: usize = 4096;
pub(super) const MAX_TTL_MILLIS: u64 = (i64::MAX as u64) / 10_000;
pub(super) const ATOM: &str = "http://www.w3.org/2005/Atom";
pub(super) const CONNECT: &str =
    "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";

struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("XML output bound"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn writer() -> Writer<Bounded> {
    Writer::new(Bounded {
        bytes: Vec::with_capacity(XML_BYTES),
        limit: XML_BYTES,
    })
}
fn scalar(writer: &mut Writer<Bounded>, name: &'static str, value: &str) -> io::Result<()> {
    if value.len() > 128 || !value.chars().all(|c| matches!(c, '\u{9}' | '\u{A}' | '\u{D}' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')) { return Err(io::Error::other("invalid XML scalar")); }
    writer
        .create_element(name)
        .write_text_content(BytesText::new(value))?;
    Ok(())
}
fn duration(millis: u64) -> String {
    let seconds = millis / 1000;
    format!(
        "P{}DT{}H{}M{}.{:03}S",
        seconds / 86400,
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60,
        millis % 1000
    )
}

pub(super) fn entry(name: &str, config: SubscriptionConfig) -> Result<Vec<u8>, Failure> {
    // The only textual identity is a domain-valid ASCII subscription name.
    domain::SubscriptionName::new(name).map_err(|_| Failure::Internal)?;
    let SubscriptionConfig {
        lock_duration_millis,
        max_delivery_count,
        default_time_to_live_millis,
        dead_lettering_on_message_expiration,
        dead_lettering_on_filter_evaluation_exceptions,
    } = config;
    if !(5000..=300000).contains(&lock_duration_millis)
        || max_delivery_count > i32::MAX as u32
        || default_time_to_live_millis.is_some_and(|ttl| !(1000..=MAX_TTL_MILLIS).contains(&ttl))
    {
        return Err(Failure::Unsupported);
    }
    let mut w = writer();
    let result = (|| -> io::Result<()> {
        w.write_event(Event::Decl(BytesDecl::new("1.0", Some("utf-8"), None)))?;
        w.write_event(Event::Start(
            BytesStart::new("entry").with_attributes([("xmlns", ATOM)]),
        ))?;
        w.create_element("title")
            .with_attribute(("type", "text"))
            .write_text_content(BytesText::new(name))?;
        w.write_event(Event::Start(
            BytesStart::new("content").with_attributes([("type", "application/xml")]),
        ))?;
        w.write_event(Event::Start(
            BytesStart::new("SubscriptionDescription").with_attributes([("xmlns", CONNECT)]),
        ))?;
        scalar(&mut w, "LockDuration", &duration(lock_duration_millis))?;
        scalar(&mut w, "RequiresSession", "false")?;
        scalar(
            &mut w,
            "DefaultMessageTimeToLive",
            &default_time_to_live_millis
                .map(duration)
                .unwrap_or_else(|| "P10675199DT2H48M5.4775807S".into()),
        )?;
        scalar(
            &mut w,
            "DeadLetteringOnMessageExpiration",
            if dead_lettering_on_message_expiration {
                "true"
            } else {
                "false"
            },
        )?;
        scalar(
            &mut w,
            "DeadLetteringOnFilterEvaluationExceptions",
            if dead_lettering_on_filter_evaluation_exceptions {
                "true"
            } else {
                "false"
            },
        )?;
        scalar(&mut w, "MaxDeliveryCount", &max_delivery_count.to_string())?;
        for name in ["SubscriptionDescription", "content", "entry"] {
            w.write_event(Event::End(BytesEnd::new(name)))?;
        }
        Ok(())
    })();
    result.map_err(|_| Failure::Internal)?;
    Ok(w.into_inner().bytes)
}

pub(super) fn error(failure: Failure) -> io::Result<Vec<u8>> {
    let mut w = writer();
    w.write_event(Event::Decl(BytesDecl::new("1.0", Some("utf-8"), None)))?;
    w.write_event(Event::Start(BytesStart::new("Error")))?;
    scalar(&mut w, "Code", &failure.status().as_u16().to_string())?;
    scalar(&mut w, "Detail", failure.message())?;
    w.write_event(Event::End(BytesEnd::new("Error")))?;
    Ok(w.into_inner().bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sink_and_scalars_fail_before_extending() {
        let mut sink = Bounded {
            bytes: Vec::new(),
            limit: 4,
        };
        sink.write_all(b"1234").unwrap();
        assert!(sink.write_all(b"5").is_err());
        assert_eq!(sink.bytes, b"1234");
        let mut full = Bounded {
            bytes: Vec::with_capacity(XML_BYTES),
            limit: XML_BYTES,
        };
        full.write_all(&vec![b'x'; XML_BYTES]).unwrap();
        assert!(full.write_all(b"x").is_err());
        assert_eq!(full.bytes.len(), XML_BYTES);
        assert_eq!(full.bytes.capacity(), XML_BYTES);
        let mut w = writer();
        scalar(&mut w, "value", "<&\"'>").unwrap();
        assert_eq!(
            std::str::from_utf8(&w.get_ref().bytes).unwrap(),
            "<value>&lt;&amp;&quot;&apos;&gt;</value>"
        );
        for value in ["\0", "\u{FFFE}", "\u{FFFF}"] {
            assert!(scalar(&mut w, "value", value).is_err());
        }
    }
}
