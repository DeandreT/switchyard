use std::io;

use crate::{Message, decode_message};

use super::{EngineError, invalid_state};

const MAX_CUSTOM_DECODERS: usize = 8;
type Decoder = fn(&[u8]) -> io::Result<Message>;

/// Message-format decoders approved by the application for one receiving link.
/// Format zero is always the built-in AMQP message decoder and cannot be replaced.
#[derive(Clone, Debug, Default)]
pub struct MessageFormatDecoders {
    custom: Vec<(u32, Decoder)>,
}

impl MessageFormatDecoders {
    pub fn with_decoder(
        mut self,
        format: u32,
        decoder: fn(&[u8]) -> io::Result<Message>,
    ) -> Result<Self, EngineError> {
        if format == 0 {
            return Err(invalid_state(
                "the built-in message format zero decoder cannot be replaced",
            ));
        }
        if self
            .custom
            .iter()
            .any(|(registered, _)| *registered == format)
        {
            return Err(invalid_state("message format already has a decoder"));
        }
        if self.custom.len() == MAX_CUSTOM_DECODERS {
            return Err(invalid_state(
                "at most eight custom message-format decoders are supported",
            ));
        }
        self.custom.push((format, decoder));
        Ok(self)
    }

    pub(super) fn is_default(&self) -> bool {
        self.custom.is_empty()
    }

    pub(super) fn decoder(&self, format: u32) -> Option<Decoder> {
        if format == 0 {
            Some(decode_message)
        } else {
            self.custom
                .iter()
                .find_map(|(registered, decoder)| (*registered == format).then_some(*decoder))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom(bytes: &[u8]) -> io::Result<Message> {
        Ok(Message::builder()
            .body(crate::Body::Data(vec![bytes.to_vec().into()]))
            .build())
    }

    #[test]
    fn built_in_zero_is_immutable_and_other_formats_are_opt_in() {
        let registry = MessageFormatDecoders::default();
        assert!(registry.decoder(0).is_some());
        assert!(registry.decoder(1).is_none());
        assert!(registry.clone().with_decoder(0, custom).is_err());
        assert!(registry.is_default());
    }

    #[test]
    fn exact_format_values_and_handlers_survive_registry_clone() {
        let registry = MessageFormatDecoders::default()
            .with_decoder(u32::MAX, custom)
            .expect("custom decoder");
        let decoded = registry
            .clone()
            .decoder(u32::MAX)
            .expect("registered handler")(b"custom")
        .expect("custom payload");
        assert_eq!(decoded, custom(b"custom").expect("expected message"));
        assert!(registry.decoder(u32::MAX - 1).is_none());
        assert!(registry.decoder(0).is_some());
        assert!(!registry.is_default());
    }

    #[test]
    fn duplicate_and_ninth_registration_are_refused_without_changing_existing_registry() {
        let mut registry = MessageFormatDecoders::default();
        for format in 1..=MAX_CUSTOM_DECODERS as u32 {
            registry = registry
                .with_decoder(format, custom)
                .expect("within registry cap");
        }
        assert!(registry.clone().with_decoder(1, custom).is_err());
        assert!(registry.clone().with_decoder(9, custom).is_err());
        for format in 0..=MAX_CUSTOM_DECODERS as u32 {
            assert!(registry.decoder(format).is_some());
        }
        assert!(registry.decoder(9).is_none());
    }
}
