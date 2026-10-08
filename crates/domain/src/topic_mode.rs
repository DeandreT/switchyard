//! Mandatory non-finite ownership metadata, separate from topic configuration.

use serde::{Deserialize, Serialize};

use crate::{BrokerError, codec};

const TOPIC_MODE_RECORD: [u8; 4] = *b"TMOD";
const TOPIC_MODE_SCHEMA: u8 = 1;
const MAX_TOPIC_MODE_RECORD_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Mode {
    NonFinite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NonFiniteTopicMode {
    record: [u8; 4],
    schema: u8,
    generation: u64,
    mode: Mode,
}

impl NonFiniteTopicMode {
    pub(crate) fn non_finite(generation: u64) -> Result<Self, BrokerError> {
        let value = Self {
            record: TOPIC_MODE_RECORD,
            schema: TOPIC_MODE_SCHEMA,
            generation,
            mode: Mode::NonFinite,
        };
        value.validate_generation(generation)?;
        Ok(value)
    }

    pub(crate) fn validate_generation(self, expected: u64) -> Result<(), BrokerError> {
        if self.record != TOPIC_MODE_RECORD
            || self.schema != TOPIC_MODE_SCHEMA
            || self.generation == 0
            || expected == 0
            || self.generation != expected
        {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        Ok(())
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, BrokerError> {
        self.validate_generation(self.generation)?;
        let bytes = codec::encode(&self).map_err(|_| BrokerError::TopicCapacityCorrupt)?;
        if bytes.first() != Some(&codec::VALUE_FORMAT_V11)
            || bytes.len() > MAX_TOPIC_MODE_RECORD_BYTES
        {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], expected: u64) -> Result<Self, BrokerError> {
        if bytes.len() > MAX_TOPIC_MODE_RECORD_BYTES {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        let (version, payload) =
            codec::split(bytes).map_err(|_| BrokerError::TopicCapacityCorrupt)?;
        if version != codec::VALUE_FORMAT_V11 {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        let value: Self =
            codec::decode_payload(payload).map_err(|_| BrokerError::TopicCapacityCorrupt)?;
        value.validate_generation(expected)?;
        if value.encode()? != bytes {
            return Err(BrokerError::TopicCapacityCorrupt);
        }
        Ok(value)
    }
}

#[cfg(test)]
#[path = "topic_mode/tests.rs"]
mod tests;
