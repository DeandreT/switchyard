use std::io;

use amqp::{AMQP_HEADER, SASL_HEADER};

enum Phase {
    Initial { sasl: bool },
    SaslFrames,
    Amqp,
}

pub(super) struct HeaderTracker {
    phase: Phase,
    size: [u8; 4],
    size_bytes: usize,
    remaining: usize,
}

impl HeaderTracker {
    pub(super) fn new(sasl: bool) -> Self {
        Self {
            phase: Phase::Initial { sasl },
            size: [0; 4],
            size_bytes: 0,
            remaining: 0,
        }
    }

    pub(super) fn message(&mut self, message: &[u8]) -> io::Result<()> {
        match self.phase {
            Phase::Initial { sasl } => {
                let expected = if sasl { SASL_HEADER } else { AMQP_HEADER };
                if message != expected {
                    return Err(header_error());
                }
                self.phase = if sasl { Phase::SaslFrames } else { Phase::Amqp };
                return Ok(());
            }
            Phase::Amqp => return Ok(()),
            Phase::SaslFrames => {}
        }
        // Only sizes are inspected: native AMQP decoding owns all frame semantics.
        let mut offset = 0;
        while offset < message.len() {
            if self.remaining != 0 {
                let count = self.remaining.min(message.len() - offset);
                self.remaining -= count;
                offset += count;
            } else if self.size_bytes == 0 && message[offset] == b'A' {
                if offset != 0 || message != AMQP_HEADER {
                    return Err(header_error());
                }
                self.phase = Phase::Amqp;
                return Ok(());
            } else {
                let count = (4 - self.size_bytes).min(message.len() - offset);
                self.size[self.size_bytes..self.size_bytes + count]
                    .copy_from_slice(&message[offset..offset + count]);
                self.size_bytes += count;
                offset += count;
                if self.size_bytes == 4 {
                    let size = u32::from_be_bytes(self.size) as usize;
                    if !(8..=super::MESSAGE_BYTES).contains(&size) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid SASL frame size",
                        ));
                    }
                    self.size_bytes = 0;
                    self.remaining = size - 4;
                }
            }
        }
        Ok(())
    }
}

fn header_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "AMQP protocol header must occupy one complete binary WebSocket message",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_headers_cannot_split_or_coalesce() {
        for sasl in [false, true] {
            let header = if sasl { SASL_HEADER } else { AMQP_HEADER };
            assert!(HeaderTracker::new(sasl).message(&header[..4]).is_err());
            let mut coalesced = header.to_vec();
            coalesced.extend_from_slice(&[0; 8]);
            assert!(HeaderTracker::new(sasl).message(&coalesced).is_err());
            let mut valid = HeaderTracker::new(sasl);
            valid.message(&header).unwrap();
        }
    }

    #[test]
    fn second_header_is_strict_while_sasl_frames_can_split() {
        let frame = [0, 0, 0, 8, 2, 1, 0, 0];
        let mut tracker = HeaderTracker::new(true);
        tracker.message(&SASL_HEADER).unwrap();
        tracker.message(&frame[..2]).unwrap();
        tracker.message(&frame[2..]).unwrap();
        assert!(tracker.message(&AMQP_HEADER[..1]).is_err());
        let mut tracker = HeaderTracker::new(true);
        tracker.message(&SASL_HEADER).unwrap();
        let mut combined = frame.to_vec();
        combined.extend_from_slice(&AMQP_HEADER);
        assert!(tracker.message(&combined).is_err());
        let mut tracker = HeaderTracker::new(true);
        tracker.message(&SASL_HEADER).unwrap();
        tracker.message(&frame).unwrap();
        tracker.message(&AMQP_HEADER).unwrap();
        tracker.message(&[0, 0]).unwrap();
        tracker.message(&[0, 8, 2, 0, 0, 0]).unwrap();
    }
}
