use std::mem::size_of;

use protobuf::{CodedInputStream, Message as ProtoMessage, wire_format::WireType};
use raft::eraftpb::{Entry, EntryType, Message, MessageType};

use crate::consensus::{
    ConsensusIdentity, MAX_CONSENSUS_CONTEXT_BYTES, MAX_CONSENSUS_ENTRY_BYTES,
    MAX_CONSENSUS_PROPOSAL_BYTES,
};

use super::{TransportError, TransportLimits};

mod wire;
use wire::{Cursor, ReadObserver};

pub const FRAME_VERSION: u16 = 1;
pub const FRAME_HEADER_BYTES: usize = 60;
// One maximum legal application entry, maximum message context, and conservative
// pinned protobuf scalar/length overhead. No reduction of the 358 data ceiling.
pub const MIN_FRAME_BYTES: usize =
    FRAME_HEADER_BYTES + MAX_CONSENSUS_ENTRY_BYTES + MAX_CONSENSUS_CONTEXT_BYTES + 256;

#[derive(Clone)]
pub struct FrameCodec {
    identity: ConsensusIdentity,
    local: u64,
    limits: TransportLimits,
}

#[derive(Clone, Copy)]
pub(super) struct Header {
    pub source: u64,
    pub body_bytes: usize,
}

impl FrameCodec {
    pub fn new(
        identity: ConsensusIdentity,
        local: u64,
        limits: TransportLimits,
    ) -> Result<Self, TransportError> {
        limits.validate()?;
        if !identity.contains(local) {
            return Err(TransportError::Identity);
        }
        Ok(Self {
            identity,
            local,
            limits,
        })
    }

    pub fn identity(&self) -> &ConsensusIdentity {
        &self.identity
    }
    pub fn local_id(&self) -> u64 {
        self.local
    }
    pub fn limits(&self) -> &TransportLimits {
        &self.limits
    }

    pub fn encode(&self, message: &Message) -> Result<Vec<u8>, TransportError> {
        let body = self.outbound_size(message)?;
        let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + body);
        frame.extend_from_slice(b"SWCP");
        frame.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&self.identity.fingerprint());
        frame.extend_from_slice(&message.from.to_be_bytes());
        frame.extend_from_slice(&message.to.to_be_bytes());
        frame.extend_from_slice(&(body as u32).to_be_bytes());
        frame.extend_from_slice(&message.write_to_bytes()?);
        Ok(frame)
    }

    pub fn decode(&self, frame: &[u8]) -> Result<Message, TransportError> {
        if frame.len() < FRAME_HEADER_BYTES {
            return Err(TransportError::Malformed("short header"));
        }
        let header = self.header(
            frame[..FRAME_HEADER_BYTES]
                .try_into()
                .expect("checked header"),
        )?;
        if frame.len() != FRAME_HEADER_BYTES + header.body_bytes {
            return Err(TransportError::Malformed("frame length"));
        }
        self.body(header, &frame[FRAME_HEADER_BYTES..])
    }

    pub(super) fn outbound_size(&self, message: &Message) -> Result<usize, TransportError> {
        self.check_message(message, self.local, message.to)?;
        let bytes = message.compute_size() as usize;
        if bytes > self.limits.max_frame_bytes - FRAME_HEADER_BYTES {
            return Err(TransportError::Limit("protobuf body"));
        }
        Ok(bytes)
    }

    pub(super) fn header(
        &self,
        bytes: &[u8; FRAME_HEADER_BYTES],
    ) -> Result<Header, TransportError> {
        if &bytes[..4] != b"SWCP"
            || u16::from_be_bytes(bytes[4..6].try_into().expect("fixed header")) != FRAME_VERSION
            || bytes[6..8] != [0, 0]
        {
            return Err(TransportError::Unsupported);
        }
        let source = u64::from_be_bytes(bytes[40..48].try_into().expect("fixed header"));
        let destination = u64::from_be_bytes(bytes[48..56].try_into().expect("fixed header"));
        if bytes[8..40] != self.identity.fingerprint()
            || destination != self.local
            || source == destination
            || !self.identity.contains(source)
        {
            return Err(TransportError::Identity);
        }
        let body_bytes =
            u32::from_be_bytes(bytes[56..60].try_into().expect("fixed header")) as usize;
        if body_bytes == 0 || body_bytes > self.limits.max_frame_bytes - FRAME_HEADER_BYTES {
            return Err(TransportError::Limit("frame body"));
        }
        Ok(Header { source, body_bytes })
    }

    pub(super) fn body(&self, header: Header, body: &[u8]) -> Result<Message, TransportError> {
        if body.len() != header.body_bytes {
            return Err(TransportError::Malformed("body length"));
        }
        preflight(body, self.limits.max_entries)?;
        let mut input = CodedInputStream::from_bytes(body);
        input.set_recursion_limit(2);
        let mut message = Message::default();
        message.merge_from(&mut input)?;
        input.check_eof()?;
        self.check_message(&message, header.source, self.local)?;
        if message.write_to_bytes()? != body {
            return Err(TransportError::Malformed("noncanonical protobuf"));
        }
        Ok(message)
    }

    fn check_message(
        &self,
        message: &Message,
        source: u64,
        destination: u64,
    ) -> Result<(), TransportError> {
        if message.from != source
            || message.to != destination
            || source == destination
            || !self.identity.contains(source)
            || !self.identity.contains(destination)
        {
            return Err(TransportError::Identity);
        }
        if raft::raw_node::is_local_msg(message.get_msg_type())
            || message.get_msg_type() == MessageType::MsgSnapshot
            || message.has_snapshot()
            || message.request_snapshot != 0
        {
            return Err(TransportError::Unsupported);
        }
        if message.unknown_fields.iter().next().is_some() {
            return Err(TransportError::Unsupported);
        }
        if message.entries.len() > self.limits.max_entries
            || message.context.len() > MAX_CONSENSUS_CONTEXT_BYTES
        {
            return Err(TransportError::Limit("entry count/message context"));
        }
        for entry in &message.entries {
            if entry.get_entry_type() != EntryType::EntryNormal
                || entry.unknown_fields.iter().next().is_some()
            {
                return Err(TransportError::Unsupported);
            }
            if entry.data.len() > MAX_CONSENSUS_PROPOSAL_BYTES
                || entry.context.len() > MAX_CONSENSUS_CONTEXT_BYTES
                || entry.compute_size() as usize > MAX_CONSENSUS_ENTRY_BYTES
            {
                return Err(TransportError::Limit("entry data/context"));
            }
        }
        Ok(())
    }
}

/// Reserve input, decoded variable data, canonical serialization, bounded Vec
/// capacity/container slack before body allocation. Task/socket overhead is
/// separately connection-capped. Trusted handler allocations are not covered.
pub(super) fn allocation_charge(frame: usize, entries: usize) -> Result<usize, TransportError> {
    frame
        .checked_mul(4)
        .and_then(|v| {
            entries
                .checked_mul(2 * size_of::<Entry>())
                .and_then(|e| v.checked_add(e))
        })
        .and_then(|v| v.checked_add(size_of::<Message>()))
        .ok_or(TransportError::Limit("allocation accounting overflow"))
}

// Allocation-free shape/work preflight of the pinned schema, using only borrowed
// slices and bounded scalar reads. Reject unknown fields/groups, duplicate singular fields,
// excess repeated entries and oversized nested/data/context lengths BEFORE the
// generated decoder grows repeated containers or copies variable data. Semantic
// Raft interpretation stays entirely in the pinned core/future driver.
fn preflight(body: &[u8], maximum_entries: usize) -> Result<(), TransportError> {
    preflight_observed(body, maximum_entries, &mut ())
}

fn preflight_observed<O: ReadObserver>(
    body: &[u8],
    maximum_entries: usize,
    observer: &mut O,
) -> Result<(), TransportError> {
    let mut input = Cursor::new(body, observer);
    let mut seen = 0u32;
    let mut entries = 0;
    while !input.is_empty() {
        let (field, wire) = input.tag()?;
        if field == 7 {
            if wire != WireType::WireTypeLengthDelimited {
                return Err(TransportError::Malformed("entry wire type"));
            }
            entries += 1;
            if entries > maximum_entries {
                return Err(TransportError::Limit("decoded entry count"));
            }
            let entry = input.bytes(
                MAX_CONSENSUS_ENTRY_BYTES,
                "nested entry",
                "truncated nested entry",
            )?;
            preflight_entry(&mut Cursor::new(entry, input.observer()))?;
            continue;
        }
        if !(1..=16).contains(&field) || field == 9 {
            return Err(TransportError::Unsupported);
        }
        if seen & (1 << field) != 0 {
            return Err(TransportError::Malformed("duplicate message field"));
        }
        seen |= 1 << field;
        if field == 12 {
            check_bytes_field(&mut input, wire, MAX_CONSENSUS_CONTEXT_BYTES)?;
        } else {
            if wire != WireType::WireTypeVarint {
                return Err(TransportError::Malformed("scalar wire type"));
            }
            input.varint()?;
        }
    }
    Ok(())
}

fn preflight_entry<O: ReadObserver>(input: &mut Cursor<'_, '_, O>) -> Result<(), TransportError> {
    let mut seen = 0u32;
    while !input.is_empty() {
        let (field, wire) = input.tag()?;
        if !(1..=6).contains(&field) {
            return Err(TransportError::Unsupported);
        }
        if seen & (1 << field) != 0 {
            return Err(TransportError::Malformed("duplicate entry field"));
        }
        seen |= 1 << field;
        if field == 4 {
            check_bytes_field(input, wire, MAX_CONSENSUS_PROPOSAL_BYTES)?;
        } else if field == 6 {
            check_bytes_field(input, wire, MAX_CONSENSUS_CONTEXT_BYTES)?;
        } else {
            if wire != WireType::WireTypeVarint {
                return Err(TransportError::Malformed("entry scalar wire type"));
            }
            let value = input.varint()?;
            if (field == 1 && value != 0) || (field == 5 && value > 1) {
                return Err(TransportError::Unsupported);
            }
        }
    }
    Ok(())
}

fn check_bytes_field<O: ReadObserver>(
    input: &mut Cursor<'_, '_, O>,
    wire: WireType,
    maximum: usize,
) -> Result<(), TransportError> {
    if wire != WireType::WireTypeLengthDelimited {
        return Err(TransportError::Malformed("bytes wire type"));
    }
    input.bytes(maximum, "decoded bytes field", "truncated bytes field")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Stack-only, per-operation read-path observations, NOT a global allocator
    // counter. Source audit of Cursor's slice/scalar-only implementation supplies
    // the no-allocation argument; these tests verify declarations never turn
    // into copied/absent spans and successful spans alias the actual input.
    struct Probe<'a> {
        input: &'a [u8],
        scalar_bytes: usize,
        requests: usize,
        last_declared: u64,
        last_available: usize,
        borrowed_bytes: usize,
        last_span: Option<(usize, usize)>,
    }

    impl<'a> Probe<'a> {
        fn new(input: &'a [u8]) -> Self {
            Self {
                input,
                scalar_bytes: 0,
                requests: 0,
                last_declared: 0,
                last_available: 0,
                borrowed_bytes: 0,
                last_span: None,
            }
        }
    }

    impl ReadObserver for Probe<'_> {
        fn scalar_byte(&mut self) {
            self.scalar_bytes += 1;
        }
        fn span_requested(&mut self, declared: u64, available: usize) {
            self.requests += 1;
            self.last_declared = declared;
            self.last_available = available;
        }
        fn borrowed(&mut self, span: &[u8]) {
            let offset = (span.as_ptr() as usize)
                .checked_sub(self.input.as_ptr() as usize)
                .expect("borrowed span starts inside original input");
            assert!(offset <= self.input.len() && span.len() <= self.input.len() - offset);
            self.borrowed_bytes += span.len();
            self.last_span = Some((offset, span.len()));
        }
    }

    #[test]
    fn within_cap_truncated_context_reads_only_real_scalar_bytes() {
        let mut storage = [0u8; 17];
        storage[..10]
            .copy_from_slice(&[0x08, 0x03, 0x10, 0x02, 0x18, 0x01, 0x62, 0x80, 0x80, 0x40]);
        for actual in [10, 11, 17] {
            let body = &storage[..actual];
            let mut probe = Probe::new(body);
            assert!(
                allocation_charge(FRAME_HEADER_BYTES + body.len(), 64).unwrap()
                    < MAX_CONSENSUS_CONTEXT_BYTES
            );
            assert!(matches!(
                preflight_observed(body, 64, &mut probe),
                Err(TransportError::Malformed("truncated bytes field"))
            ));
            assert_eq!(probe.scalar_bytes, 10);
            assert_eq!(probe.requests, 1);
            assert_eq!(probe.last_declared, MAX_CONSENSUS_CONTEXT_BYTES as u64);
            assert_eq!(probe.last_available, actual - 10);
            assert_eq!(probe.borrowed_bytes, 0);
            assert_eq!(probe.last_span, None);
        }
    }

    #[test]
    fn nested_lengths_are_checked_against_the_enclosing_borrowed_slice() {
        for field in [0x22, 0x32] {
            let body = [
                0x08, 0x03, 0x10, 0x02, 0x18, 0x01, 0x3a, 0x04, field, 0x80, 0x80, 0x40,
            ];
            let mut probe = Probe::new(&body);
            assert!(matches!(
                preflight_observed(&body, 64, &mut probe),
                Err(TransportError::Malformed("truncated bytes field"))
            ));
            assert_eq!(probe.scalar_bytes, body.len());
            assert_eq!(probe.requests, 2);
            assert_eq!(probe.last_declared, MAX_CONSENSUS_CONTEXT_BYTES as u64);
            assert_eq!(probe.last_available, 0);
            assert_eq!(
                probe.borrowed_bytes, 4,
                "only the actually present entry container was borrowed"
            );
            assert_eq!(probe.last_span, Some((8, 4)));
        }
        let body = [0x08, 0x03, 0x10, 0x02, 0x18, 0x01, 0x3a, 0x80, 0x80, 0x40];
        let mut probe = Probe::new(&body);
        assert!(matches!(
            preflight_observed(&body, 64, &mut probe),
            Err(TransportError::Malformed("truncated nested entry"))
        ));
        assert_eq!(probe.scalar_bytes, body.len());
        assert_eq!(probe.requests, 1);
        assert_eq!(probe.last_declared, 1024 * 1024);
        assert_eq!(probe.last_available, 0);
        assert_eq!(probe.borrowed_bytes, 0);

        // Outer bytes cannot satisfy a data declaration that crosses its entry
        // boundary, even though the complete message has enough bytes left.
        let body = [0x3a, 3, 0x22, 3, 0xaa, 0x62, 2, 0xbb, 0xcc];
        let mut probe = Probe::new(&body);
        assert!(matches!(
            preflight_observed(&body, 64, &mut probe),
            Err(TransportError::Malformed("truncated bytes field"))
        ));
        assert_eq!(probe.last_declared, 3);
        assert_eq!(probe.last_available, 1);
        assert_eq!(probe.borrowed_bytes, 3);
        assert_eq!(probe.last_span, Some((2, 3)));
    }

    #[test]
    fn successful_context_data_and_entry_spans_alias_the_original_input() {
        let context = [0x62, 3, 0xaa, 0xbb, 0xcc];
        let mut probe = Probe::new(&context);
        preflight_observed(&context, 64, &mut probe).unwrap();
        assert_eq!(probe.scalar_bytes, 2);
        assert_eq!(probe.borrowed_bytes, 3);
        assert_eq!(probe.last_span, Some((2, 3)));

        let body = [0x3a, 7, 0x22, 1, 0x7a, 0x32, 2, 0x7b, 0x7c];
        let mut probe = Probe::new(&body);
        preflight_observed(&body, 64, &mut probe).unwrap();
        assert_eq!(probe.scalar_bytes, 6);
        assert_eq!(probe.requests, 3);
        assert_eq!(
            probe.borrowed_bytes,
            7 + 1 + 2,
            "container and leaf views, not copies"
        );
        assert_eq!(probe.last_span, Some((7, 2)));
    }

    #[test]
    fn unknown_fixed_group_and_noncanonical_paths_never_skip_declared_data() {
        for body in [
            &[0xa2, 1, 0x80, 0x80, 0x40][..], // unknown length-delimited field
            &[0xa1, 1][..],
            &[0xa5, 1][..], // unknown fixed64/fixed32
            &[0x09][..],
            &[0x0d][..], // known scalar with wrong fixed wire
            &[0x0b][..],
            &[0x0c][..],             // groups
            &[0x65][..],             // context with fixed32 wire
            &[0x08, 0x83, 0x00][..], // nonminimal scalar varint
            &[0x62, 0x80, 0x00][..], // nonminimal length varint
            &[0xff; 10][..],         // overflowing tag
            &[0x00][..],
            &[0x08, 0x80][..], // invalid tag/truncated scalar
        ] {
            let mut probe = Probe::new(body);
            assert!(preflight_observed(body, 64, &mut probe).is_err());
            assert!(probe.scalar_bytes <= body.len());
            assert_eq!(probe.requests, 0);
            assert_eq!(probe.borrowed_bytes, 0);
            assert_eq!(probe.last_span, None);
        }
    }
}
