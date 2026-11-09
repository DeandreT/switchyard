//! Borrow-only structural reads, not a protobuf message codec. No owned buffer,
//! generic field skip, copy, reserve or declaration-sized allocation is used.

use protobuf::wire_format::WireType;

use crate::transport::TransportError;

/// Private operation-local instrumentation. Production uses the zero-sized ()
/// observer; tests inspect reads and borrowed spans without a global allocator,
/// unsafe code, thread-local attribution or interference from parallel tests.
pub(super) trait ReadObserver {
    fn scalar_byte(&mut self) {}
    fn span_requested(&mut self, _declared: u64, _available: usize) {}
    fn borrowed(&mut self, _span: &[u8]) {}
}
impl ReadObserver for () {}

pub(super) struct Cursor<'bytes, 'observer, O> {
    remaining: &'bytes [u8],
    observer: &'observer mut O,
}

impl<'bytes, 'observer, O: ReadObserver> Cursor<'bytes, 'observer, O> {
    pub fn new(bytes: &'bytes [u8], observer: &'observer mut O) -> Self {
        Self {
            remaining: bytes,
            observer,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
    pub fn observer(&mut self) -> &mut O {
        self.observer
    }

    pub fn varint(&mut self) -> Result<u64, TransportError> {
        let mut value = 0u64;
        for byte_index in 0..10 {
            let (&byte, remaining) = self
                .remaining
                .split_first()
                .ok_or(TransportError::Malformed("truncated varint"))?;
            self.remaining = remaining;
            self.observer.scalar_byte();
            // The tenth byte has just one representable bit. In particular,
            // reject overflow/continued tags or lengths before any conversion.
            if byte_index == 9 && byte > 1 {
                return Err(TransportError::Malformed("varint overflow"));
            }
            value |= u64::from(byte & 0x7f) << (7 * byte_index);
            if byte & 0x80 == 0 {
                if byte_index > 0 && byte == 0 {
                    return Err(TransportError::Malformed("noncanonical varint"));
                }
                return Ok(value);
            }
        }
        Err(TransportError::Malformed("unterminated varint"))
    }

    pub fn tag(&mut self) -> Result<(u32, WireType), TransportError> {
        let tag = self.varint()?;
        let tag =
            u32::try_from(tag).map_err(|_| TransportError::Malformed("protobuf tag range"))?;
        let field = tag >> 3;
        if field == 0 {
            return Err(TransportError::Malformed("zero protobuf field"));
        }
        let wire = WireType::new(tag & 7).ok_or(TransportError::Malformed("protobuf wire type"))?;
        Ok((field, wire))
    }

    /// Read a length, then return a view ONLY after both the profile cap and
    /// actual enclosing-slice remainder are checked. A truncated declaration
    /// neither borrows absent bytes nor materializes a replacement buffer.
    pub fn bytes(
        &mut self,
        maximum: usize,
        limit: &'static str,
        truncated: &'static str,
    ) -> Result<&'bytes [u8], TransportError> {
        let declared = self.varint()?;
        if declared > maximum as u64 {
            return Err(TransportError::Limit(limit));
        }
        self.observer.span_requested(declared, self.remaining.len());
        let length = usize::try_from(declared).map_err(|_| TransportError::Limit(limit))?;
        let (span, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(TransportError::Malformed(truncated))?;
        self.remaining = remaining;
        self.observer.borrowed(span);
        Ok(span)
    }
}
