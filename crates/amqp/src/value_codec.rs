//! Bounded decoding of AMQP values, including shared array constructors.

use std::{io, str};

use serde_amqp::{
    Value,
    described::Described,
    descriptor::Descriptor,
    primitives::{Array, OrderedMap, Symbol, Timestamp},
};

pub(crate) const MAX_VALUE_NESTING: usize = 68;
pub(crate) const MAX_VALUE_ELEMENTS: usize = 132_096;
pub(crate) const MAX_EXPANDED_VALUE_BYTES: usize = 4 * 1024 * 1024;

/// Cumulative allocation limits shared by independently encoded messages.
/// Charges already made are retained when decoding fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageDecodeBudget {
    remaining_values: usize,
    remaining_copied_bytes: usize,
}

impl MessageDecodeBudget {
    /// Creates a smaller budget without permitting either hard limit to grow.
    pub fn new(max_values: usize, max_copied_bytes: usize) -> io::Result<Self> {
        if max_values > MAX_VALUE_ELEMENTS || max_copied_bytes > MAX_EXPANDED_VALUE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message decode budget exceeds the hard allocation limits",
            ));
        }
        Ok(Self {
            remaining_values: max_values,
            remaining_copied_bytes: max_copied_bytes,
        })
    }

    pub fn remaining_values(&self) -> usize {
        self.remaining_values
    }

    pub fn remaining_copied_bytes(&self) -> usize {
        self.remaining_copied_bytes
    }
}

impl Default for MessageDecodeBudget {
    fn default() -> Self {
        Self {
            remaining_values: MAX_VALUE_ELEMENTS,
            remaining_copied_bytes: MAX_EXPANDED_VALUE_BYTES,
        }
    }
}

/// Decodes one value and reports its consumed wire length.
pub(crate) fn decode_value(bytes: &[u8]) -> io::Result<(Value, usize)> {
    ValueDecoder::new(bytes, &mut MessageDecodeBudget::default()).next_value()
}

enum Constructor {
    Primitive(u8),
    Described(Descriptor, Box<Self>),
}

impl Constructor {
    fn descriptor_bytes(&self) -> io::Result<usize> {
        match self {
            Self::Primitive(_) => Ok(0),
            Self::Described(descriptor, base) => {
                let own_bytes = match descriptor {
                    Descriptor::Code(_) => 0,
                    Descriptor::Name(name) => name.as_str().len(),
                };
                own_bytes
                    .checked_add(base.descriptor_bytes()?)
                    .ok_or_else(|| invalid("AMQP descriptor expansion size overflow"))
            }
        }
    }
}

/// Shares allocation budgets across consecutive values in one wire message.
pub(crate) struct ValueDecoder<'a, 'budget> {
    bytes: &'a [u8],
    position: usize,
    limit: usize,
    budget: &'budget mut MessageDecodeBudget,
}

impl<'a, 'budget> ValueDecoder<'a, 'budget> {
    pub(crate) fn new(bytes: &'a [u8], budget: &'budget mut MessageDecodeBudget) -> Self {
        Self {
            bytes,
            position: 0,
            limit: bytes.len(),
            budget,
        }
    }

    /// Returns this value's consumed length; stop decoding after any error.
    pub(crate) fn next_value(&mut self) -> io::Result<(Value, usize)> {
        let start = self.position;
        let value = self.value(0)?;
        Ok((value, self.position - start))
    }

    fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.limit)
            .ok_or_else(|| invalid("truncated AMQP value"))?;
        let bytes = &self.bytes[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn octet(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn fixed<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| invalid("invalid AMQP scalar width"))
    }

    fn length(&mut self, width: usize) -> io::Result<usize> {
        match width {
            1 => Ok(usize::from(self.octet()?)),
            4 => usize::try_from(u32::from_be_bytes(self.fixed()?))
                .map_err(|_| invalid("AMQP length is not representable")),
            _ => Err(invalid("invalid AMQP length width")),
        }
    }

    fn depth(depth: usize) -> io::Result<()> {
        if depth > MAX_VALUE_NESTING {
            return Err(invalid("AMQP value nesting limit exceeded"));
        }
        Ok(())
    }

    fn spend_value(&mut self) -> io::Result<()> {
        self.budget.remaining_values = self
            .budget
            .remaining_values
            .checked_sub(1)
            .ok_or_else(|| invalid("AMQP value element limit exceeded"))?;
        Ok(())
    }

    fn spend_bytes(&mut self, bytes: usize) -> io::Result<()> {
        self.budget.remaining_copied_bytes = self
            .budget
            .remaining_copied_bytes
            .checked_sub(bytes)
            .ok_or_else(|| invalid("AMQP expanded value byte limit exceeded"))?;
        Ok(())
    }

    fn constructor(&mut self, depth: usize) -> io::Result<Constructor> {
        Self::depth(depth)?;
        let code = self.octet()?;
        if code != 0x00 {
            if !matches!(
                code,
                0x40..=0x45 | 0x50..=0x56 | 0x60..=0x61 | 0x70..=0x74
                    | 0x80..=0x84 | 0x94 | 0x98 | 0xa0 | 0xa1 | 0xa3
                    | 0xb0 | 0xb1 | 0xb3 | 0xc0 | 0xc1 | 0xd0 | 0xd1
                    | 0xe0 | 0xf0
            ) {
                return Err(invalid(format!("unknown AMQP format code {code:#04x}")));
            }
            return Ok(Constructor::Primitive(code));
        }
        let descriptor = self.descriptor()?;
        let base = self.constructor(depth + 1)?;
        Ok(Constructor::Described(descriptor, Box::new(base)))
    }

    fn descriptor(&mut self) -> io::Result<Descriptor> {
        match self.octet()? {
            0x44 => Ok(Descriptor::Code(0)),
            0x53 => Ok(Descriptor::Code(u64::from(self.octet()?))),
            0x80 => Ok(Descriptor::Code(u64::from_be_bytes(self.fixed()?))),
            0xa3 => Ok(Descriptor::Name(self.symbol(1)?)),
            0xb3 => Ok(Descriptor::Name(self.symbol(4)?)),
            _ => Err(invalid("AMQP descriptor must be a symbol or ulong")),
        }
    }

    fn value(&mut self, depth: usize) -> io::Result<Value> {
        let constructor = self.constructor(depth)?;
        self.constructed(&constructor, depth)
    }

    fn constructed(&mut self, constructor: &Constructor, depth: usize) -> io::Result<Value> {
        Self::depth(depth)?;
        self.spend_value()?;
        match constructor {
            Constructor::Described(descriptor, base) => {
                if let Descriptor::Name(name) = descriptor {
                    self.spend_bytes(name.as_str().len())?;
                }
                Ok(Value::Described(Box::new(Described {
                    descriptor: descriptor.clone(),
                    value: self.constructed(base, depth + 1)?,
                })))
            }
            Constructor::Primitive(code) => self.primitive(*code, depth),
        }
    }

    fn primitive(&mut self, code: u8, depth: usize) -> io::Result<Value> {
        Ok(match code {
            0x40 => Value::Null,
            0x41 => Value::Bool(true),
            0x42 => Value::Bool(false),
            0x43 => Value::Uint(0),
            0x44 => Value::Ulong(0),
            0x45 => Value::List(Vec::new()),
            0x50 => Value::Ubyte(self.octet()?),
            0x51 => Value::Byte(i8::from_be_bytes(self.fixed()?)),
            0x52 => Value::Uint(u32::from(self.octet()?)),
            0x53 => Value::Ulong(u64::from(self.octet()?)),
            0x54 => Value::Int(i32::from(i8::from_be_bytes(self.fixed()?))),
            0x55 => Value::Long(i64::from(i8::from_be_bytes(self.fixed()?))),
            0x56 => Value::Bool(match self.octet()? {
                0 => false,
                1 => true,
                _ => return Err(invalid("invalid AMQP boolean value")),
            }),
            0x60 => Value::Ushort(u16::from_be_bytes(self.fixed()?)),
            0x61 => Value::Short(i16::from_be_bytes(self.fixed()?)),
            0x70 => Value::Uint(u32::from_be_bytes(self.fixed()?)),
            0x71 => Value::Int(i32::from_be_bytes(self.fixed()?)),
            0x72 => Value::Float(f32::from_bits(u32::from_be_bytes(self.fixed()?)).into()),
            0x73 => Value::Char(
                char::from_u32(u32::from_be_bytes(self.fixed()?))
                    .ok_or_else(|| invalid("invalid AMQP Unicode character"))?,
            ),
            0x74 => Value::Decimal32(self.fixed::<4>()?.into()),
            0x80 => Value::Ulong(u64::from_be_bytes(self.fixed()?)),
            0x81 => Value::Long(i64::from_be_bytes(self.fixed()?)),
            0x82 => Value::Double(f64::from_bits(u64::from_be_bytes(self.fixed()?)).into()),
            0x83 => Value::Timestamp(Timestamp::from_milliseconds(i64::from_be_bytes(
                self.fixed()?,
            ))),
            0x84 => Value::Decimal64(self.fixed::<8>()?.into()),
            0x94 => Value::Decimal128(self.fixed::<16>()?.into()),
            0x98 => Value::Uuid(self.fixed::<16>()?.into()),
            0xa0 => Value::Binary(self.binary(1)?.into()),
            0xb0 => Value::Binary(self.binary(4)?.into()),
            0xa1 => Value::String(self.string(1)?),
            0xb1 => Value::String(self.string(4)?),
            0xa3 => Value::Symbol(self.symbol(1)?),
            0xb3 => Value::Symbol(self.symbol(4)?),
            0xc0 => self.list(1, depth)?,
            0xd0 => self.list(4, depth)?,
            0xc1 => self.map(1, depth)?,
            0xd1 => self.map(4, depth)?,
            0xe0 => self.array(1, depth)?,
            0xf0 => self.array(4, depth)?,
            _ => return Err(invalid(format!("unknown AMQP format code {code:#04x}"))),
        })
    }

    fn binary(&mut self, width: usize) -> io::Result<Vec<u8>> {
        let length = self.length(width)?;
        self.spend_bytes(length)?;
        Ok(self.take(length)?.to_vec())
    }

    fn string(&mut self, width: usize) -> io::Result<String> {
        let length = self.length(width)?;
        self.spend_bytes(length)?;
        let bytes = self.take(length)?;
        str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| invalid("invalid AMQP UTF-8 string"))
    }

    fn symbol(&mut self, width: usize) -> io::Result<Symbol> {
        let length = self.length(width)?;
        self.spend_bytes(length)?;
        let bytes = self.take(length)?;
        if !bytes.is_ascii() {
            return Err(invalid("AMQP symbol must contain only ASCII bytes"));
        }
        Ok(Symbol::from(
            str::from_utf8(bytes)
                .map_err(|_| invalid("invalid AMQP symbol"))?
                .to_owned(),
        ))
    }

    // Container size includes its count field but excludes the size field.
    // Temporarily narrowing the cursor prevents children escaping that boundary.
    fn enter_container(&mut self, width: usize) -> io::Result<(usize, usize, usize)> {
        let size = self.length(width)?;
        if size < width {
            return Err(invalid("AMQP container size is smaller than its count"));
        }
        let end = self
            .position
            .checked_add(size)
            .filter(|end| *end <= self.limit)
            .ok_or_else(|| invalid("truncated AMQP container"))?;
        let old_limit = self.limit;
        self.limit = end;
        let count = self.length(width)?;
        if count > self.budget.remaining_values {
            return Err(invalid("AMQP value element limit exceeded"));
        }
        Ok((count, end, old_limit))
    }

    fn finish_container(&mut self, end: usize, old_limit: usize) -> io::Result<()> {
        if self.position != end {
            return Err(invalid("AMQP container has unconsumed bytes"));
        }
        self.limit = old_limit;
        Ok(())
    }

    fn list(&mut self, width: usize, depth: usize) -> io::Result<Value> {
        let (count, end, old_limit) = self.enter_container(width)?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.value(depth + 1)?);
        }
        self.finish_container(end, old_limit)?;
        Ok(Value::List(values))
    }

    fn map(&mut self, width: usize, depth: usize) -> io::Result<Value> {
        let (count, end, old_limit) = self.enter_container(width)?;
        if count % 2 != 0 {
            return Err(invalid("AMQP map count must be even"));
        }
        let mut entries = OrderedMap::with_capacity(count / 2);
        for _ in 0..count / 2 {
            let key = self.value(depth + 1)?;
            if entries.contains_key(&key) {
                return Err(invalid("AMQP map contains duplicate keys"));
            }
            let value = self.value(depth + 1)?;
            entries.insert(key, value);
        }
        self.finish_container(end, old_limit)?;
        Ok(Value::Map(entries))
    }

    fn array(&mut self, width: usize, depth: usize) -> io::Result<Value> {
        let (count, end, old_limit) = self.enter_container(width)?;
        let constructor = self.constructor(depth + 1)?;
        let repeated_descriptor_bytes = constructor
            .descriptor_bytes()?
            .checked_mul(count)
            .ok_or_else(|| invalid("AMQP descriptor expansion size overflow"))?;
        if repeated_descriptor_bytes > self.budget.remaining_copied_bytes {
            return Err(invalid("AMQP expanded value byte limit exceeded"));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.constructed(&constructor, depth + 1)?);
        }
        self.finish_container(end, old_limit)?;
        Ok(Value::Array(Array::from(values)))
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(wire: &[u8]) -> Value {
        let (value, consumed) = decode_value(wire).expect("valid wire fixture");
        assert_eq!(consumed, wire.len());
        value
    }

    fn counted(code: u8, count: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![code];
        bytes.extend_from_slice(
            &(u32::try_from(payload.len()).expect("small fixture") + 4).to_be_bytes(),
        );
        bytes.extend_from_slice(&count.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn described(code: u64, value: Value) -> Value {
        Value::Described(Box::new(Described {
            descriptor: Descriptor::Code(code),
            value,
        }))
    }

    #[test]
    fn scalar_format_codes_and_compact_numbers_have_independent_wire_fixtures() {
        for (wire, expected) in [
            (vec![0x40], Value::Null),
            (vec![0x41], Value::Bool(true)),
            (vec![0x42], Value::Bool(false)),
            (vec![0x56, 0], Value::Bool(false)),
            (vec![0x56, 1], Value::Bool(true)),
            (vec![0x43], Value::Uint(0)),
            (vec![0x44], Value::Ulong(0)),
            (vec![0x45], Value::List(Vec::new())),
            (vec![0x50, 0xff], Value::Ubyte(255)),
            (vec![0x51, 0x80], Value::Byte(-128)),
            (vec![0x52, 0xff], Value::Uint(255)),
            (vec![0x53, 0xff], Value::Ulong(255)),
            (vec![0x54, 0xff], Value::Int(-1)),
            (vec![0x55, 0x80], Value::Long(-128)),
            (vec![0x60, 0xff, 0xff], Value::Ushort(u16::MAX)),
            (vec![0x61, 0x80, 0], Value::Short(i16::MIN)),
            (vec![0x70, 0xff, 0xff, 0xff, 0xff], Value::Uint(u32::MAX)),
            (vec![0x71, 0x80, 0, 0, 0], Value::Int(i32::MIN)),
            (vec![0x73, 0, 1, 0xf6, 0], Value::Char('\u{1f600}')),
            (
                vec![0x74, 1, 2, 3, 4],
                Value::Decimal32([1, 2, 3, 4].into()),
            ),
            (
                vec![0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                Value::Ulong(u64::MAX),
            ),
            (vec![0x81, 0x80, 0, 0, 0, 0, 0, 0, 0], Value::Long(i64::MIN)),
            (
                vec![0x83, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                Value::Timestamp((-1_i64).into()),
            ),
            (
                vec![0x84, 1, 2, 3, 4, 5, 6, 7, 8],
                Value::Decimal64([1, 2, 3, 4, 5, 6, 7, 8].into()),
            ),
            (vec![0xa0, 2, 0, 0xff], Value::Binary(vec![0, 0xff].into())),
            (
                vec![0xb0, 0, 0, 0, 2, 0, 0xff],
                Value::Binary(vec![0, 0xff].into()),
            ),
            (vec![0xa1, 2, b'o', b'k'], Value::String("ok".to_owned())),
            (
                vec![0xb1, 0, 0, 0, 2, b'o', b'k'],
                Value::String("ok".to_owned()),
            ),
            (vec![0xa3, 1, b'x'], Value::Symbol("x".into())),
            (vec![0xb3, 0, 0, 0, 1, b'x'], Value::Symbol("x".into())),
        ] {
            assert_eq!(decoded(&wire), expected, "{wire:x?}");
            let mut trailing = wire.clone();
            trailing.push(0x40);
            assert_eq!(
                decode_value(&trailing).expect("first value decodes").1,
                wire.len()
            );
        }
        for (code, expected) in [
            (0x94, Value::Decimal128([7; 16].into())),
            (0x98, Value::Uuid([7; 16].into())),
        ] {
            let mut wire = vec![code];
            wire.extend_from_slice(&[7; 16]);
            assert_eq!(decoded(&wire), expected);
        }
        let wire = [0x72, 0x7f, 0xc1, 0x23, 0x45];
        let Value::Float(value) = decoded(&wire) else {
            panic!("float expected")
        };
        assert_eq!(value.0.to_bits(), 0x7fc1_2345);
        let wire = [0x82, 0xff, 0xf8, 1, 0x23, 0x45, 0x67, 0x89, 0xab];
        let Value::Double(value) = decoded(&wire) else {
            panic!("double expected")
        };
        assert_eq!(value.0.to_bits(), 0xfff8_0123_4567_89ab);
    }

    #[test]
    fn arrays_apply_their_constructor_once_even_for_zero_width_values() {
        assert_eq!(
            decoded(&[0xe0, 2, 2, 0x40]),
            Value::Array(vec![Value::Null, Value::Null].into())
        );
        assert_eq!(
            decoded(&[0xe0, 2, 3, 0x41]),
            Value::Array(vec![Value::Bool(true); 3].into())
        );
        assert_eq!(
            decoded(&[0xe0, 2, 0, 0x70]),
            Value::Array(Vec::<Value>::new().into())
        );
        assert_eq!(
            decoded(&[0xe0, 4, 2, 0x54, 0xff, 0x80]),
            Value::Array(vec![Value::Int(-1), Value::Int(-128)].into())
        );
    }

    #[test]
    fn compound_and_described_array_elements_keep_their_boundaries() {
        let list_array = [
            0xe0, 0x13, 2, 0xd0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0x40,
        ];
        assert_eq!(
            decoded(&list_array),
            Value::Array(vec![Value::List(Vec::new()), Value::List(vec![Value::Null])].into())
        );

        let nested_array = [
            0xe0, 0x18, 2, 0xf0, 0, 0, 0, 5, 0, 0, 0, 2, 0x40, 0, 0, 0, 9, 0, 0, 0, 1, 0x71, 0, 0,
            0, 3,
        ];
        assert_eq!(
            decoded(&nested_array),
            Value::Array(
                vec![
                    Value::Array(vec![Value::Null, Value::Null].into()),
                    Value::Array(vec![Value::Int(3)].into()),
                ]
                .into()
            )
        );

        assert_eq!(
            decoded(&[0xe0, 5, 2, 0, 0x53, 123, 0x40]),
            Value::Array(vec![described(123, Value::Null); 2].into())
        );
        let described_array = [
            0xe0, 0x15, 2, 0, 0x53, 123, 0x81, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
        ];
        assert_eq!(
            decoded(&described_array),
            Value::Array(
                vec![
                    described(123, Value::Long(1)),
                    described(123, Value::Long(-1))
                ]
                .into()
            )
        );

        let map_payload = [0, 0, 0, 6, 0, 0, 0, 2, 0x43, 0x40];
        let mut map_array = vec![0xe0, 12, 1, 0xd1];
        map_array.extend_from_slice(&map_payload);
        let entries = [(Value::Uint(0), Value::Null)].into_iter().collect();
        assert_eq!(
            decoded(&map_array),
            Value::Array(vec![Value::Map(entries)].into())
        );
    }

    #[test]
    fn nested_described_values_and_symbol_descriptors_decode_without_serde() {
        assert_eq!(
            decoded(&[0, 0x53, 7, 0, 0x44, 0x40]),
            described(7, described(0, Value::Null))
        );
        assert_eq!(
            decoded(&[0, 0xa3, 1, b'x', 0x52, 5]),
            Value::Described(Box::new(Described {
                descriptor: Descriptor::Name("x".into()),
                value: Value::Uint(5),
            }))
        );
    }

    #[test]
    fn malformed_values_and_container_mismatches_are_rejected() {
        for wire in [
            vec![],
            vec![0xff],
            vec![0x56, 2],
            vec![0xa1, 1, 0xff],
            vec![0xa3, 1, 0x80],
            vec![0x73, 0, 0, 0xd8, 0],
            vec![0x73, 0, 0x11, 0, 0],
            vec![0xb0, 0, 0, 0, 2, 1],
            vec![0xc0, 0, 0],
            vec![0xc0, 2, 2, 0x40],
            vec![0xc0, 3, 1, 0x40, 0x40],
            vec![0xc1, 2, 1, 0x40],
            vec![0xc1, 5, 4, 0x43, 0x40, 0x43, 0x40],
            vec![0xe0, 1, 0],
            vec![0xe0, 2, 0, 0xff],
            vec![0xe0, 4, 2, 0x70, 0, 0],
            vec![0xe0, 3, 2, 0x40, 0x40],
            vec![0, 0x43, 0x40],
        ] {
            assert!(decode_value(&wire).is_err(), "accepted {wire:x?}");
        }
        for valid in [
            vec![0x80, 1, 2, 3, 4, 5, 6, 7, 8],
            vec![0xa1, 3, b'a', b'b', b'c'],
            vec![0xc0, 2, 1, 0x40],
            vec![0xe0, 6, 1, 0x70, 1, 2, 3, 4],
            vec![0, 0x53, 5, 0x52, 1],
        ] {
            decoded(&valid);
            for length in 0..valid.len() {
                assert!(
                    decode_value(&valid[..length]).is_err(),
                    "accepted truncated {valid:x?} at {length}"
                );
            }
        }
    }

    #[test]
    fn duplicate_float_keys_are_rejected_before_a_map_can_collapse_them() {
        for (first, second) in [(0_u32, 0x8000_0000_u32), (0x7fc0_0001, 0xffc0_0002)] {
            let mut payload = Vec::new();
            for bits in [first, second] {
                payload.push(0x72);
                payload.extend_from_slice(&bits.to_be_bytes());
                payload.push(0x40);
            }
            assert!(decode_value(&counted(0xd1, 4, &payload)).is_err());
        }
    }

    #[test]
    fn zero_width_array_amplification_and_nested_values_obey_both_budgets() {
        let array = counted(
            0xf0,
            u32::try_from(MAX_VALUE_ELEMENTS - 1).expect("small budget"),
            &[0x40],
        );
        let Value::Array(values) = decoded(&array) else {
            panic!("array expected")
        };
        assert_eq!(values.len(), MAX_VALUE_ELEMENTS - 1);
        assert!(
            decode_value(&counted(
                0xf0,
                u32::try_from(MAX_VALUE_ELEMENTS).expect("small budget"),
                &[0x40]
            ))
            .is_err()
        );
        assert!(decode_value(&counted(0xf0, u32::MAX, &[0x40])).is_err());

        let mut wire = vec![0x40];
        for _ in 0..MAX_VALUE_NESTING {
            wire = counted(0xd0, 1, &wire);
        }
        decoded(&wire);
        assert!(decode_value(&counted(0xd0, 1, &wire)).is_err());
    }

    fn named_null_array(name: &str, count: u32) -> Vec<u8> {
        let mut constructor = vec![0x00, 0xb3];
        constructor.extend_from_slice(
            &u32::try_from(name.len())
                .expect("small descriptor")
                .to_be_bytes(),
        );
        constructor.extend_from_slice(name.as_bytes());
        constructor.push(0x40);
        counted(0xf0, count, &constructor)
    }

    #[test]
    fn shared_named_descriptors_cannot_amplify_small_input_past_the_byte_budget() {
        let wire = named_null_array(&"x".repeat(4096), 2048);
        assert!(wire.len() < 8192);
        let error =
            decode_value(&wire).expect_err("eight MiB of descriptor clones exceeds the budget");
        assert!(error.to_string().contains("expanded value byte limit"));

        let wire = named_null_array("abcdefgh", 2);
        for (bytes, accepted) in [(24, true), (23, false)] {
            let mut budget =
                MessageDecodeBudget::new(MAX_VALUE_ELEMENTS, bytes).expect("bounded budget");
            let mut decoder = ValueDecoder::new(&wire, &mut budget);
            assert_eq!(decoder.value(0).is_ok(), accepted);
        }
    }

    #[test]
    fn consecutive_values_share_the_element_budget_and_report_per_value_lengths() {
        let wire = [0xe0, 2, 2, 0x40, 0x52, 7, 0x40, 0x40];
        let mut budget =
            MessageDecodeBudget::new(5, MAX_EXPANDED_VALUE_BYTES).expect("bounded budget");
        let mut decoder = ValueDecoder::new(&wire, &mut budget);
        assert_eq!(
            decoder.next_value().expect("first array"),
            (Value::Array(vec![Value::Null, Value::Null].into()), 4)
        );
        assert_eq!(
            decoder.next_value().expect("compact uint"),
            (Value::Uint(7), 2)
        );
        assert_eq!(
            decoder.next_value().expect("last allowed node"),
            (Value::Null, 1)
        );
        let error = decoder.next_value().expect_err("no element budget remains");
        assert!(error.to_string().contains("element limit"));

        let wire = [0xe0, 2, 2, 0x40, 0xe0, 2, 2, 0x40];
        let mut budget =
            MessageDecodeBudget::new(5, MAX_EXPANDED_VALUE_BYTES).expect("bounded budget");
        let mut decoder = ValueDecoder::new(&wire, &mut budget);
        decoder.next_value().expect("first array uses three nodes");
        assert!(
            decoder.next_value().is_err(),
            "second array cannot reset the budget"
        );
    }

    #[test]
    fn consecutive_values_share_the_expanded_byte_budget() {
        let wire = [0xa1, 2, b'a', b'b', 0xa0, 2, 1, 2, 0xa3, 1, b'x'];
        let mut budget = MessageDecodeBudget::new(MAX_VALUE_ELEMENTS, 4).expect("bounded budget");
        let mut decoder = ValueDecoder::new(&wire, &mut budget);
        assert_eq!(
            decoder.next_value().expect("string consumes two bytes"),
            (Value::String("ab".to_owned()), 4)
        );
        assert_eq!(
            decoder.next_value().expect("binary consumes two bytes"),
            (Value::Binary(vec![1, 2].into()), 4)
        );
        let error = decoder
            .next_value()
            .expect_err("no copied-byte budget remains");
        assert!(error.to_string().contains("expanded value byte limit"));

        let array = named_null_array("abcdefgh", 2);
        let wire = [array.as_slice(), array.as_slice()].concat();
        let mut budget = MessageDecodeBudget::new(MAX_VALUE_ELEMENTS, 47).expect("bounded budget");
        let mut decoder = ValueDecoder::new(&wire, &mut budget);
        decoder.next_value().expect("first array consumes 24 bytes");
        assert!(
            decoder.next_value().is_err(),
            "a shared constructor must not get a new expansion budget"
        );
    }
}
