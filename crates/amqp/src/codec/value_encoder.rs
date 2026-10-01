//! Borrowed value encoding with a shared measuring and writing traversal.

use std::io;

use serde_amqp::{Value, descriptor::Descriptor};

use crate::value_codec::{MAX_VALUE_ELEMENTS, MAX_VALUE_NESTING};

#[cfg(test)]
thread_local! {
    static BUFFER_ALLOCATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn buffer_allocations() -> usize {
    BUFFER_ALLOCATIONS.with(std::cell::Cell::get)
}

#[derive(Clone, Copy)]
pub(super) enum Field<'a> {
    Null,
    Bool(bool),
    Ubyte(u8),
    Uint(u32),
    Ulong(u64),
    Timestamp(i64),
    String(&'a str),
    Binary(&'a [u8]),
    Symbol(&'a str),
    Uuid(&'a [u8; 16]),
    Value(&'a Value),
}

enum Sink {
    Count,
    Write(Vec<u8>),
}

pub(super) struct Encoder {
    sink: Sink,
    position: usize,
    maximum: usize,
    depth: usize,
    remaining_values: usize,
}

pub(super) fn measure(encode: impl FnOnce(&mut Encoder) -> io::Result<()>) -> io::Result<usize> {
    let mut encoder = Encoder {
        sink: Sink::Count,
        position: 0,
        maximum: usize::MAX,
        depth: 0,
        remaining_values: MAX_VALUE_ELEMENTS,
    };
    encode(&mut encoder)?;
    Ok(encoder.position)
}

pub(super) fn write_prepared(
    encoded_len: usize,
    encode: impl FnOnce(&mut Encoder) -> io::Result<()>,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(encoded_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "AMQP message buffer allocation failed",
        )
    })?;
    #[cfg(test)]
    if encoded_len != 0 {
        BUFFER_ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
    }
    let mut encoder = Encoder {
        sink: Sink::Write(bytes),
        position: 0,
        maximum: encoded_len,
        depth: 0,
        remaining_values: MAX_VALUE_ELEMENTS,
    };
    encode(&mut encoder)?;
    if encoder.position != encoded_len {
        return Err(invalid("prepared AMQP message length changed"));
    }
    let Sink::Write(bytes) = encoder.sink else {
        unreachable!("a prepared encoder always has a writing sink")
    };
    Ok(bytes)
}

impl Encoder {
    pub(super) fn field(&mut self, field: Field<'_>) -> io::Result<()> {
        if let Field::Value(value) = field {
            return self.value(value);
        }
        self.visit()?;
        match field {
            Field::Null => self.bytes(&[0x40]),
            Field::Bool(value) => self.bytes(&[if value { 0x41 } else { 0x42 }]),
            Field::Ubyte(value) => self.fixed(0x50, &[value]),
            Field::Uint(value) => self.uint(value),
            Field::Ulong(value) => self.ulong(value),
            Field::Timestamp(value) => self.fixed(0x83, &value.to_be_bytes()),
            Field::String(value) => self.variable(0xa1, 0xb1, value.as_bytes()),
            Field::Binary(value) => self.variable(0xa0, 0xb0, value),
            Field::Symbol(value) => {
                ascii_symbol(value)?;
                self.variable(0xa3, 0xb3, value.as_bytes())
            }
            Field::Uuid(value) => self.fixed(0x98, value),
            Field::Value(_) => unreachable!("borrowed values are handled before field visits"),
        }
    }

    pub(super) fn described(
        &mut self,
        descriptor: u64,
        body: impl FnOnce(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        self.visit()?;
        self.bytes(&[0x00])?;
        self.ulong(descriptor)?;
        self.children(body)
    }

    pub(super) fn list<'a>(
        &mut self,
        count: usize,
        values: impl IntoIterator<Item = Field<'a>>,
    ) -> io::Result<()> {
        self.visit()?;
        let mut values = values.into_iter();
        if count == 0 {
            if values.next().is_some() {
                return Err(invalid("AMQP list count does not match its fields"));
            }
            return self.bytes(&[0x45]);
        }
        self.counted(Some((0xc0, 0xd0)), count, |encoder| {
            encoder.children(|encoder| {
                let mut actual = 0_usize;
                for value in values {
                    actual = actual
                        .checked_add(1)
                        .ok_or_else(|| invalid("AMQP collection count overflow"))?;
                    encoder.field(value)?;
                }
                if actual != count {
                    return Err(invalid("AMQP list count does not match its fields"));
                }
                Ok(())
            })
        })
    }

    pub(super) fn map<'a>(
        &mut self,
        pair_count: usize,
        entries: impl IntoIterator<Item = (Field<'a>, Field<'a>)>,
    ) -> io::Result<()> {
        self.visit()?;
        let count = pair_count
            .checked_mul(2)
            .ok_or_else(|| invalid("AMQP map count overflow"))?;
        self.counted(Some((0xc1, 0xd1)), count, |encoder| {
            encoder.children(|encoder| {
                let mut actual = 0_usize;
                for (key, value) in entries {
                    actual = actual
                        .checked_add(1)
                        .ok_or_else(|| invalid("AMQP collection count overflow"))?;
                    encoder.field(key)?;
                    encoder.field(value)?;
                }
                if actual != pair_count {
                    return Err(invalid("AMQP map count does not match its entries"));
                }
                Ok(())
            })
        })
    }

    pub(super) fn value(&mut self, value: &Value) -> io::Result<()> {
        match value {
            Value::List(values) => self.list(values.len(), values.iter().map(Field::Value)),
            Value::Map(entries) => self.map(
                entries.len(),
                entries
                    .iter()
                    .map(|(key, value)| (Field::Value(key), Field::Value(value))),
            ),
            Value::Array(values) => {
                self.visit()?;
                self.counted(Some((0xe0, 0xf0)), values.len(), |encoder| {
                    encoder.array_body(values)
                })
            }
            Value::Described(value) => {
                self.visit()?;
                self.bytes(&[0x00])?;
                self.descriptor(&value.descriptor, true)?;
                self.children(|encoder| encoder.value(&value.value))
            }
            Value::Null => self.field(Field::Null),
            Value::Bool(value) => self.field(Field::Bool(*value)),
            Value::Ubyte(value) => self.field(Field::Ubyte(*value)),
            Value::Uint(value) => self.field(Field::Uint(*value)),
            Value::Ulong(value) => self.field(Field::Ulong(*value)),
            Value::Int(value) if i8::try_from(*value).is_ok() => {
                self.visit()?;
                self.fixed(0x54, &[*value as u8])
            }
            Value::Long(value) if i8::try_from(*value).is_ok() => {
                self.visit()?;
                self.fixed(0x55, &[*value as u8])
            }
            Value::Binary(value) => self.field(Field::Binary(value)),
            Value::String(value) => self.field(Field::String(value)),
            Value::Symbol(value) => self.field(Field::Symbol(value.as_str())),
            _ => self.array_element(value, true),
        }
    }

    fn visit(&mut self) -> io::Result<()> {
        depth(self.depth)?;
        self.remaining_values = self
            .remaining_values
            .checked_sub(1)
            .ok_or_else(|| invalid("AMQP value element limit exceeded"))?;
        Ok(())
    }

    fn children<T>(&mut self, encode: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        self.depth += 1;
        let result = encode(self);
        self.depth -= 1;
        result
    }

    fn bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("AMQP encoded message size overflow"))?;
        if end > self.maximum {
            return Err(invalid("prepared AMQP message length exceeded"));
        }
        if let Sink::Write(buffer) = &mut self.sink {
            buffer.extend_from_slice(bytes);
        }
        self.position = end;
        Ok(())
    }

    fn fixed(&mut self, code: u8, bytes: &[u8]) -> io::Result<()> {
        self.bytes(&[code])?;
        self.bytes(bytes)
    }

    fn uint(&mut self, value: u32) -> io::Result<()> {
        match value {
            0 => self.bytes(&[0x43]),
            1..=255 => self.fixed(0x52, &[value as u8]),
            _ => self.fixed(0x70, &value.to_be_bytes()),
        }
    }

    fn ulong(&mut self, value: u64) -> io::Result<()> {
        match value {
            0 => self.bytes(&[0x44]),
            1..=255 => self.fixed(0x53, &[value as u8]),
            _ => self.fixed(0x80, &value.to_be_bytes()),
        }
    }

    fn descriptor(&mut self, descriptor: &Descriptor, write: bool) -> io::Result<()> {
        match descriptor {
            Descriptor::Code(value) if write => self.ulong(*value),
            Descriptor::Code(_) => Ok(()),
            Descriptor::Name(value) => {
                ascii_symbol(value.as_str())?;
                if write {
                    self.variable(0xa3, 0xb3, value.as_str().as_bytes())
                } else {
                    Ok(())
                }
            }
        }
    }

    fn variable(&mut self, short: u8, long: u8, bytes: &[u8]) -> io::Result<()> {
        if let Ok(size) = u8::try_from(bytes.len()) {
            self.bytes(&[short, size])?;
        } else {
            self.bytes(&[long])?;
            self.length(bytes.len())?;
        }
        self.bytes(bytes)
    }

    fn length(&mut self, length: usize) -> io::Result<()> {
        let length = u32::try_from(length).map_err(|_| invalid("AMQP value size overflow"))?;
        self.bytes(&length.to_be_bytes())
    }

    fn counted(
        &mut self,
        compact: Option<(u8, u8)>,
        count: usize,
        body: impl FnOnce(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        let count = u32::try_from(count).map_err(|_| invalid("AMQP collection count overflow"))?;
        let start = self.position;
        if let Some((short, _)) = compact {
            self.bytes(&[short, 0, 0])?;
        } else {
            self.bytes(&[0; 8])?;
        }
        let body_start = self.position;
        body(self)?;
        let body_len = self.position - body_start;
        let size = body_len
            .checked_add(4)
            .and_then(|size| u32::try_from(size).ok())
            .ok_or_else(|| invalid("AMQP collection size overflow"))?;
        match compact {
            Some((_, long)) if body_len > 254 || count > u32::from(u8::MAX) => {
                // A short provisional header keeps intermediate output within the
                // exact final capacity, including nested collections that expand.
                let old_end = self.position;
                self.bytes(&[0; 6])?;
                if let Sink::Write(buffer) = &mut self.sink {
                    buffer.copy_within(start + 3..old_end, start + 9);
                    buffer[start] = long;
                    buffer[start + 1..start + 5].copy_from_slice(&size.to_be_bytes());
                    buffer[start + 5..start + 9].copy_from_slice(&count.to_be_bytes());
                }
            }
            Some(_) => {
                if let Sink::Write(buffer) = &mut self.sink {
                    buffer[start + 1] = (body_len + 1) as u8;
                    buffer[start + 2] = count as u8;
                }
            }
            None => {
                if let Sink::Write(buffer) = &mut self.sink {
                    buffer[start..start + 4].copy_from_slice(&size.to_be_bytes());
                    buffer[start + 4..start + 8].copy_from_slice(&count.to_be_bytes());
                }
            }
        }
        Ok(())
    }

    fn array_body(&mut self, values: &[Value]) -> io::Result<()> {
        let Some(first) = values.first() else {
            return Err(invalid("empty array has no retained element constructor"));
        };
        self.children(|encoder| {
            encoder.array_element(first, true)?;
            for value in &values[1..] {
                if !same_constructor(first, value, encoder.depth)? {
                    return Err(invalid("array elements have incompatible constructors"));
                }
                encoder.array_element(value, false)?;
            }
            Ok(())
        })
    }

    fn array_element(&mut self, value: &Value, write_constructor: bool) -> io::Result<()> {
        self.visit()?;
        if let Value::Described(value) = value {
            if write_constructor {
                self.bytes(&[0x00])?;
            }
            self.descriptor(&value.descriptor, write_constructor)?;
            return self.children(|encoder| encoder.array_element(&value.value, write_constructor));
        }
        let code = constructor(value);
        if write_constructor {
            self.bytes(&[code])?;
        }
        match value {
            Value::Null => Ok(()),
            Value::Bool(value) => self.bytes(&[u8::from(*value)]),
            Value::Ubyte(value) => self.bytes(&[*value]),
            Value::Ushort(value) => self.bytes(&value.to_be_bytes()),
            Value::Uint(value) => self.bytes(&value.to_be_bytes()),
            Value::Ulong(value) => self.bytes(&value.to_be_bytes()),
            Value::Byte(value) => self.bytes(&value.to_be_bytes()),
            Value::Short(value) => self.bytes(&value.to_be_bytes()),
            Value::Int(value) => self.bytes(&value.to_be_bytes()),
            Value::Long(value) => self.bytes(&value.to_be_bytes()),
            Value::Float(value) => self.bytes(&value.0.to_bits().to_be_bytes()),
            Value::Double(value) => self.bytes(&value.0.to_bits().to_be_bytes()),
            Value::Decimal32(value) => self.bytes(&value.clone().into_inner()),
            Value::Decimal64(value) => self.bytes(&value.clone().into_inner()),
            Value::Decimal128(value) => self.bytes(&value.clone().into_inner()),
            Value::Char(value) => self.bytes(&u32::from(*value).to_be_bytes()),
            Value::Timestamp(value) => self.bytes(&value.milliseconds().to_be_bytes()),
            Value::Uuid(value) => self.bytes(value.as_ref()),
            Value::Binary(value) => {
                self.length(value.len())?;
                self.bytes(value)
            }
            Value::String(value) => {
                self.length(value.len())?;
                self.bytes(value.as_bytes())
            }
            Value::Symbol(value) => {
                ascii_symbol(value.as_str())?;
                self.length(value.as_str().len())?;
                self.bytes(value.as_str().as_bytes())
            }
            Value::List(values) => self.counted(None, values.len(), |encoder| {
                encoder.children(|encoder| {
                    for value in values {
                        encoder.value(value)?;
                    }
                    Ok(())
                })
            }),
            Value::Map(entries) => {
                let count = entries
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| invalid("AMQP map count overflow"))?;
                self.counted(None, count, |encoder| {
                    encoder.children(|encoder| {
                        for (key, value) in entries {
                            encoder.value(key)?;
                            encoder.value(value)?;
                        }
                        Ok(())
                    })
                })
            }
            Value::Array(values) => {
                self.counted(None, values.len(), |encoder| encoder.array_body(values))
            }
            Value::Described(_) => unreachable!("described array constructors are handled first"),
        }
    }
}

fn same_constructor(left: &Value, right: &Value, nesting: usize) -> io::Result<bool> {
    depth(nesting)?;
    match (left, right) {
        (Value::Described(left), Value::Described(right)) => Ok(left.descriptor
            == right.descriptor
            && same_constructor(&left.value, &right.value, nesting + 1)?),
        (Value::Described(_), _) | (_, Value::Described(_)) => Ok(false),
        _ => Ok(constructor(left) == constructor(right)),
    }
}

fn constructor(value: &Value) -> u8 {
    match value {
        Value::Null => 0x40,
        Value::Bool(_) => 0x56,
        Value::Ubyte(_) => 0x50,
        Value::Ushort(_) => 0x60,
        Value::Uint(_) => 0x70,
        Value::Ulong(_) => 0x80,
        Value::Byte(_) => 0x51,
        Value::Short(_) => 0x61,
        Value::Int(_) => 0x71,
        Value::Long(_) => 0x81,
        Value::Float(_) => 0x72,
        Value::Double(_) => 0x82,
        Value::Decimal32(_) => 0x74,
        Value::Decimal64(_) => 0x84,
        Value::Decimal128(_) => 0x94,
        Value::Char(_) => 0x73,
        Value::Timestamp(_) => 0x83,
        Value::Uuid(_) => 0x98,
        Value::Binary(_) => 0xb0,
        Value::String(_) => 0xb1,
        Value::Symbol(_) => 0xb3,
        Value::List(_) => 0xd0,
        Value::Map(_) => 0xd1,
        Value::Array(_) => 0xf0,
        Value::Described(_) => unreachable!("described values have a compound constructor"),
    }
}

fn ascii_symbol(value: &str) -> io::Result<()> {
    if value.is_ascii() {
        Ok(())
    } else {
        Err(invalid("AMQP symbol contains non-ASCII characters"))
    }
}

fn depth(depth: usize) -> io::Result<()> {
    if depth > MAX_VALUE_NESTING {
        Err(invalid("AMQP value nesting limit exceeded"))
    } else {
        Ok(())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
