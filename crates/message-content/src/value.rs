use crate::limits::{Budget, Error, Limits};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Width {
    One,
    Four,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binary {
    pub bytes: Vec<u8>,
    pub width: Width,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Text {
    pub text: String,
    pub width: Width,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Symbol {
    pub text: String,
    pub width: Width,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Descriptor {
    ULong(u64),
    Symbol(Symbol),
}

/// Exact shared array constructor (including zero-width and described constructors).
/// Null is not a legal array-element constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PrimitiveConstructor {
    True = 0x41,
    False = 0x42,
    UInt0 = 0x43,
    ULong0 = 0x44,
    List0 = 0x45,
    UByte = 0x50,
    Byte = 0x51,
    SmallUInt = 0x52,
    SmallULong = 0x53,
    SmallInt = 0x54,
    SmallLong = 0x55,
    Bool = 0x56,
    UShort = 0x60,
    Short = 0x61,
    UInt = 0x70,
    Int = 0x71,
    Float = 0x72,
    Char = 0x73,
    Decimal32 = 0x74,
    ULong = 0x80,
    Long = 0x81,
    Double = 0x82,
    Timestamp = 0x83,
    Decimal64 = 0x84,
    Decimal128 = 0x94,
    Uuid = 0x98,
    Binary8 = 0xa0,
    String8 = 0xa1,
    Symbol8 = 0xa3,
    Binary32 = 0xb0,
    String32 = 0xb1,
    Symbol32 = 0xb3,
    List8 = 0xc0,
    Map8 = 0xc1,
    List32 = 0xd0,
    Map32 = 0xd1,
    Array8 = 0xe0,
    Array32 = 0xf0,
}

impl PrimitiveConstructor {
    fn from_code(code: u8) -> Result<Self, Error> {
        Ok(match code {
            0x41 => Self::True,
            0x42 => Self::False,
            0x43 => Self::UInt0,
            0x44 => Self::ULong0,
            0x45 => Self::List0,
            0x50 => Self::UByte,
            0x51 => Self::Byte,
            0x52 => Self::SmallUInt,
            0x53 => Self::SmallULong,
            0x54 => Self::SmallInt,
            0x55 => Self::SmallLong,
            0x56 => Self::Bool,
            0x60 => Self::UShort,
            0x61 => Self::Short,
            0x70 => Self::UInt,
            0x71 => Self::Int,
            0x72 => Self::Float,
            0x73 => Self::Char,
            0x74 => Self::Decimal32,
            0x80 => Self::ULong,
            0x81 => Self::Long,
            0x82 => Self::Double,
            0x83 => Self::Timestamp,
            0x84 => Self::Decimal64,
            0x94 => Self::Decimal128,
            0x98 => Self::Uuid,
            0xa0 => Self::Binary8,
            0xa1 => Self::String8,
            0xa3 => Self::Symbol8,
            0xb0 => Self::Binary32,
            0xb1 => Self::String32,
            0xb3 => Self::Symbol32,
            0xc0 => Self::List8,
            0xc1 => Self::Map8,
            0xd0 => Self::List32,
            0xd1 => Self::Map32,
            0xe0 => Self::Array8,
            0xf0 => Self::Array32,
            _ => return Err(Error::InvalidConstructor),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Constructor {
    /// No constructor in a compatibility encoding of an empty array.
    /// Only valid directly on an array with no elements, never under `Described`.
    Absent,
    /// A null sentinel in a compatibility encoding of an empty array.
    /// This is not an element constructor; the same restrictions as `Absent` apply.
    Null,
    Primitive(PrimitiveConstructor),
    Described {
        descriptor: Descriptor,
        constructor: Box<Constructor>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Array {
    pub constructor: Constructor,
    /// Underlying values only; described wrappers are shared on `constructor`.
    pub elements: Vec<Value>,
}

/// Semantic AMQP values. Floating-point and decimal representations are raw bits,
/// so NaN payloads, signed zero and all decimal encodings have reflexive equality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Bool(bool),
    UByte(u8),
    UShort(u16),
    UInt(u32),
    ULong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Decimal32(u32),
    Decimal64(u64),
    Decimal128([u8; 16]),
    Char(char),
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Binary),
    String(Text),
    Symbol(Symbol),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Array(Array),
    Described {
        descriptor: Descriptor,
        value: Box<Value>,
    },
}

pub fn decode_value(bytes: &[u8], limits: Limits) -> Result<Value, Error> {
    let mut reader = Reader::new(bytes, limits)?;
    let value = reader.value(bytes.len(), 1)?;
    if reader.pos != bytes.len() {
        return Err(Error::TrailingBytes);
    }
    Ok(value)
}

pub fn encode_value(value: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    let mut writer = Writer::new(limits, true);
    writer.value(value, 1)?;
    Ok(writer.bytes)
}

/// Validate a public, possibly adversarially constructed tree without cloning it or
/// allocating an encoded buffer. Uses exactly the encoder's budgets and constructor checks.
pub fn validate_value(value: &Value, limits: Limits) -> Result<(), Error> {
    Writer::new(limits, false).value(value, 1)
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pub(crate) pos: usize,
    pub(crate) budget: Budget,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], limits: Limits) -> Result<Self, Error> {
        if bytes.len() > limits.max_bytes {
            return Err(Error::ByteLimit);
        }
        Ok(Self {
            bytes,
            pos: 0,
            budget: Budget::new(limits),
        })
    }

    fn take(&mut self, n: usize, end: usize) -> Result<&'a [u8], Error> {
        let next = self.pos.checked_add(n).ok_or(Error::Overflow)?;
        if next > end || next > self.bytes.len() {
            return Err(Error::Truncated);
        }
        self.budget.work(n)?;
        let bytes = &self.bytes[self.pos..next];
        self.pos = next;
        Ok(bytes)
    }

    fn fixed<const N: usize>(&mut self, end: usize) -> Result<[u8; N], Error> {
        self.take(N, end)?.try_into().map_err(|_| Error::Truncated)
    }

    fn byte(&mut self, end: usize) -> Result<u8, Error> {
        Ok(self.fixed::<1>(end)?[0])
    }

    fn size(&mut self, width: Width, end: usize) -> Result<usize, Error> {
        match width {
            Width::One => Ok(usize::from(self.byte(end)?)),
            Width::Four => {
                usize::try_from(u32::from_be_bytes(self.fixed(end)?)).map_err(|_| Error::Overflow)
            }
        }
    }

    fn variable(&mut self, width: Width, end: usize) -> Result<&'a [u8], Error> {
        let size = self.size(width, end)?;
        self.take(size, end)
    }

    fn descriptor(&mut self, end: usize, depth: usize) -> Result<Descriptor, Error> {
        self.budget.node(depth)?;
        Ok(match self.byte(end)? {
            0x44 => Descriptor::ULong(0),
            0x53 => Descriptor::ULong(u64::from(self.byte(end)?)),
            0x80 => Descriptor::ULong(u64::from_be_bytes(self.fixed(end)?)),
            code @ (0xa3 | 0xb3) => {
                let width = if code == 0xa3 {
                    Width::One
                } else {
                    Width::Four
                };
                let text = std::str::from_utf8(self.variable(width, end)?)
                    .map_err(|_| Error::InvalidUtf8)?
                    .to_owned();
                Descriptor::Symbol(Symbol { text, width })
            }
            _ => return Err(Error::InvalidDescriptor),
        })
    }

    pub(crate) fn value(&mut self, end: usize, depth: usize) -> Result<Value, Error> {
        self.budget.node(depth)?;
        match self.byte(end)? {
            0x00 => {
                let descriptor = self.descriptor(end, depth + 1)?;
                let value = Box::new(self.value(end, depth + 1)?);
                Ok(Value::Described { descriptor, value })
            }
            0x40 => Ok(Value::Null),
            code => self.payload(PrimitiveConstructor::from_code(code)?, end, depth),
        }
    }

    fn constructor(&mut self, end: usize, depth: usize) -> Result<Constructor, Error> {
        self.budget.node(depth)?;
        let code = self.byte(end)?;
        if code == 0x00 {
            let descriptor = self.descriptor(end, depth + 1)?;
            let constructor = Box::new(self.constructor(end, depth + 1)?);
            Ok(Constructor::Described {
                descriptor,
                constructor,
            })
        } else {
            Ok(Constructor::Primitive(PrimitiveConstructor::from_code(
                code,
            )?))
        }
    }

    fn array_element(
        &mut self,
        constructor: &Constructor,
        end: usize,
        depth: usize,
    ) -> Result<Value, Error> {
        match constructor {
            Constructor::Absent | Constructor::Null => Err(Error::InvalidConstructor),
            Constructor::Primitive(code) => {
                self.budget.node(depth)?;
                self.payload(*code, end, depth)
            }
            Constructor::Described { constructor, .. } => {
                self.budget.depth(depth)?;
                self.budget.work(1)?;
                self.array_element(constructor, end, depth + 1)
            }
        }
    }

    fn container(&mut self, width: Width, parent_end: usize) -> Result<(usize, usize), Error> {
        let size = self.size(width, parent_end)?;
        let end = self.pos.checked_add(size).ok_or(Error::Overflow)?;
        if end > parent_end {
            return Err(Error::Truncated);
        }
        let count = self.size(width, end)?;
        self.budget.children(count)?;
        Ok((end, count))
    }

    fn payload(
        &mut self,
        code: PrimitiveConstructor,
        end: usize,
        depth: usize,
    ) -> Result<Value, Error> {
        use PrimitiveConstructor as C;
        Ok(match code {
            C::True => Value::Bool(true),
            C::False => Value::Bool(false),
            C::UInt0 => Value::UInt(0),
            C::ULong0 => Value::ULong(0),
            C::List0 => Value::List(Vec::new()),
            C::Bool => Value::Bool(match self.byte(end)? {
                0 => false,
                1 => true,
                _ => return Err(Error::InvalidValue),
            }),
            C::UByte => Value::UByte(self.byte(end)?),
            C::Byte => Value::Byte(i8::from_be_bytes(self.fixed(end)?)),
            C::SmallUInt => Value::UInt(u32::from(self.byte(end)?)),
            C::SmallULong => Value::ULong(u64::from(self.byte(end)?)),
            C::SmallInt => Value::Int(i32::from(i8::from_be_bytes(self.fixed(end)?))),
            C::SmallLong => Value::Long(i64::from(i8::from_be_bytes(self.fixed(end)?))),
            C::UShort => Value::UShort(u16::from_be_bytes(self.fixed(end)?)),
            C::Short => Value::Short(i16::from_be_bytes(self.fixed(end)?)),
            C::UInt => Value::UInt(u32::from_be_bytes(self.fixed(end)?)),
            C::Int => Value::Int(i32::from_be_bytes(self.fixed(end)?)),
            C::ULong => Value::ULong(u64::from_be_bytes(self.fixed(end)?)),
            C::Long => Value::Long(i64::from_be_bytes(self.fixed(end)?)),
            C::Float => Value::Float(u32::from_be_bytes(self.fixed(end)?)),
            C::Double => Value::Double(u64::from_be_bytes(self.fixed(end)?)),
            C::Decimal32 => Value::Decimal32(u32::from_be_bytes(self.fixed(end)?)),
            C::Decimal64 => Value::Decimal64(u64::from_be_bytes(self.fixed(end)?)),
            C::Decimal128 => Value::Decimal128(self.fixed(end)?),
            C::Char => Value::Char(
                char::from_u32(u32::from_be_bytes(self.fixed(end)?)).ok_or(Error::InvalidValue)?,
            ),
            C::Timestamp => Value::Timestamp(i64::from_be_bytes(self.fixed(end)?)),
            C::Uuid => Value::Uuid(self.fixed(end)?),
            C::Binary8 | C::Binary32 => {
                let width = if code == C::Binary8 {
                    Width::One
                } else {
                    Width::Four
                };
                Value::Binary(Binary {
                    bytes: self.variable(width, end)?.to_vec(),
                    width,
                })
            }
            C::String8 | C::String32 | C::Symbol8 | C::Symbol32 => {
                let width = if matches!(code, C::String8 | C::Symbol8) {
                    Width::One
                } else {
                    Width::Four
                };
                let text = std::str::from_utf8(self.variable(width, end)?)
                    .map_err(|_| Error::InvalidUtf8)?
                    .to_owned();
                if matches!(code, C::String8 | C::String32) {
                    Value::String(Text { text, width })
                } else {
                    Value::Symbol(Symbol { text, width })
                }
            }
            C::List8 | C::List32 | C::Map8 | C::Map32 => {
                let width = if matches!(code, C::List8 | C::Map8) {
                    Width::One
                } else {
                    Width::Four
                };
                let (inner_end, count) = self.container(width, end)?;
                let map = matches!(code, C::Map8 | C::Map32);
                if map && !count.is_multiple_of(2) {
                    return Err(Error::OddMapCount);
                }
                // Each standalone element must at least contain its constructor byte.
                if count > inner_end - self.pos {
                    return Err(Error::InvalidLength);
                }
                let value = if map {
                    let mut pairs = Vec::new();
                    for _ in 0..count / 2 {
                        let key = self.value(inner_end, depth + 1)?;
                        let value = self.value(inner_end, depth + 1)?;
                        pairs.push((key, value));
                    }
                    Value::Map(pairs)
                } else {
                    let mut values = Vec::new();
                    for _ in 0..count {
                        values.push(self.value(inner_end, depth + 1)?);
                    }
                    Value::List(values)
                };
                if self.pos != inner_end {
                    return Err(Error::InvalidLength);
                }
                value
            }
            C::Array8 | C::Array32 => {
                let width = if code == C::Array8 {
                    Width::One
                } else {
                    Width::Four
                };
                let (inner_end, count) = self.container(width, end)?;
                // The existing public codec accepts these two empty-only forms.
                // Do not let them bypass constructor validation for nonempty arrays
                // or for a described constructor's underlying type.
                let constructor = if count == 0 && self.pos == inner_end {
                    Constructor::Absent
                } else if count == 0 && inner_end - self.pos == 1 && self.bytes[self.pos] == 0x40 {
                    self.budget.node(depth + 1)?;
                    self.byte(inner_end)?;
                    Constructor::Null
                } else {
                    self.constructor(inner_end, depth + 1)?
                };
                // Constructor work is also spent before reserving any element storage.
                self.budget.children(count)?;
                let mut elements = Vec::new();
                for _ in 0..count {
                    elements.push(self.array_element(&constructor, inner_end, depth + 1)?);
                }
                if self.pos != inner_end {
                    return Err(Error::InvalidLength);
                }
                Value::Array(Array {
                    constructor,
                    elements,
                })
            }
        })
    }
}

pub(crate) struct Writer {
    pub(crate) bytes: Vec<u8>,
    len: usize,
    retain: bool,
    compact: bool,
    container_end: Option<usize>,
    pub(crate) budget: Budget,
}

impl Writer {
    pub(crate) fn new(limits: Limits, retain: bool) -> Self {
        Self {
            bytes: Vec::new(),
            len: 0,
            retain,
            compact: false,
            container_end: None,
            budget: Budget::new(limits),
        }
    }

    pub(crate) fn put(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let next = self.len.checked_add(bytes.len()).ok_or(Error::Overflow)?;
        if next > self.budget.limits.max_bytes {
            return Err(Error::ByteLimit);
        }
        if self.container_end.is_some_and(|end| next > end) {
            return Err(Error::InvalidLength);
        }
        self.budget.work(bytes.len())?;
        if self.retain {
            self.bytes.extend_from_slice(bytes);
        }
        self.len = next;
        Ok(())
    }

    fn size(&mut self, value: usize, width: Width) -> Result<(), Error> {
        match width {
            Width::One => self.put(&[u8::try_from(value).map_err(|_| Error::InvalidLength)?]),
            Width::Four => self.put(
                &u32::try_from(value)
                    .map_err(|_| Error::Overflow)?
                    .to_be_bytes(),
            ),
        }
    }

    fn variable(&mut self, bytes: &[u8], width: Width) -> Result<(), Error> {
        self.size(bytes.len(), width)?;
        self.put(bytes)
    }

    pub(crate) fn descriptor(
        &mut self,
        descriptor: &Descriptor,
        depth: usize,
    ) -> Result<(), Error> {
        self.budget.node(depth)?;
        match descriptor {
            Descriptor::ULong(0) if self.compact => self.put(&[0x44]),
            Descriptor::ULong(n) if *n <= 255 => self.put(&[0x53, *n as u8]),
            Descriptor::ULong(n) => {
                self.put(&[0x80])?;
                self.put(&n.to_be_bytes())
            }
            Descriptor::Symbol(symbol) => {
                self.put(&[if symbol.width == Width::One {
                    0xa3
                } else {
                    0xb3
                }])?;
                self.variable(symbol.text.as_bytes(), symbol.width)
            }
        }
    }

    pub(crate) fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
        self.budget.node(depth)?;
        if let Value::Described { descriptor, value } = value {
            self.put(&[0x00])?;
            self.descriptor(descriptor, depth + 1)?;
            return self.value(value, depth + 1);
        }
        if matches!(value, Value::Null) {
            return self.put(&[0x40]);
        }
        // Retained short array constructors constrain their payload. Compact
        // descendants keep originally valid short containers re-encodable.
        if self.compact {
            match value {
                Value::List(values) if values.is_empty() => return self.put(&[0x45]),
                Value::List(values) => {
                    return self.adaptive_container(0xc0, 0xd0, values.len(), |w| {
                        for value in values {
                            w.value(value, depth + 1)?;
                        }
                        Ok(())
                    });
                }
                Value::Map(pairs) => {
                    return self.adaptive_container(
                        0xc1,
                        0xd1,
                        pairs.len().checked_mul(2).ok_or(Error::Overflow)?,
                        |w| {
                            for (key, value) in pairs {
                                w.value(key, depth + 1)?;
                                w.value(value, depth + 1)?;
                            }
                            Ok(())
                        },
                    );
                }
                Value::Array(array) => {
                    return self.adaptive_container(0xe0, 0xf0, array.elements.len(), |w| {
                        w.array_contents(array, depth)
                    });
                }
                _ => {}
            }
        }
        let code = match value {
            Value::Bool(true) if self.compact => PrimitiveConstructor::True,
            Value::Bool(false) if self.compact => PrimitiveConstructor::False,
            Value::Bool(_) => PrimitiveConstructor::Bool,
            Value::UByte(_) => PrimitiveConstructor::UByte,
            Value::UShort(_) => PrimitiveConstructor::UShort,
            Value::UInt(0) if self.compact => PrimitiveConstructor::UInt0,
            Value::UInt(n) if self.compact && *n <= 255 => PrimitiveConstructor::SmallUInt,
            Value::UInt(_) => PrimitiveConstructor::UInt,
            Value::ULong(0) if self.compact => PrimitiveConstructor::ULong0,
            Value::ULong(n) if self.compact && *n <= 255 => PrimitiveConstructor::SmallULong,
            Value::ULong(_) => PrimitiveConstructor::ULong,
            Value::Byte(_) => PrimitiveConstructor::Byte,
            Value::Short(_) => PrimitiveConstructor::Short,
            Value::Int(n) if self.compact && i8::try_from(*n).is_ok() => {
                PrimitiveConstructor::SmallInt
            }
            Value::Int(_) => PrimitiveConstructor::Int,
            Value::Long(n) if self.compact && i8::try_from(*n).is_ok() => {
                PrimitiveConstructor::SmallLong
            }
            Value::Long(_) => PrimitiveConstructor::Long,
            Value::Float(_) => PrimitiveConstructor::Float,
            Value::Double(_) => PrimitiveConstructor::Double,
            Value::Decimal32(_) => PrimitiveConstructor::Decimal32,
            Value::Decimal64(_) => PrimitiveConstructor::Decimal64,
            Value::Decimal128(_) => PrimitiveConstructor::Decimal128,
            Value::Char(_) => PrimitiveConstructor::Char,
            Value::Timestamp(_) => PrimitiveConstructor::Timestamp,
            Value::Uuid(_) => PrimitiveConstructor::Uuid,
            Value::Binary(b) => {
                if b.width == Width::One {
                    PrimitiveConstructor::Binary8
                } else {
                    PrimitiveConstructor::Binary32
                }
            }
            Value::String(t) => {
                if t.width == Width::One {
                    PrimitiveConstructor::String8
                } else {
                    PrimitiveConstructor::String32
                }
            }
            Value::Symbol(s) => {
                if s.width == Width::One {
                    PrimitiveConstructor::Symbol8
                } else {
                    PrimitiveConstructor::Symbol32
                }
            }
            Value::List(_) => PrimitiveConstructor::List32,
            Value::Map(_) => PrimitiveConstructor::Map32,
            Value::Array(_) => PrimitiveConstructor::Array32,
            Value::Null | Value::Described { .. } => return Err(Error::InvalidValue),
        };
        self.put(&[code as u8])?;
        self.payload(value, code, depth)
    }

    fn constructor(&mut self, constructor: &Constructor, depth: usize) -> Result<(), Error> {
        self.budget.node(depth)?;
        match constructor {
            Constructor::Absent | Constructor::Null => Err(Error::InvalidConstructor),
            Constructor::Primitive(code) => self.put(&[*code as u8]),
            Constructor::Described {
                descriptor,
                constructor,
            } => {
                self.put(&[0])?;
                self.descriptor(descriptor, depth + 1)?;
                self.constructor(constructor, depth + 1)
            }
        }
    }

    fn array_contents(&mut self, array: &Array, depth: usize) -> Result<(), Error> {
        match &array.constructor {
            Constructor::Absent if array.elements.is_empty() => Ok(()),
            Constructor::Null if array.elements.is_empty() => {
                self.budget.node(depth + 1)?;
                self.put(&[0x40])
            }
            Constructor::Absent | Constructor::Null => Err(Error::InvalidConstructor),
            constructor => {
                self.constructor(constructor, depth + 1)?;
                self.budget.children(array.elements.len())?;
                for value in &array.elements {
                    self.array_element(value, constructor, depth + 1)?;
                }
                Ok(())
            }
        }
    }

    fn array_element(
        &mut self,
        value: &Value,
        constructor: &Constructor,
        depth: usize,
    ) -> Result<(), Error> {
        match constructor {
            Constructor::Absent | Constructor::Null => Err(Error::InvalidConstructor),
            Constructor::Primitive(code) => {
                self.budget.node(depth)?;
                self.payload(value, *code, depth)
            }
            Constructor::Described { constructor, .. } => {
                self.budget.depth(depth)?;
                self.budget.work(1)?;
                self.array_element(value, constructor, depth + 1)
            }
        }
    }

    pub(crate) fn container<F>(&mut self, count: usize, width: Width, write: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Self) -> Result<(), Error>,
    {
        self.budget.children(count)?;
        let position = self.len;
        self.size(0, width)?;
        let start = self.len;
        self.size(count, width)?;
        let previous_end = self.container_end;
        if width == Width::One {
            let end = start.checked_add(255).ok_or(Error::Overflow)?;
            self.container_end = Some(previous_end.map_or(end, |outer| outer.min(end)));
        }
        let result = write(self);
        self.container_end = previous_end;
        result?;
        let size = self.len.checked_sub(start).ok_or(Error::Overflow)?;
        // Patching does not re-spend work already charged for the placeholder bytes.
        match width {
            Width::One => {
                let size = u8::try_from(size).map_err(|_| Error::InvalidLength)?;
                if self.retain {
                    self.bytes[position] = size;
                }
            }
            Width::Four => {
                let size = u32::try_from(size).map_err(|_| Error::Overflow)?;
                if self.retain {
                    self.bytes[position..start].copy_from_slice(&size.to_be_bytes());
                }
            }
        }
        Ok(())
    }

    /// Begin short, widening only if the actual compact payload requires it.
    /// Additional bytes and movement are budgeted before buffer mutation.
    fn adaptive_container<F>(
        &mut self,
        short: u8,
        long: u8,
        count: usize,
        write: F,
    ) -> Result<(), Error>
    where
        F: FnOnce(&mut Self) -> Result<(), Error>,
    {
        if count > 255 {
            self.put(&[long])?;
            return self.container(count, Width::Four, write);
        }
        self.budget.children(count)?;
        let code_position = self.len;
        self.put(&[short, 0, count as u8])?;
        let payload_start = self.len;
        write(self)?;
        let old_end = self.len;
        let payload_size = old_end.checked_sub(payload_start).ok_or(Error::Overflow)?;
        let short_size = payload_size.checked_add(1).ok_or(Error::Overflow)?;
        if let Ok(size) = u8::try_from(short_size) {
            if self.retain {
                self.bytes[code_position + 1] = size;
            }
        } else {
            let size = u32::try_from(payload_size.checked_add(4).ok_or(Error::Overflow)?)
                .map_err(|_| Error::Overflow)?;
            self.budget.work(payload_size)?;
            self.put(&[0; 6])?;
            if self.retain {
                // Successful writes prove these offsets fit in usize and the buffer.
                self.bytes
                    .copy_within(payload_start..old_end, payload_start + 6);
                self.bytes[code_position] = long;
                self.bytes[code_position + 1..code_position + 5]
                    .copy_from_slice(&size.to_be_bytes());
                self.bytes[code_position + 5..payload_start + 6]
                    .copy_from_slice(&(count as u32).to_be_bytes());
            }
        }
        Ok(())
    }

    fn compact_scope<F>(&mut self, compact: bool, write: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Self) -> Result<(), Error>,
    {
        let previous = self.compact;
        self.compact |= compact;
        let result = write(self);
        self.compact = previous;
        result
    }

    fn payload(
        &mut self,
        value: &Value,
        code: PrimitiveConstructor,
        depth: usize,
    ) -> Result<(), Error> {
        use PrimitiveConstructor as C;
        match (code, value) {
            (C::True, Value::Bool(true))
            | (C::False, Value::Bool(false))
            | (C::UInt0, Value::UInt(0))
            | (C::ULong0, Value::ULong(0)) => Ok(()),
            (C::List0, Value::List(values)) if values.is_empty() => Ok(()),
            (C::Bool, Value::Bool(b)) => self.put(&[u8::from(*b)]),
            (C::UByte, Value::UByte(n)) => self.put(&[*n]),
            (C::Byte, Value::Byte(n)) => self.put(&n.to_be_bytes()),
            (C::SmallUInt, Value::UInt(n)) => {
                self.put(&[u8::try_from(*n).map_err(|_| Error::InvalidValue)?])
            }
            (C::SmallULong, Value::ULong(n)) => {
                self.put(&[u8::try_from(*n).map_err(|_| Error::InvalidValue)?])
            }
            (C::SmallInt, Value::Int(n)) => self.put(
                &i8::try_from(*n)
                    .map_err(|_| Error::InvalidValue)?
                    .to_be_bytes(),
            ),
            (C::SmallLong, Value::Long(n)) => self.put(
                &i8::try_from(*n)
                    .map_err(|_| Error::InvalidValue)?
                    .to_be_bytes(),
            ),
            (C::UShort, Value::UShort(n)) => self.put(&n.to_be_bytes()),
            (C::Short, Value::Short(n)) => self.put(&n.to_be_bytes()),
            (C::UInt, Value::UInt(n))
            | (C::Float, Value::Float(n))
            | (C::Decimal32, Value::Decimal32(n)) => self.put(&n.to_be_bytes()),
            (C::Int, Value::Int(n)) => self.put(&n.to_be_bytes()),
            (C::ULong, Value::ULong(n))
            | (C::Double, Value::Double(n))
            | (C::Decimal64, Value::Decimal64(n)) => self.put(&n.to_be_bytes()),
            (C::Long, Value::Long(n)) | (C::Timestamp, Value::Timestamp(n)) => {
                self.put(&n.to_be_bytes())
            }
            (C::Decimal128, Value::Decimal128(n)) | (C::Uuid, Value::Uuid(n)) => self.put(n),
            (C::Char, Value::Char(c)) => self.put(&u32::from(*c).to_be_bytes()),
            (C::Binary8 | C::Binary32, Value::Binary(b)) => {
                let width = if code == C::Binary8 {
                    Width::One
                } else {
                    Width::Four
                };
                if b.width != width {
                    return Err(Error::InvalidValue);
                }
                self.variable(&b.bytes, width)
            }
            (C::String8 | C::String32, Value::String(t)) => {
                let width = if code == C::String8 {
                    Width::One
                } else {
                    Width::Four
                };
                if t.width != width {
                    return Err(Error::InvalidValue);
                }
                self.variable(t.text.as_bytes(), width)
            }
            (C::Symbol8 | C::Symbol32, Value::Symbol(s)) => {
                let width = if code == C::Symbol8 {
                    Width::One
                } else {
                    Width::Four
                };
                if s.width != width {
                    return Err(Error::InvalidValue);
                }
                self.variable(s.text.as_bytes(), width)
            }
            (C::List8 | C::List32, Value::List(values)) => {
                let width = if code == C::List8 {
                    Width::One
                } else {
                    Width::Four
                };
                self.compact_scope(width == Width::One, |w| {
                    w.container(values.len(), width, |w| {
                        for value in values {
                            w.value(value, depth + 1)?;
                        }
                        Ok(())
                    })
                })
            }
            (C::Map8 | C::Map32, Value::Map(pairs)) => {
                let width = if code == C::Map8 {
                    Width::One
                } else {
                    Width::Four
                };
                let count = pairs.len().checked_mul(2).ok_or(Error::Overflow)?;
                self.compact_scope(width == Width::One, |w| {
                    w.container(count, width, |w| {
                        for (key, value) in pairs {
                            w.value(key, depth + 1)?;
                            w.value(value, depth + 1)?;
                        }
                        Ok(())
                    })
                })
            }
            (C::Array8 | C::Array32, Value::Array(array)) => {
                let width = if code == C::Array8 {
                    Width::One
                } else {
                    Width::Four
                };
                self.compact_scope(width == Width::One, |w| {
                    w.container(array.elements.len(), width, |w| {
                        w.array_contents(array, depth)
                    })
                })
            }
            _ => Err(Error::InvalidValue),
        }
    }
}
