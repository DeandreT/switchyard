use std::sync::Arc;

use crate::{
    Binary, Descriptor, Error, Limits, Symbol, Text, Value, Width,
    value::{Reader, Writer},
};

/// A field missing from the end of a composite list, an explicit null, or a value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Field<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub durable: Field<bool>,
    pub priority: Field<u8>,
    pub ttl: Field<u32>,
    pub first_acquirer: Field<bool>,
    pub delivery_count: Field<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageId {
    String(Text),
    ULong(u64),
    Uuid([u8; 16]),
    Binary(Binary),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties {
    pub message_id: Field<MessageId>,
    pub user_id: Field<Binary>,
    pub to: Field<Text>,
    pub subject: Field<Text>,
    pub reply_to: Field<Text>,
    pub correlation_id: Field<MessageId>,
    pub content_type: Field<Symbol>,
    pub content_encoding: Field<Symbol>,
    pub absolute_expiry_time: Field<i64>,
    pub creation_time: Field<i64>,
    pub group_id: Field<Text>,
    pub group_sequence: Field<u32>,
    pub reply_to_group_id: Field<Text>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnnotationKey {
    Symbol(Symbol),
    ULong(u64),
}

/// Ordered entries, including explicit nulls and compound values.
pub type Annotations = Vec<(AnnotationKey, Value)>;
pub type ApplicationProperties = Vec<(Text, Value)>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Body {
    #[default]
    Empty,
    /// Nonempty section vector; an individual section can have zero bytes.
    Data(Vec<Binary>),
    /// Nonempty section vector; an individual section can be an empty list.
    Sequence(Vec<Vec<Value>>),
    Value(Value),
}

/// A transient semantic view, deliberately not a serde persistence schema.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Content {
    pub header: Option<Header>,
    pub delivery_annotations: Option<Annotations>,
    pub message_annotations: Option<Annotations>,
    pub properties: Option<Properties>,
    pub application_properties: Option<ApplicationProperties>,
    pub body: Body,
    pub footer: Option<Annotations>,
}

/// One authoritative encoded source; typed materializations are not retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedContent {
    bytes: Arc<[u8]>,
}

impl EncodedContent {
    /// Validate before copying borrowed input into immutable owned storage.
    pub fn from_bytes(bytes: &[u8], limits: Limits) -> Result<Self, Error> {
        decode_content(bytes, limits)?;
        Ok(Self {
            bytes: Arc::from(bytes),
        })
    }

    pub fn from_vec(bytes: Vec<u8>, limits: Limits) -> Result<Self, Error> {
        decode_content(&bytes, limits)?;
        Ok(Self {
            bytes: bytes.into(),
        })
    }

    pub fn original_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn view(&self, limits: Limits) -> Result<Content, Error> {
        decode_content(&self.bytes, limits)
    }
}

pub fn decode_content(bytes: &[u8], limits: Limits) -> Result<Content, Error> {
    let mut reader = Reader::new(bytes, limits)?;
    let mut content = Content::default();
    let mut last_rank = 0;
    while reader.pos < bytes.len() {
        let Value::Described { descriptor, value } = reader.value(bytes.len(), 1)? else {
            return Err(Error::InvalidSection);
        };
        let code = section_code(descriptor)?;
        let rank = match code {
            0x70..=0x74 => code - 0x6f,
            0x75..=0x77 => 6,
            0x78 => 7,
            _ => return Err(Error::InvalidSection),
        };
        if rank < last_rank {
            return Err(Error::SectionOrder);
        }
        last_rank = rank;
        match code {
            0x70 => set_once(&mut content.header, header(*value)?)?,
            0x71 => set_once(&mut content.delivery_annotations, annotations(*value)?)?,
            0x72 => set_once(&mut content.message_annotations, annotations(*value)?)?,
            0x73 => set_once(&mut content.properties, properties(*value)?)?,
            0x74 => set_once(
                &mut content.application_properties,
                application_properties(*value)?,
            )?,
            0x75 => {
                let Value::Binary(section) = *value else {
                    return Err(Error::InvalidSection);
                };
                match &mut content.body {
                    Body::Empty => content.body = Body::Data(vec![section]),
                    Body::Data(sections) => sections.push(section),
                    _ => return Err(Error::MixedBody),
                }
            }
            0x76 => {
                let Value::List(section) = *value else {
                    return Err(Error::InvalidSection);
                };
                match &mut content.body {
                    Body::Empty => content.body = Body::Sequence(vec![section]),
                    Body::Sequence(sections) => sections.push(section),
                    _ => return Err(Error::MixedBody),
                }
            }
            0x77 => {
                if !matches!(content.body, Body::Empty) {
                    return Err(Error::MixedBody);
                }
                content.body = Body::Value(*value);
            }
            0x78 => set_once(&mut content.footer, annotations(*value)?)?,
            _ => return Err(Error::InvalidSection),
        }
    }
    Ok(content)
}

fn section_code(descriptor: Descriptor) -> Result<u64, Error> {
    match descriptor {
        Descriptor::ULong(code) => Ok(code),
        Descriptor::Symbol(symbol) => Ok(match symbol.text.as_str() {
            "amqp:header:list" => 0x70,
            "amqp:delivery-annotations:map" => 0x71,
            "amqp:message-annotations:map" => 0x72,
            "amqp:properties:list" => 0x73,
            "amqp:application-properties:map" => 0x74,
            "amqp:data:binary" => 0x75,
            "amqp:amqp-sequence:list" => 0x76,
            "amqp:amqp-value:*" => 0x77,
            "amqp:footer:map" => 0x78,
            _ => return Err(Error::InvalidSection),
        }),
    }
}

fn set_once<T>(target: &mut Option<T>, value: T) -> Result<(), Error> {
    if target.is_some() {
        return Err(Error::DuplicateSection);
    }
    *target = Some(value);
    Ok(())
}

fn fields(value: Value, max: usize) -> Result<std::vec::IntoIter<Value>, Error> {
    let Value::List(values) = value else {
        return Err(Error::InvalidField);
    };
    if values.len() > max {
        return Err(Error::InvalidField);
    }
    Ok(values.into_iter())
}

fn next<T>(
    fields: &mut impl Iterator<Item = Value>,
    convert: impl FnOnce(Value) -> Result<T, Error>,
) -> Result<Field<T>, Error> {
    match fields.next() {
        None => Ok(Field::Absent),
        Some(Value::Null) => Ok(Field::Null),
        Some(value) => convert(value).map(Field::Value),
    }
}

fn bool_value(value: Value) -> Result<bool, Error> {
    if let Value::Bool(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn ubyte_value(value: Value) -> Result<u8, Error> {
    if let Value::UByte(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn uint_value(value: Value) -> Result<u32, Error> {
    if let Value::UInt(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn timestamp_value(value: Value) -> Result<i64, Error> {
    if let Value::Timestamp(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn text_value(value: Value) -> Result<Text, Error> {
    if let Value::String(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn binary_value(value: Value) -> Result<Binary, Error> {
    if let Value::Binary(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn symbol_value(value: Value) -> Result<Symbol, Error> {
    if let Value::Symbol(v) = value {
        Ok(v)
    } else {
        Err(Error::InvalidField)
    }
}
fn id_value(value: Value) -> Result<MessageId, Error> {
    Ok(match value {
        Value::String(v) => MessageId::String(v),
        Value::ULong(v) => MessageId::ULong(v),
        Value::Uuid(v) => MessageId::Uuid(v),
        Value::Binary(v) => MessageId::Binary(v),
        _ => return Err(Error::InvalidField),
    })
}

fn header(value: Value) -> Result<Header, Error> {
    let mut f = fields(value, 5)?;
    Ok(Header {
        durable: next(&mut f, bool_value)?,
        priority: next(&mut f, ubyte_value)?,
        ttl: next(&mut f, uint_value)?,
        first_acquirer: next(&mut f, bool_value)?,
        delivery_count: next(&mut f, uint_value)?,
    })
}

fn properties(value: Value) -> Result<Properties, Error> {
    let mut f = fields(value, 13)?;
    Ok(Properties {
        message_id: next(&mut f, id_value)?,
        user_id: next(&mut f, binary_value)?,
        to: next(&mut f, text_value)?,
        subject: next(&mut f, text_value)?,
        reply_to: next(&mut f, text_value)?,
        correlation_id: next(&mut f, id_value)?,
        content_type: next(&mut f, symbol_value)?,
        content_encoding: next(&mut f, symbol_value)?,
        absolute_expiry_time: next(&mut f, timestamp_value)?,
        creation_time: next(&mut f, timestamp_value)?,
        group_id: next(&mut f, text_value)?,
        group_sequence: next(&mut f, uint_value)?,
        reply_to_group_id: next(&mut f, text_value)?,
    })
}

fn annotations(value: Value) -> Result<Annotations, Error> {
    let Value::Map(pairs) = value else {
        return Err(Error::InvalidSection);
    };
    pairs
        .into_iter()
        .map(|(key, value)| {
            let key = match key {
                Value::Symbol(s) => AnnotationKey::Symbol(s),
                Value::ULong(n) => AnnotationKey::ULong(n),
                _ => return Err(Error::InvalidAnnotationKey),
            };
            Ok((key, value))
        })
        .collect()
}

fn application_properties(value: Value) -> Result<ApplicationProperties, Error> {
    let Value::Map(pairs) = value else {
        return Err(Error::InvalidSection);
    };
    pairs
        .into_iter()
        .map(|(key, value)| {
            let Value::String(key) = key else {
                return Err(Error::InvalidApplicationKey);
            };
            Ok((key, value))
        })
        .collect()
}

/// Encode a semantic view, validating public trees without cloning them. Output is
/// intentionally not a wire-canonicalization or persistence-version contract.
pub fn encode_content(content: &Content, limits: Limits) -> Result<Vec<u8>, Error> {
    let mut w = Writer::new(limits, true);
    if let Some(header) = &content.header {
        section(&mut w, 0x70, |w, d| {
            write_fields(
                w,
                &[
                    leaf(&header.durable, |v| Leaf::Bool(*v)),
                    leaf(&header.priority, |v| Leaf::UByte(*v)),
                    leaf(&header.ttl, |v| Leaf::UInt(*v)),
                    leaf(&header.first_acquirer, |v| Leaf::Bool(*v)),
                    leaf(&header.delivery_count, |v| Leaf::UInt(*v)),
                ],
                d,
            )
        })?;
    }
    if let Some(a) = &content.delivery_annotations {
        section(&mut w, 0x71, |w, d| write_annotations(w, a, d))?;
    }
    if let Some(a) = &content.message_annotations {
        section(&mut w, 0x72, |w, d| write_annotations(w, a, d))?;
    }
    if let Some(p) = &content.properties {
        section(&mut w, 0x73, |w, d| {
            write_fields(
                w,
                &[
                    leaf(&p.message_id, id_leaf),
                    leaf(&p.user_id, Leaf::Binary),
                    leaf(&p.to, Leaf::Text),
                    leaf(&p.subject, Leaf::Text),
                    leaf(&p.reply_to, Leaf::Text),
                    leaf(&p.correlation_id, id_leaf),
                    leaf(&p.content_type, Leaf::Symbol),
                    leaf(&p.content_encoding, Leaf::Symbol),
                    leaf(&p.absolute_expiry_time, |v| Leaf::Timestamp(*v)),
                    leaf(&p.creation_time, |v| Leaf::Timestamp(*v)),
                    leaf(&p.group_id, Leaf::Text),
                    leaf(&p.group_sequence, |v| Leaf::UInt(*v)),
                    leaf(&p.reply_to_group_id, Leaf::Text),
                ],
                d,
            )
        })?;
    }
    if let Some(a) = &content.application_properties {
        section(&mut w, 0x74, |w, depth| {
            w.budget.node(depth)?;
            w.put(&[0xd1])?;
            w.container(
                a.len().checked_mul(2).ok_or(Error::Overflow)?,
                Width::Four,
                |w| {
                    for (key, value) in a {
                        Leaf::Text(key).write(w, depth + 1)?;
                        w.value(value, depth + 1)?;
                    }
                    Ok(())
                },
            )
        })?;
    }
    match &content.body {
        Body::Empty => {}
        Body::Data(sections) => {
            if sections.is_empty() {
                return Err(Error::InvalidSection);
            }
            w.budget.children(sections.len())?;
            for bytes in sections {
                section(&mut w, 0x75, |w, d| Leaf::Binary(bytes).write(w, d))?;
            }
        }
        Body::Sequence(sections) => {
            if sections.is_empty() {
                return Err(Error::InvalidSection);
            }
            w.budget.children(sections.len())?;
            for values in sections {
                section(&mut w, 0x76, |w, depth| {
                    w.budget.node(depth)?;
                    w.put(&[0xd0])?;
                    w.container(values.len(), Width::Four, |w| {
                        for value in values {
                            w.value(value, depth + 1)?;
                        }
                        Ok(())
                    })
                })?;
            }
        }
        Body::Value(value) => section(&mut w, 0x77, |w, d| w.value(value, d))?,
    }
    if let Some(a) = &content.footer {
        section(&mut w, 0x78, |w, d| write_annotations(w, a, d))?;
    }
    Ok(w.bytes)
}

fn section(
    w: &mut Writer,
    code: u64,
    write: impl FnOnce(&mut Writer, usize) -> Result<(), Error>,
) -> Result<(), Error> {
    w.budget.node(1)?;
    w.put(&[0x00])?;
    w.descriptor(&Descriptor::ULong(code), 2)?;
    write(w, 2)
}

enum Leaf<'a> {
    Null,
    Bool(bool),
    UByte(u8),
    UInt(u32),
    ULong(u64),
    Timestamp(i64),
    Uuid(&'a [u8; 16]),
    Binary(&'a Binary),
    Text(&'a Text),
    Symbol(&'a Symbol),
}

fn leaf<'a, T>(f: &'a Field<T>, convert: impl FnOnce(&'a T) -> Leaf<'a>) -> Option<Leaf<'a>> {
    match f {
        Field::Absent => None,
        Field::Null => Some(Leaf::Null),
        Field::Value(v) => Some(convert(v)),
    }
}

fn id_leaf(id: &MessageId) -> Leaf<'_> {
    match id {
        MessageId::String(t) => Leaf::Text(t),
        MessageId::ULong(n) => Leaf::ULong(*n),
        MessageId::Uuid(u) => Leaf::Uuid(u),
        MessageId::Binary(b) => Leaf::Binary(b),
    }
}

impl Leaf<'_> {
    fn write(&self, w: &mut Writer, depth: usize) -> Result<(), Error> {
        w.budget.node(depth)?;
        match self {
            Self::Null => w.put(&[0x40]),
            Self::Bool(b) => w.put(&[0x56, u8::from(*b)]),
            Self::UByte(n) => w.put(&[0x50, *n]),
            Self::UInt(n) => {
                w.put(&[0x70])?;
                w.put(&n.to_be_bytes())
            }
            Self::ULong(n) => {
                w.put(&[0x80])?;
                w.put(&n.to_be_bytes())
            }
            Self::Timestamp(n) => {
                w.put(&[0x83])?;
                w.put(&n.to_be_bytes())
            }
            Self::Uuid(u) => {
                w.put(&[0x98])?;
                w.put(*u)
            }
            Self::Binary(b) => write_bytes(w, &b.bytes, b.width, 0xa0, 0xb0),
            Self::Text(t) => write_bytes(w, t.text.as_bytes(), t.width, 0xa1, 0xb1),
            Self::Symbol(s) => write_bytes(w, s.text.as_bytes(), s.width, 0xa3, 0xb3),
        }
    }
}

fn write_bytes(
    w: &mut Writer,
    bytes: &[u8],
    width: Width,
    short: u8,
    long: u8,
) -> Result<(), Error> {
    match width {
        Width::One => {
            w.put(&[
                short,
                u8::try_from(bytes.len()).map_err(|_| Error::InvalidLength)?,
            ])?;
        }
        Width::Four => {
            w.put(&[long])?;
            w.put(
                &u32::try_from(bytes.len())
                    .map_err(|_| Error::Overflow)?
                    .to_be_bytes(),
            )?;
        }
    }
    w.put(bytes)
}

fn write_fields(w: &mut Writer, fields: &[Option<Leaf<'_>>], depth: usize) -> Result<(), Error> {
    let count = fields
        .iter()
        .rposition(Option::is_some)
        .map_or(0, |i| i + 1);
    if fields[..count].iter().any(Option::is_none) {
        return Err(Error::AbsentBeforePresent);
    }
    w.budget.node(depth)?;
    w.put(&[0xd0])?;
    w.container(count, Width::Four, |w| {
        for value in &fields[..count] {
            value
                .as_ref()
                .ok_or(Error::AbsentBeforePresent)?
                .write(w, depth + 1)?;
        }
        Ok(())
    })
}

fn write_annotations(w: &mut Writer, pairs: &Annotations, depth: usize) -> Result<(), Error> {
    w.budget.node(depth)?;
    w.put(&[0xd1])?;
    w.container(
        pairs.len().checked_mul(2).ok_or(Error::Overflow)?,
        Width::Four,
        |w| {
            for (key, value) in pairs {
                match key {
                    AnnotationKey::Symbol(s) => Leaf::Symbol(s),
                    AnnotationKey::ULong(n) => Leaf::ULong(*n),
                }
                .write(w, depth + 1)?;
                w.value(value, depth + 1)?;
            }
            Ok(())
        },
    )
}
