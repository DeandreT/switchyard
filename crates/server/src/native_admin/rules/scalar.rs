use admin_api::v1::{RuleNullValue, RuleScalarValue, rule_scalar_value::Value};
use domain::MessageValue;
use tonic::Status;

fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid scalar byte width"))
}

pub(super) fn validate(input: &RuleScalarValue) -> Result<usize, Status> {
    let value = input
        .value
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("a scalar value is required"))?;
    match value {
        Value::UbyteValue(value) if u8::try_from(*value).is_err() => {
            Err(Status::invalid_argument("ubyte value is outside its width"))
        }
        Value::UshortValue(value) if u16::try_from(*value).is_err() => Err(
            Status::invalid_argument("ushort value is outside its width"),
        ),
        Value::ByteValue(value) if i8::try_from(*value).is_err() => {
            Err(Status::invalid_argument("byte value is outside its width"))
        }
        Value::ShortValue(value) if i16::try_from(*value).is_err() => {
            Err(Status::invalid_argument("short value is outside its width"))
        }
        Value::Decimal32Bytes(value) => fixed::<4>(value).map(|_| 0),
        Value::Decimal64Bytes(value) => fixed::<8>(value).map(|_| 0),
        Value::Decimal128Bytes(value) | Value::UuidBytes(value) => fixed::<16>(value).map(|_| 0),
        Value::CharCodepoint(value) if char::from_u32(*value).is_none() => Err(
            Status::invalid_argument("char value is not a Unicode scalar"),
        ),
        Value::SymbolValue(value) if !value.is_ascii() => {
            Err(Status::invalid_argument("symbol value requires ASCII"))
        }
        Value::BinaryValue(value) => Ok(value.len()),
        Value::StringValue(value) | Value::SymbolValue(value) => Ok(value.len()),
        _ => Ok(0),
    }
}

pub(super) fn read(input: &RuleScalarValue) -> Result<MessageValue, Status> {
    if validate(input)? > domain::MAX_RULE_BYTES {
        return Err(Status::resource_exhausted(
            "scalar value exceeds the rule byte limit",
        ));
    }
    let value = input
        .value
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("a scalar value is required"))?;
    Ok(match value {
        Value::NullValue(_) => MessageValue::Null,
        Value::BoolValue(value) => MessageValue::Bool(*value),
        Value::UbyteValue(value) => MessageValue::Ubyte(
            u8::try_from(*value).map_err(|_| Status::invalid_argument("invalid ubyte value"))?,
        ),
        Value::UshortValue(value) => MessageValue::Ushort(
            u16::try_from(*value).map_err(|_| Status::invalid_argument("invalid ushort value"))?,
        ),
        Value::UintValue(value) => MessageValue::Uint(*value),
        Value::UlongValue(value) => MessageValue::Ulong(*value),
        Value::ByteValue(value) => MessageValue::Byte(
            i8::try_from(*value).map_err(|_| Status::invalid_argument("invalid byte value"))?,
        ),
        Value::ShortValue(value) => MessageValue::Short(
            i16::try_from(*value).map_err(|_| Status::invalid_argument("invalid short value"))?,
        ),
        Value::IntValue(value) => MessageValue::Int(*value),
        Value::LongValue(value) => MessageValue::Long(*value),
        Value::FloatBits(value) => MessageValue::Float(*value),
        Value::DoubleBits(value) => MessageValue::Double(*value),
        Value::Decimal32Bytes(value) => MessageValue::Decimal32(fixed(value)?),
        Value::Decimal64Bytes(value) => MessageValue::Decimal64(fixed(value)?),
        Value::Decimal128Bytes(value) => MessageValue::Decimal128(fixed(value)?),
        Value::CharCodepoint(value) => MessageValue::Char(
            char::from_u32(*value).ok_or_else(|| Status::invalid_argument("invalid char value"))?,
        ),
        Value::TimestampMillis(value) => MessageValue::Timestamp(*value),
        Value::UuidBytes(value) => MessageValue::Uuid(fixed(value)?),
        Value::BinaryValue(value) => MessageValue::Binary(value.clone()),
        Value::StringValue(value) => MessageValue::String(value.clone()),
        Value::SymbolValue(value) => MessageValue::Symbol(value.clone()),
    })
}

pub(super) fn write(value: &MessageValue) -> Result<RuleScalarValue, Status> {
    let value = match value {
        MessageValue::Null => Value::NullValue(RuleNullValue {}),
        MessageValue::Bool(value) => Value::BoolValue(*value),
        MessageValue::Ubyte(value) => Value::UbyteValue(u32::from(*value)),
        MessageValue::Ushort(value) => Value::UshortValue(u32::from(*value)),
        MessageValue::Uint(value) => Value::UintValue(*value),
        MessageValue::Ulong(value) => Value::UlongValue(*value),
        MessageValue::Byte(value) => Value::ByteValue(i32::from(*value)),
        MessageValue::Short(value) => Value::ShortValue(i32::from(*value)),
        MessageValue::Int(value) => Value::IntValue(*value),
        MessageValue::Long(value) => Value::LongValue(*value),
        MessageValue::Float(value) => Value::FloatBits(*value),
        MessageValue::Double(value) => Value::DoubleBits(*value),
        MessageValue::Decimal32(value) => Value::Decimal32Bytes(value.to_vec()),
        MessageValue::Decimal64(value) => Value::Decimal64Bytes(value.to_vec()),
        MessageValue::Decimal128(value) => Value::Decimal128Bytes(value.to_vec()),
        MessageValue::Char(value) => Value::CharCodepoint(u32::from(*value)),
        MessageValue::Timestamp(value) => Value::TimestampMillis(*value),
        MessageValue::Uuid(value) => Value::UuidBytes(value.to_vec()),
        MessageValue::Binary(value) => Value::BinaryValue(value.clone()),
        MessageValue::String(value) => Value::StringValue(value.clone()),
        MessageValue::Symbol(value) if value.is_ascii() => Value::SymbolValue(value.clone()),
        _ => return Err(Status::internal("invalid stored correlation scalar")),
    };
    Ok(RuleScalarValue { value: Some(value) })
}
