use admin_api::v1::{RuleNullValue, RuleScalarValue, rule_scalar_value::Value};
use serde::{Deserialize, Serialize};

use super::super::CliError;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum JsonScalar {
    Null {},
    Bool { value: bool },
    Ubyte { value: u8 },
    Ushort { value: u16 },
    Uint { value: u32 },
    Ulong { value: String },
    Byte { value: i8 },
    Short { value: i16 },
    Int { value: i32 },
    Long { value: String },
    FloatBits { value: String },
    DoubleBits { value: String },
    Decimal32 { value: String },
    Decimal64 { value: String },
    Decimal128 { value: String },
    Char { value: u32 },
    Timestamp { value: String },
    Uuid { value: String },
    Binary { value: String },
    String { value: String },
    Symbol { value: String },
}

fn invalid() -> CliError {
    CliError::Input("invalid rule scalar value")
}

fn unsigned(value: &str) -> Result<u64, CliError> {
    let parsed = value.parse::<u64>().map_err(|_| invalid())?;
    if parsed.to_string() != value {
        return Err(invalid());
    }
    Ok(parsed)
}

fn signed(value: &str) -> Result<i64, CliError> {
    let parsed = value.parse::<i64>().map_err(|_| invalid())?;
    if parsed.to_string() != value {
        return Err(invalid());
    }
    Ok(parsed)
}

fn validate_hex(value: &str, bytes: Option<usize>) -> Result<(), CliError> {
    if !value.len().is_multiple_of(2)
        || bytes.is_some_and(|bytes| value.len() != bytes * 2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    Ok(())
}

fn nibble(byte: u8) -> u8 {
    if byte.is_ascii_digit() {
        byte - b'0'
    } else {
        byte - b'a' + 10
    }
}

fn octets(value: &str, width: Option<usize>) -> Result<Vec<u8>, CliError> {
    validate_hex(value, width)?;
    Ok(value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}

pub(super) fn into_protobuf(value: JsonScalar) -> Result<RuleScalarValue, CliError> {
    let value = match value {
        JsonScalar::Null {} => Value::NullValue(RuleNullValue {}),
        JsonScalar::Bool { value } => Value::BoolValue(value),
        JsonScalar::Ubyte { value } => Value::UbyteValue(u32::from(value)),
        JsonScalar::Ushort { value } => Value::UshortValue(u32::from(value)),
        JsonScalar::Uint { value } => Value::UintValue(value),
        JsonScalar::Ulong { value } => Value::UlongValue(unsigned(&value)?),
        JsonScalar::Byte { value } => Value::ByteValue(i32::from(value)),
        JsonScalar::Short { value } => Value::ShortValue(i32::from(value)),
        JsonScalar::Int { value } => Value::IntValue(value),
        JsonScalar::Long { value } => Value::LongValue(signed(&value)?),
        JsonScalar::FloatBits { value } => {
            validate_hex(&value, Some(4))?;
            Value::FloatBits(u32::from_str_radix(&value, 16).map_err(|_| invalid())?)
        }
        JsonScalar::DoubleBits { value } => {
            validate_hex(&value, Some(8))?;
            Value::DoubleBits(u64::from_str_radix(&value, 16).map_err(|_| invalid())?)
        }
        JsonScalar::Decimal32 { value } => Value::Decimal32Bytes(octets(&value, Some(4))?),
        JsonScalar::Decimal64 { value } => Value::Decimal64Bytes(octets(&value, Some(8))?),
        JsonScalar::Decimal128 { value } => Value::Decimal128Bytes(octets(&value, Some(16))?),
        JsonScalar::Char { value } => {
            char::from_u32(value).ok_or_else(invalid)?;
            Value::CharCodepoint(value)
        }
        JsonScalar::Timestamp { value } => Value::TimestampMillis(signed(&value)?),
        JsonScalar::Uuid { value } => Value::UuidBytes(octets(&value, Some(16))?),
        JsonScalar::Binary { value } => Value::BinaryValue(octets(&value, None)?),
        JsonScalar::String { value } => Value::StringValue(value),
        JsonScalar::Symbol { value } => {
            if !value.is_ascii() {
                return Err(invalid());
            }
            Value::SymbolValue(value)
        }
    };
    Ok(RuleScalarValue { value: Some(value) })
}

pub(super) fn from_protobuf(input: RuleScalarValue) -> Result<JsonScalar, CliError> {
    Ok(match input.value.ok_or_else(invalid)? {
        Value::NullValue(_) => JsonScalar::Null {},
        Value::BoolValue(value) => JsonScalar::Bool { value },
        Value::UbyteValue(value) => JsonScalar::Ubyte {
            value: value.try_into().map_err(|_| invalid())?,
        },
        Value::UshortValue(value) => JsonScalar::Ushort {
            value: value.try_into().map_err(|_| invalid())?,
        },
        Value::UintValue(value) => JsonScalar::Uint { value },
        Value::UlongValue(value) => JsonScalar::Ulong {
            value: value.to_string(),
        },
        Value::ByteValue(value) => JsonScalar::Byte {
            value: value.try_into().map_err(|_| invalid())?,
        },
        Value::ShortValue(value) => JsonScalar::Short {
            value: value.try_into().map_err(|_| invalid())?,
        },
        Value::IntValue(value) => JsonScalar::Int { value },
        Value::LongValue(value) => JsonScalar::Long {
            value: value.to_string(),
        },
        Value::FloatBits(value) => JsonScalar::FloatBits {
            value: format!("{value:08x}"),
        },
        Value::DoubleBits(value) => JsonScalar::DoubleBits {
            value: format!("{value:016x}"),
        },
        Value::Decimal32Bytes(value) if value.len() == 4 => {
            JsonScalar::Decimal32 { value: hex(&value) }
        }
        Value::Decimal64Bytes(value) if value.len() == 8 => {
            JsonScalar::Decimal64 { value: hex(&value) }
        }
        Value::Decimal128Bytes(value) if value.len() == 16 => {
            JsonScalar::Decimal128 { value: hex(&value) }
        }
        Value::CharCodepoint(value) => {
            char::from_u32(value).ok_or_else(invalid)?;
            JsonScalar::Char { value }
        }
        Value::TimestampMillis(value) => JsonScalar::Timestamp {
            value: value.to_string(),
        },
        Value::UuidBytes(value) if value.len() == 16 => JsonScalar::Uuid { value: hex(&value) },
        Value::BinaryValue(value) => JsonScalar::Binary { value: hex(&value) },
        Value::StringValue(value) => JsonScalar::String { value },
        Value::SymbolValue(value) if value.is_ascii() => JsonScalar::Symbol { value },
        _ => return Err(invalid()),
    })
}
