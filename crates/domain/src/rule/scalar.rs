use std::{collections::BTreeMap, fmt};

use serde::{
    Deserialize, Deserializer,
    de::{self, EnumAccess, MapAccess, VariantAccess, Visitor},
};

use crate::MessageValue;

use super::MAX_CORRELATION_RULE_CONDITIONS;

// Keep these names and indices identical to MessageValue. Compound variants
// are recognized only to reject their tag without decoding recursive children.
const VARIANTS: &[&str] = &[
    "Null",
    "Bool",
    "Ubyte",
    "Ushort",
    "Uint",
    "Ulong",
    "Byte",
    "Short",
    "Int",
    "Long",
    "Float",
    "Double",
    "Decimal32",
    "Decimal64",
    "Decimal128",
    "Char",
    "Timestamp",
    "Uuid",
    "Binary",
    "String",
    "Symbol",
    "List",
    "Map",
    "Array",
    "Described",
];

struct Variant(u32);

impl<'de> Deserialize<'de> for Variant {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VariantVisitor;
        impl Visitor<'_> for VariantVisitor {
            type Value = Variant;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a MessageValue variant")
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                if value < VARIANTS.len() as u64 {
                    Ok(Variant(value as u32))
                } else {
                    Err(E::invalid_value(de::Unexpected::Unsigned(value), &self))
                }
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                VARIANTS
                    .iter()
                    .position(|name| *name == value)
                    .map(|index| Variant(index as u32))
                    .ok_or_else(|| E::unknown_variant(value, VARIANTS))
            }
            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                VARIANTS
                    .iter()
                    .position(|name| name.as_bytes() == value)
                    .map(|index| Variant(index as u32))
                    .ok_or_else(|| E::invalid_value(de::Unexpected::Bytes(value), &self))
            }
        }
        deserializer.deserialize_identifier(VariantVisitor)
    }
}

struct ScalarValue(MessageValue);

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ScalarVisitor;
        impl<'de> Visitor<'de> for ScalarVisitor {
            type Value = ScalarValue;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a scalar correlation property value")
            }
            fn visit_enum<A: EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
                let (Variant(index), variant) = access.variant::<Variant>()?;
                let value = match index {
                    0 => {
                        variant.unit_variant()?;
                        MessageValue::Null
                    }
                    1 => MessageValue::Bool(variant.newtype_variant()?),
                    2 => MessageValue::Ubyte(variant.newtype_variant()?),
                    3 => MessageValue::Ushort(variant.newtype_variant()?),
                    4 => MessageValue::Uint(variant.newtype_variant()?),
                    5 => MessageValue::Ulong(variant.newtype_variant()?),
                    6 => MessageValue::Byte(variant.newtype_variant()?),
                    7 => MessageValue::Short(variant.newtype_variant()?),
                    8 => MessageValue::Int(variant.newtype_variant()?),
                    9 => MessageValue::Long(variant.newtype_variant()?),
                    10 => MessageValue::Float(variant.newtype_variant()?),
                    11 => MessageValue::Double(variant.newtype_variant()?),
                    12 => MessageValue::Decimal32(variant.newtype_variant()?),
                    13 => MessageValue::Decimal64(variant.newtype_variant()?),
                    14 => MessageValue::Decimal128(variant.newtype_variant()?),
                    15 => MessageValue::Char(variant.newtype_variant()?),
                    16 => MessageValue::Timestamp(variant.newtype_variant()?),
                    17 => MessageValue::Uuid(variant.newtype_variant()?),
                    18 => MessageValue::Binary(variant.newtype_variant()?),
                    19 => MessageValue::String(variant.newtype_variant()?),
                    20 => MessageValue::Symbol(variant.newtype_variant()?),
                    _ => {
                        return Err(de::Error::custom(
                            "correlation property conditions require scalar values",
                        ));
                    }
                };
                Ok(ScalarValue(value))
            }
        }
        deserializer.deserialize_enum("MessageValue", VARIANTS, ScalarVisitor)
    }
}

pub(super) fn deserialize_properties<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, MessageValue>, D::Error> {
    struct PropertiesVisitor;
    impl<'de> Visitor<'de> for PropertiesVisitor {
        type Value = BTreeMap<String, MessageValue>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 32 unique scalar correlation properties")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
            if access
                .size_hint()
                .is_some_and(|count| count > MAX_CORRELATION_RULE_CONDITIONS)
            {
                return Err(de::Error::custom(
                    "too many correlation property conditions",
                ));
            }
            let mut properties = BTreeMap::new();
            let mut processed = 0;
            while let Some(key) = access.next_key::<String>()? {
                if processed == MAX_CORRELATION_RULE_CONDITIONS {
                    return Err(de::Error::custom(
                        "too many correlation property conditions",
                    ));
                }
                if properties.contains_key(&key) {
                    return Err(de::Error::custom(
                        "correlation property names must be unique",
                    ));
                }
                let ScalarValue(value) = access.next_value::<ScalarValue>()?;
                processed += 1;
                properties.insert(key, value);
            }
            Ok(properties)
        }
    }
    deserializer.deserialize_map(PropertiesVisitor)
}

#[cfg(test)]
mod tests {
    use serde::{Serialize, ser::SerializeMap};

    use super::*;
    use crate::{BrokerError, CorrelationFilter, codec};

    #[test]
    fn every_scalar_preserves_original_bytes_and_bits() -> Result<(), BrokerError> {
        for value in [
            MessageValue::Null,
            MessageValue::Bool(true),
            MessageValue::Ubyte(255),
            MessageValue::Ushort(u16::MAX),
            MessageValue::Uint(u32::MAX),
            MessageValue::Ulong(u64::MAX),
            MessageValue::Byte(-1),
            MessageValue::Short(i16::MIN),
            MessageValue::Int(i32::MIN),
            MessageValue::Long(i64::MIN),
            MessageValue::Float(0x8000_0000),
            MessageValue::Double(0x7ff8_0000_0000_0001),
            MessageValue::Decimal32([1; 4]),
            MessageValue::Decimal64([2; 8]),
            MessageValue::Decimal128([3; 16]),
            MessageValue::Char('\u{1f600}'),
            MessageValue::Timestamp(i64::MIN),
            MessageValue::Uuid([4; 16]),
            MessageValue::Binary(vec![0, 255]),
            MessageValue::String(String::from("text")),
            MessageValue::Symbol(String::from("symbol")),
        ] {
            let bytes = postcard::to_stdvec(&value).map_err(|_| crate::CodecError::Encode)?;
            let ScalarValue(decoded) = postcard::from_bytes::<ScalarValue>(&bytes)
                .map_err(|_| crate::CodecError::Decode)?;
            assert_eq!(decoded, value);
            assert_eq!(
                postcard::to_stdvec(&decoded).map_err(|_| crate::CodecError::Encode)?,
                bytes
            );
            let filter = CorrelationFilter {
                properties: BTreeMap::from([(String::from("key"), value)]),
                ..CorrelationFilter::default()
            };
            let bytes = codec::encode(&filter)?;
            assert_eq!(codec::decode::<CorrelationFilter>(&bytes)?, filter);
            assert_eq!(
                codec::encode(&codec::decode::<CorrelationFilter>(&bytes)?)?,
                bytes
            );
        }
        Ok(())
    }

    #[test]
    fn compound_and_unknown_tags_reject_before_any_payload_decode() {
        for index in 21_u32..=25 {
            let bytes = postcard::to_stdvec(&index).expect("variant tag");
            assert!(matches!(
                postcard::from_bytes::<ScalarValue>(&bytes),
                Err(postcard::Error::SerdeDeCustom)
            ));
        }
    }

    #[test]
    fn nested_compounds_are_refused_without_deserializing_children() -> Result<(), crate::CodecError>
    {
        let mut value = MessageValue::Null;
        for _ in 0..256 {
            value = MessageValue::List(vec![value]);
        }
        let filter = CorrelationFilter {
            properties: BTreeMap::from([(String::from("key"), value)]),
            ..CorrelationFilter::default()
        };
        let bytes = codec::encode(&filter)?;
        assert!(codec::decode::<CorrelationFilter>(&bytes).is_err());
        Ok(())
    }

    #[derive(Deserialize)]
    struct PropertyOnly {
        #[serde(deserialize_with = "deserialize_properties")]
        _properties: BTreeMap<String, MessageValue>,
    }

    #[test]
    fn oversized_map_length_refuses_before_reading_the_first_entry() {
        let bytes = postcard::to_stdvec(&33_usize).expect("map length");
        assert!(matches!(
            postcard::from_bytes::<PropertyOnly>(&bytes),
            Err(postcard::Error::SerdeDeCustom)
        ));
    }

    struct DuplicateProperties;
    impl Serialize for DuplicateProperties {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("same", &MessageValue::Null)?;
            map.serialize_entry("same", &MessageValue::Null)?;
            map.end()
        }
    }

    #[test]
    fn duplicate_keys_do_not_silently_overwrite_a_condition() {
        let bytes = postcard::to_stdvec(&DuplicateProperties).expect("duplicate map");
        assert!(matches!(
            postcard::from_bytes::<PropertyOnly>(&bytes),
            Err(postcard::Error::SerdeDeCustom)
        ));
    }
}
