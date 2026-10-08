use std::fmt;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Failure {
    Malformed,
    Duplicate,
    Depth,
    Nodes,
}

struct Budget {
    remaining: usize,
    maximum_depth: usize,
    failure: Option<Failure>,
}

impl Budget {
    fn refuse<E: serde::de::Error>(&mut self, failure: Failure) -> E {
        self.failure = Some(failure);
        E::custom("JWT JSON rejected")
    }
}

pub(super) fn parse(
    bytes: &[u8],
    maximum_depth: usize,
    maximum_nodes: usize,
) -> Result<Value, Failure> {
    let mut budget = Budget {
        remaining: maximum_nodes,
        maximum_depth,
        failure: None,
    };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = ValueSeed {
        budget: &mut budget,
        depth: 1,
    }
    .deserialize(&mut decoder)
    .map_err(|_| budget.failure.unwrap_or(Failure::Malformed))?;
    decoder.end().map_err(|_| Failure::Malformed)?;
    Ok(value)
}

struct ValueSeed<'a> {
    budget: &'a mut Budget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;

    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        if self.depth > self.budget.maximum_depth {
            return Err(self.budget.refuse(Failure::Depth));
        }
        if self.budget.remaining == 0 {
            return Err(self.budget.refuse(Failure::Nodes));
        }
        self.budget.remaining -= 1;
        decoder.deserialize_any(ValueVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

struct ValueVisitor<'a> {
    budget: &'a mut Budget,
    depth: usize,
}

impl<'de> Visitor<'de> for ValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("JWT JSON rejected"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(ValueSeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut object: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(self.budget.refuse(Failure::Duplicate));
            }
            let value = object.next_value_seed(ValueSeed {
                budget: &mut *self.budget,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
