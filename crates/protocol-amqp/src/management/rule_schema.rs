//! Lossless rule request structure checks, after operation authorization.
//! SQL compilation belongs to the domain, not this wire schema validator.

use std::collections::BTreeSet;

use message_content::{Body, Value, decode_content};

use super::rules::{ADD_RULE_OPERATION, ENUMERATE_RULES_OPERATION, REMOVE_RULE_OPERATION};

const INVALID: &str = "invalid rule request map: unknown, duplicate or nontext field";

pub(super) fn validate(encoded: &[u8], operation: &str) -> Result<(), &'static str> {
    let limits = message_content::Limits {
        max_bytes: crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES,
        ..domain::CONTENT_LIMITS
    };
    // The maintained decoder checks bytes, aggregate work/nodes and depth before
    // allocating. Its Vec map retains even byte-identical repeated keys.
    let content = decode_content(encoded, limits).map_err(|_| "invalid bounded rule request")?;
    let Body::Value(body) = &content.body else {
        return Err("rule request body must be an AMQP value map");
    };
    match operation {
        ADD_RULE_OPERATION => {
            let body = fields(body, &["rule-name", "rule-description"])?;
            if let Some(description) = entry(body, "rule-description") {
                let description = fields(
                    description,
                    &[
                        "rule-name",
                        "sql-filter",
                        "correlation-filter",
                        "sql-rule-action",
                    ],
                )?;
                if let Some(filter) = entry(description, "sql-filter") {
                    fields(filter, &["expression", "compatibility-level"])?;
                }
                if let Some(filter) = entry(description, "correlation-filter") {
                    let filter = fields(
                        filter,
                        &[
                            "correlation-id",
                            "message-id",
                            "to",
                            "reply-to",
                            "label",
                            "session-id",
                            "reply-to-session-id",
                            "content-type",
                            "properties",
                        ],
                    )?;
                    if let Some(properties) = entry(filter, "properties")
                        && !matches!(properties, Value::Null)
                    {
                        // Application names retain their existing case-exact,
                        // string/symbol identity; they are not schema keywords.
                        unique(properties, None)?;
                    }
                }
                if let Some(action @ Value::Map(_)) = entry(description, "sql-rule-action") {
                    fields(action, &["expression", "compatibility-level"])?;
                }
            }
        }
        REMOVE_RULE_OPERATION => {
            fields(body, &["rule-name"])?;
        }
        ENUMERATE_RULES_OPERATION => {
            fields(body, &["skip", "top"])?;
        }
        _ => unreachable!("only rule operations use the rule schema"),
    }
    Ok(())
}

fn text(value: &Value) -> Option<&str> {
    match value {
        Value::String(text) => Some(&text.text),
        Value::Symbol(symbol) => Some(&symbol.text),
        _ => None,
    }
}

fn fields<'a>(value: &'a Value, allowed: &[&str]) -> Result<&'a [(Value, Value)], &'static str> {
    unique(value, Some(allowed))
}

fn unique<'a>(
    value: &'a Value,
    allowed: Option<&[&str]>,
) -> Result<&'a [(Value, Value)], &'static str> {
    let Value::Map(entries) = value else {
        return Err(INVALID);
    };
    let mut seen = BTreeSet::new();
    for (key, _) in entries {
        let name = text(key).ok_or(INVALID)?;
        if allowed.is_some_and(|allowed| !allowed.contains(&name)) || !seen.insert(name) {
            return Err(INVALID);
        }
    }
    Ok(entries)
}

fn entry<'a>(entries: &'a [(Value, Value)], name: &str) -> Option<&'a Value> {
    entries
        .iter()
        .find_map(|(key, value)| (text(key) == Some(name)).then_some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use message_content::{Content, Symbol, Text, Width, encode_content};

    fn string(text: &str) -> Value {
        Value::String(Text {
            text: text.into(),
            width: Width::One,
        })
    }
    fn pair(key: &str, value: Value) -> (Value, Value) {
        (string(key), value)
    }
    fn encoded(body: Value) -> Vec<u8> {
        encode_content(
            &Content {
                body: Body::Value(body),
                ..Content::default()
            },
            domain::CONTENT_LIMITS,
        )
        .unwrap()
    }
    fn request(scope: usize, alias: bool) -> Vec<u8> {
        let mut maps = [
            vec![pair("rule-name", string("action"))],
            vec![pair("rule-name", string("action"))],
            vec![pair("expression", string("1=1"))],
            vec![pair("expression", string("REMOVE stale"))],
        ];
        let mut duplicate = maps[scope][0].clone();
        if alias {
            duplicate.0 = Value::Symbol(Symbol {
                text: text(&duplicate.0).unwrap().into(),
                width: Width::One,
            });
        }
        maps[scope].push(duplicate);
        let [mut body, mut description, filter, action] = maps;
        description.push(pair("sql-filter", Value::Map(filter)));
        description.push(pair("sql-rule-action", Value::Map(action)));
        body.push(pair("rule-description", Value::Map(description)));
        encoded(Value::Map(body))
    }

    #[test]
    fn actual_encoded_identical_and_string_symbol_duplicates_refuse_at_every_schema_scope() {
        for scope in 0..4 {
            for alias in [false, true] {
                let bytes = request(scope, alias);
                assert_eq!(
                    validate(&bytes, ADD_RULE_OPERATION),
                    Err(INVALID),
                    "scope {scope}, alias {alias}"
                );
            }
        }
        for operation in [REMOVE_RULE_OPERATION, ENUMERATE_RULES_OPERATION] {
            let name = if operation == REMOVE_RULE_OPERATION {
                "rule-name"
            } else {
                "top"
            };
            assert_eq!(
                validate(
                    &encoded(Value::Map(vec![
                        pair(name, Value::Int(1)),
                        pair(name, Value::Int(1))
                    ])),
                    operation
                ),
                Err(INVALID)
            );
        }
    }

    #[test]
    fn unknown_and_nontext_settings_refuse_without_echoing_input() {
        for setting in [
            pair("private-source", string("secret")),
            (Value::Int(1), string("secret")),
        ] {
            let bytes = encoded(Value::Map(vec![
                pair("rule-name", string("action")),
                pair(
                    "rule-description",
                    Value::Map(vec![
                        pair(
                            "sql-filter",
                            Value::Map(vec![pair("expression", string("1=1"))]),
                        ),
                        pair(
                            "sql-rule-action",
                            Value::Map(vec![pair("expression", string("REMOVE x")), setting]),
                        ),
                    ]),
                ),
            ]));
            assert_eq!(validate(&bytes, ADD_RULE_OPERATION), Err(INVALID));
        }
        assert!(
            validate(
                &vec![0; crate::SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES + 1],
                ADD_RULE_OPERATION
            )
            .is_err()
        );
    }

    #[test]
    fn schema_unknowns_and_correlation_duplicates_are_losslessly_refused() {
        for scope in 0..3 {
            for key in [string("Expression"), string("parameters"), Value::Int(20)] {
                let mut maps = [
                    vec![pair("rule-name", string("rule"))],
                    vec![],
                    vec![pair("expression", string("1=1"))],
                ];
                maps[scope].push((key, Value::Null));
                let [mut body, mut description, filter] = maps;
                description.push(pair("sql-filter", Value::Map(filter)));
                body.push(pair("rule-description", Value::Map(description)));
                assert_eq!(
                    validate(&encoded(Value::Map(body)), ADD_RULE_OPERATION),
                    Err(INVALID)
                );
            }
        }
        let correlation = |properties| {
            encoded(Value::Map(vec![
                pair("rule-name", string("rule")),
                pair(
                    "rule-description",
                    Value::Map(vec![pair(
                        "correlation-filter",
                        Value::Map(vec![pair("properties", Value::Map(properties))]),
                    )]),
                ),
            ]))
        };
        for alias in [false, true] {
            let mut second = pair("Priority", Value::Int(7));
            if alias {
                second.0 = Value::Symbol(Symbol {
                    text: "Priority".into(),
                    width: Width::One,
                });
            }
            assert_eq!(
                validate(
                    &correlation(vec![pair("Priority", Value::Int(7)), second]),
                    ADD_RULE_OPERATION
                ),
                Err(INVALID)
            );
        }
        // Structural validation does not lower-case correlation names or
        // change their scalar constructor. Domain correlation admission keeps
        // its existing separate case-collision rule.
        assert_eq!(
            validate(
                &correlation(vec![
                    pair("Priority", Value::Int(7)),
                    pair("priority", Value::Long(7))
                ]),
                ADD_RULE_OPERATION
            ),
            Ok(())
        );
        let symbol_map = Value::Map(vec![
            (
                Value::Symbol(Symbol {
                    text: "skip".into(),
                    width: Width::One,
                }),
                Value::Int(0),
            ),
            (
                Value::Symbol(Symbol {
                    text: "top".into(),
                    width: Width::One,
                }),
                Value::Int(100),
            ),
        ]);
        assert_eq!(
            validate(&encoded(symbol_map), ENUMERATE_RULES_OPERATION),
            Ok(())
        );
    }
}
