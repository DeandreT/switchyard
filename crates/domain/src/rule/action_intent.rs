use serde::{Deserialize, Serialize};

use super::action::{SqlActionCompileError, SqlActionProgram};

/// Exact SQL action source intent. A stored or proposed string is not a compiled
/// program: policy boundaries must compile it before accepting or selecting it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SqlRuleAction(String);

impl SqlRuleAction {
    /// Builds unvalidated intent, including intent that admission will refuse.
    pub fn new(expression: impl Into<String>) -> Self {
        Self(expression.into())
    }

    pub fn expression(&self) -> &str {
        &self.0
    }

    pub fn compile(&self) -> Result<SqlActionProgram, SqlActionCompileError> {
        SqlActionProgram::compile(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_is_retained_exactly_and_compiled_under_the_bounded_profile() {
        let source = " SET user.flag = TRUE; REMOVE user.stale; ";
        let action = SqlRuleAction::new(source);
        assert_eq!(action.expression(), source);
        assert_eq!(
            action
                .compile()
                .expect("supported action")
                .metrics()
                .statements,
            2
        );
        let bytes = postcard::to_stdvec(&action).expect("source intent");
        let restored: SqlRuleAction = postcard::from_bytes(&bytes).expect("source intent");
        assert_eq!(restored, action);
        assert_eq!(
            restored
                .compile()
                .expect("recompiled action")
                .metrics()
                .statements,
            2
        );
    }

    #[test]
    fn serde_and_builder_do_not_turn_unsupported_source_into_a_program() {
        let action = SqlRuleAction::new("SET sys.Subject = 'unsupported'");
        assert!(action.compile().is_err());
        let bytes = postcard::to_stdvec(&action).expect("unvalidated intent");
        let restored: SqlRuleAction = postcard::from_bytes(&bytes).expect("unvalidated intent");
        assert!(restored.compile().is_err());
    }

    #[test]
    fn literal_v3_rule_layout_retains_display_identity_and_action_source() {
        let definition = crate::RuleDefinition {
            name: crate::RuleName::new("Exact-Name").unwrap(),
            filter: crate::RuleFilter::True,
            created_at: crate::Timestamp::from_millis(7),
            action: Some(SqlRuleAction::new("REMOVE user.old")),
        };
        let expected = "030a45786163742d4e616d6500070f52454d4f564520757365722e6f6c64";
        let bytes = (0..expected.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(crate::codec::rule::encode(&definition).unwrap(), bytes);
        assert_eq!(crate::codec::rule::decode(&bytes).unwrap(), definition);
        assert!(crate::codec::decode::<crate::RuleDefinition>(&bytes).is_err());
    }

    #[test]
    fn generic_legacy_envelope_serialization_cannot_drop_action_copy_changes() {
        let original = crate::MessageEnvelope::new(Vec::new());
        assert!(postcard::to_stdvec(&original).is_ok());
        let set = original.clone().with_overlay(vec![crate::OverlayProperty {
            name: "set".into(),
            value: vec![0x41],
        }]);
        let removed = original.with_removals(vec!["removed".into()]);
        assert!(postcard::to_stdvec(&set).is_err());
        assert!(postcard::to_stdvec(&removed).is_err());
    }

    #[test]
    fn legacy_rule_serde_preserves_actionless_bytes_and_refuses_action_loss() {
        use crate::{RuleDefinition, RuleFilter, RuleName, Timestamp};

        let definition = RuleDefinition {
            name: RuleName::new("sample").expect("valid name"),
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(7),
            action: None,
        };
        let legacy = super::super::LegacyRuleRef {
            name: &definition.name,
            filter: &definition.filter,
            created_at: definition.created_at,
        };
        let bytes = postcard::to_stdvec(&legacy).expect("frozen V1 fields");
        assert_eq!(
            postcard::to_stdvec(&definition).expect("legacy serde"),
            bytes
        );
        assert_eq!(
            postcard::from_bytes::<RuleDefinition>(&bytes).expect("legacy restore"),
            definition
        );
        let with_action = RuleDefinition {
            action: Some(SqlRuleAction::new("REMOVE user.stale")),
            ..definition
        };
        assert!(postcard::to_stdvec(&with_action).is_err());
        assert!(crate::codec::encode(&with_action).is_err());
        let bytes = crate::codec::rule::encode(&with_action).expect("V3 action record");
        assert_eq!(bytes[0], 3);
        assert_eq!(crate::codec::rule::decode(&bytes).unwrap(), with_action);
    }
}
