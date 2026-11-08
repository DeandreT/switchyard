//! Frozen SQL rule-creation intent. The expression is retained as sent and is
//! never compiled here: an invalid predicate must survive replication for
//! deterministic refusal. Do not reorder, extend or reuse these DTOs for a
//! later proposal version.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de::Visitor};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, NamespaceName, RuleFilter,
    RuleName, SqlRuleFilter, Timestamp,
};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    authority: Authority,
    namespace: Text,
    entity: Text,
    issued_at: u64,
    kind: Kind,
}

#[derive(Serialize, Deserialize)]
enum Authority {
    Unbound,
    Bound {
        namespace: Text,
        target: Text,
        owner: Text,
        kind: OwnerKind,
        generation: u64,
    },
}

#[derive(Serialize, Deserialize)]
enum OwnerKind {
    Queue,
    Topic,
    Subscription,
}

#[derive(Serialize, Deserialize)]
enum Kind {
    CreateSqlRule {
        canonical_name: Text,
        display_name: Text,
        expression: Text,
    },
}

#[derive(Serialize)]
#[serde(transparent)]
struct Text(String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Text;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V6 text")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V6 text exceeds envelope bound"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

// Every copy charges its encoded length, so capture refuses oversized intent
// before building the whole DTO.
struct CopyBudget(usize);

impl CopyBudget {
    fn text(&mut self, value: &str) -> Result<Text, Error> {
        self.0 = self.0.checked_add(value.len()).ok_or(Error::TooLarge)?;
        if self.0 > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(Text(value.to_owned()))
    }
}

impl Text {
    fn canonical(self) -> Result<String, Error> {
        if self.0.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(Error::NonCanonical);
        }
        Ok(self.0)
    }
    fn namespace(self) -> Result<NamespaceName, Error> {
        NamespaceName::new(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
    fn entity(self) -> Result<EntityPath, Error> {
        EntityPath::from_internal(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
}

impl Proposal {
    pub(super) fn capture(value: &DurableProposal) -> Result<Self, Error> {
        let CommandKind::CreateRule {
            name,
            filter: RuleFilter::Sql(filter),
        } = &value.command.kind
        else {
            return Err(Error::UnsupportedCommandVersion(6));
        };
        let mut budget = CopyBudget(0);
        let kind = Kind::CreateSqlRule {
            canonical_name: budget.text(name.as_str())?,
            display_name: budget.text(name.display_name())?,
            expression: budget.text(filter.expression())?,
        };
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                // Rule creation keeps exactly V1's authority rules. No catalog
                // lookup or rebind.
                super::v1::validate_binding(binding, &value.command)?;
                Authority::Bound {
                    namespace: budget.text(binding.namespace().as_str())?,
                    target: budget.text(binding.target().as_str())?,
                    owner: budget.text(binding.owner().as_str())?,
                    kind: match binding.kind() {
                        EntityBindingKind::Queue => OwnerKind::Queue,
                        EntityBindingKind::Topic => OwnerKind::Topic,
                        EntityBindingKind::Subscription => OwnerKind::Subscription,
                    },
                    generation: binding.generation(),
                }
            }
        };
        Ok(Self {
            authority,
            namespace: budget.text(value.command.namespace.as_str())?,
            entity: budget.text(value.command.entity.as_str())?,
            issued_at: value.command.issued_at.as_millis(),
            kind,
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let Kind::CreateSqlRule {
            canonical_name,
            display_name,
            expression,
        } = self.kind;
        let name = RuleName::new(display_name.0).map_err(|_| Error::InvalidIdentifier)?;
        if name.as_str() != canonical_name.0 {
            return Err(Error::NonCanonical);
        }
        let command = Command::new(
            self.namespace.namespace()?,
            self.entity.entity()?,
            Timestamp::from_millis(self.issued_at),
            CommandKind::CreateRule {
                name,
                filter: RuleFilter::Sql(SqlRuleFilter::new(expression.0)),
            },
        );
        let binding = match self.authority {
            Authority::Unbound => None,
            Authority::Bound {
                namespace,
                target,
                owner,
                kind,
                generation,
            } => {
                let binding = EntityBinding::new(
                    namespace.namespace()?,
                    target.entity()?,
                    owner.entity()?,
                    match kind {
                        OwnerKind::Queue => EntityBindingKind::Queue,
                        OwnerKind::Topic => EntityBindingKind::Topic,
                        OwnerKind::Subscription => EntityBindingKind::Subscription,
                    },
                    generation,
                );
                super::v1::validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(expression: &str) -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t/subscriptions/s").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateRule {
                name: RuleName::new("Hot").unwrap(),
                filter: RuleFilter::Sql(SqlRuleFilter::new(expression)),
            },
        )))
        .unwrap()
    }

    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 6).unwrap())
    }

    #[test]
    fn sql_rules_refuse_older_versions_and_legacy_rules_refuse_v5() {
        let p = decode(&proposal("a = 1")).unwrap();
        for capture in [
            super::super::v1::Proposal::capture(&p).map(drop),
            super::super::v2::Proposal::capture(&p).map(drop),
            super::super::v3::Proposal::capture(&p).map(drop),
            super::super::v4::Proposal::capture(&p).map(drop),
            super::super::v5::Proposal::capture(&p).map(drop),
        ] {
            assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
        }
        let legacy = DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("t/subscriptions/s").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateRule {
                name: RuleName::new("r").unwrap(),
                filter: RuleFilter::True,
            },
        ));
        assert!(matches!(
            Proposal::capture(&legacy),
            Err(Error::UnsupportedCommandVersion(6))
        ));
    }

    #[test]
    fn invalid_expressions_survive_and_identities_are_never_repaired() {
        // Uncompilable intent decodes so the state machine refuses it.
        let p = decode(&proposal("not sql at all (")).unwrap();
        let CommandKind::CreateRule {
            filter: RuleFilter::Sql(filter),
            ..
        } = &p.command().kind
        else {
            panic!("a SQL rule");
        };
        assert_eq!(filter.expression(), "not sql at all (");

        let mut p = proposal("a = 1");
        p.kind = Kind::CreateSqlRule {
            canonical_name: Text("Hot".into()),
            display_name: Text("Hot".into()),
            expression: Text("a = 1".into()),
        };
        assert_eq!(decode(&p), Err(Error::NonCanonical));
        let mut p = proposal("a = 1");
        p.kind = Kind::CreateSqlRule {
            canonical_name: Text(String::new()),
            display_name: Text(String::new()),
            expression: Text("a = 1".into()),
        };
        assert_eq!(decode(&p), Err(Error::InvalidIdentifier));
        for value in ["UPPER", "", "t\0bad"] {
            let mut p = proposal("a = 1");
            p.entity = Text(value.into());
            assert!(matches!(
                decode(&p),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
        let mut p = proposal("a = 1");
        p.authority = Authority::Bound {
            namespace: Text("n".into()),
            target: Text("t/subscriptions/s".into()),
            owner: Text("other".into()),
            kind: OwnerKind::Subscription,
            generation: 1,
        };
        assert_eq!(decode(&p), Err(Error::InvalidAuthority));
    }
}
