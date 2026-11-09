//! Frozen action intent: one exact V1/V6 actionless creation envelope followed
//! by exact action source bytes. The nested format is checked before decoding;
//! no recursive V10 envelopes are accepted. Invalid programs remain intent and
//! receive deterministic admission refusal, just like invalid SQL filters.

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};
use crate::{CommandKind, EntityBindingKind, SqlRuleAction};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};
use std::fmt;

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    rule: Bytes,
    action: Bytes,
}

struct Bytes(Vec<u8>);
impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}
impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Buffer;
        impl<'de> Visitor<'de> for Buffer {
            type Value = Bytes;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V10 bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Bytes, E> {
                if bytes.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V10 bytes exceed envelope bound"));
                }
                Ok(Bytes(bytes.to_vec()))
            }
        }
        deserializer.deserialize_byte_buf(Buffer)
    }
}

pub(super) fn validate(proposal: &DurableProposal) -> Result<(), Error> {
    if let Some(binding) = &proposal.binding {
        super::v1::validate_binding(binding, &proposal.command)?;
        if binding.kind() != EntityBindingKind::Subscription || binding.owner() != binding.target()
        {
            return Err(Error::InvalidAuthority);
        }
    }
    Ok(())
}

impl Proposal {
    pub(super) fn capture(proposal: &DurableProposal) -> Result<Self, Error> {
        let CommandKind::CreateRuleWithAction {
            name,
            filter,
            action,
        } = &proposal.command.kind
        else {
            return Err(Error::UnsupportedCommandVersion(10));
        };
        validate(proposal)?;
        if action.expression().len() > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        // Bound variable intent before cloning the public command. The frozen
        // nested capture remains authoritative for encoded size/canonicality.
        let filter_bytes = match filter {
            crate::RuleFilter::True | crate::RuleFilter::False => 0,
            crate::RuleFilter::Sql(sql) => sql.expression().len(),
            crate::RuleFilter::Correlation(correlation) => {
                let system = [
                    &correlation.correlation_id,
                    &correlation.message_id,
                    &correlation.to,
                    &correlation.reply_to,
                    &correlation.subject,
                    &correlation.session_id,
                    &correlation.reply_to_session_id,
                    &correlation.content_type,
                ]
                .into_iter()
                .flatten()
                .fold(0usize, |n, text| n.saturating_add(text.len()));
                correlation
                    .application_properties
                    .iter()
                    .fold(system, |n, (name, value)| {
                        n.saturating_add(2)
                            .saturating_add(name.len())
                            .saturating_add(value.as_bytes().len())
                    })
            }
        };
        if filter_bytes.saturating_add(action.expression().len()) > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        let mut legacy = proposal.clone();
        legacy.command.kind = CommandKind::CreateRule {
            name: name.clone(),
            filter: filter.clone(),
        };
        let rule = legacy.encode()?;
        if rule.len().saturating_add(action.expression().len()) > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(Self {
            rule: Bytes(rule),
            action: Bytes(action.expression().as_bytes().to_vec()),
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        if self.rule.0.get(..8) != Some(b"SWDP\0\0\0\x01")
            && self.rule.0.get(..8) != Some(b"SWDP\0\0\0\x06")
        {
            return Err(Error::Malformed);
        }
        let mut proposal = DurableProposal::decode(&self.rule.0)?;
        let CommandKind::CreateRule { name, filter } = proposal.command.kind else {
            return Err(Error::Malformed);
        };
        let source = String::from_utf8(self.action.0).map_err(|_| Error::Malformed)?;
        proposal.command.kind = CommandKind::CreateRuleWithAction {
            name,
            filter,
            action: SqlRuleAction::new(source),
        };
        validate(&proposal)?;
        Ok(proposal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Command, EntityBinding, EntityPath, NamespaceName, RuleFilter, RuleName, Timestamp,
    };

    fn intent(source: &str) -> DurableProposal {
        DurableProposal::unbound(Command::new(
            NamespaceName::new("tenant").unwrap(),
            EntityPath::new("events")
                .unwrap()
                .subscription(&crate::SubscriptionName::new("sub").unwrap())
                .unwrap(),
            Timestamp::from_millis(7),
            CommandKind::CreateRuleWithAction {
                name: RuleName::new("Exact-Name").unwrap(),
                filter: RuleFilter::True,
                action: SqlRuleAction::new(source),
            },
        ))
    }

    #[test]
    fn exact_intent_round_trip_and_all_old_captures_refuse() {
        for source in [" REMOVE user.old; ", "SET sys.Label = 'unsupported'"] {
            let proposal = intent(source);
            let bytes = proposal.encode().unwrap();
            assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
            assert_eq!(proposal.encode().unwrap(), bytes);
            assert!(super::super::v1::Proposal::capture(&proposal).is_err());
            assert!(super::super::v2::Proposal::capture(&proposal).is_err());
            assert!(super::super::v3::Proposal::capture(&proposal).is_err());
            assert!(super::super::v4::Proposal::capture(&proposal).is_err());
            assert!(super::super::v5::Proposal::capture(&proposal).is_err());
            assert!(super::super::v6::Proposal::capture(&proposal).is_err());
            assert!(super::super::v7::Proposal::capture(&proposal).is_err());
            assert!(super::super::v8::Proposal::capture(&proposal).is_err());
            assert!(super::super::v9::Proposal::capture(&proposal).is_err());
            for version in 1u32..10 {
                let mut old = bytes.clone();
                old[4..8].copy_from_slice(&version.to_be_bytes());
                assert!(DurableProposal::decode(&old).is_err(), "V{version}");
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(DurableProposal::decode(&trailing).is_err());
        }
    }

    #[test]
    fn literal_action_intent_layout_is_frozen() {
        let expected = "535744500000000a000000574653574450000000010000003a000674656e616e74186576656e74732f737562736372697074696f6e732f73756207030a65786163742d6e616d650a45786163742d4e616d65000f52454d4f564520757365722e6f6c64";
        let bytes = (0..expected.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let proposal = intent("REMOVE user.old");
        assert_eq!(proposal.encode().unwrap(), bytes);
        assert_eq!(DurableProposal::decode(&bytes).unwrap(), proposal);
    }

    #[test]
    fn action_authority_is_exact_subscription_generation_not_a_rebinding() {
        let mut proposal = intent("REMOVE user.old");
        let command = &proposal.command;
        proposal.binding = Some(EntityBinding::new(
            command.namespace.clone(),
            command.entity.clone(),
            command.entity.clone(),
            EntityBindingKind::Subscription,
            42,
        ));
        assert_eq!(
            DurableProposal::decode(&proposal.encode().unwrap()).unwrap(),
            proposal
        );
        for (kind, generation) in [
            (EntityBindingKind::Queue, 42),
            (EntityBindingKind::Topic, 42),
            (EntityBindingKind::Subscription, 0),
        ] {
            proposal.binding = Some(EntityBinding::new(
                proposal.command.namespace.clone(),
                proposal.command.entity.clone(),
                proposal.command.entity.clone(),
                kind,
                generation,
            ));
            assert_eq!(proposal.encode(), Err(Error::InvalidAuthority));
        }
    }

    #[test]
    fn recursive_wrong_family_and_invalid_utf8_are_refused() {
        let valid = Proposal::capture(&intent("REMOVE user.old")).unwrap();
        let bad = Proposal {
            rule: Bytes(intent("REMOVE user.old").encode().unwrap()),
            action: valid.action,
        };
        assert!(bad.restore().is_err());
        let mut bad = Proposal::capture(&intent("REMOVE user.old")).unwrap();
        bad.action = Bytes(vec![0xff]);
        assert!(bad.restore().is_err());
        let mut bad = Proposal::capture(&intent("REMOVE user.old")).unwrap();
        bad.rule.0.push(0);
        assert!(bad.restore().is_err());
    }
}
