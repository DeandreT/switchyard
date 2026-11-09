//! Pure canonical address/profile recognition. No backend or generation reads.
use super::Refusal;
use auth::ResourceScope;
use domain::{EntityPath, SubscriptionName};
use hyper::Uri;
use percent_encoding::percent_decode_str;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Address {
    Namespace,
    Entity(EntityPath),
    Subscriptions(EntityPath),
    Subscription {
        topic: EntityPath,
        name: SubscriptionName,
    },
    Unsupported,
}

impl Address {
    pub(super) fn scope(&self, root: &ResourceScope) -> Result<ResourceScope, Refusal> {
        let path = match self {
            Self::Namespace | Self::Unsupported => return Ok(root.clone()),
            Self::Entity(path) | Self::Subscriptions(path) => path.to_string(),
            Self::Subscription { topic, name } => topic
                .subscription(name)
                .map_err(|_| Refusal::Invalid)?
                .to_string(),
        };
        ResourceScope::entity(root.host(), path).map_err(|_| Refusal::Invalid)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    DefinitionSingleton,
    RuntimeUnavailable,
    Unsupported,
}

pub(super) fn address(uri: &Uri) -> Result<Address, Refusal> {
    if uri.scheme().is_some() || uri.authority().is_some() || uri.path().len() > 2048 {
        return Err(Refusal::Invalid);
    }
    let path = uri.path().strip_prefix('/').ok_or(Refusal::Invalid)?;
    if path.is_empty() {
        return Ok(Address::Namespace);
    }
    let parts = path.split('/').map(decode).collect::<Result<Vec<_>, _>>()?;
    if parts.iter().any(|part| {
        part.is_empty()
            || part == "."
            || part == ".."
            || part.contains(['/', '\\', '?', '#'])
            || part.chars().any(char::is_control)
    }) {
        return Err(Refusal::Invalid);
    }
    if parts[0].starts_with('$') {
        return Ok(
            if parts.len() == 2
                && parts[0].eq_ignore_ascii_case("$resources")
                && ["queues", "topics"]
                    .iter()
                    .any(|kind| parts[1].eq_ignore_ascii_case(kind))
            {
                Address::Namespace
            } else {
                Address::Unsupported
            },
        );
    }
    // The well-known segment is reserved only BELOW a nonempty topic path.
    // Leading `subscriptions` and ordinary `rules` segments are valid entity
    // names; a subscription's name may itself be either word. Extra suffixes
    // below that subscription (including /Rules[/name]) remain unsupported.
    if let Some(index) = parts.iter().enumerate().find_map(|(index, part)| {
        (index != 0 && part.eq_ignore_ascii_case("subscriptions")).then_some(index)
    }) {
        let topic = ordinary(parts[..index].join("/"))?;
        return match parts.len() - index {
            1 => Ok(Address::Subscriptions(topic)),
            2 => Ok(Address::Subscription {
                topic,
                name: SubscriptionName::new(parts[index + 1].clone())
                    .map_err(|_| Refusal::Invalid)?,
            }),
            _ => Ok(Address::Unsupported),
        };
    }
    Ok(Address::Entity(ordinary(parts.join("/"))?))
}

fn ordinary(value: String) -> Result<EntityPath, Refusal> {
    let path = EntityPath::new(value).map_err(|_| Refusal::Invalid)?;
    if path.is_subscription() || path.is_management() || path.is_dead_letter_queue() {
        return Err(Refusal::Invalid);
    }
    Ok(path)
}

pub(super) fn profile(uri: &Uri, address: &Address) -> Result<Profile, Refusal> {
    let mut values = BTreeMap::new();
    let query = uri.query().ok_or(Refusal::Invalid)?;
    if query.len() > 2048 {
        return Err(Refusal::Invalid);
    }
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').ok_or(Refusal::Invalid)?;
        let key = decode(&key.replace('+', " "))?;
        let value = decode(&value.replace('+', " "))?;
        if !["api-version", "enrich", "$skip", "$top"].contains(&key.as_str())
            || values.insert(key, value).is_some()
        {
            return Err(Refusal::Invalid);
        }
    }
    if !matches!(
        values.get("api-version").map(String::as_str),
        Some("2021-05" | "2024-05")
    ) {
        return Err(Refusal::Invalid);
    }
    if !matches!(
        values.get("enrich").map(String::as_str),
        None | Some("True" | "False")
    ) {
        return Err(Refusal::Invalid);
    }
    let collection = matches!(address, Address::Namespace | Address::Subscriptions(_));
    if values.contains_key("$skip") || values.contains_key("$top") {
        if !collection {
            return Err(Refusal::Invalid);
        }
        let number = |key| {
            values
                .get(key)
                .filter(|value| {
                    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(Refusal::Invalid)
        };
        number("$skip")?;
        if !(1..=100).contains(&number("$top")?) {
            return Err(Refusal::Invalid);
        }
    }
    if matches!(address, Address::Unsupported) {
        return Ok(Profile::Unsupported);
    }
    if collection || values.get("enrich").map(String::as_str) != Some("False") {
        return Ok(Profile::RuntimeUnavailable);
    }
    Ok(Profile::DefinitionSingleton)
}

fn decode(value: &str) -> Result<String, Refusal> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(Refusal::Invalid);
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| Refusal::Invalid)
}
