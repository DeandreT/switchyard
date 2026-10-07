use std::collections::BTreeMap;

use auth::ResourceScope;
use domain::{EntityPath, SubscriptionName};
use hyper::{
    HeaderMap, Method, Uri, Version,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HOST, IF_MATCH, TRAILER, TRANSFER_ENCODING, UPGRADE},
};
use percent_encoding::percent_decode_str;

use super::super::xml::QUEUE_COLLECTION_PATH;
use super::{
    MAX_BODY_BYTES, MAX_HEADER_BYTES, MAX_HEADER_COUNT, MAX_TARGET_BYTES, response::RequestFailure,
};

pub(super) enum Target {
    Collection,
    Entity(String),
    Subscription {
        topic: String,
        name: String,
        canonical: String,
    },
}

impl Target {
    pub(super) fn scope(&self, audience: &ResourceScope) -> Result<ResourceScope, RequestFailure> {
        match self {
            Self::Collection => Ok(audience.clone()),
            Self::Entity(path)
            | Self::Subscription {
                canonical: path, ..
            } => {
                ResourceScope::entity(audience.host(), path).map_err(|_| RequestFailure::BadRequest)
            }
        }
    }

    pub(super) fn subscription(&self) -> Result<(EntityPath, SubscriptionName), RequestFailure> {
        let Self::Subscription { topic, name, .. } = self else {
            return Err(RequestFailure::BadRequest);
        };
        let topic = EntityPath::new(topic).map_err(|_| RequestFailure::BadRequest)?;
        if topic.is_dead_letter_queue() || topic.is_subscription_path() {
            return Err(RequestFailure::BadRequest);
        }
        let name = SubscriptionName::new(name).map_err(|_| RequestFailure::BadRequest)?;
        topic
            .subscription(&name)
            .and_then(|entity| entity.dead_letter_queue())
            .map_err(|_| RequestFailure::BadRequest)?;
        Ok((topic, name))
    }

    pub(super) fn entity(&self) -> Result<EntityPath, RequestFailure> {
        let Self::Entity(path) = self else {
            return Err(RequestFailure::BadRequest);
        };
        let entity = EntityPath::new(path).map_err(|_| RequestFailure::BadRequest)?;
        if entity.is_dead_letter_queue() || entity.is_subscription_path() {
            return Err(RequestFailure::BadRequest);
        }
        Ok(entity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    Get,
    List { skip: usize, top: usize },
    Create,
    Update,
    Delete,
}

pub(super) fn target(uri: &Uri, headers: &HeaderMap) -> Result<Target, RequestFailure> {
    if headers.len() > MAX_HEADER_COUNT {
        return Err(RequestFailure::HeaderTooLarge);
    }
    let mut bytes = 0usize;
    for (name, value) in headers {
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|n| n.checked_add(value.len()))
            .and_then(|n| n.checked_add(4))
            .ok_or(RequestFailure::HeaderTooLarge)?;
        if bytes > MAX_HEADER_BYTES {
            return Err(RequestFailure::HeaderTooLarge);
        }
    }
    if uri
        .path_and_query()
        .is_none_or(|part| part.as_str().len() > MAX_TARGET_BYTES)
        || uri.scheme().is_some()
        || uri.authority().is_some()
    {
        return Err(RequestFailure::BadRequest);
    }
    let raw = uri
        .path()
        .strip_prefix('/')
        .ok_or(RequestFailure::BadRequest)?;
    let mut segments = Vec::new();
    for segment in raw.split('/') {
        strict_percent(segment)?;
        let segment = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| RequestFailure::BadRequest)?;
        if segment.is_empty()
            || matches!(segment.as_ref(), "." | "..")
            || segment.contains('/')
            || segment.contains('\\')
            || segment.chars().any(char::is_control)
        {
            return Err(RequestFailure::BadRequest);
        }
        segments.push(segment.into_owned());
    }
    if segments.len() >= 3
        && matches!(
            segments[segments.len() - 2].as_str(),
            "Subscriptions" | "subscriptions"
        )
    {
        let name = segments.pop().ok_or(RequestFailure::BadRequest)?;
        segments.pop();
        let topic = segments.join("/");
        let canonical = format!("{topic}/subscriptions/{name}");
        return Ok(Target::Subscription {
            topic,
            name,
            canonical,
        });
    }
    let path = segments.join("/");
    Ok(if path == QUEUE_COLLECTION_PATH {
        Target::Collection
    } else {
        Target::Entity(path)
    })
}

pub(super) fn operation(
    method: &Method,
    version: Version,
    uri: &Uri,
    headers: &HeaderMap,
    target: &Target,
) -> Result<Operation, RequestFailure> {
    if *method != Method::GET && *method != Method::PUT && *method != Method::DELETE {
        return Err(RequestFailure::MethodNotAllowed);
    }
    if !matches!(version, Version::HTTP_10 | Version::HTTP_11)
        || singleton(headers, HOST)?.is_none()
        || headers.contains_key(TRAILER)
        || headers.contains_key(UPGRADE)
    {
        return Err(RequestFailure::BadRequest);
    }
    if let Some(value) = singleton(headers, CONTENT_LENGTH)? {
        let length = decimal(value)?;
        if length > MAX_BODY_BYTES {
            return Err(RequestFailure::BadRequest);
        }
    }
    if let Some(value) = singleton(headers, TRANSFER_ENCODING)?
        && (!value.eq_ignore_ascii_case("chunked") || headers.contains_key(CONTENT_LENGTH))
    {
        return Err(RequestFailure::BadRequest);
    }
    let query = query(uri.query().ok_or(RequestFailure::BadRequest)?)?;
    let version = query.get("api-version").ok_or(RequestFailure::BadRequest)?;
    if !matches!(version.as_str(), "2024-05" | "2021-05") {
        return Err(RequestFailure::BadRequest);
    }
    let allowed: &[&str] = if *method == Method::GET {
        if matches!(target, Target::Collection) {
            &["api-version", "enrich", "$skip", "$top"]
        } else {
            &["api-version", "enrich"]
        }
    } else {
        &["api-version"]
    };
    if query.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(RequestFailure::BadRequest);
    }
    if query
        .get("enrich")
        .is_some_and(|value| !value.eq_ignore_ascii_case("false"))
    {
        return Err(RequestFailure::BadRequest);
    }
    match (method, target) {
        (&Method::GET, Target::Collection) => {
            if headers.contains_key(IF_MATCH) {
                return Err(RequestFailure::BadRequest);
            }
            let skip = query
                .get("$skip")
                .map(|value| decimal(value))
                .transpose()?
                .unwrap_or(0);
            let top = query
                .get("$top")
                .map(|value| decimal(value))
                .transpose()?
                .unwrap_or(100);
            if skip > 1_000 || !(1..=100).contains(&top) {
                return Err(RequestFailure::BadRequest);
            }
            Ok(Operation::List { skip, top })
        }
        (&Method::GET, Target::Entity(_) | Target::Subscription { .. }) => {
            if headers.contains_key(IF_MATCH) {
                return Err(RequestFailure::BadRequest);
            }
            Ok(Operation::Get)
        }
        (&Method::PUT, Target::Entity(_) | Target::Subscription { .. }) => {
            if !singleton(headers, CONTENT_TYPE)?
                .is_some_and(|value| value.eq_ignore_ascii_case("application/atom+xml"))
            {
                return Err(RequestFailure::BadRequest);
            }
            match singleton(headers, IF_MATCH)? {
                None => Ok(Operation::Create),
                Some("*") if matches!(target, Target::Subscription { .. }) => {
                    Err(RequestFailure::BadRequest)
                }
                Some("*") => Ok(Operation::Update),
                _ => Err(RequestFailure::BadRequest),
            }
        }
        (&Method::DELETE, Target::Entity(_) | Target::Subscription { .. }) => {
            if headers.contains_key(IF_MATCH) {
                return Err(RequestFailure::BadRequest);
            }
            Ok(Operation::Delete)
        }
        _ => Err(RequestFailure::MethodNotAllowed),
    }
}

pub(super) fn singleton(
    headers: &HeaderMap,
    key: hyper::header::HeaderName,
) -> Result<Option<&str>, RequestFailure> {
    let all = headers.get_all(key);
    let mut values = all.iter();
    let first = values
        .next()
        .map(|value| value.to_str().map_err(|_| RequestFailure::BadRequest))
        .transpose()?;
    if values.next().is_some() {
        return Err(RequestFailure::BadRequest);
    }
    Ok(first)
}

fn query(raw: &str) -> Result<BTreeMap<String, String>, RequestFailure> {
    if raw.split('&').count() > 4 || raw.split('&').any(str::is_empty) {
        return Err(RequestFailure::BadRequest);
    }
    for pair in raw.split('&') {
        let (key, value) = pair.split_once('=').ok_or(RequestFailure::BadRequest)?;
        for component in [key, value] {
            strict_percent(component)?;
            percent_decode_str(component)
                .decode_utf8()
                .map_err(|_| RequestFailure::BadRequest)?;
        }
    }
    let mut parsed = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        if parsed
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return Err(RequestFailure::BadRequest);
        }
    }
    Ok(parsed)
}

fn decimal(value: &str) -> Result<usize, RequestFailure> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RequestFailure::BadRequest);
    }
    value.parse().map_err(|_| RequestFailure::BadRequest)
}

fn strict_percent(value: &str) -> Result<(), RequestFailure> {
    let bytes = value.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] == b'%' {
            if !bytes.get(cursor + 1).is_some_and(u8::is_ascii_hexdigit)
                || !bytes.get(cursor + 2).is_some_and(u8::is_ascii_hexdigit)
            {
                return Err(RequestFailure::BadRequest);
            }
            cursor += 3;
        } else {
            cursor += 1;
        }
    }
    Ok(())
}
