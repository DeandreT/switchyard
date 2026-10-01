use domain::{SubscriptionName, TopicCursor};
use protocol_amqp::EntityMetadata;

use super::*;

const TOPIC_TOKEN_PREFIX: &str = "topic.v1.";
const SUBSCRIPTION_TOKEN_PREFIX: &str = "subscription.v1.";

#[derive(Clone, PartialEq, prost::Message)]
struct Cursor {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(enumeration = "EntityKind", tag = "2")]
    kind: i32,
    #[prost(string, tag = "3")]
    parent_topic: String,
    #[prost(string, tag = "4")]
    after: String,
}

fn decode(
    token: &str,
    namespace: &NamespaceName,
    kind: EntityKind,
    parent: &str,
) -> Result<Option<String>, Status> {
    if token.is_empty() {
        return Ok(None);
    }
    if token.len() > MAX_PAGE_TOKEN_BYTES {
        return Err(Status::invalid_argument("page token is too large"));
    }
    let prefix = match kind {
        EntityKind::Topic => TOPIC_TOKEN_PREFIX,
        EntityKind::Subscription => SUBSCRIPTION_TOKEN_PREFIX,
        _ => return Err(Status::invalid_argument("invalid page token kind")),
    };
    let encoded = token
        .strip_prefix(prefix)
        .ok_or_else(|| Status::invalid_argument("page token belongs to another entity kind"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| Status::invalid_argument("malformed page token"))?;
    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(Status::invalid_argument("noncanonical page token"));
    }
    let cursor = Cursor::decode(bytes.as_slice())
        .map_err(|_| Status::invalid_argument("malformed page token"))?;
    if cursor.encode_to_vec() != bytes {
        return Err(Status::invalid_argument("noncanonical page token"));
    }
    if cursor.namespace != namespace.as_str()
        || cursor.kind != kind as i32
        || cursor.parent_topic != parent
    {
        return Err(Status::invalid_argument(
            "page token belongs to another listing",
        ));
    }
    Ok(Some(cursor.after))
}

fn encode(namespace: &NamespaceName, kind: EntityKind, parent: &str, after: &str) -> String {
    let prefix = if kind == EntityKind::Topic {
        TOPIC_TOKEN_PREFIX
    } else {
        SUBSCRIPTION_TOKEN_PREFIX
    };
    let cursor = Cursor {
        namespace: namespace.as_str().to_owned(),
        kind: kind as i32,
        parent_topic: parent.to_owned(),
        after: after.to_owned(),
    };
    format!("{prefix}{}", URL_SAFE_NO_PAD.encode(cursor.encode_to_vec()))
}

impl NativeAdminService {
    pub(super) async fn list_topics(
        &self,
        input: &ListEntitiesRequest,
        page_size: usize,
    ) -> Result<ListEntitiesResponse, Status> {
        let after = decode(&input.page_token, &self.namespace, EntityKind::Topic, "")?
            .map(|path| {
                self.entity_path(&path).map(|entity| TopicCursor {
                    namespace: self.namespace.clone(),
                    entity,
                })
            })
            .transpose()?;
        let page = self
            .broker
            .topics_page(Some(self.namespace.clone()), after, page_size)
            .await
            .map_err(read_status)?;
        let token = page
            .continuation
            .as_ref()
            .map_or_else(String::new, |cursor| {
                encode(
                    &self.namespace,
                    EntityKind::Topic,
                    "",
                    cursor.entity.as_str(),
                )
            });
        let mut entities = Vec::with_capacity(page.topics.len());
        for (_, path) in page.topics {
            let entity = self.read_target(AdminTarget::Primary(path)).await?;
            if entity.kind != EntityKind::Topic as i32 {
                return Err(Status::internal("unexpected topic metadata"));
            }
            entities.push(entity);
        }
        Ok(ListEntitiesResponse {
            entities,
            next_page_token: token,
        })
    }

    pub(super) async fn list_subscriptions(
        &self,
        input: &ListEntitiesRequest,
        page_size: usize,
    ) -> Result<ListEntitiesResponse, Status> {
        let topic = self.entity_path(&input.parent_topic)?;
        let after = decode(
            &input.page_token,
            &self.namespace,
            EntityKind::Subscription,
            topic.as_str(),
        )?
        .map(|name| {
            SubscriptionName::new(name).map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .transpose()?;
        let subscriptions = self
            .broker
            .subscriptions(self.namespace.clone(), topic.clone())
            .await
            .map_err(read_status)?;
        let mut eligible = subscriptions.into_iter().filter(|definition| {
            after
                .as_ref()
                .is_none_or(|after| definition.name.as_str() > after.as_str())
        });
        let mut entities = Vec::with_capacity(page_size.min(domain::MAX_TOPIC_SUBSCRIPTIONS));
        let mut last_name = None;
        for definition in eligible.by_ref().take(page_size) {
            last_name = Some(definition.name);
            entities.push(topology::response(
                &self.namespace,
                &definition.entity,
                EntityMetadata::Subscription(definition.config),
            )?);
        }
        let token = if eligible.next().is_some() {
            last_name.as_ref().map_or_else(String::new, |name| {
                encode(
                    &self.namespace,
                    EntityKind::Subscription,
                    topic.as_str(),
                    name.as_str(),
                )
            })
        } else {
            String::new()
        };
        Ok(ListEntitiesResponse {
            entities,
            next_page_token: token,
        })
    }
}
