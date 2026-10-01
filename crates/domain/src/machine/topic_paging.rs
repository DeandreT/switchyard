use super::*;

/// Configurations inspected by one topic page, excluding its single lookahead.
pub const MAX_TOPIC_PAGE_SIZE: usize = 1_024;

/// Exclusive position in topic-configuration key order, independent of queues.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicCursor {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPage {
    pub topics: Vec<(NamespaceName, EntityPath)>,
    /// The last returned topic, only when the page has entries after it.
    pub continuation: Option<TopicCursor>,
}

impl<S: StateStore> StateMachine<S> {
    /// One bounded keyset page of topics, never their subscription queues.
    /// Pages reflect individual reads, not a frozen cross-page snapshot.
    pub fn topics_page(
        &self,
        namespace: Option<&NamespaceName>,
        after: Option<&TopicCursor>,
        limit: usize,
    ) -> Result<TopicPage, BrokerError> {
        if limit > MAX_TOPIC_PAGE_SIZE {
            return Err(BrokerError::TopicPageLimitExceeded {
                limit,
                maximum: MAX_TOPIC_PAGE_SIZE,
            });
        }
        if let (Some(namespace), Some(after)) = (namespace, after)
            && namespace != &after.namespace
        {
            return Err(BrokerError::TopicCursorNamespaceMismatch {
                namespace: namespace.clone(),
                cursor_namespace: after.namespace.clone(),
            });
        }
        if limit == 0 {
            return Ok(TopicPage {
                topics: Vec::new(),
                continuation: None,
            });
        }
        let prefix = namespace.map_or_else(
            keys::topic_config_prefix,
            keys::namespace_topic_config_prefix,
        );
        let start = after.map_or_else(
            || prefix.clone(),
            |after| {
                let mut start = keys::topic_config(&after.namespace, &after.entity);
                start.push(0);
                start
            },
        );
        let records = self.store.scan_from(&prefix, &start, limit + 1)?;
        let has_more = records.len() > limit;
        let topics = records
            .into_iter()
            .take(limit)
            .map(|(key, _)| {
                let (namespace, entity) =
                    keys::entity_scope_parts(&key).ok_or(BrokerError::MalformedIndexKey)?;
                let namespace = NamespaceName::new(namespace)?;
                let entity = EntityPath::new(entity)?;
                if keys::topic_config(&namespace, &entity) != key {
                    return Err(BrokerError::MalformedIndexKey);
                }
                Ok((namespace, entity))
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        let continuation = if has_more {
            topics.last().map(|(namespace, entity)| TopicCursor {
                namespace: namespace.clone(),
                entity: entity.clone(),
            })
        } else {
            None
        };
        Ok(TopicPage {
            topics,
            continuation,
        })
    }
}
