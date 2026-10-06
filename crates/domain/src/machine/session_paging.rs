use serde::{Deserialize, Serialize};

use super::*;

/// Distinct ready-index session groups inspected by one acceptance page.
pub const MAX_SESSION_PAGE_GROUPS: usize = 32;

/// Exclusive position after one session's entire ready-index group.
///
/// This is a scoped keyset position, not a session hold or a frozen snapshot.
/// The cursor's session need not still have ready entries when it is resumed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionCursor {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub session_id: SessionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionPageOutcome {
    Accepted(AcceptedSession),
    /// A full held page; another page must observe exhaustion or a grant.
    Continue(SessionCursor),
    End,
}

impl<S: StateStore> StateMachine<S> {
    pub(super) fn accept_next_session_page(
        &self,
        command: &Command,
        after: Option<&SessionCursor>,
        lock_duration_millis: Option<u64>,
        batch: &mut WriteBatch,
    ) -> Result<CommandOutcome, BrokerError> {
        let config = self.load_config(command)?;
        if !config.requires_session {
            return Err(BrokerError::SessionNotSupported);
        }
        if let Some(cursor) = after {
            if NamespaceName::new(cursor.namespace.as_str()).is_err()
                || EntityPath::new(cursor.entity.as_str()).is_err()
                || SessionId::new(cursor.session_id.as_str()).is_err()
            {
                return Err(BrokerError::InvalidSessionCursor);
            }
            if cursor.namespace != command.namespace || cursor.entity != command.entity {
                return Err(BrokerError::SessionCursorScopeMismatch {
                    namespace: command.namespace.clone(),
                    entity: command.entity.clone(),
                    cursor_namespace: cursor.namespace.clone(),
                    cursor_entity: cursor.entity.clone(),
                });
            }
        }

        let namespace = &command.namespace;
        let entity = &command.entity;
        let prefix = keys::entity_session_ready_prefix(namespace, entity);
        let mut start = after.map_or_else(
            || prefix.clone(),
            |cursor| keys::after_session_ready(namespace, entity, &cursor.session_id),
        );
        let locked_until = command
            .issued_at
            .saturating_add_millis(lock_duration_millis.unwrap_or(config.lock_duration_millis));
        let mut remaining = MAX_SESSION_PAGE_GROUPS;
        loop {
            let Some((key, _)) = self.store.scan_from(&prefix, &start, 1)?.into_iter().next()
            else {
                return Ok(CommandOutcome::SessionPage(SessionPageOutcome::End));
            };
            let session_id = SessionId::new(
                keys::session_id_after(&prefix, &key).ok_or(BrokerError::MalformedIndexKey)?,
            )?;
            let record = self.load_session(command, &session_id)?;
            if record.live_lock_at(command.issued_at).is_none() {
                match self.lock_session(command, &session_id, record, locked_until, batch) {
                    Ok(accepted) => {
                        return Ok(CommandOutcome::SessionPage(SessionPageOutcome::Accepted(
                            accepted,
                        )));
                    }
                    Err(BrokerError::SessionTakeoverPending { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
            remaining -= 1;
            if remaining == 0 {
                return Ok(CommandOutcome::SessionPage(SessionPageOutcome::Continue(
                    SessionCursor {
                        namespace: namespace.clone(),
                        entity: entity.clone(),
                        session_id,
                    },
                )));
            }
            start = keys::after_session_ready(namespace, entity, &session_id);
        }
    }
}
