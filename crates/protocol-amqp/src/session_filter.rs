//! The session filter a receiving link carries on its source.
//!
//! A Service Bus client asks for a session by putting
//! `com.microsoft:session-filter` in the source filter of its attach: a string
//! names the session, a null asks for whichever session the broker grants. The
//! broker answers by echoing the filter with the granted session's identifier,
//! which is the only way a next-available receiver learns what it got.

use amqp::Source;
use domain::SessionId;
use serde_amqp::{Value, primitives::Symbol};

use crate::{ProtocolError, parse_session_id};

pub const SESSION_FILTER: &str = "com.microsoft:session-filter";

/// What a receiving link's source asked for, session-wise.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionRequest {
    /// No session filter: an ordinary queue receiver.
    None,
    /// A filter with a null value: whichever session the broker grants.
    NextAvailable,
    Named(SessionId),
}

/// Reads the session filter off a link's source.
pub fn read_session_filter(source: Option<&Source>) -> Result<SessionRequest, ProtocolError> {
    let Some(value) = source
        .and_then(|source| source.filter.as_ref())
        .and_then(|filter| filter.get(&Symbol::from(SESSION_FILTER)))
    else {
        return Ok(SessionRequest::None);
    };

    match value {
        Value::Null => Ok(SessionRequest::NextAvailable),
        Value::String(session_id) => Ok(SessionRequest::Named(parse_session_id(session_id)?)),
        other => Err(ProtocolError::InvalidSessionId {
            session_id: format!("{other:?}"),
            detail: String::from("a session filter carries a string or null"),
        }),
    }
}

/// Writes the granted session into the source, which the attach response echoes.
pub fn stamp_session_filter(source: &mut Source, session_id: &SessionId) {
    source.filter.get_or_insert_with(Default::default).insert(
        Symbol::from(SESSION_FILTER),
        Value::String(session_id.as_str().to_owned()),
    );
}

#[cfg(test)]
mod tests {
    use amqp::FilterSet;

    use super::*;

    fn source_with(value: Option<Value>) -> Source {
        let mut filter = FilterSet::default();
        if let Some(value) = value {
            filter.insert(Symbol::from(SESSION_FILTER), value);
        }
        Source {
            address: Some(String::from("orders")),
            filter: Some(filter),
            ..Source::default()
        }
    }

    #[test]
    fn no_filter_is_an_ordinary_receiver() {
        assert_eq!(
            read_session_filter(None).expect("readable"),
            SessionRequest::None
        );
        assert_eq!(
            read_session_filter(Some(&source_with(None))).expect("readable"),
            SessionRequest::None
        );
    }

    #[test]
    fn a_null_filter_asks_for_the_next_available_session() {
        assert_eq!(
            read_session_filter(Some(&source_with(Some(Value::Null)))).expect("readable"),
            SessionRequest::NextAvailable
        );
    }

    #[test]
    fn a_string_filter_names_its_session() {
        assert_eq!(
            read_session_filter(Some(&source_with(Some(Value::String(String::from(
                "cart-1"
            ))))))
            .expect("readable"),
            SessionRequest::Named(SessionId::new("cart-1").expect("valid"))
        );
    }

    #[test]
    fn a_filter_of_the_wrong_shape_is_refused() {
        assert!(matches!(
            read_session_filter(Some(&source_with(Some(Value::Long(7))))),
            Err(ProtocolError::InvalidSessionId { .. })
        ));
    }

    #[test]
    fn stamping_overwrites_the_request_with_the_grant() {
        let mut source = source_with(Some(Value::Null));
        stamp_session_filter(&mut source, &SessionId::new("cart-9").expect("valid"));

        assert_eq!(
            read_session_filter(Some(&source)).expect("readable"),
            SessionRequest::Named(SessionId::new("cart-9").expect("valid"))
        );
    }
}
