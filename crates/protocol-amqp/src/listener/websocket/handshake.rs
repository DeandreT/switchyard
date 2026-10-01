use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{StatusCode, Version, header, uri::Authority};

#[allow(
    clippy::result_large_err,
    reason = "The WebSocket handshake callback requires an unboxed HTTP error response"
)]
pub(super) fn validate_upgrade(
    request: &Request,
    mut response: Response,
) -> Result<Response, ErrorResponse> {
    if request.version() != Version::HTTP_11 {
        return Err(refusal(StatusCode::BAD_REQUEST, "HTTP/1.1 is required"));
    }
    let uri = request.uri();
    if uri.scheme().is_some()
        || uri.authority().is_some()
        || uri.query().is_some()
        || uri.path() != "/$servicebus/websocket/"
    {
        return Err(refusal(
            StatusCode::NOT_FOUND,
            "WebSocket endpoint not found",
        ));
    }
    let headers = request.headers();
    for name in [
        header::HOST,
        header::UPGRADE,
        header::SEC_WEBSOCKET_KEY,
        header::SEC_WEBSOCKET_VERSION,
        header::CONTENT_LENGTH,
    ] {
        if headers.get_all(&name).iter().count() > 1 {
            return Err(refusal(StatusCode::BAD_REQUEST, "Duplicate upgrade header"));
        }
    }
    let host = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok());
    if !host.is_some_and(valid_host) {
        return Err(refusal(StatusCode::BAD_REQUEST, "Invalid upgrade Host"));
    }
    if headers.contains_key(header::TRANSFER_ENCODING)
        || headers
            .get(header::CONTENT_LENGTH)
            .is_some_and(|length| length != "0")
    {
        return Err(refusal(
            StatusCode::BAD_REQUEST,
            "Upgrade request body is not supported",
        ));
    }
    let mut offered = false;
    for protocols in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        let Ok(protocols) = protocols.to_str() else {
            return Err(refusal(
                StatusCode::BAD_REQUEST,
                "Invalid WebSocket subprotocol",
            ));
        };
        for protocol in protocols
            .split(',')
            .map(|protocol| protocol.trim_matches([' ', '\t']))
        {
            if protocol.is_empty() || !protocol.bytes().all(token_byte) {
                return Err(refusal(
                    StatusCode::BAD_REQUEST,
                    "Invalid WebSocket subprotocol",
                ));
            }
            offered |= protocol == "amqp";
        }
    }
    if !offered {
        return Err(refusal(
            StatusCode::BAD_REQUEST,
            "The amqp WebSocket subprotocol is required",
        ));
    }
    response.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        header::HeaderValue::from_static("amqp"),
    );
    Ok(response)
}

fn valid_host(host: &str) -> bool {
    let Ok(authority) = host.parse::<Authority>() else {
        return false;
    };
    !authority.host().is_empty()
        && !host.contains('@')
        && (authority.as_str() == authority.host()
            || authority.port_u16().is_some_and(|port| port != 0))
}

fn token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn refusal(status: StatusCode, reason: &'static str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(reason.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::http::Uri;

    fn request(uri: &str, protocol: &str) -> Request {
        let mut request = Request::new(());
        *request.version_mut() = Version::HTTP_11;
        *request.uri_mut() = uri.parse::<Uri>().unwrap();
        request.headers_mut().insert(
            header::HOST,
            header::HeaderValue::from_static("localhost:1234"),
        );
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, protocol.parse().unwrap());
        request
    }

    #[test]
    fn exact_protocol_is_selected_from_an_offer() {
        let response = validate_upgrade(
            &request("/$servicebus/websocket/", "other, amqp"),
            Response::new(()),
        )
        .unwrap();
        assert_eq!(response.headers()[header::SEC_WEBSOCKET_PROTOCOL], "amqp");
        assert!(
            validate_upgrade(
                &request("/$servicebus/websocket/", "AMQP"),
                Response::new(())
            )
            .is_err()
        );
    }

    #[test]
    fn endpoint_and_singleton_headers_are_strict() {
        for uri in [
            "/",
            "/$servicebus/websocket/?x=1",
            "http://localhost/$servicebus/websocket/",
        ] {
            assert_eq!(
                validate_upgrade(&request(uri, "amqp"), Response::new(()))
                    .unwrap_err()
                    .status(),
                StatusCode::NOT_FOUND
            );
        }
        let mut request = request("/$servicebus/websocket/", "amqp");
        request
            .headers_mut()
            .append(header::HOST, header::HeaderValue::from_static("other"));
        assert!(validate_upgrade(&request, Response::new(())).is_err());
    }

    #[test]
    fn body_and_missing_host_are_refused() {
        let mut request = request("/$servicebus/websocket/", "amqp");
        request.headers_mut().remove(header::HOST);
        assert!(validate_upgrade(&request, Response::new(())).is_err());
        request
            .headers_mut()
            .insert(header::HOST, header::HeaderValue::from_static("localhost"));
        request.headers_mut().insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_static("1"),
        );
        assert!(validate_upgrade(&request, Response::new(())).is_err());
    }

    #[test]
    fn host_authority_rejects_user_info_and_invalid_ports() {
        for host in [
            "user@localhost",
            "localhost:abc",
            "localhost:",
            "localhost:0",
            "localhost:65536",
        ] {
            assert!(!valid_host(host), "{host}");
        }
        for host in ["localhost", "localhost:443", "[::1]:1234"] {
            assert!(valid_host(host), "{host}");
        }
    }
}
