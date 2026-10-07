use super::*;

fn arguments(options: &[&str]) -> Arguments {
    let mut arguments = vec!["switchyardctl"];
    arguments.extend_from_slice(options);
    arguments.extend(["queue", "get", "orders"]);
    Arguments::try_parse_from(arguments).unwrap()
}

#[test]
fn bearer_headers_are_opaque_bounded_sensitive_metadata() {
    for credential in [
        "Bearer opaque-not-client-validated",
        "Bearer a.b.c\r\n",
        "  Bearer a.b.c  \n",
    ] {
        let metadata = token_metadata(credential.as_bytes()).unwrap();
        assert_eq!(metadata.to_str().unwrap(), credential.trim());
        assert!(metadata.is_sensitive());
        let settings = ConnectionSettings {
            endpoint: "https://localhost:9443".into(),
            tls: true,
            ca_pem: None,
            tls_server_name: None,
            token: Some(metadata),
        };
        let request = settings.request(());
        let transmitted = request.metadata().get("authorization").unwrap();
        assert!(transmitted.is_sensitive());
        assert_eq!(transmitted.to_str().unwrap(), credential.trim());
    }
    let maximum = format!("Bearer {}", "a".repeat(MAX_BEARER_TOKEN_BYTES));
    assert_eq!(maximum.len(), 8199);
    assert!(token_metadata(maximum.as_bytes()).is_ok());
    let oversized = format!("{maximum}a");
    assert!(token_metadata(oversized.as_bytes()).is_err());
}

#[test]
fn bearer_headers_reject_empty_wrong_scheme_whitespace_and_non_ascii() {
    for credential in [
        &b"Bearer"[..],
        &b"Bearer "[..],
        &b"bearer opaque"[..],
        &b"BEARER opaque"[..],
        &b"Bearer\topaque"[..],
        &b"Bearer one two"[..],
        &b"Bearer one\ttwo"[..],
        &b"Bearer one\ntwo"[..],
        &b"Bearer one\rtwo"[..],
        &b"Bearer one\0two"[..],
        &b"Bearer \xc3\xa9"[..],
        &b"Bearer \xff"[..],
    ] {
        let error = token_metadata(credential).unwrap_err();
        assert_eq!(error.to_string(), "invalid authorization token file");
    }
}

#[test]
fn bearer_file_bounds_and_https_prerequisite_stay_before_file_io() {
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("absent-token");
    let absent = absent.to_str().unwrap();
    let input = arguments(&[
        "--endpoint",
        "http://127.0.0.1:1",
        "--allow-insecure",
        "--token-file",
        absent,
    ]);
    let error = ConnectionSettings::prepare(&input).err().unwrap();
    assert_eq!(error.to_string(), "authorization tokens require HTTPS");
    assert!(!error.to_string().contains(absent));
    let source = directory.path().join("oversized-token");
    std::fs::write(&source, vec![b'a'; MAX_TOKEN_FILE_BYTES + 1]).unwrap();
    let error = read_file(
        &source,
        MAX_TOKEN_FILE_BYTES,
        "could not read the token file",
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "credential file exceeds its size limit");
}

#[test]
fn bearer_errors_and_debug_metadata_do_not_disclose_credentials() {
    let secret = "sentinel-bearer-secret";
    let malformed = format!("Bearer {secret} extra");
    let error = token_metadata(malformed.as_bytes()).unwrap_err();
    assert!(!format!("{error}").contains(secret));
    assert!(!format!("{error:?}").contains(secret));
    let metadata = token_metadata(format!("Bearer {secret}").as_bytes()).unwrap();
    assert!(!format!("{metadata:?}").contains(secret));
    assert!(
        Arguments::try_parse_from(["switchyardctl", "queue", "get", "orders", "--token", secret,])
            .is_err()
    );
}
