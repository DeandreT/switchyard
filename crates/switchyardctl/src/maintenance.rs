use admin_api::v1::{
    ClockReadinessResponse, GetClockReadinessRequest, MaintenanceClockState,
    maintenance_service_client::MaintenanceServiceClient,
};

use super::*;

#[derive(Serialize)]
struct AssessmentOutput {
    scope: &'static str,
    state: &'static str,
}

fn output(response: ClockReadinessResponse) -> Result<(AssessmentOutput, bool), CliError> {
    let state = MaintenanceClockState::try_from(response.state)
        .map_err(|_| CliError::Input("invalid maintenance clock assessment"))?;
    let name = match state {
        MaintenanceClockState::Unknown => "unknown",
        MaintenanceClockState::Ready => "ready",
        MaintenanceClockState::Unsafe => "unsafe",
        MaintenanceClockState::Unavailable => "unavailable",
        MaintenanceClockState::Stopped => "stopped",
    };
    Ok((
        AssessmentOutput {
            scope: "development_maintenance_clock",
            state: name,
        },
        state == MaintenanceClockState::Ready,
    ))
}

pub(super) async fn execute(arguments: &Arguments) -> Result<(), CliError> {
    let settings = ConnectionSettings::prepare(arguments)?;
    let mut client = MaintenanceServiceClient::new(settings.connect_channel().await?)
        .max_encoding_message_size(MAX_REQUEST_BYTES)
        .max_decoding_message_size(MAX_RESPONSE_BYTES);
    let response = tokio::time::timeout(
        REQUEST_TIMEOUT,
        client.get_clock_readiness(settings.request(GetClockReadinessRequest {
            namespace: arguments.namespace.clone(),
        })),
    )
    .await
    .map_err(|_| CliError::Timeout)?
    .map_err(|status| CliError::Request(status.code()))?
    .into_inner();
    let (value, ready) = output(response)?;
    write_output(&value)?;
    if ready {
        Ok(())
    } else {
        Err(CliError::Input("maintenance clock is not ready"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_reuses_existing_credential_and_transport_policy() {
        let parse = |extra: &[&str]| {
            let mut argv = vec!["switchyardctl"];
            argv.extend_from_slice(extra);
            argv.push("maintenance-clock");
            Arguments::try_parse_from(argv).expect("maintenance command")
        };
        assert!(ConnectionSettings::prepare(&parse(&[])).is_err());
        for options in [
            vec!["--endpoint", "http://example.com", "--allow-insecure"],
            vec![
                "--endpoint",
                "http://localhost",
                "--allow-insecure",
                "--token-file",
                "missing",
            ],
            vec![
                "--endpoint",
                "http://localhost",
                "--allow-insecure",
                "--ca-certificate",
                "missing",
            ],
        ] {
            assert!(ConnectionSettings::prepare(&parse(&options)).is_err());
        }
        assert!(
            ConnectionSettings::prepare(&parse(&[
                "--endpoint",
                "http://localhost",
                "--allow-insecure"
            ]))
            .is_ok()
        );
        assert!(ConnectionSettings::prepare(&parse(&["--endpoint", "http://localhost"])).is_err());
        assert!(
            ConnectionSettings::prepare(&parse(&["--tls-server-name", "invalid/name"])).is_err()
        );
        let credentials = tempfile::TempDir::new().expect("credential fixture");
        let ca = credentials.path().join("ca.pem");
        let token = credentials.path().join("token");
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("test certificate");
        std::fs::write(&ca, cert.pem()).expect("test CA file");
        std::fs::write(&token, "SharedAccessSignature private-test-token")
            .expect("test token file");
        let ca_path = ca.to_str().expect("test CA path");
        let token_path = token.to_str().expect("test token path");
        let settings = ConnectionSettings::prepare(&parse(&[
            "--ca-certificate",
            ca_path,
            "--tls-server-name",
            "localhost",
            "--token-file",
            token_path,
        ]))
        .expect("existing HTTPS credentials");
        assert!(settings.tls);
        let request = settings.request(GetClockReadinessRequest {
            namespace: "tenant".into(),
        });
        assert!(
            request
                .metadata()
                .get("authorization")
                .expect("token metadata")
                .is_sensitive()
        );
        std::fs::write(&token, vec![b'x'; MAX_TOKEN_FILE_BYTES + 1])
            .expect("oversized token fixture");
        assert!(
            ConnectionSettings::prepare(&parse(&[
                "--ca-certificate",
                ca_path,
                "--token-file",
                token_path
            ]))
            .is_err()
        );
        std::fs::write(&ca, vec![b'x'; MAX_CA_FILE_BYTES + 1]).expect("oversized CA fixture");
        assert!(ConnectionSettings::prepare(&parse(&["--ca-certificate", ca_path])).is_err());
        assert!(
            Arguments::try_parse_from([
                "switchyardctl",
                "--token",
                "inline-secret",
                "maintenance-clock"
            ])
            .is_err()
        );
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(5));
        assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn cli_output_is_fixed_redacted_and_nonready_fails() {
        for (state, expected) in [
            (0, "unknown"),
            (1, "ready"),
            (2, "unsafe"),
            (3, "unavailable"),
            (4, "stopped"),
        ] {
            let (value, ready) = output(ClockReadinessResponse { state }).expect("known state");
            let bytes = serde_json::to_vec_pretty(&value).expect("fixed JSON");
            assert!(bytes.len() + 1 < 128);
            assert_eq!(ready, state == 1);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).expect("JSON"),
                serde_json::json!({"scope":"development_maintenance_clock","state":expected})
            );
        }
        assert_eq!(
            CliError::Input("maintenance clock is not ready").to_string(),
            "maintenance clock is not ready"
        );
    }

    #[test]
    fn cli_rejects_unknown_enum_and_transport_payloads() {
        for state in [-1, 5, i32::MAX] {
            let error = output(ClockReadinessResponse { state })
                .err()
                .expect("invalid enum");
            assert_eq!(error.to_string(), "invalid maintenance clock assessment");
        }
        let status = tonic::Status::unavailable("secret remote clock details");
        let error = CliError::Request(status.code()).to_string();
        assert!(!error.contains("secret"));
        assert!(error.contains("Unavailable"));
        assert_eq!(
            CliError::Timeout.to_string(),
            "administration request timed out"
        );
    }
}
