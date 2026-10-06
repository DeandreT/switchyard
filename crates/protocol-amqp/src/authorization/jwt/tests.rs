use std::{
    future::Future,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    task::{Context, Waker},
    time::Duration,
};

use super::*;
use auth::{Permission, PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};

const HOST: &str = "tenant.example";
const AUDIENCE: &str = "amqps://tenant.example/orders";
// Public RSA test material and fixed-epoch signatures, not production credentials.
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoiYXQrand0In0.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlLyIsInN1YiI6InByb2R1Y2VyIiwiYXVkIjoidXJuOnN3aXRjaHlhcmQ6dGVuYW50IiwiaWF0IjoxMDAsImV4cCI6MjAwfQ.E22KZCEthMh4ydXiib-QujIOnbAWg2KURc0S_Ix6s2vE69MXwtha8VwyxYUh5vYGzZ48Kx0lilx5O-vYoeu1b29_oPNb5DTsNuqrKtYXI69-VA9Xd_J0EX7k0ZyMrQ6uU0oe2UqhOxs0J9VsHHXlgzelsLsZpWyJ90fs2kLgHu72yWq8Xa1KB_gTeclCeCrForm1KmK8KBUHVWpEqKtX_EftYMrjPZygQ68r_KSqd08k-Cq6hCsZeOdcu_-Z7xzUe3fW6Ck8no2xvnR9lGTrCYDVaW9tInvoZjInFzLoxM5pguj__pS3_cTwbjZGut6YCY33rTchlcU4Xtrv_YnpOA";
const TOKEN_AT_ZERO: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoiYXQrand0In0.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlLyIsInN1YiI6InByb2R1Y2VyIiwiYXVkIjoidXJuOnN3aXRjaHlhcmQ6dGVuYW50IiwiaWF0IjowLCJleHAiOjIwMH0.lpsGNbWa4d6nomt3TF8O_4i8ylySWa3FGGzQ_ms1NzQ2wxOipSdL9ewULSwZu5FrYcOLWM2P5XHsPatkCUSyuZX2OHT3UqXRtOquhQ6TCvKceOEoNIgBcFpxfMSd9pamdCaPUvEl2rTbnUAnDQ4yTpizFTNcaP-Yr3lU8g-q-8IePrcQsPDuE81BgbI3dVBdQcP6fhyf4JRIqeQRilqlzwWMj4I99DSARnMPi8bS4uZSTnmtUzpjv2qBvHrjtHRAxEXfwQez4AmtY7nz6Ed_WcZ3yAnbQ7B6CQW3ilwi7dTEi8g5cEy4_EoZnd9x1qlymYSELg2rzn_zM_cs8jLc7Q";
const OTHER_ISSUER_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoiYXQrand0In0.eyJpc3MiOiJodHRwczovL290aGVyLWlzc3Vlci5leGFtcGxlLyIsInN1YiI6InByb2R1Y2VyIiwiYXVkIjoidXJuOnN3aXRjaHlhcmQ6dGVuYW50IiwiaWF0IjoxMDAsImV4cCI6MjAwfQ.EpH3ZeL_Tc-WmEdypG-prZR9ao9Jr5yyrmL9caGyDojnLxcSU9SngTt8l3OsJXzFXXBCWpoRQ9QTCR9cFvpwc8bp_ZuWUe2k2Ur7ZOnMiEDxP2lcIyD8qZLgTNpEG6l01q_48IMiFh51k6JumHvVxGOq8wKNaDWY1Lexsc6tboIHj1S7gg1IrKY45SOtRN0U-SF8PJSCjtswGr70ynuOmbw0At8pnM1NsUi6m9c999niBq7grIozqeY2EadT7blJQU0hly-8Ytu7XUHdBx1QLMroC0_gOqRh_vIKtonKW3cojNbsYXGl8_-y2IoTIqPetoS52PYHA9JO3LLoN5j_1A";

fn jwt_policy() -> JwtPolicy {
    JwtPolicy::from_json(&format!(r#"{{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"producer","scope":"{AUDIENCE}","permissions":["send"]}}]}}"#)).expect("bounded public JWT fixture")
}

fn configuration(enabled: bool) -> SharedAccessAuthentication {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "producer",
        ResourceScope::parse(AUDIENCE).expect("scope"),
        SharedAccessKey::new("secret").expect("public test key"),
        None,
        PermissionSet::LISTEN,
    )
    .expect("rule")])
    .expect("policy");
    let config = SharedAccessAuthentication::new(policy, HOST).expect("authentication");
    if enabled {
        config.with_offline_jwt_policy(jwt_policy())
    } else {
        config
    }
}

fn connection(
    enabled: bool,
    transport: ConnectionTransport,
    initial_sas: bool,
) -> std::sync::Arc<ConnectionAuthorization> {
    let config = configuration(enabled);
    let grant = initial_sas.then(|| {
        config
            .policy()
            .authenticate_plain("producer", "secret")
            .expect("original SAS grant")
    });
    ConnectionAuthorization::new_on_transport(config, grant, transport)
}

#[test]
fn optional_jwt_configuration_preserves_sas_defaults_and_plain_credentials() {
    let default = configuration(false);
    assert!(!default.requires_tls());
    assert!(
        default
            .policy()
            .authenticate_plain("producer", "secret")
            .is_ok()
    );
    let enabled = configuration(true);
    assert!(enabled.requires_tls());
    assert!(
        enabled
            .policy()
            .authenticate_plain("producer", "secret")
            .is_ok()
    );
    let empty = SharedAccessAuthentication::new(
        SharedAccessPolicy::new([]).expect("existing empty policy"),
        HOST,
    )
    .expect("authentication")
    .with_offline_jwt_policy(jwt_policy());
    assert!(empty.requires_tls());
    assert!(
        empty
            .policy()
            .authenticate_plain("producer", "secret")
            .is_err()
    );
    let sasl = super::super::SharedAccessSaslAcceptor::new(&enabled);
    let plain = amqp::SaslInit {
        mechanism: "PLAIN".into(),
        initial_response: Some(format!("\0producer\0{TOKEN}").into_bytes().into()),
        hostname: None,
    };
    assert_eq!(
        amqp::SaslAuthenticator::authenticate(&sasl, &plain),
        amqp::SaslCode::Auth
    );
    assert!(sasl.grant().is_none());
}

#[tokio::test]
async fn disabled_and_plaintext_jwt_refuse_before_validation_or_mutation() {
    for (enabled, transport, expected) in [
        (
            false,
            ConnectionTransport::TlsEstablished,
            JwtAuthorizationError::Disabled,
        ),
        (
            true,
            ConnectionTransport::Plaintext,
            JwtAuthorizationError::TransportRequired,
        ),
    ] {
        let connection = connection(enabled, transport, true);
        let before = connection.grants.read().await.clone();
        let state = connection.initial_control_state().await;
        let result = connection
            .validate_jwt_with_clock(TOKEN, AUDIENCE, || {
                panic!("a refused transport or disabled policy must not validate")
            })
            .await;
        assert_eq!(result, Err(expected));
        assert!(*connection.grants.read().await == before);
        assert_eq!(connection.initial_control_state().await, state);
    }
}

#[tokio::test]
async fn tls_jwt_grants_only_local_scope_permissions_and_expiry() {
    let connection = connection(true, ConnectionTransport::TlsEstablished, false);
    connection
        .validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(100))
        .await
        .expect("valid TLS grant");
    let grants = connection.grants.read().await;
    assert_eq!(grants.len(), 1);
    let grant = &grants[0];
    assert_eq!(
        grant.scope(),
        &ResourceScope::parse(AUDIENCE).expect("exact scope")
    );
    assert_eq!(grant.permissions(), PermissionSet::SEND);
    assert_eq!(grant.expires_at_epoch_seconds(), 200);
    assert!(grant.allows(
        &ResourceScope::parse(AUDIENCE).expect("scope"),
        Permission::Send,
        199
    ));
    assert!(!grant.allows(
        &ResourceScope::parse(AUDIENCE).expect("scope"),
        Permission::Listen,
        199
    ));
    assert!(!grant.allows(
        &ResourceScope::parse(AUDIENCE).expect("scope"),
        Permission::Send,
        200
    ));
    assert!(!grant.allows(
        &ResourceScope::entity(HOST, "orders-archive").expect("sibling"),
        Permission::Send,
        100
    ));
}

#[tokio::test]
async fn jwt_refresh_preserves_sas_same_name_scope_and_rejects_other_issuer() {
    let connection = connection(true, ConnectionTransport::TlsEstablished, true);
    connection
        .validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(100))
        .await
        .expect("first JWT grant");
    let before = connection.grants.read().await.clone();
    assert_eq!(before.len(), 2);
    assert_eq!(before[0].subject(), before[1].subject());
    assert_eq!(before[0].scope(), before[1].scope());
    assert!(!before[0].same_principal(&before[1]));
    assert_eq!(
        connection
            .validate_jwt_with_clock(OTHER_ISSUER_TOKEN, AUDIENCE, || Ok(100))
            .await,
        Err(JwtAuthorizationError::InvalidToken(
            JwtError::IssuerMismatch
        ))
    );
    assert!(*connection.grants.read().await == before);
    connection
        .validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(150))
        .await
        .expect("JWT refresh");
    let refreshed = connection.grants.read().await;
    assert_eq!(refreshed.len(), 2);
    assert!(refreshed[0] == before[0]);
    assert!(refreshed[1].same_principal(&before[1]));
}

async fn clock_failure_before_wait_preserves_publication() {
    let before_unix = UNIX_EPOCH
        .checked_sub(Duration::from_secs(1))
        .expect("representable negative epoch fixture");
    assert_eq!(checked_epoch_seconds_at(UNIX_EPOCH), Ok(0));
    assert_eq!(
        checked_epoch_seconds_at(before_unix),
        Err(JwtAuthorizationError::ClockUnavailable)
    );
    jwt_policy()
        .validate(
            TOKEN_AT_ZERO,
            &ResourceScope::parse(AUDIENCE).expect("scope"),
            0,
        )
        .expect("signed token is valid at representable epoch zero");
    for initial_sas in [false, true] {
        let connection = connection(true, ConnectionTransport::TlsEstablished, initial_sas);
        connection
            .enable_initial_control_grace(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        let before = connection.grants.read().await.clone();
        let state = connection.initial_control_state().await;
        let mut changed = Box::pin(connection.grant_changed.notified());
        changed.as_mut().enable();
        let mut context = Context::from_waker(Waker::noop());
        assert!(changed.as_mut().poll(&mut context).is_pending());
        let grants = connection.grants.write().await;
        let control = connection.initial_control.lock().await;
        let calls = AtomicUsize::new(0);
        let mut publishing =
            Box::pin(
                connection.validate_jwt_with_clock(TOKEN_AT_ZERO, AUDIENCE, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    checked_epoch_seconds_at(before_unix)
                }),
            );
        assert!(matches!(
            publishing.as_mut().poll(&mut context),
            std::task::Poll::Ready(Err(JwtAuthorizationError::ClockUnavailable))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(changed.as_mut().poll(&mut context).is_pending());
        assert!(*grants == before);
        drop(publishing);
        drop(control);
        drop(grants);
        assert!(*connection.grants.read().await == before);
        assert_eq!(connection.initial_control_state().await, state);
    }
}

async fn blocked_clock_failure_preserves_publication(control_lock: bool) {
    let before_unix = UNIX_EPOCH
        .checked_sub(Duration::from_secs(1))
        .expect("representable negative epoch fixture");
    for initial_sas in [false, true] {
        let connection = connection(true, ConnectionTransport::TlsEstablished, initial_sas);
        connection
            .enable_initial_control_grace(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;
        let before = connection.grants.read().await.clone();
        let state = connection.initial_control_state().await;
        let mut changed = Box::pin(connection.grant_changed.notified());
        changed.as_mut().enable();
        let mut context = Context::from_waker(Waker::noop());
        assert!(changed.as_mut().poll(&mut context).is_pending());
        let grants = if control_lock {
            None
        } else {
            Some(connection.grants.write().await)
        };
        let control = if control_lock {
            Some(connection.initial_control.lock().await)
        } else {
            None
        };
        let failed = AtomicBool::new(false);
        let calls = AtomicUsize::new(0);
        let mut publishing =
            Box::pin(
                connection.validate_jwt_with_clock(TOKEN_AT_ZERO, AUDIENCE, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    checked_epoch_seconds_at(if failed.load(Ordering::SeqCst) {
                        before_unix
                    } else {
                        UNIX_EPOCH
                    })
                }),
            );
        assert!(publishing.as_mut().poll(&mut context).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        failed.store(true, Ordering::SeqCst);
        drop(grants);
        drop(control);
        let result = tokio::time::timeout(Duration::from_secs(1), publishing)
            .await
            .expect("original publication resumed");
        assert_eq!(result, Err(JwtAuthorizationError::ClockUnavailable));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(changed.as_mut().poll(&mut context).is_pending());
        assert!(*connection.grants.read().await == before);
        assert_eq!(connection.initial_control_state().await, state);
    }
}

async fn blocked_clock_refresh(control_lock: bool, resumed_epoch: u64, expected: JwtError) {
    for initial_sas in [false, true] {
        let connection = connection(true, ConnectionTransport::TlsEstablished, initial_sas);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        connection.enable_initial_control_grace(deadline).await;
        let before = connection.grants.read().await.clone();
        let state = connection.initial_control_state().await;
        let clock = AtomicU64::new(100);
        let grant_guard = if control_lock {
            None
        } else {
            Some(connection.grants.write().await)
        };
        let control_guard = if control_lock {
            Some(connection.initial_control.lock().await)
        } else {
            None
        };
        let mut publishing =
            Box::pin(
                connection
                    .validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(clock.load(Ordering::SeqCst))),
            );
        let mut context = Context::from_waker(Waker::noop());
        assert!(publishing.as_mut().poll(&mut context).is_pending());
        clock.store(resumed_epoch, Ordering::SeqCst);
        drop(grant_guard);
        drop(control_guard);
        let result = tokio::time::timeout(Duration::from_secs(1), publishing)
            .await
            .expect("original publication resumed");
        assert_eq!(result, Err(JwtAuthorizationError::InvalidToken(expected)));
        assert!(*connection.grants.read().await == before);
        assert_eq!(connection.initial_control_state().await, state);
    }
}

#[tokio::test]
async fn jwt_publication_rechecks_expiry_after_grant_lock_wait() {
    clock_failure_before_wait_preserves_publication().await;
    blocked_clock_refresh(false, 200, JwtError::Expired).await;
    blocked_clock_failure_preserves_publication(false).await;
}

#[tokio::test]
async fn jwt_publication_rechecks_time_after_initial_control_lock_wait() {
    blocked_clock_refresh(true, 200, JwtError::Expired).await;
    blocked_clock_refresh(true, 99, JwtError::NotYetValid).await;
    blocked_clock_failure_preserves_publication(true).await;
}

#[tokio::test]
async fn canceled_jwt_publication_never_exposes_grant_without_history() {
    let connection = connection(true, ConnectionTransport::TlsEstablished, false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    connection.enable_initial_control_grace(deadline).await;
    let state = connection.initial_control_state().await;
    let initial_control = connection.initial_control.lock().await;
    let mut publishing = Box::pin(connection.validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(100)));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    assert!(connection.grants.try_read().is_err());
    drop(publishing);
    assert!(connection.grants.read().await.is_empty());
    drop(initial_control);
    assert_eq!(connection.initial_control_state().await, state);
    connection
        .validate_jwt_with_clock(TOKEN, AUDIENCE, || Ok(100))
        .await
        .expect("healthy retry");
    assert_eq!(
        connection.initial_control_state().await,
        crate::authorization::InitialControlState::Authorized
    );
}

#[tokio::test]
async fn invalid_jwt_scope_and_token_leave_both_authorization_states_unchanged() {
    let connection = connection(true, ConnectionTransport::TlsEstablished, true);
    let before = connection.grants.read().await.clone();
    let state = connection.initial_control_state().await;
    for (token, audience) in [
        (TOKEN, "amqps://other.example/orders"),
        (TOKEN, "amqps://tenant.example/orders-archive"),
        ("not-a-jwt", AUDIENCE),
    ] {
        assert!(
            connection
                .validate_jwt_with_clock(token, audience, || Ok(100))
                .await
                .is_err()
        );
        assert!(*connection.grants.read().await == before);
        assert_eq!(connection.initial_control_state().await, state);
    }
}
