use std::{
    cell::Cell,
    future::Future,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
};

use auth::{JwtPolicy, PermissionSet, SharedAccessKey, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::*;

const HOST: &str = "tenant.servicebus.windows.net";
const ISSUER: &str = "https://issuer.example/";
const OTHER: &str = "https://other-issuer.example/";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn shared_access_policy(permissions: PermissionSet) -> SharedAccessPolicy {
    SharedAccessPolicy::new([SharedAccessRule::new(
        "rule",
        ResourceScope::namespace(HOST).unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        permissions,
    )
    .unwrap()])
    .unwrap()
}

fn connection(initial: Option<AccessGrant>) -> Arc<ConnectionAuthorization> {
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(shared_access_policy(PermissionSet::SEND), HOST).unwrap(),
        initial,
    )
}

fn verified_jwt(token: &str, issuer: &str, scope: &str, permissions: &str) -> AccessGrant {
    let configuration = format!(
        r#"{{"version":1,"issuer":"{issuer}","audience":"urn:switchyard:tenant",
        "keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],
        "bindings":[{{"subject":"rule","scope":"{scope}","permissions":[{permissions}]}}]}}"#
    );
    JwtPolicy::from_json(&configuration)
        .unwrap()
        .validate(token, &ResourceScope::parse(scope).unwrap(), 170)
        .expect("fixture must pass the real offline signature/claims/policy verification")
}

fn sas_token(audience: &str, expiry: u64) -> String {
    let encoded =
        percent_encoding::utf8_percent_encode(audience, percent_encoding::NON_ALPHANUMERIC);
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature =
        percent_encoding::utf8_percent_encode(&signature, percent_encoding::NON_ALPHANUMERIC);
    format!("SharedAccessSignature sr={encoded}&sig={signature}&se={expiry}&skn=rule")
}

#[tokio::test]
async fn authorization_samples_time_after_the_grants_lock_frontier() {
    let authorization = connection(Some(verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send""#)));
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let now = Cell::new(150);
    let lock = authorization.grant_write_lock().await;
    let mut check = pin!(authorization.authorize_resource_any_with_clock(
        &resource,
        &[Permission::Send],
        || now.get()
    ));
    assert!(poll_once(check.as_mut()).is_pending());
    now.set(200);
    drop(lock);
    assert!(
        check.await.is_err(),
        "the pre-await epoch cannot keep a grant alive"
    );
}

#[tokio::test]
async fn same_principal_and_scope_refresh_replaces_rights_and_both_validity_bounds() {
    let first = verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send","listen""#);
    let refreshed = verified_jwt(REFRESH, ISSUER, AUDIENCE, r#""listen""#);
    assert!(first.same_principal(&refreshed));
    let authorization = connection(Some(first));
    assert!(authorization.has_valid_grant_at(149).await);
    authorization.add_verified_grant(refreshed.clone()).await;
    assert_eq!(authorization.grant_snapshot().await, vec![refreshed]);

    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    assert!(
        authorization
            .authorize_resource_any_at(&resource, &[Permission::Send], 170)
            .await
            .is_err()
    );
    for (now, valid) in [(149, false), (150, true), (179, true), (180, false)] {
        assert_eq!(authorization.has_valid_grant_at(now).await, valid);
        assert_eq!(
            authorization
                .authorize_resource_any_at(&resource, &[Permission::Listen], now)
                .await
                .is_ok(),
            valid
        );
    }
}

#[tokio::test]
async fn identical_subjects_from_jwt_issuers_and_shared_access_do_not_replace_each_other() {
    let sas = shared_access_policy(PermissionSet::SEND)
        .validate_sas(&sas_token(AUDIENCE, 300), AUDIENCE, 170)
        .unwrap();
    let first = verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send""#);
    let other = verified_jwt(OTHER_ISSUER, OTHER, AUDIENCE, r#""listen""#);
    let refreshed = verified_jwt(REFRESH, ISSUER, AUDIENCE, r#""listen""#);
    assert_eq!(sas.subject(), first.subject());
    assert_eq!(first.subject(), other.subject());
    assert_eq!(sas.scope(), first.scope());
    assert_eq!(first.scope(), other.scope());

    // Exercise both arrival orders: a SAS refresh must not remove a JWT either.
    for grants in [
        [sas.clone(), first.clone(), other.clone()],
        [other.clone(), first.clone(), sas.clone()],
    ] {
        let authorization = connection(None);
        for grant in grants {
            authorization.add_verified_grant(grant).await;
        }
        authorization.add_verified_grant(refreshed.clone()).await;
        let stored = authorization.grant_snapshot().await;
        assert_eq!(stored.len(), 3);
        for expected in [&sas, &other, &refreshed] {
            assert!(stored.contains(expected));
        }
        assert!(!stored.contains(&first));
    }
}

#[tokio::test]
async fn refresh_preserves_distinct_scopes_for_the_same_verified_principal() {
    let namespace = format!("amqps://{HOST}");
    let sibling = format!("amqps://{HOST}/orders-archive");
    let grants = [
        verified_jwt(FIRST, ISSUER, &namespace, r#""send""#),
        verified_jwt(FIRST, ISSUER, &sibling, r#""listen""#),
        verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send","listen""#),
    ];
    assert!(grants.iter().all(|grant| grant.same_principal(&grants[0])));
    let authorization = connection(None);
    for grant in &grants {
        authorization.add_verified_grant(grant.clone()).await;
    }
    let refreshed = verified_jwt(REFRESH, ISSUER, AUDIENCE, r#""listen""#);
    authorization.add_verified_grant(refreshed.clone()).await;
    assert_eq!(
        authorization.grant_snapshot().await,
        vec![grants[0].clone(), grants[1].clone(), refreshed]
    );

    for (path, permission, allowed) in [
        ("orders", Permission::Send, true), // Independent namespace grant remains.
        ("orders/$DeadLetterQueue", Permission::Listen, true),
        ("orders-archive", Permission::Listen, true),
        ("orders-archive-extra", Permission::Listen, false),
        ("other", Permission::Listen, false),
    ] {
        assert_eq!(
            authorization
                .authorize_resource_any_at(
                    &ResourceScope::entity(HOST, path).unwrap(),
                    &[permission],
                    170,
                )
                .await
                .is_ok(),
            allowed,
            "{path}: {permission:?}"
        );
    }
    assert!(
        authorization
            .authorize_resource_any_at(
                &ResourceScope::entity("other.example", "orders").unwrap(),
                &[Permission::Send, Permission::Listen],
                170,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn connection_operations_and_waits_recheck_full_interval_with_backwards_time() {
    let grant = verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send""#);
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let authorization = connection(Some(grant));
    // The lower bound is nbf=120, not merely iat=100. Revisit it after valid time.
    for (now, valid) in [
        (99, false),
        (100, false),
        (119, false),
        (120, true),
        (199, true),
        (119, false),
        (120, true),
        (200, false),
        (201, false),
    ] {
        assert_eq!(authorization.has_valid_grant_at(now).await, valid);
        for permissions in [
            &[Permission::Send][..],
            &[Permission::Listen, Permission::Send],
        ] {
            assert_eq!(
                authorization
                    .authorize_resource_any_at(&resource, permissions, now)
                    .await
                    .is_ok(),
                valid
            );
            let mut wait = pin!(authorization.wait_until_unauthorized_with_clock(
                &resource,
                permissions,
                || now,
            ));
            assert_eq!(poll_once(wait.as_mut()).is_pending(), valid);
        }
    }
}

#[tokio::test]
async fn unauthorized_wait_ignores_future_grants_and_uses_latest_current_permission() {
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let authorization = connection(Some(verified_jwt(FIRST, ISSUER, AUDIENCE, r#""send""#)));
    authorization
        .add_verified_grant(verified_jwt(OTHER_ISSUER, OTHER, AUDIENCE, r#""listen""#))
        .await;
    let now = Cell::new(150);
    let mut send = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &[Permission::Send],
        || now.get(),
    ));
    let permissions = [Permission::Send, Permission::Listen];
    let mut either = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &permissions,
        || now.get(),
    ));
    assert!(poll_once(send.as_mut()).is_pending());
    assert!(poll_once(either.as_mut()).is_pending());
    now.set(200);
    authorization.grant_changed.notify_waiters();
    assert!(poll_once(send.as_mut()).is_ready());
    assert!(
        poll_once(either.as_mut()).is_pending(),
        "Listen remains valid until 250"
    );

    // Replacing Send with a later-starting grant must not keep its link alive
    // when an injected clock moves backwards before that grant's validity start.
    authorization
        .add_verified_grant(verified_jwt(LATER, ISSUER, AUDIENCE, r#""send""#))
        .await;
    now.set(169);
    let mut send = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &[Permission::Send],
        || now.get(),
    ));
    assert!(poll_once(send.as_mut()).is_ready());
    assert!(poll_once(either.as_mut()).is_pending());
    now.set(119);
    authorization.grant_changed.notify_waiters();
    assert!(poll_once(either.as_mut()).is_ready());
}

#[tokio::test]
async fn sas_refresh_replaces_plain_and_sas_validity_without_widening_scope() {
    let policy = shared_access_policy(PermissionSet::SEND);
    let plain = policy.authenticate_plain("rule", "secret").unwrap();
    let authorization = connection(Some(plain.clone()));
    let namespace = format!("amqps://{HOST}");
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let mut wait = pin!(authorization.wait_until_unauthorized(&resource, Permission::Send));
    assert!(
        poll_once(wait.as_mut()).is_pending(),
        "PLAIN remains non-expiring"
    );

    for expiry in [epoch_seconds() + 3600, epoch_seconds() + 1800] {
        authorization
            .validate_and_add(&sas_token(&namespace, expiry), &namespace)
            .await
            .unwrap();
        let grants = authorization.grant_snapshot().await;
        assert_eq!(grants.len(), 1);
        assert!(plain.same_principal(&grants[0]));
        assert_eq!(grants[0].valid_from_epoch_seconds(), 0);
        assert_eq!(grants[0].expires_at_epoch_seconds(), expiry);
        assert_eq!(grants[0].permissions(), PermissionSet::SEND);
        assert!(poll_once(wait.as_mut()).is_pending());
    }
    let before = authorization.grant_snapshot().await;
    assert!(
        authorization
            .validate_and_add("invalid", &namespace)
            .await
            .is_err()
    );
    assert_eq!(authorization.grant_snapshot().await, before);
    assert!(
        authorization
            .authorize_resource(&resource, Permission::Send)
            .await
            .is_ok()
    );
    assert!(
        authorization
            .authorize_resource(&resource, Permission::Listen)
            .await
            .is_err()
    );

    let expiry = epoch_seconds() + 600;
    authorization
        .validate_and_add(&sas_token(AUDIENCE, expiry), AUDIENCE)
        .await
        .unwrap();
    let grants = authorization.grant_snapshot().await;
    assert_eq!(
        grants.len(),
        2,
        "different scopes do not replace one another"
    );
    assert!(grants.contains(&before[0]));
}

#[tokio::test(start_paused = true)]
async fn distant_sas_expiry_bounds_timer_without_shortening_grant_validity() {
    let authorization = connection(None);
    let expiry = u64::MAX - 1;
    authorization
        .validate_and_add(&sas_token(AUDIENCE, expiry), AUDIENCE)
        .await
        .unwrap();
    let stored = authorization.grant_snapshot().await;
    assert_eq!(stored[0].expires_at_epoch_seconds(), expiry);
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let now = Cell::new(epoch_seconds());
    let mut wait = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &[Permission::Send],
        || now.get(),
    ));
    // Polling constructs the sleep: an unbounded duration panics here when it
    // overflows Instant. A bounded timer must leave the actual expiry intact.
    assert!(poll_once(wait.as_mut()).is_pending());
    // Advance the real production clamp without changing grants. The same
    // original wait must wake and recheck injected wall time, not only react
    // to a grant refresh or a short final-expiry timer.
    now.set(expiry);
    tokio::time::advance(MAX_AUTHORIZATION_WAIT).await;
    assert!(poll_once(wait.as_mut()).is_ready());
    now.set(expiry - 1);
    let mut wait = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &[Permission::Send],
        || now.get(),
    ));
    assert!(poll_once(wait.as_mut()).is_pending());
    now.set(expiry);
    // The final one-second timer, not a grant-change notification, must wake the
    // same wait and recheck the clock against the original exclusive expiry.
    tokio::time::timeout(Duration::from_secs(30), wait.as_mut())
        .await
        .expect("timer wake must recheck grant validity");
    assert_eq!(authorization.grant_snapshot().await, stored);
}

#[tokio::test(start_paused = true)]
async fn maximum_sas_expiry_uses_the_same_exclusive_timer_boundary() {
    let grant = shared_access_policy(PermissionSet::SEND)
        .validate_sas(&sas_token(AUDIENCE, u64::MAX), AUDIENCE, u64::MAX - 1)
        .unwrap();
    let authorization = connection(Some(grant));
    let resource = ResourceScope::parse(AUDIENCE).unwrap();
    let now = Cell::new(u64::MAX - 1);
    let mut wait = pin!(authorization.wait_until_unauthorized_with_clock(
        &resource,
        &[Permission::Send],
        || now.get(),
    ));
    assert!(poll_once(wait.as_mut()).is_pending());
    now.set(u64::MAX);
    assert!(
        authorization
            .authorize_resource_any_at(&resource, &[Permission::Send], now.get())
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        poll_once(wait.as_mut()).is_ready(),
        "maximum expiry is exclusive, not an infinite wait"
    );
}

// Pre-signed with the public jsonwebtoken v11.1.0 RSA test fixture already used
// in auth::jwt::tests. No signing API or unchecked grant constructor is exposed;
// every fixture above passes JwtPolicy::validate before reaching the consumer.
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const FIRST: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoic3dpdGNoeWFyZCtqd3QifQ.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlLyIsInN1YiI6InJ1bGUiLCJhdWQiOiJ1cm46c3dpdGNoeWFyZDp0ZW5hbnQiLCJpYXQiOjEwMCwibmJmIjoxMjAsImV4cCI6MjAwfQ.jjtwkx43JX3H0TMcQ821YgHsDlwEKKHvIa99bmQbJHA8sRAIymNdYGYJmnrNFaX5WrZ_BOluw-ova6m03BsFd4YKZY0nMhgKZLjVh65zsepOw7U-e0FDH2nHjDVTml9F7I5BBTtDSLF3gltj8yIR4vBavi7NveuC7rL0Ot6cUUJQjkM7S0USdBhx3UzMalEIO1W4lK5OCav_LEPlv0lo9UTn5WFSwYFQ-S387IdgbMK0lZp15WyDFv0XHMbFLsrOeixG5BgS8_-2BlYTs_pF5xRI-c3reJwKvyt8YRoLnYK0BCFhTwqrhZKJRtU43-hnLql5PISHc0nDKOAvih56Pw";
const REFRESH: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoic3dpdGNoeWFyZCtqd3QifQ.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlLyIsInN1YiI6InJ1bGUiLCJhdWQiOiJ1cm46c3dpdGNoeWFyZDp0ZW5hbnQiLCJpYXQiOjE0MCwibmJmIjoxNTAsImV4cCI6MTgwfQ.eJljODpZJSPwyNx8VanuqTn4jN5H5fZBCrW4o8t_wEVsUfVXf53VLpkhMmP3afN1SKm9jxGvwMQXDWXd2BYl1Ec-Bm8RoT6OvQFSQZhNwafMXMzFxjdsKM_AhaQ4KuK4QKMT7UtCAICFSV_eKiS-4xjF8okHEjvDl5hBhCg0oiF9RVJKAjzbl5jJhimwhsPHYvECq-fqvQuEf1rvdTL1F0VrywnFxpO-OL1_gtl_KCX8V0U6qf_SMjUN8xSeI-C616M87GlsQpVlqQjI9NrLqAOPC_7p2S7-_VEOY80Ew3_1T8sF9KT7SqCRUe_bEZTerWv_LRix7-GImegc7ApleQ";
const OTHER_ISSUER: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoic3dpdGNoeWFyZCtqd3QifQ.eyJpc3MiOiJodHRwczovL290aGVyLWlzc3Vlci5leGFtcGxlLyIsInN1YiI6InJ1bGUiLCJhdWQiOiJ1cm46c3dpdGNoeWFyZDp0ZW5hbnQiLCJpYXQiOjEwMCwibmJmIjoxMjAsImV4cCI6MjUwfQ.vvb-FP90aRr_8s7sdrF2tJmUS3iqgYNZud4ZtSqPj3tJG9Z0FTIIvQkhYu1zQzbIgcLDVh_ospPDnCmKLuWIwY8-t3NPETrLSeafxEORv09I7L4pYXwGuqKa9t3FjZf1o3sS2nQ-wDQlEiWHW_yv1nUxfLgn86LnGmNMKNPcTzMcSH7QpkykxqYD3CklLZz7XKfyeKWEFOBLQ3OQlzIoxMHY6VHh3oz5LnU5AhVShqWFjdSPujJlAy4QrXCClx2UiwQTjCc6f6ouAT74_VMNTjgN8AoXDMVa2VBtqylQW41ahO_zkaKX7HPLDqQF8RZAWL8SaqdkX0bEh2B3gK7RDA";
const LATER: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtleS0xIiwidHlwIjoic3dpdGNoeWFyZCtqd3QifQ.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlLyIsInN1YiI6InJ1bGUiLCJhdWQiOiJ1cm46c3dpdGNoeWFyZDp0ZW5hbnQiLCJpYXQiOjE2MCwibmJmIjoxNzAsImV4cCI6MjQwfQ.rTCwz4Eja6WeKwBHKcxOs-5Gjw1vK8n16HvaP1SDT9d3wPsHEH2JZFVDM--vpsWonQi6HYYxrzi63fOIM8quFHckpn8yesXdmhn28Qe6kSOAr1ojfJ5AN9d_d_rKNNJcNxByn2pvqmVtIfuQ72HhF0DOqi7xTAXFAp6QOSf10TyuUAtzNnPepHJTU2TYWVTAhGKOZjdpgSr5ZPXqJvf9yOjIZKFbINFAjbIrw9kB53EC3G4L7m9N03EkougSnTwH79V6_Y5zYIaVlkV8Jqd6IcI6I0I2nQEUPueGgxR8C_XJaCZQ4lLO8stPQkvojbCrAHXQlypOgkdmW5bgzlthDg";
