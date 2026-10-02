use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use auth::{
    Permission, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};

use super::super::{ConnectionAuthorization, SharedAccessAuthentication};
use super::*;

const HOST: &str = "tenant.servicebus.windows.net";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";
const TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=R8KtgcCb7NeOCrECrMXtQ13KLGC8CiJYw0fUnUQCznw%3D&se=2000000000&skn=send";
const EXPIRED_TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=u5V5z4Naqt4QutP4QaTK8PVuLE1WtVjCymBxbG5L62k%3D&se=1&skn=send";

fn policy() -> SharedAccessPolicy {
    SharedAccessPolicy::new([SharedAccessRule::new(
        "send",
        ResourceScope::namespace(HOST).expect("namespace"),
        SharedAccessKey::new("secret").expect("key"),
        None,
        PermissionSet::SEND,
    )
    .expect("rule")])
    .expect("policy")
}

fn connection() -> Arc<ConnectionAuthorization> {
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy(), HOST).expect("authentication"),
        None,
    )
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3600)
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn disabled_history_stays_disabled_until_explicit_installation() {
    let now = Instant::now();
    let mut grace = InitialControlGrace::new(false);
    assert_eq!(grace.state(now), InitialControlState::Disabled);
    grace.authorized(now);
    assert_eq!(grace.state(now), InitialControlState::Disabled);
    grace.enable(now, now);
    assert_eq!(grace.state(now), InitialControlState::Authorized);
}

#[test]
fn installation_is_once_and_deadline_equality_is_terminal() {
    let now = Instant::now();
    let first = now + Duration::from_secs(20);
    let mut grace = InitialControlGrace::new(false);
    grace.enable(first, now);
    grace.enable(first + Duration::from_secs(100), now);
    assert_eq!(
        grace.state(now),
        InitialControlState::Initial { deadline: first }
    );
    assert_eq!(grace.state(first), InitialControlState::InitialExpired);
    grace.authorized(first);
    grace.enable(first + Duration::from_secs(100), first);
    assert_eq!(grace.state(first), InitialControlState::InitialExpired);
}

#[test]
fn grant_at_deadline_cannot_revive_grace_without_a_prior_expiry_poll() {
    let now = Instant::now();
    let first = now + Duration::from_secs(20);
    let mut grace = InitialControlGrace::new(false);
    grace.enable(first, now);
    grace.authorized(first);
    assert_eq!(grace.state(first), InitialControlState::InitialExpired);
}

#[test]
fn prior_authorization_and_first_timely_publication_are_sticky() {
    let now = Instant::now();
    let first = now + Duration::from_secs(20);
    for prior in [false, true] {
        let mut grace = InitialControlGrace::new(prior);
        grace.enable(first, now);
        if !prior {
            grace.authorized(first - Duration::from_nanos(1));
        }
        assert_eq!(grace.state(first), InitialControlState::Authorized);
        grace.enable(first + Duration::from_secs(100), first);
        assert_eq!(
            grace.state(first + Duration::from_secs(1000)),
            InitialControlState::Authorized
        );
    }
}

#[tokio::test]
async fn explicit_initial_metadata_does_not_grant_queue_or_commit_authority() {
    let connection = connection();
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Disabled
    );
    assert!(!connection.can_control_metadata().await);
    let first = deadline();
    connection.enable_initial_control_grace(first).await;
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Initial { deadline: first }
    );
    assert!(connection.can_control_metadata().await);
    assert!(!connection.has_valid_grant().await);
    assert!(!connection.can_control_commit().await);
    assert!(
        connection
            .authorize_entity("orders", Permission::Send)
            .await
            .is_err()
    );
    assert!(
        connection
            .authorize_entity("orders", Permission::Listen)
            .await
            .is_err()
    );
    assert!(
        connection
            .any_grant_claim_expiry_epoch_seconds()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn publication_updates_history_before_any_watcher_or_driver_poll() {
    let connection = connection();
    connection.enable_initial_control_grace(deadline()).await;
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("valid grant");
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Authorized
    );
    assert!(connection.has_valid_grant().await);
    assert!(connection.can_control_metadata().await);
    assert!(connection.can_control_commit().await);
    assert!(
        connection
            .authorize_entity("orders", Permission::Send)
            .await
            .is_ok()
    );
    assert!(
        connection
            .authorize_entity("Orders", Permission::Send)
            .await
            .is_err()
    );
    assert!(
        connection
            .authorize_entity("orders", Permission::Listen)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn publication_before_installation_never_opens_initial_grace() {
    let connection = connection();
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("valid grant");
    connection.grants.write().await.clear();
    connection.enable_initial_control_grace(deadline()).await;
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Authorized
    );
    assert!(!connection.can_control_metadata().await);
}

#[tokio::test]
async fn initial_authenticated_history_survives_an_expired_or_removed_grant() {
    for grant in [
        policy()
            .authenticate_sas(EXPIRED_TOKEN, 0)
            .expect("previously authenticated finite grant"),
        policy()
            .authenticate_plain("send", "secret")
            .expect("PLAIN grant"),
    ] {
        let connection = ConnectionAuthorization::new(
            SharedAccessAuthentication::new(policy(), HOST).expect("authentication"),
            Some(grant),
        );
        connection
            .grants
            .write()
            .await
            .retain(|grant| grant.expires_at_epoch_seconds() != u64::MAX);
        assert!(!connection.has_valid_grant().await);
        connection.enable_initial_control_grace(deadline()).await;
        assert_eq!(
            connection.initial_control_state().await,
            InitialControlState::Authorized
        );
        assert!(!connection.can_control_metadata().await);
    }
}

#[tokio::test]
async fn invalid_tokens_and_repeated_installation_do_not_reset_deadline() {
    let connection = connection();
    let first = deadline();
    connection.enable_initial_control_grace(first).await;
    assert!(
        connection
            .validate_and_add(&TOKEN.replace("sig=", "sig=invalid"), AUDIENCE)
            .await
            .is_err()
    );
    connection
        .enable_initial_control_grace(first + Duration::from_secs(100))
        .await;
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Initial { deadline: first }
    );
    assert!(connection.grants.read().await.is_empty());
    connection.initial_control.lock().await.state(first);
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("cryptographically valid late grant");
    assert!(connection.has_valid_grant().await);
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::InitialExpired
    );
    assert!(!connection.can_control_metadata().await);
    assert!(!connection.can_control_commit().await);
}

#[tokio::test]
async fn already_elapsed_installation_refuses_metadata_and_finishes_watchdog() {
    let connection = connection();
    connection
        .enable_initial_control_grace(Instant::now())
        .await;
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::InitialExpired
    );
    assert!(!connection.can_control_metadata().await);
    tokio::time::timeout(
        Duration::from_secs(1),
        connection.wait_until_control_unauthorized(),
    )
    .await
    .expect("elapsed grace cannot wait for a token");
}

#[tokio::test]
async fn notification_watcher_survives_first_grant_then_observes_last_grant_loss() {
    let connection = connection();
    connection.enable_initial_control_grace(deadline()).await;
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(connection.wait_until_control_unauthorized());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("first valid grant");
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    connection.grants.write().await.clear();
    connection.grant_changed.notify_waiters();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("last-grant loss wakes controller");
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Authorized
    );
    assert!(!connection.can_control_metadata().await);
    connection.enable_initial_control_grace(deadline()).await;
    assert!(!connection.can_control_metadata().await);
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("fresh valid grant");
    assert!(connection.can_control_metadata().await);
    assert!(connection.can_control_commit().await);
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Authorized
    );
}

#[tokio::test]
async fn canceling_a_grace_watch_has_no_authorization_or_reset_effect() {
    let connection = connection();
    let first = deadline();
    connection.enable_initial_control_grace(first).await;
    let mut waiting = Box::pin(connection.wait_until_control_unauthorized());
    let mut context = Context::from_waker(Waker::noop());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    drop(waiting);
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Initial { deadline: first }
    );
    assert!(!connection.has_valid_grant().await);
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("later publication");
    connection.grants.write().await.clear();
    tokio::time::timeout(
        Duration::from_secs(1),
        connection.wait_until_control_unauthorized(),
    )
    .await
    .expect("a fresh watcher immediately observes unauthorized sticky history");
}

#[tokio::test]
async fn canceled_publication_cannot_expose_a_grant_without_its_history() {
    let connection = connection();
    let first = deadline();
    connection.enable_initial_control_grace(first).await;
    let initial_control = connection.initial_control.lock().await;
    let mut publishing = Box::pin(connection.validate_and_add(TOKEN, AUDIENCE));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    assert!(connection.grants.try_read().is_err());
    drop(publishing);
    assert!(connection.grants.read().await.is_empty());
    drop(initial_control);
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Initial { deadline: first }
    );
    connection
        .validate_and_add(TOKEN, AUDIENCE)
        .await
        .expect("healthy retry");
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Authorized
    );
}

#[tokio::test]
async fn disabled_watchdog_preserves_strict_no_grant_refusal() {
    let connection = connection();
    tokio::time::timeout(
        Duration::from_secs(1),
        connection.wait_until_control_unauthorized(),
    )
    .await
    .expect("no initial policy was installed");
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::Disabled
    );
    assert!(!connection.can_control_metadata().await);
}

#[tokio::test]
async fn blocked_commit_check_cannot_use_publication_after_initial_expiry() {
    let connection = connection();
    let first = deadline();
    connection.enable_initial_control_grace(first).await;
    let mut state = connection.initial_control.lock().await;
    let mut publishing = Box::pin(connection.validate_and_add(TOKEN, AUDIENCE));
    let mut context = Context::from_waker(Waker::noop());
    assert!(publishing.as_mut().poll(&mut context).is_pending());
    let mut checking = Box::pin(connection.can_control_commit());
    assert!(checking.as_mut().poll(&mut context).is_pending());

    assert_eq!(state.state(first), InitialControlState::InitialExpired);
    drop(state);
    publishing.await.expect("actual late grant publication");
    assert!(connection.has_valid_grant().await);
    assert!(!checking.await);
    assert_eq!(
        connection.initial_control_state().await,
        InitialControlState::InitialExpired
    );
}
