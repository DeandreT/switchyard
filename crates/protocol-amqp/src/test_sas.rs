//! Short-lived SAS grants for tests that need a real positive expiry.

use std::{
    future::Future,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{authorization::ConnectionAuthorization, test_deadline::WAIT};

const ENCODED_AUDIENCE: &str = "amqps%3A%2F%2Ftenant.servicebus.windows.net";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net";

/// Replaces the `key_name` grant with a SAS token that expires about two
/// seconds from now, and returns once `still_authorized` confirms it.
///
/// Authorization compares whole wall-clock seconds, so a scheduling delay can
/// let the token expire before the caller checks it. A token is only retried
/// when the wall clock has reached its expiry; any other refusal fails the test.
pub(crate) async fn add_short_lived_grant<F, Checked>(
    connection: &ConnectionAuthorization,
    key_name: &str,
    still_authorized: F,
) where
    F: Fn() -> Checked,
    Checked: Future<Output = bool>,
{
    let deadline = Instant::now() + WAIT;
    loop {
        let expiry = epoch_seconds() + 2;
        let added = connection
            .validate_and_add(&token(key_name, expiry), AUDIENCE)
            .await;
        if added.is_ok() && still_authorized().await {
            return;
        }
        assert!(
            epoch_seconds() >= expiry,
            "fresh SAS token refused before its expiry: {added:?}"
        );
        assert!(
            Instant::now() < deadline,
            "no short-lived SAS token stayed valid long enough to check"
        );
    }
}

fn token(key_name: &str, expiry: u64) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{ENCODED_AUDIENCE}\n{expiry}").as_bytes());
    let signature = STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D");
    format!(
        "SharedAccessSignature sr={ENCODED_AUDIENCE}&sig={signature}&se={expiry}&skn={key_name}"
    )
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
