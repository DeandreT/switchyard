use super::*;

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use url::form_urlencoded::byte_serialize;

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "cold-first-private-key";
pub(super) const SEND: &str = "cold-send";
pub(super) const LISTEN_OTHER: &str = "cold-listen-other";
pub(super) const WRONG_CASE_SEND: &str = "cold-send-wrong-case";

pub(super) struct Security {
    tls: rustls::ServerConfig,
    authentication: protocol_amqp::SharedAccessAuthentication,
    pub(super) certificate: rustls::pki_types::CertificateDer<'static>,
}

impl Security {
    pub(super) fn new(authorization_timeout: Duration) -> TestResult<Self> {
        let key = auth::SharedAccessKey::new(KEY)?;
        let rules = [
            (SEND, "orders", auth::PermissionSet::SEND),
            (LISTEN_OTHER, "other", auth::PermissionSet::LISTEN),
            (WRONG_CASE_SEND, "Orders", auth::PermissionSet::SEND),
        ]
        .into_iter()
        .map(
            |(name, entity, permissions)| -> TestResult<auth::SharedAccessRule> {
                Ok(auth::SharedAccessRule::new(
                    name,
                    auth::ResourceScope::entity(HOST, entity)?,
                    key.clone(),
                    None,
                    permissions,
                )?)
            },
        )
        .collect::<TestResult<Vec<_>>>()?;
        let authentication = protocol_amqp::SharedAccessAuthentication::new(
            auth::SharedAccessPolicy::new(rules)?,
            HOST,
        )?
        .with_authorization_timeout(authorization_timeout);
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        Ok(Self {
            tls,
            authentication,
            certificate: cert.der().clone(),
        })
    }

    pub(super) fn server(
        &self,
    ) -> (
        rustls::ServerConfig,
        protocol_amqp::SharedAccessAuthentication,
    ) {
        (self.tls.clone(), self.authentication.clone())
    }
}

pub(super) fn sas_token(audience: &str, rule: &str) -> TestResult<String> {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .checked_add(600)
        .ok_or("SAS expiry overflow")?;
    let resource: String = byte_serialize(audience.as_bytes()).collect();
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    hmac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let signature: String = byte_serialize(signature.as_bytes()).collect();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}"
    ))
}
