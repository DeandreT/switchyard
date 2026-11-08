//! Test-only public RSA fixture shared with the auth and entity-handler tests.
//! No signing interface or runtime key provisioning is exposed by the server.
use admin_api::wire::{self, entity_service_client::EntityServiceClient};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair},
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::{
    Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

pub const ROOT: &str = "amqps://tenant.servicebus.windows.net";
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const PRIVATE_DER: &str = concat!(
    "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL",
    "UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V",
    "rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H",
    "oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI",
    "Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/",
    "by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd",
    "WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ",
    "Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j",
    "E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn",
    "LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS",
    "bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE",
    "8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl",
    "xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY",
    "2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1",
    "9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0",
    "7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ",
    "mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7",
    "MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4",
    "t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam",
    "QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA",
    "2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ",
    "4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn",
    "PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH",
    "UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8",
    "oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==",
);

pub fn policy() -> Value {
    json!({"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant",
    "keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":MODULUS,"e":"AQAB"}],
    "bindings":[
        {"subject":"operator","scope":ROOT,"permissions":["manage"]},
        {"subject":"sender","scope":ROOT,"permissions":["send"]},
        {"subject":"entity","scope":format!("{ROOT}/orders"),"permissions":["manage"]},
        {"subject":"foreign","scope":"amqps://other.servicebus.windows.net","permissions":["manage"]}
    ]})
}

pub fn claims(subject: &str) -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    json!({"iss":"https://issuer.example/","sub":subject,"aud":"urn:switchyard:tenant","iat":now-1,"exp":now+600})
}

pub fn sign(claims: Value) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","kid":"key-1","typ":"switchyard+jwt"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let key = RsaKeyPair::from_der(&STANDARD.decode(PRIVATE_DER).unwrap()).unwrap();
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .unwrap();
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
}

pub fn request<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

pub fn list() -> wire::ListEntitiesRequest {
    wire::ListEntitiesRequest {
        namespace: "tenant".into(),
        page_size: 100,
        ..Default::default()
    }
}

pub async fn client(
    address: SocketAddr,
    certificate: &str,
    hostname: &str,
) -> Result<EntityServiceClient<Channel>, tonic::transport::Error> {
    let channel = Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(120))
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(certificate))
                .domain_name(hostname),
        )?
        .connect()
        .await?;
    Ok(EntityServiceClient::new(channel))
}
