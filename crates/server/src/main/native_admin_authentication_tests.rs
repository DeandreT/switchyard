use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetClockReadinessRequest, MaintenanceClockState,
    entity_service_server::EntityService, maintenance_service_client::MaintenanceServiceClient,
    maintenance_service_server::MaintenanceService,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{NamespaceName, StateMachine};
use futures_util::FutureExt;
use std::{
    error::Error,
    panic::{AssertUnwindSafe, resume_unwind},
};
use storage::{MemoryStore, StateStore};
use tonic::{Code, Request, transport::Endpoint};

use super::*;

type TestResult = Result<(), Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const POLICY: &str = r#"{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:native-admin","keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ","e":"AQAB"}],"bindings":[{"subject":"administrator","scope":"amqps://tenant.servicebus.windows.net","permissions":["manage"]}]}"#;

fn sas_authentication() -> SharedAccessAuthentication {
    SharedAccessAuthentication::new(
        SharedAccessPolicy::new([SharedAccessRule::new(
            "fixture-rule",
            ResourceScope::namespace(HOST).expect("namespace"),
            SharedAccessKey::new("fixture-key").expect("fixture key"),
            None,
            PermissionSet::MANAGE,
        )
        .expect("rule")])
        .expect("policy"),
        HOST,
    )
    .expect("authentication")
}

fn create() -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: "orders".into(),
        kind: EntityKind::Queue as i32,
        ..Default::default()
    }
}

fn bearer<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", "Bearer fixture-token".parse().unwrap());
    request
}

#[tokio::test]
async fn native_setup_preserves_default_and_sas_only_authorization() -> TestResult {
    let store = MemoryStore::default();
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        server::ManualClock::at(1_000),
    ));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
        let namespace = NamespaceName::new("tenant")?;
        native_admin_service(broker.handle(), namespace.clone(), None, false)?
            .create_entity(Request::new(create()))
            .await?;
        let before = store.snapshot()?;
        let authentication = sas_authentication();
        assert!(authentication.offline_jwt_policy().is_none());
        let service =
            native_admin_service(broker.handle(), namespace, Some(&authentication), false)?;
        let refused = service.create_entity(bearer(create())).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated);
        assert_eq!(refused.message(), "invalid or expired shared-access token");
        assert_eq!(store.snapshot()?, before);
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    drop(broker);
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_reuses_the_loaded_policy_after_its_file_is_removed() -> TestResult {
    let directory = tempfile::TempDir::new()?;
    let path = directory.path().join("policy.json");
    fs::write(&path, POLICY)?;
    let authentication = load_offline_jwt_policy(Some(&path), true, Some(sas_authentication()))?
        .expect("configured authentication");
    fs::remove_file(&path)?;
    let store = MemoryStore::default();
    let before = store.snapshot()?;
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        server::ManualClock::at(1_000),
    ));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
        assert!(authentication.offline_jwt_policy().is_some());
        let service = native_admin_service(
            broker.handle(),
            NamespaceName::new("tenant")?,
            Some(&authentication),
            false,
        )?;
        let refused = service.create_entity(bearer(create())).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated);
        assert_eq!(refused.message(), "offline JWT requires a TLS transport");
        assert_eq!(store.snapshot()?, before);
        assert!(!path.exists());
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    drop(broker);
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_retains_readiness_opt_in_and_authentication_ordering() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        server::ManualClock::at(1_000),
    ));
    let observed = AssertUnwindSafe(async {
        let namespace = NamespaceName::new("tenant")?;
        let request = || {
            Request::new(GetClockReadinessRequest {
                namespace: "tenant".into(),
            })
        };
        for enabled in [false, true] {
            let service = native_admin_service(broker.handle(), namespace.clone(), None, enabled)?;
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = format!("http://{}", socket.local_addr()?);
            let listener = tokio::spawn(NativeAdminListener::new(service).serve(socket));
            let route = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let channel = Endpoint::from_shared(endpoint)?
                    .connect_timeout(Duration::from_secs(2))
                    .timeout(Duration::from_secs(2))
                    .connect()
                    .await?;
                let response = MaintenanceServiceClient::new(channel)
                    .get_clock_readiness(request())
                    .await;
                if enabled {
                    assert!(MaintenanceClockState::try_from(response?.into_inner().state).is_ok());
                } else {
                    assert_eq!(response.unwrap_err().code(), Code::Unimplemented);
                }
                Ok::<(), Box<dyn Error>>(())
            }))
            .catch_unwind()
            .await;
            listener.abort();
            let joined = listener.await;
            match route {
                Err(payload) => resume_unwind(payload),
                Ok(result) => result??,
            }
            match joined {
                Err(error) if error.is_cancelled() => (),
                Err(error) => return Err(error.into()),
                Ok(result) => result?,
            }
        }
        let authentication =
            sas_authentication().with_offline_jwt_policy(auth::JwtPolicy::from_json(POLICY)?);
        for enabled in [false, true] {
            let service = native_admin_service(
                broker.handle(),
                namespace.clone(),
                Some(&authentication),
                enabled,
            )?;
            let refused = tokio::time::timeout(
                Duration::from_secs(5),
                service.get_clock_readiness(request()),
            )
            .await?
            .unwrap_err();
            assert_eq!(refused.code(), Code::Unauthenticated);
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    drop(broker);
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result?,
    }
    Ok(())
}
