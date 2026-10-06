//! Bounded binary preflight probes for the optional offline JWT policy loader.

use std::{
    error::Error,
    ffi::OsString,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use tempfile::TempDir;

use super::atomic_messaging::process;

type TestResult = Result<(), Box<dyn Error>>;

// Public RSA geometry fixture; no operational credential is present.
const POLICY: &str = r#"{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant","keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ","e":"AQAB"}],"bindings":[{"subject":"producer","scope":"amqps://tenant.servicebus.windows.net/orders","permissions":["send"]}]}"#;

struct Files {
    _directory: TempDir,
    certificate: PathBuf,
    private_key: PathBuf,
    shared_key: PathBuf,
    policy: PathBuf,
    store: PathBuf,
}

impl Files {
    fn new() -> Self {
        let directory = TempDir::new().expect("binary credential fixture");
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec![String::from("localhost")])
                .expect("disposable TLS identity");
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        let shared_key = directory.path().join("shared-key");
        let policy = directory.path().join("offline-policy.json");
        let store = directory.path().join("uncreated-parent/store");
        fs::write(&certificate, cert.pem()).expect("fixture certificate");
        fs::write(&private_key, key_pair.serialize_pem()).expect("fixture private key");
        fs::write(&shared_key, "binary-fixture-key\n").expect("fixture shared key");
        fs::write(&policy, POLICY).expect("fixture public policy");
        Self {
            _directory: directory,
            certificate,
            private_key,
            shared_key,
            policy,
            store,
        }
    }

    fn arguments(&self, policy: &Path) -> Vec<OsString> {
        let mut arguments: Vec<OsString> = vec![
            "--namespace".into(),
            "tenant".into(),
            "--storage".into(),
            "fjall".into(),
        ];
        for (flag, value) in [
            ("--data-dir", self.store.as_path()),
            ("--tls-certificate", self.certificate.as_path()),
            ("--tls-private-key", self.private_key.as_path()),
            ("--shared-access-key-file", self.shared_key.as_path()),
            ("--offline-jwt-policy-file", policy),
        ] {
            arguments.push(flag.into());
            arguments.push(value.as_os_str().to_owned());
        }
        arguments.extend(["--shared-access-key-name".into(), "fixture-rule".into()]);
        arguments
    }

    fn unopened_storage(&self) {
        assert!(!self.store.exists());
        assert!(!self.store.parent().expect("unopened parent").exists());
    }
}

fn assert_refused(output: &process::Output, description: &str) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty(), "preflight refusal wrote stdout");
    assert!(
        output.stderr == format!("switchyard: {description}\n"),
        "preflight refusal was not the exact static error"
    );
    for sentinel in [
        "binary-fixture-key",
        "sentinel-secret-policy",
        "sentinel-private-path",
        "MALFORMED_SENTINEL",
    ] {
        assert!(
            !output.stderr.contains(sentinel),
            "preflight refusal exposed fixture data"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_offline_jwt_check_config_never_opens_storage_or_binds_listeners() -> TestResult {
    let files = Files::new();
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let mut arguments = files.arguments(&files.policy);
    for flag in [
        "--listen",
        "--websocket-listen",
        "--admin-listen",
        "--experimental-atomic-messaging-listen",
    ] {
        arguments.push(flag.into());
        arguments.push(address.clone().into());
    }
    let output = process::run_switchyard_check_config(&arguments).await?;
    assert!(
        output.status.success(),
        "valid binary preflight was refused"
    );
    assert!(output.stdout.is_empty(), "binary preflight emitted stdout");
    assert!(output.stderr.is_empty(), "binary preflight emitted stderr");
    files.unopened_storage();
    assert_eq!(occupied.local_addr()?.to_string(), address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_offline_jwt_fifo_and_symlink_are_bounded_regular_file_refusals() -> TestResult {
    use rustix::fs::{CWD, Mode, mkfifoat};
    use std::os::unix::fs::symlink;

    let files = Files::new();
    let fifo = files._directory.path().join("sentinel-private-path-fifo");
    let link = files._directory.path().join("sentinel-private-path-link");
    mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR)?;
    symlink(&fifo, &link)?;
    for path in [&fifo, &link] {
        let output = process::run_switchyard_check_config(&files.arguments(path)).await?;
        assert_refused(&output, "the offline JWT policy must be a regular file");
        files.unopened_storage();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_offline_jwt_prerequisites_and_errors_remain_static_before_policy_io() -> TestResult
{
    let files = Files::new();
    let missing = files
        ._directory
        .path()
        .join("sentinel-private-path-missing-policy");
    let plaintext = vec![
        OsString::from("--offline-jwt-policy-file"),
        missing.as_os_str().to_owned(),
        OsString::from("--storage"),
        OsString::from("fjall"),
        OsString::from("--data-dir"),
        files.store.as_os_str().to_owned(),
    ];
    let output = process::run_switchyard_check_config(&plaintext).await?;
    assert_refused(&output, "offline JWT authentication requires TLS");
    files.unopened_storage();

    let mut no_authentication = plaintext.clone();
    for (flag, path) in [
        ("--tls-certificate", &files.certificate),
        ("--tls-private-key", &files.private_key),
    ] {
        no_authentication.push(flag.into());
        no_authentication.push(path.as_os_str().to_owned());
    }
    let output = process::run_switchyard_check_config(&no_authentication).await?;
    assert_refused(
        &output,
        "offline JWT authentication requires configured shared-access authentication",
    );
    files.unopened_storage();
    assert!(!missing.exists());

    fs::write(&files.policy, [0xff, 0x00])?;
    let output = process::run_switchyard_check_config(&files.arguments(&files.policy)).await?;
    assert_refused(
        &output,
        "the offline JWT policy file must contain UTF-8 JSON",
    );
    files.unopened_storage();
    Ok(())
}
