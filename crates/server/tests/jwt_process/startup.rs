use super::*;

#[test]
fn amqp_jwt_startup_refuses_before_data_creation_or_mutation_and_keeps_guard_priority() -> Result {
    let (root, _) = fixture()?;
    let state = root.path().join("state");
    let policy_path = root.path().join("amqp-policy.json");
    let tls: Vec<OsString> = vec![
        "--tls-certificate".into(),
        root.path().join("cert.pem").into(),
        "--tls-private-key".into(),
        root.path().join("key.pem").into(),
    ];
    let jwt: Vec<OsString> = vec!["--jwt-policy-file".into(), policy_path.clone().into()];
    let run = |extra: &[OsString]| {
        bounded_output(
            Command::new(env!("CARGO_BIN_EXE_switchyard"))
                .args(["--namespace", "tenant", "--storage", "fjall", "--data-dir"])
                .arg(&state)
                .args(extra),
        )
    };
    let check = |args: &[OsString], expected: &str| -> Result {
        let output = run(args)?;
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr)?;
        assert!(stderr.contains(expected), "{stderr}");
        assert!(!stderr.contains("PRIVATE-POLICY-MARKER"));
        assert!(!stderr.contains(policy_path.to_str().unwrap()));
        Ok(())
    };
    for existing in [false, true] {
        if existing {
            fs::create_dir(&state)?;
            fs::write(state.join("sentinel"), b"untouched")?;
        }
        // TLS is checked before reading even a missing policy, then incomplete SAS.
        check(&jwt, "requires a TLS listener")?;
        let mut plaintext_wss = jwt.clone();
        plaintext_wss.extend(["--transport".into(), "amqp-websockets".into()]);
        check(
            &plaintext_wss,
            "AMQP over WebSockets requires a TLS certificate and private key",
        )?;
        let mut partial = vec![
            "--tls-certificate".into(),
            root.path().join("cert.pem").into(),
        ];
        partial.extend(jwt.clone());
        check(&partial, "TLS configuration requires both")?;
        let mut args = tls.clone();
        args.extend(jwt.clone());
        fs::write(&policy_path, "PRIVATE-POLICY-MARKER invalid JSON")?;
        let mut incomplete = args.clone();
        incomplete.extend(["--shared-access-key-name".into(), "sas".into()]);
        check(
            &incomplete,
            "requires both --shared-access-key-name and --shared-access-key-file",
        )?;
        check(&args, "policy file is invalid")?;
        let mut oversized = amqp_policy().to_string().into_bytes();
        oversized.resize(64 * 1024 + 1, b' ');
        fs::write(&policy_path, oversized)?;
        check(&args, "policy file is invalid")?;
        fs::write(&policy_path, [0xff])?;
        check(&args, "could not read policy file")?;
        // A syntactically valid but incomplete policy must not get as far as storage.
        fs::write(&policy_path, "{\"version\":1}")?;
        check(&args, "policy file is invalid")?;
        let mut missing = tls.clone();
        missing.extend([
            "--jwt-policy-file".into(),
            root.path().join("PRIVATE-POLICY-MARKER-missing").into(),
        ]);
        check(&missing, "could not read policy file")?;
        let production: Vec<OsString> = ["--mode", "production", "--voters", "3"]
            .into_iter()
            .map(Into::into)
            .collect();
        check(
            &production,
            "production mode requires a TLS certificate and private key",
        )?;
        let mut unauthenticated = production.clone();
        unauthenticated.extend(tls.clone());
        check(
            &unauthenticated,
            "production mode requires a shared-access policy",
        )?;
        fs::write(&policy_path, amqp_policy().to_string())?;
        let mut guarded = production;
        guarded.extend(args);
        check(&guarded, "requires quorum replication")?;
        if existing {
            assert_eq!(fs::read(state.join("sentinel"))?, b"untouched");
            assert_eq!(fs::read_dir(&state)?.count(), 1);
        } else {
            assert!(!state.exists());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_jwt_listener_retains_the_original_shared_shutdown_budget() -> Result {
    let (root, cert) = fixture()?;
    fs::write(
        root.path().join("amqp-policy.json"),
        amqp_policy().to_string(),
    )?;
    let mut args = arguments(root.path(), true, false);
    args.extend(["--shutdown-timeout-secs".into(), "5".into()]);
    let args: Vec<_> = args.iter().map(|s| s.to_str().unwrap()).collect();
    let (mut process, rpc) = Process::start(
        root.path(),
        &cert,
        &args,
        &[("SWITCHYARD_TEST_STALL_TIMER_MILLIS", "20000")],
    )
    .await?;
    let mut connection = connect(process.amqp, &cert, true).await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put(&mut session, "jwt", ROOT, token("ns-manage")).await?,
        202
    );
    process.signal(Signal::TERM)?;
    let deadline = Instant::now() + WAIT;
    loop {
        if fs::read_to_string(root.path().join("process.log"))?.contains("stage=protocol-cleanup") {
            break;
        }
        if Instant::now() >= deadline {
            return Err("original protocol cleanup did not finish".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(TcpStream::connect(process.amqp).await.is_err());
    assert!(TcpStream::connect(process.address).await.is_err());
    process.signal(Signal::INT)?;
    assert_eq!(
        process.wait().await?,
        i32::from(server::SHUTDOWN_EXPIRED_EXIT_CODE)
    );
    let log = fs::read_to_string(root.path().join("process.log"))?;
    assert!(log.contains("shutdown already in progress"), "{log}");
    assert!(
        log.contains("budget expired during the timer stage"),
        "{log}"
    );
    assert_eq!(log.matches("stage=listener-admission").count(), 1);
    assert_eq!(log.matches("stage=protocol-cleanup").count(), 1);
    assert!(!log.contains("stage=broker"));
    // Expiry is failure evidence, never permission to reopen this store.
    drop((session, connection, rpc));
    Ok(())
}
