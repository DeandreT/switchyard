use super::*;

pub(super) async fn server_statuses() -> TestResult {
    let node = Node::start(true).await?;
    node.topology().await?;
    node.token(CHILD, "manage")?;
    let filter = node.filter("filter.json", &json!({"type":"true"}))?;
    let before = node.store.snapshot()?;
    let applied = node.broker.handle().last_applied_blocking()?;
    for (name, value, code) in [
        (
            "Syntax",
            json!({"type":"sql","expression":"REMOVE"}),
            "InvalidArgument",
        ),
        (
            "Unsupported",
            json!({"type":"sql","expression":"SET sys.Subject = 'secret'"}),
            "Unimplemented",
        ),
        (
            "Future",
            json!({"type":"sql","expression":"REMOVE","semantic_version":3}),
            "Unimplemented",
        ),
        (
            "Zero",
            json!({"type":"sql","expression":"REMOVE","semantic_version":0}),
            "Unimplemented",
        ),
        (
            "SourceLimit",
            json!({"type":"sql","expression":"x".repeat(4097)}),
            "ResourceExhausted",
        ),
    ] {
        let action = node.filter(&format!("{name}-action.json"), &value)?;
        let stderr = failed(
            &node
                .run(&[
                    "rule",
                    "create",
                    "Orders",
                    "Alpha",
                    name,
                    "--filter-file",
                    &filter,
                    "--action-file",
                    &action,
                ])
                .await?,
        );
        assert!(
            stderr.contains(&format!("administration request failed ({code})")),
            "unexpected status: {stderr}"
        );
        assert!(!stderr.contains("secret"));
        assert!(!stderr.contains("REMOVE"));
        assert!(!stderr.contains("SET"));
        assert_eq!(node.store.snapshot()?, before);
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    }
    let action = node.filter(
        "healthy-action.json",
        &json!({"type":"sql","expression":"REMOVE [private]"}),
    )?;
    node.json(&[
        "rule",
        "create",
        "Orders",
        "Alpha",
        "Healthy",
        "--filter-file",
        &filter,
        "--action-file",
        &action,
    ])
    .await?;
    assert_eq!(
        node.json(&["rule", "get", "Orders", "Alpha", "Healthy"])
            .await?["action"],
        json!({"type":"sql","expression":"REMOVE [private]","semantic_version":2})
    );
    node.json(&["rule", "delete", "Orders", "Alpha", "Healthy"])
        .await?;
    Ok(())
}

async fn local_failure(
    socket: &TcpListener,
    filter: &std::path::Path,
    action: &std::path::Path,
) -> TestResult {
    let mut arguments = crate::fixture::local_arguments(socket, filter)?;
    arguments.extend(["--action-file".into(), action.display().to_string()]);
    let stderr = failed(&run(arguments).await?);
    assert!(stderr.starts_with("switchyardctl:"));
    assert!(!stderr.contains("could not establish"));
    assert!(!stderr.contains("timed out"));
    assert!(!stderr.contains("private"));
    assert!(
        timeout(Duration::from_millis(50), socket.accept())
            .await
            .is_err(),
        "local action refusal must precede opening a connection"
    );
    Ok(())
}

pub(super) async fn local_inputs() -> TestResult {
    let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
    let files = TempDir::new()?;
    let filter = files.path().join("filter.json");
    std::fs::write(&filter, br#"{"type":"true"}"#)?;
    let action = files.path().join("private-action.json");
    for bytes in [
        br#"{"expression":"REMOVE private"}"#.as_slice(),
        br#"{"type":"sql","expression":"REMOVE private","semantic_version":null}"#,
        br#"{"type":"sql","expression":"REMOVE private","semantic_version":"1"}"#,
        br#"{"type":"sql","expression":"REMOVE private","parameters":[]}"#,
        br#"{"type":"sql","expression":"REMOVE private","expression":"REMOVE other"}"#,
        b"private invalid JSON",
    ] {
        std::fs::write(&action, bytes)?;
        local_failure(&socket, &filter, &action).await?;
    }
    std::fs::write(&action, vec![b' '; 32 * 1024 + 1])?;
    local_failure(&socket, &filter, &action).await?;
    local_failure(&socket, &filter, &files.path().join("private-missing.json")).await?;
    local_failure(&socket, &filter, files.path()).await?;
    #[cfg(unix)]
    {
        let fifo = files.path().join("private.fifo");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )?;
        local_failure(&socket, &filter, &fifo).await?;
    }
    std::fs::write(&action, br#"{"type":"sql","expression":"REMOVE x"}"#)?;
    std::fs::write(&filter, b"private invalid filter")?;
    local_failure(&socket, &filter, &action).await?;
    let oversized = json!({"type":"correlation","properties":[{"name":"large","value":{"type":"binary","value":"ab".repeat(64 * 1024)}}]});
    std::fs::write(&filter, serde_json::to_vec(&oversized)?)?;
    local_failure(&socket, &filter, &action).await?;
    Ok(())
}
