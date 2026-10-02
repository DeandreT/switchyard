use super::*;
use fixture::local_arguments;

async fn no_connection(socket: &TcpListener) {
    assert!(
        timeout(Duration::from_millis(50), socket.accept())
            .await
            .is_err(),
        "local input refusal must precede opening a socket"
    );
}

async fn local_failure(socket: &TcpListener, file: &std::path::Path) -> TestResult {
    let stderr = failed(&run(local_arguments(socket, file)?).await?);
    assert!(stderr.starts_with("switchyardctl:"));
    assert!(!stderr.contains("could not establish"));
    assert!(!stderr.contains("timed out"));
    no_connection(socket).await;
    Ok(())
}

pub(super) async fn invalid() -> TestResult {
    let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
    let files = TempDir::new()?;
    let inputs = [
        json!({}),
        json!({"type":"unknown"}),
        json!({"type":"true","unexpected":KEY}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"null"}},{"name":"x","value":{"type":"bool","value":true}}]}),
        json!({"type":"correlation","properties":(0..33).map(|index|json!({"name":format!("p{index}"),"value":{"type":"null"}})).collect::<Vec<_>>()}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"ubyte","value":256}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"ulong","value":18446744073709551615_u64}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"long","value":"-0"}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"timestamp","value":"01"}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"float_bits","value":"7FC00001"}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"decimal64","value":"000102"}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"char","value":55296}}]}),
        json!({"type":"correlation","properties":[{"name":"x","value":{"type":"symbol","value":"non-ascii-\u{3bb}"}}]}),
    ];
    for (index, input) in inputs.into_iter().enumerate() {
        let file = files.path().join(format!("invalid-{index}.json"));
        std::fs::write(&file, serde_json::to_vec(&input)?)?;
        local_failure(&socket, &file).await?;
    }
    let file = files.path().join("invalid.json");
    std::fs::write(&file, b"not JSON")?;
    local_failure(&socket, &file).await?;
    let file = files.path().join("oversized.json");
    std::fs::write(&file, vec![b' '; 512 * 1024 + 1])?;
    local_failure(&socket, &file).await?;
    local_failure(&socket, &files.path().join("missing.json")).await?;
    local_failure(&socket, files.path()).await?;
    let valid = files.path().join("valid.json");
    std::fs::write(&valid, b"{\"type\":\"true\"}")?;
    for (index, value) in [
        (7, "Orders/subscriptions/Nested"),
        (8, "bad/name"),
        (9, "bad/name"),
    ] {
        let mut arguments = local_arguments(&socket, &valid)?;
        arguments[index] = value.into();
        let stderr = failed(&run(arguments).await?);
        assert!(!stderr.contains("could not establish"));
        no_connection(&socket).await;
    }
    Ok(())
}

pub(super) async fn request_limit() -> TestResult {
    let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
    let files = TempDir::new()?;
    let file = files.path().join("large-request.json");
    let filter = json!({"type":"correlation","properties":[
        {"name":"large","value":{"type":"binary","value":"ab".repeat(64 * 1024)}}]});
    let bytes = serde_json::to_vec(&filter)?;
    assert!(bytes.len() < 512 * 1024);
    std::fs::write(&file, bytes)?;
    let stderr = failed(&run(local_arguments(&socket, &file)?).await?);
    assert!(
        stderr.contains("request") && stderr.contains("limit"),
        "expected request preflight refusal: {stderr}"
    );
    no_connection(&socket).await;
    Ok(())
}
