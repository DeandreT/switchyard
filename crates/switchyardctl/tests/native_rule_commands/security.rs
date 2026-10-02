use super::*;

pub(super) async fn exact_scope() -> TestResult {
    let node = Node::start(true).await?;
    node.topology().await?;
    node.token(CHILD, "manage")?;
    let filter = node.filter("allowed.json", &json!({"type":"true"}))?;
    node.json(&[
        "rule",
        "create",
        "Orders",
        "Alpha",
        "Allowed",
        "--filter-file",
        &filter,
    ])
    .await?;
    assert_eq!(
        node.json(&["rule", "get", "Orders", "Alpha", "Allowed"])
            .await?["subscription_path"],
        CHILD
    );
    assert_eq!(
        node.json(&["rule", "list", "Orders", "Alpha"]).await?["rules"]
            .as_array()
            .expect("rules")
            .len(),
        2
    );
    let before = node.store.snapshot()?;
    for (parent, child) in [("Orders", "Beta"), ("orders", "Alpha"), ("Orders", "alpha")] {
        for command in [
            vec!["rule", "list", parent, child],
            vec!["rule", "get", parent, child, "$Default"],
            vec!["rule", "delete", parent, child, "$Default"],
            vec![
                "rule",
                "create",
                parent,
                child,
                "Denied",
                "--filter-file",
                &filter,
            ],
        ] {
            let stderr = failed(&node.run(&command).await?);
            assert!(stderr.contains("administration request failed (PermissionDenied)"));
            assert!(!stderr.contains("SharedAccessSignature"));
        }
        assert_eq!(node.store.snapshot()?, before);
    }
    node.token("", "send")?;
    assert!(
        failed(&node.run(&["rule", "list", "Orders", "Alpha"]).await?).contains("PermissionDenied")
    );
    std::fs::write(
        node.files.path().join("token"),
        format!(
            "{}\n",
            fixture::sas("", "manage").replace("sig=", "sig=forged")
        ),
    )?;
    assert!(
        failed(&node.run(&["rule", "list", "Orders", "Alpha"]).await?).contains("Unauthenticated")
    );
    std::fs::write(node.files.path().join("token"), vec![b'x'; 16 * 1024 + 1])?;
    let stderr = failed(&node.run(&["rule", "list", "Orders", "Alpha"]).await?);
    assert!(!stderr.contains("administration request failed"));
    assert!(!stderr.contains("could not establish"));
    assert_eq!(node.store.snapshot()?, before);
    node.token(CHILD, "manage")?;
    node.json(&["rule", "delete", "Orders", "Alpha", "Allowed"])
        .await?;
    assert_eq!(
        node.json(&["rule", "list", "Orders", "Alpha"]).await?["rules"]
            .as_array()
            .expect("rules")
            .len(),
        1
    );
    Ok(())
}
