use super::*;

fn correlation() -> Value {
    let values = [
        json!({"type":"null"}),
        json!({"type":"bool","value":true}),
        json!({"type":"ubyte","value":255}),
        json!({"type":"ushort","value":65535}),
        json!({"type":"uint","value":4294967295_u64}),
        json!({"type":"ulong","value":"18446744073709551615"}),
        json!({"type":"byte","value":-128}),
        json!({"type":"short","value":-32768}),
        json!({"type":"int","value":-2147483648_i64}),
        json!({"type":"long","value":"-9223372036854775808"}),
        json!({"type":"float_bits","value":"7fc00001"}),
        json!({"type":"double_bits","value":"8000000000000000"}),
        json!({"type":"decimal32","value":"00010203"}),
        json!({"type":"decimal64","value":"0001020304050607"}),
        json!({"type":"decimal128","value":"000102030405060708090a0b0c0d0e0f"}),
        json!({"type":"char","value":955}),
        json!({"type":"timestamp","value":"-1234"}),
        json!({"type":"uuid","value":"000102030405060708090a0b0c0d0e0f"}),
        json!({"type":"binary","value":"00ff01"}),
        json!({"type":"string","value":"text"}),
        json!({"type":"symbol","value":"ascii-symbol"}),
    ];
    json!({"type":"correlation","correlation_id":"correlation","message_id":"message",
        "to":"destination","reply_to":"reply","subject":"subject","session_id":"session",
        "reply_to_session_id":"reply-session","content_type":"text/plain",
        "properties":values.into_iter().enumerate().map(|(index,value)|
            json!({"name":format!("p{index:02}"),"value":value})).collect::<Vec<_>>()})
}

pub(super) async fn round_trip() -> TestResult {
    let node = Node::start(false).await?;
    node.topology().await?;
    let default = node
        .json(&["rule", "get", "Orders", "Alpha", "$Default"])
        .await?;
    assert_eq!(default["namespace"], "tenant");
    assert_eq!(default["subscription_path"], CHILD);
    assert_eq!(default["name"], "$Default");
    assert_eq!(default["created_at_unix_millis"], "9007199254740993");
    assert_eq!(default["filter"], json!({"type":"true"}));
    let mut expected = vec![default];
    for (name, filter) in [
        ("Always", json!({"type":"true"})),
        ("Correlated", correlation()),
        ("Never", json!({"type":"false"})),
        ("Sql", json!({"type":"sql","expression":"color = 'red'"})),
    ] {
        let file = node.filter(&format!("{name}.json"), &filter)?;
        assert_eq!(
            node.json(&[
                "rule",
                "create",
                "Orders",
                "Alpha",
                name,
                "--filter-file",
                &file
            ])
            .await?,
            json!({"namespace":"tenant","subscription_path":CHILD,"name":name,"completed":true})
        );
        let output = node.json(&["rule", "get", "Orders", "Alpha", name]).await?;
        let filter = if name == "Sql" {
            json!({"type":"sql","expression":"color = 'red'","semantic_version":1})
        } else {
            filter
        };
        assert_eq!(
            output,
            json!({"namespace":"tenant","subscription_path":CHILD,
            "name":name,"filter":filter,"created_at_unix_millis":"9007199254740993"})
        );
        expected.push(output);
    }
    let before = node.store.snapshot()?;
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    assert_eq!(
        node.json(&["rule", "list", "Orders", "Alpha"]).await?,
        json!({"rules":expected})
    );
    for rule in &expected {
        assert_eq!(
            node.json(&[
                "rule",
                "get",
                "Orders",
                "Alpha",
                rule["name"].as_str().expect("name")
            ])
            .await?,
            *rule
        );
    }
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    node.clock.set(9_007_199_254_740_993);
    let file = node.filter("duplicate.json", &json!({"type":"false"}))?;
    assert!(
        failed(
            &node
                .run(&[
                    "rule",
                    "create",
                    "Orders",
                    "Alpha",
                    "Always",
                    "--filter-file",
                    &file
                ])
                .await?
        )
        .contains("AlreadyExists")
    );
    assert!(
        failed(
            &node
                .run(&["rule", "get", "Orders", "Alpha", "always"])
                .await?
        )
        .contains("NotFound")
    );
    for rule in expected {
        let name = rule["name"].as_str().expect("name");
        assert_eq!(
            node.json(&["rule", "delete", "Orders", "Alpha", name])
                .await?,
            json!({"namespace":"tenant","subscription_path":CHILD,"name":name,"completed":true})
        );
    }
    assert_eq!(
        node.json(&["rule", "list", "Orders", "Alpha"]).await?,
        json!({"rules":[]})
    );
    assert!(
        failed(
            &node
                .run(&["rule", "delete", "Orders", "Alpha", "$Default"])
                .await?
        )
        .contains("NotFound")
    );
    assert_eq!(
        node.json(&["subscription", "get", "Orders", "Alpha"])
            .await?["kind"],
        "subscription"
    );
    Ok(())
}

pub(super) async fn server_statuses() -> TestResult {
    let node = Node::start(false).await?;
    node.topology().await?;
    let before = node.store.snapshot()?;
    for (name, filter, code) in [
        (
            "Syntax",
            json!({"type":"sql","expression":"color ="}),
            "InvalidArgument",
        ),
        (
            "Future",
            json!({"type":"sql","expression":"not valid SQL","semantic_version":2}),
            "Unimplemented",
        ),
        (
            "Zero",
            json!({"type":"sql","expression":"not valid SQL","semantic_version":0}),
            "Unimplemented",
        ),
    ] {
        let file = node.filter(&format!("{name}.json"), &filter)?;
        let stderr = failed(
            &node
                .run(&[
                    "rule",
                    "create",
                    "Orders",
                    "Alpha",
                    name,
                    "--filter-file",
                    &file,
                ])
                .await?,
        );
        assert!(stderr.contains(&format!("administration request failed ({code})")));
        assert!(!stderr.contains("color ="));
        assert!(!stderr.contains("not valid SQL"));
        assert_eq!(node.store.snapshot()?, before);
    }
    let file = node.filter(
        "healthy.json",
        &json!({"type":"sql","expression":"color = 'blue'"}),
    )?;
    node.json(&[
        "rule",
        "create",
        "Orders",
        "Alpha",
        "Healthy",
        "--filter-file",
        &file,
    ])
    .await?;
    assert_eq!(
        node.json(&["rule", "get", "Orders", "Alpha", "Healthy"])
            .await?["filter"],
        json!({"type":"sql","expression":"color = 'blue'","semantic_version":1})
    );
    Ok(())
}
