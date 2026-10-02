use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::*;

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-rule-administration-test-key";
const EXPIRY: u64 = 4_102_444_800;

fn policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([
        SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
        SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::SEND,
        )?,
    ])?)
}

fn token(audience: &str, rule: &str, expiry: u64) -> String {
    let resource: String = url::form_urlencoded::byte_serialize(audience.as_bytes()).collect();
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("test key");
    hmac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let signature: String = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

fn authorized<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", token.parse().expect("ASCII token"));
    request
}

async fn denied<P: StoreProvider>(
    node: &Node<P>,
    path: &str,
    token: Option<&str>,
    expected: Code,
) -> TestResult {
    let request = |body| match token {
        Some(token) => authorized(body, token),
        None => Request::new(body),
    };
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .create_rule(request(create(path, "bad/name", sql("broken =", Some(0))))),
        )
        .await?,
        expected,
    );
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service.get_rule(match token {
                Some(token) => authorized(get(path, "$Default"), token),
                None => Request::new(get(path, "$Default")),
            }),
        )
        .await?,
        expected,
    );
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service.list_rules(match token {
                Some(token) => authorized(list(path), token),
                None => Request::new(list(path)),
            }),
        )
        .await?,
        expected,
    );
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service.delete_rule(match token {
                Some(token) => authorized(delete(path, "$Default"), token),
                None => Request::new(delete(path, "$Default")),
            }),
        )
        .await?,
        expected,
    );
    Ok(())
}

pub(super) async fn manage_scope_precedes_store_and_sql_validation<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    node.topology("Literal/$Management", "Alpha").await?;
    node.topology("Literal/$management", "Alpha").await?;
    tokio::time::timeout(
        DEADLINE,
        node.broker.handle().submit(
            namespace()?,
            EntityPath::new("Orders")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Beta")?,
                config: SubscriptionConfig::default(),
            },
        ),
    )
    .await??;
    node.service = node
        .service
        .clone()
        .with_shared_access_policy(policy()?, HOST)?;
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    let reads = node.reads();
    denied(&node, PATH, None, Code::Unauthenticated).await?;
    for bad in [
        "not-a-token".into(),
        token(&format!("amqps://{HOST}"), "manage", 1),
        token(&format!("amqps://{HOST}"), "manage", EXPIRY).replace("sig=", "sig=invalid"),
    ] {
        denied(&node, PATH, Some(&bad), Code::Unauthenticated).await?;
    }
    let send_only = token(&format!("amqps://{HOST}/{PATH}"), "send", EXPIRY);
    denied(&node, PATH, Some(&send_only), Code::PermissionDenied).await?;
    let child = token(&format!("amqps://{HOST}/{PATH}"), "manage", EXPIRY);
    for path in [
        "Orders/subscriptions/Beta",
        "orders/subscriptions/Alpha",
        "Orders/subscriptions/alpha",
    ] {
        denied(&node, path, Some(&child), Code::PermissionDenied).await?;
    }
    let foreign = token(
        "amqps://other.servicebus.windows.net/Orders/subscriptions/Alpha",
        "manage",
        EXPIRY,
    );
    denied(&node, PATH, Some(&foreign), Code::Unauthenticated).await?;
    let namespace_token = token(&format!("amqps://{HOST}"), "manage", EXPIRY);
    let mut wrong_namespace = list(PATH);
    wrong_namespace.namespace = "foreign".into();
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .list_rules(authorized(wrong_namespace, &namespace_token)),
        )
        .await?,
        Code::PermissionDenied,
    );
    for path in [
        "Orders",
        "Orders/subscriptions/Alpha/$DeadLetterQueue",
        "Orders/subscriptions/Alpha/extra",
    ] {
        code(
            tokio::time::timeout(
                DEADLINE,
                node.service
                    .list_rules(authorized(list(path), &namespace_token)),
            )
            .await?,
            Code::InvalidArgument,
        );
    }
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service.create_rule(authorized(
                create(PATH, "compile", sql("broken =", None)),
                &child,
            )),
        )
        .await?,
        Code::InvalidArgument,
    );
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service.create_rule(authorized(
                create(PATH, "version", sql("broken =", Some(2))),
                &child,
            )),
        )
        .await?,
        Code::Unimplemented,
    );
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;

    tokio::time::timeout(
        DEADLINE,
        node.service.create_rule(authorized(
            create("Orders/SUBSCRIPTIONS/Alpha", "exact-child", true_filter()),
            &child,
        )),
    )
    .await??;
    let exact = tokio::time::timeout(
        DEADLINE,
        node.service
            .get_rule(authorized(get(PATH, "exact-child"), &child)),
    )
    .await??
    .into_inner();
    assert_eq!(exact.subscription_path, PATH);
    let parent = token(&format!("amqps://{HOST}/Orders"), "manage", EXPIRY);
    assert_eq!(
        tokio::time::timeout(
            DEADLINE,
            node.service.list_rules(authorized(list(PATH), &parent))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        2
    );
    assert_eq!(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .list_rules(authorized(list("Orders/subscriptions/Beta"), &parent))
        )
        .await??
        .into_inner()
        .rules
        .len(),
        1
    );
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    node.clock.manual.set(0);
    tokio::time::timeout(
        DEADLINE,
        node.service
            .get_rule(authorized(get(PATH, "exact-child"), &namespace_token)),
    )
    .await??;
    node.unchanged(&before, writes, clocks)?;
    node.clock.manual.set(2_000);
    tokio::time::timeout(
        DEADLINE,
        node.service
            .delete_rule(authorized(delete(PATH, "exact-child"), &child)),
    )
    .await??;
    let literal = "Literal/$Management/subscriptions/Alpha";
    let literal_child = token(&format!("amqps://{HOST}/{literal}"), "manage", EXPIRY);
    let literal_parent = token(
        &format!("amqps://{HOST}/Literal/$Management"),
        "manage",
        EXPIRY,
    );
    let before = node.snapshot()?;
    let writes = node.writes();
    let clocks = node.clocks();
    let reads = node.reads();
    denied(
        &node,
        "Literal/$management/subscriptions/Alpha",
        Some(&literal_child),
        Code::PermissionDenied,
    )
    .await?;
    denied(
        &node,
        "Literal/$Management/subscriptions/Missing",
        Some(&literal_child),
        Code::PermissionDenied,
    )
    .await?;
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    tokio::time::timeout(
        DEADLINE,
        node.service.create_rule(authorized(
            create(
                "Literal/$Management/SUBSCRIPTIONS/Alpha",
                " Literal Rule ",
                false_filter(),
            ),
            &literal_child,
        )),
    )
    .await??;
    let literal_rule = tokio::time::timeout(
        DEADLINE,
        node.service
            .get_rule(authorized(get(literal, " Literal Rule "), &literal_parent)),
    )
    .await??
    .into_inner();
    assert_eq!(literal_rule.namespace, "tenant");
    assert_eq!(literal_rule.subscription_path, literal);
    assert_eq!(literal_rule.name, " Literal Rule ");
    assert_eq!(literal_rule.filter, false_filter());
    let listed = tokio::time::timeout(
        DEADLINE,
        node.service
            .list_rules(authorized(list(literal), &literal_child)),
    )
    .await??
    .into_inner()
    .rules;
    assert_eq!(
        listed.iter().find(|rule| rule.name == literal_rule.name),
        Some(&literal_rule)
    );
    tokio::time::timeout(
        DEADLINE,
        node.service.delete_rule(authorized(
            delete(literal, " Literal Rule "),
            &literal_parent,
        )),
    )
    .await??;
    let before = node.snapshot()?;
    let mut node = node.reopen()?;
    node.service = node
        .service
        .clone()
        .with_shared_access_policy(policy()?, HOST)?;
    node.clock.manual.set(0);
    let literal_rules = tokio::time::timeout(
        DEADLINE,
        node.service
            .list_rules(authorized(list(literal), &literal_child)),
    )
    .await??
    .into_inner()
    .rules;
    assert_eq!(literal_rules.len(), 1);
    assert_eq!(literal_rules[0].subscription_path, literal);
    assert_eq!(literal_rules[0].name, "$Default");
    assert_eq!(node.snapshot()?, before);
    assert_eq!(node.writes(), 0);
    assert_eq!(node.clocks(), 0);
    Ok(())
}

pub(super) async fn manage_scope_precedes_action_validation_and_reads<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    node.topology("Orders", "Alpha").await?;
    node.topology("Literal/$Management", "Alpha").await?;
    node.service = node
        .service
        .clone()
        .with_shared_access_policy(policy()?, HOST)?;
    let child = token(&format!("amqps://{HOST}/{PATH}"), "manage", EXPIRY);
    let namespace_token = token(&format!("amqps://{HOST}"), "manage", EXPIRY);
    let before = node.snapshot()?;
    let reads = node.reads();
    let writes = node.writes();
    let clocks = node.clocks();
    for (path, credential, expected) in [
        (PATH, None, Code::Unauthenticated),
        (PATH, Some("not-a-token".into()), Code::Unauthenticated),
        (
            PATH,
            Some(token(&format!("amqps://{HOST}/{PATH}"), "send", EXPIRY)),
            Code::PermissionDenied,
        ),
        (
            "Orders/subscriptions/Beta",
            Some(child.clone()),
            Code::PermissionDenied,
        ),
        (
            "Orders/subscriptions/alpha",
            Some(child.clone()),
            Code::PermissionDenied,
        ),
        (
            "orders/subscriptions/Alpha",
            Some(child.clone()),
            Code::PermissionDenied,
        ),
        (
            "Orders/subscriptions/Alpha/$DeadLetterQueue",
            Some(child.clone()),
            Code::InvalidArgument,
        ),
    ] {
        let input = create_action(
            path,
            "bad/name",
            true_filter(),
            sql_action("SET private-action = 'secret-action'", Some(2)),
        );
        let request = match credential.as_deref() {
            Some(token) => authorized(input, token),
            None => Request::new(input),
        };
        let error = code(
            tokio::time::timeout(DEADLINE, node.service.create_rule_with_action(request)).await?,
            expected,
        );
        assert!(!error.message().contains("secret-action"));
        let mut input = get(path, "$Default");
        input.include_actions = true;
        let request = match credential.as_deref() {
            Some(token) => authorized(input, token),
            None => Request::new(input),
        };
        code(
            tokio::time::timeout(DEADLINE, node.service.get_rule(request)).await?,
            expected,
        );
        let mut input = list(path);
        input.include_actions = true;
        let request = match credential.as_deref() {
            Some(token) => authorized(input, token),
            None => Request::new(input),
        };
        code(
            tokio::time::timeout(DEADLINE, node.service.list_rules(request)).await?,
            expected,
        );
    }
    let mut input = create_action(PATH, "foreign", true_filter(), sql_action("REMOVE x", None));
    input.namespace = "foreign".into();
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .create_rule_with_action(authorized(input, &namespace_token)),
        )
        .await?,
        Code::PermissionDenied,
    );
    for (source, version, expected) in [
        ("REMOVE", None, Code::InvalidArgument),
        (
            "SET private-action = 'secret-action'",
            None,
            Code::Unimplemented,
        ),
        ("broken-private-source", Some(2), Code::Unimplemented),
    ] {
        code(
            tokio::time::timeout(
                DEADLINE,
                node.service.create_rule_with_action(authorized(
                    create_action(PATH, "invalid", true_filter(), sql_action(source, version)),
                    &child,
                )),
            )
            .await?,
            expected,
        );
    }
    assert_eq!(node.reads(), reads);
    node.unchanged(&before, writes, clocks)?;
    let literal = "Literal/$Management/subscriptions/Alpha";
    let literal_token = token(
        &format!("amqps://{HOST}/Literal/$Management"),
        "manage",
        EXPIRY,
    );
    let source = " /* retained literal */ REMOVE user.[audit]; ";
    tokio::time::timeout(
        DEADLINE,
        node.service.create_rule_with_action(authorized(
            create_action(
                literal,
                " Literal Action ",
                false_filter(),
                sql_action(source, None),
            ),
            &literal_token,
        )),
    )
    .await??;
    let before = node.snapshot()?;
    let reads = node.reads();
    let writes = node.writes();
    let clocks = node.clocks();
    let mut wrong_case = get(
        "Literal/$management/subscriptions/Alpha",
        " Literal Action ",
    );
    wrong_case.include_actions = true;
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .get_rule(authorized(wrong_case, &literal_token)),
        )
        .await?,
        Code::PermissionDenied,
    );
    assert_eq!(node.reads(), reads);
    node.clock.manual.set(0);
    let mut input = get(literal, " Literal Action ");
    input.include_actions = true;
    let rule = tokio::time::timeout(
        DEADLINE,
        node.service.get_rule(authorized(input, &literal_token)),
    )
    .await??
    .into_inner();
    assert_eq!(rule.namespace, "tenant");
    assert_eq!(rule.subscription_path, literal);
    assert_eq!(rule.name, " Literal Action ");
    assert_eq!(rule.action, sql_action(source, Some(1)));
    let mut input = list(literal);
    input.include_actions = true;
    let listed = tokio::time::timeout(
        DEADLINE,
        node.service.list_rules(authorized(input, &literal_token)),
    )
    .await??
    .into_inner();
    assert_eq!(
        listed.rules.iter().find(|item| item.name == rule.name),
        Some(&rule)
    );
    code(
        tokio::time::timeout(
            DEADLINE,
            node.service
                .list_rules(authorized(list(literal), &literal_token)),
        )
        .await?,
        Code::Unimplemented,
    );
    node.unchanged(&before, writes, clocks)?;
    node.clock.manual.set(2_000);
    tokio::time::timeout(
        DEADLINE,
        node.service.delete_rule(authorized(
            delete(literal, " Literal Action "),
            &literal_token,
        )),
    )
    .await??;
    Ok(())
}
