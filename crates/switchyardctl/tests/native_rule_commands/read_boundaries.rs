use domain::{
    CommandKind, EntityPath, NamespaceName, RuleFilter, RuleName, SqlAction, SubscriptionName,
};

use super::*;

pub(super) async fn actions() -> TestResult {
    let node = Node::start(false).await?;
    node.topology().await?;
    timeout(
        DEADLINE,
        node.broker.handle().submit(
            NamespaceName::new("tenant")?,
            EntityPath::new("Orders")?,
            CommandKind::CreateRuleWithAction {
                subscription: SubscriptionName::new("Alpha")?,
                name: RuleName::new("Action")?,
                filter: RuleFilter::True,
                action: SqlAction::new("REMOVE [private-action-source]")?,
            },
        ),
    )
    .await??;
    let before = node.store.snapshot()?;
    let action =
        json!({"type":"sql","expression":"REMOVE [private-action-source]","semantic_version":2});
    for command in [
        vec!["rule", "get", "Orders", "Alpha", "Action"],
        vec!["rule", "list", "Orders", "Alpha"],
    ] {
        let output = node.json(&command).await?;
        let rule = if command[1] == "get" {
            &output
        } else {
            let rules = output["rules"].as_array().expect("rules");
            assert_eq!(rules.len(), 2);
            assert!(rules[0].get("action").is_none());
            &rules[1]
        };
        assert_eq!(rule["name"], "Action");
        assert_eq!(rule["action"], action);
    }
    assert_eq!(
        node.json(&["rule", "get", "Orders", "Alpha", "$Default"])
            .await?["filter"],
        json!({"type":"true"})
    );
    assert_eq!(
        node.json(&["rule", "list", "Orders", "Beta"]).await?["rules"]
            .as_array()
            .expect("rules")
            .len(),
        1
    );
    assert_eq!(node.store.snapshot()?, before);
    node.json(&["rule", "delete", "Orders", "Alpha", "Action"])
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
