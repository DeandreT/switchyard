//! Test-only closed #31 validator. Not a runtime request parser or SDK substitute.
pub const ATOM: &str = "http://www.w3.org/2005/Atom";
pub const CONNECT: &str = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
pub const FIELDS: [&str; 6] = [
    "LockDuration",
    "RequiresSession",
    "DefaultMessageTimeToLive",
    "DeadLetteringOnMessageExpiration",
    "DeadLetteringOnFilterEvaluationExceptions",
    "MaxDeliveryCount",
];

pub fn entry(bytes: &[u8]) -> Result<Vec<String>, String> {
    if bytes.len() > 4096 {
        return Err("byte bound".into());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "UTF8")?;
    if text.contains("<![CDATA[") {
        return Err("CDATA".into());
    }
    let doc = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 64,
            ..Default::default()
        },
    )
    .map_err(|_| "XML")?;
    for node in doc.descendants() {
        if node.is_comment()
            || node.is_pi()
            || node.ancestors().filter(|n| n.is_element()).count() > 4
        {
            return Err("node/depth".into());
        }
    }
    let root = doc.root_element();
    check(root, ATOM, "entry", &[])?;
    let children = elements(root, 2)?;
    check(children[0], ATOM, "title", &[("type", "text")])?;
    let title = scalar(children[0])?;
    if title.is_empty() || title.len() > 50 {
        return Err("title".into());
    }
    check(children[1], ATOM, "content", &[("type", "application/xml")])?;
    let descriptions = elements(children[1], 1)?;
    let description = descriptions[0];
    check(description, CONNECT, "SubscriptionDescription", &[])?;
    let fields = elements(description, 6)?;
    let mut values = vec![title];
    for (node, name) in fields.into_iter().zip(FIELDS) {
        check(node, CONNECT, name, &[])?;
        values.push(scalar(node)?);
    }
    if values[2] != "false"
        || !["false", "true"].contains(&values[4].as_str())
        || !["false", "true"].contains(&values[5].as_str())
    {
        return Err("boolean".into());
    }
    let count = values[6].parse::<i32>().map_err(|_| "count")?;
    if count <= 0 {
        return Err("count".into());
    }
    Ok(values)
}
fn check(
    node: roxmltree::Node<'_, '_>,
    ns: &str,
    name: &str,
    attrs: &[(&str, &str)],
) -> Result<(), String> {
    if node.tag_name().name() != name
        || node.tag_name().namespace() != Some(ns)
        || node.attributes().len() != attrs.len()
    {
        return Err("name/attributes".into());
    }
    for attribute in node.attributes() {
        if attribute.namespace().is_some()
            || !attrs.contains(&(attribute.name(), attribute.value()))
        {
            return Err("attribute".into());
        }
    }
    Ok(())
}
fn elements<'a, 'b>(
    node: roxmltree::Node<'a, 'b>,
    count: usize,
) -> Result<Vec<roxmltree::Node<'a, 'b>>, String> {
    if node
        .children()
        .any(|n| n.is_text() && !n.text().unwrap_or("").trim().is_empty())
    {
        return Err("mixed content".into());
    }
    let children: Vec<_> = node.children().filter(|n| n.is_element()).collect();
    if children.len() != count {
        return Err("child count".into());
    }
    Ok(children)
}
fn scalar(node: roxmltree::Node<'_, '_>) -> Result<String, String> {
    if node.children().any(|n| !n.is_text()) {
        return Err("structured scalar".into());
    }
    let value: String = node.children().filter_map(|n| n.text()).collect();
    if value.len() > 128 {
        return Err("scalar bound".into());
    }
    Ok(value)
}

pub fn error(bytes: &[u8]) -> Result<(u16, String), String> {
    if bytes.len() > 4096 {
        return Err("byte bound".into());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "UTF8")?;
    if text.contains("<![CDATA[") {
        return Err("CDATA".into());
    }
    let doc = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 64,
            ..Default::default()
        },
    )
    .map_err(|_| "XML")?;
    if doc.descendants().any(|n| {
        n.is_comment() || n.is_pi() || n.ancestors().filter(|n| n.is_element()).count() > 4
    }) {
        return Err("node/depth".into());
    }
    let root = doc.root_element();
    let children = elements(root, 2)?;
    for (node, name) in [
        (root, "Error"),
        (children[0], "Code"),
        (children[1], "Detail"),
    ] {
        if node.tag_name().namespace().is_some()
            || node.tag_name().name() != name
            || node.attributes().len() != 0
        {
            return Err("error shape".into());
        }
    }
    let code: u16 = scalar(children[0])?.parse().map_err(|_| "code")?;
    if ![400, 403, 404, 500, 501, 503].contains(&code) {
        return Err("code".into());
    }
    Ok((code, scalar(children[1])?))
}
