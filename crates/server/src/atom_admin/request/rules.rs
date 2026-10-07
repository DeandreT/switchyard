use super::*;

pub(super) async fn process<B>(
    target: Target,
    operation: Operation,
    body: B,
    context: &RequestContext,
    authorization: Authorization,
    epoch: &impl Fn() -> Result<u64, RequestFailure>,
) -> Result<Response<Full<Bytes>>, RequestFailure>
where
    B: Body<Data = Bytes> + Unpin,
{
    use crate::atom_admin::xml::rules;

    let (topic, subscription) = target.subscription()?;
    let name = if matches!(target, Target::Rule { .. }) {
        Some(target.rule_name()?)
    } else {
        None
    };
    let body = timeout(BODY_TIMEOUT, collect_body(body))
        .await
        .map_err(|_| RequestFailure::Unavailable)??;
    let definition = if operation == Operation::Create {
        let definition = rules::decode_definition(&body)?;
        if Some(&definition.name) != name.as_ref() {
            return Err(RequestFailure::BadRequest);
        }
        Some(definition)
    } else {
        if !body.is_empty() {
            return Err(RequestFailure::BadRequest);
        }
        None
    };
    authorization.recheck(epoch)?;
    match operation {
        Operation::Create => {
            let definition = timeout(
                OWNER_TIMEOUT,
                context.broker.create_atom_rule(
                    context.namespace.clone(),
                    topic,
                    subscription,
                    definition.ok_or(RequestFailure::Internal)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(
                StatusCode::CREATED,
                rules::encode_entry(&definition)?,
                "application/atom+xml",
            ))
        }
        Operation::Get => {
            let definition = timeout(
                OWNER_TIMEOUT,
                context.broker.get_atom_rule(
                    context.namespace.clone(),
                    topic,
                    subscription,
                    name.ok_or(RequestFailure::BadRequest)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??
            .ok_or(RequestFailure::RuleNotFound)?;
            Ok(response(
                StatusCode::OK,
                rules::encode_entry(&definition)?,
                "application/atom+xml",
            ))
        }
        Operation::List { skip, top } => {
            let definitions = timeout(
                OWNER_TIMEOUT,
                context.broker.list_atom_rules(
                    context.namespace.clone(),
                    topic,
                    subscription,
                    skip,
                    top,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(
                StatusCode::OK,
                rules::encode_feed(&definitions)?,
                "application/atom+xml",
            ))
        }
        Operation::Delete => {
            timeout(
                OWNER_TIMEOUT,
                context.broker.delete_atom_rule(
                    context.namespace.clone(),
                    topic,
                    subscription,
                    name.ok_or(RequestFailure::BadRequest)?,
                ),
            )
            .await
            .map_err(|_| RequestFailure::Unavailable)??;
            Ok(response(StatusCode::OK, Vec::new(), "application/atom+xml"))
        }
        Operation::Update => Err(RequestFailure::BadRequest),
    }
}
