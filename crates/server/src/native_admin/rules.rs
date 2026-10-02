use admin_api::v1::{
    CreateRuleRequest, DeleteRuleRequest, GetRuleRequest, ListRulesRequest, ListRulesResponse,
    Rule, RuleMutationResponse, rule_service_server::RuleService,
};
use domain::{EntityBinding, RuleDefinition, RuleName, SubscriptionName, Timestamp};
use protocol_amqp::EntityMetadata;

use super::{
    AdminTarget, CommandKind, CommandOutcome, EntityPath, NativeAdminService, Request, Response,
    Status, topology,
};

mod filter;
mod scalar;
mod status;
#[cfg(test)]
mod tests;

struct RuleTarget {
    topic: EntityPath,
    subscription: SubscriptionName,
    path: EntityPath,
}

impl RuleTarget {
    fn parse(path: &str) -> Result<Self, Status> {
        let AdminTarget::Subscription { topic, name } = topology::target(path)? else {
            return Err(Status::invalid_argument(
                "a subscription entity path is required",
            ));
        };
        let path = topic
            .subscription(&name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Self {
            topic,
            subscription: name,
            path,
        })
    }
}

fn rule_name(name: &str) -> Result<RuleName, Status> {
    if name
        .encode_utf16()
        .take(domain::MAX_RULE_NAME_LENGTH + 1)
        .count()
        > domain::MAX_RULE_NAME_LENGTH
    {
        return Err(Status::invalid_argument(
            "rule name exceeds its UTF-16-unit limit",
        ));
    }
    RuleName::new(name).map_err(|error| Status::invalid_argument(error.to_string()))
}

impl NativeAdminService {
    async fn bind_rule_target(&self, target: &RuleTarget) -> Result<EntityBinding, Status> {
        let admission = self
            .broker
            .bind_admin(
                self.namespace.clone(),
                AdminTarget::Subscription {
                    topic: target.topic.clone(),
                    name: target.subscription.clone(),
                },
            )
            .await
            .map_err(status::read)?
            .ok_or_else(|| Status::not_found("subscription does not exist"))?;
        if !matches!(admission.metadata, EntityMetadata::Subscription(_)) {
            return Err(Status::internal("unexpected subscription metadata"));
        }
        Ok(admission.binding)
    }

    fn rule_response(&self, target: &RuleTarget, rule: RuleDefinition) -> Result<Rule, Status> {
        rule.validate().map_err(status::stored)?;
        if rule.action.is_some() {
            return Err(Status::unimplemented(
                "native rule responses cannot represent SQL actions yet",
            ));
        }
        Ok(Rule {
            namespace: self.namespace.as_str().to_owned(),
            subscription_path: target.path.as_str().to_owned(),
            name: rule.name.as_str().to_owned(),
            filter: Some(filter::write(&rule.filter)?),
            created_at_unix_millis: rule.created_at.as_millis(),
        })
    }
}

#[tonic::async_trait]
impl RuleService for NativeAdminService {
    async fn create_rule(
        &self,
        request: Request<CreateRuleRequest>,
    ) -> Result<Response<RuleMutationResponse>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.subscription_path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let target = RuleTarget::parse(&input.subscription_path)?;
        let name = rule_name(&input.name)?;
        let filter = filter::read(input.filter.as_ref())?;
        let rule = RuleDefinition {
            name,
            filter,
            created_at: Timestamp::UNIX_EPOCH,
            action: None,
        };
        rule.encoded_size().map_err(status::input)?;
        let binding = self.bind_rule_target(&target).await?;
        let outcome = self
            .broker
            .submit_fenced(
                binding,
                target.topic,
                CommandKind::CreateRule {
                    subscription: target.subscription,
                    name: rule.name,
                    filter: rule.filter,
                },
            )
            .await
            .map_err(status::mutation)?;
        if outcome != CommandOutcome::RuleCreated {
            return Err(Status::internal("unexpected rule creation result"));
        }
        Ok(Response::new(RuleMutationResponse {}))
    }

    async fn get_rule(&self, request: Request<GetRuleRequest>) -> Result<Response<Rule>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.subscription_path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let target = RuleTarget::parse(&input.subscription_path)?;
        let name = rule_name(&input.name)?;
        let binding = self.bind_rule_target(&target).await?;
        let rules = self
            .broker
            .rules_fenced(binding, target.topic.clone(), target.subscription.clone())
            .await
            .map_err(status::read)?;
        let rule = rules
            .into_iter()
            .find(|rule| rule.name == name)
            .ok_or_else(|| Status::not_found("rule does not exist"))?;
        Ok(Response::new(self.rule_response(&target, rule)?))
    }

    async fn list_rules(
        &self,
        request: Request<ListRulesRequest>,
    ) -> Result<Response<ListRulesResponse>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.subscription_path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let target = RuleTarget::parse(&input.subscription_path)?;
        let binding = self.bind_rule_target(&target).await?;
        let rules = self
            .broker
            .rules_fenced(binding, target.topic.clone(), target.subscription.clone())
            .await
            .map_err(status::read)?;
        if rules.len() > domain::MAX_SUBSCRIPTION_RULES {
            return Err(Status::internal("invalid stored rule set"));
        }
        let rules = rules
            .into_iter()
            .map(|rule| self.rule_response(&target, rule))
            .collect::<Result<Vec<_>, _>>()?;
        let response = ListRulesResponse { rules };
        if prost::Message::encoded_len(&response) > crate::NATIVE_ADMIN_RESPONSE_LIMIT {
            return Err(Status::resource_exhausted(
                "rule response exceeds its encoded-byte limit",
            ));
        }
        Ok(Response::new(response))
    }

    async fn delete_rule(
        &self,
        request: Request<DeleteRuleRequest>,
    ) -> Result<Response<RuleMutationResponse>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.subscription_path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let target = RuleTarget::parse(&input.subscription_path)?;
        let name = rule_name(&input.name)?;
        let binding = self.bind_rule_target(&target).await?;
        let outcome = self
            .broker
            .submit_fenced(
                binding,
                target.topic,
                CommandKind::DeleteRule {
                    subscription: target.subscription,
                    name,
                },
            )
            .await
            .map_err(status::mutation)?;
        if outcome != CommandOutcome::RuleDeleted {
            return Err(Status::internal("unexpected rule deletion result"));
        }
        Ok(Response::new(RuleMutationResponse {}))
    }
}
