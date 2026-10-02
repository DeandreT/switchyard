use std::collections::{BTreeMap, BTreeSet};

use admin_api::v1::{self, rule_filter::Filter};
use domain::{CorrelationFilter, RuleFilter, SqlFilter};
use tonic::Status;

use super::{scalar, status};

pub(super) fn read(input: Option<&v1::RuleFilter>) -> Result<RuleFilter, Status> {
    let filter = input
        .and_then(|input| input.filter.as_ref())
        .ok_or_else(|| Status::invalid_argument("a rule filter is required"))?;
    match filter {
        Filter::TrueFilter(_) => Ok(RuleFilter::True),
        Filter::FalseFilter(_) => Ok(RuleFilter::False),
        Filter::CorrelationFilter(input) => correlation(input).map(RuleFilter::Correlation),
        Filter::SqlFilter(input) => {
            if input
                .semantic_version
                .is_some_and(|version| version != domain::SQL_FILTER_SEMANTIC_VERSION)
            {
                return Err(Status::unimplemented(
                    "unsupported SQL filter semantic version",
                ));
            }
            if input.expression.len() > domain::MAX_SQL_EXPRESSION_BYTES
                || input
                    .expression
                    .encode_utf16()
                    .take(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1)
                    .count()
                    > domain::MAX_SQL_EXPRESSION_UTF16_UNITS
            {
                return Err(Status::resource_exhausted("SQL source limit reached"));
            }
            SqlFilter::new(input.expression.clone())
                .map(RuleFilter::Sql)
                .map_err(status::compilation)
        }
    }
}

fn correlation(input: &v1::CorrelationRuleFilter) -> Result<CorrelationFilter, Status> {
    let system = [
        input.correlation_id.as_ref(),
        input.message_id.as_ref(),
        input.to.as_ref(),
        input.reply_to.as_ref(),
        input.subject.as_ref(),
        input.session_id.as_ref(),
        input.reply_to_session_id.as_ref(),
        input.content_type.as_ref(),
    ];
    if system
        .iter()
        .filter(|value| value.is_some())
        .count()
        .saturating_add(input.properties.len())
        > domain::MAX_CORRELATION_RULE_CONDITIONS
    {
        return Err(Status::resource_exhausted(
            "correlation condition limit reached",
        ));
    }
    let mut bytes = system
        .into_iter()
        .flatten()
        .fold(0_usize, |bytes, value| bytes.saturating_add(value.len()));
    let mut names = BTreeSet::new();
    for property in &input.properties {
        if !names.insert(property.name.as_str()) {
            return Err(Status::invalid_argument(
                "duplicate correlation property name",
            ));
        }
        let value = property
            .value
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("a correlation property value is required"))?;
        bytes = bytes
            .saturating_add(property.name.len())
            .saturating_add(scalar::validate(value)?);
        if bytes > domain::MAX_RULE_BYTES {
            return Err(Status::resource_exhausted(
                "correlation filter exceeds the rule byte limit",
            ));
        }
    }
    if bytes > domain::MAX_RULE_BYTES {
        return Err(Status::resource_exhausted(
            "correlation filter exceeds the rule byte limit",
        ));
    }
    let mut properties = BTreeMap::new();
    for property in &input.properties {
        let value = property
            .value
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("a correlation property value is required"))?;
        properties.insert(property.name.clone(), scalar::read(value)?);
    }
    Ok(CorrelationFilter {
        correlation_id: input.correlation_id.clone(),
        message_id: input.message_id.clone(),
        to: input.to.clone(),
        reply_to: input.reply_to.clone(),
        subject: input.subject.clone(),
        session_id: input.session_id.clone(),
        reply_to_session_id: input.reply_to_session_id.clone(),
        content_type: input.content_type.clone(),
        properties,
    })
}

pub(super) fn write(filter: &RuleFilter) -> Result<v1::RuleFilter, Status> {
    filter.validate().map_err(status::stored)?;
    let filter = match filter {
        RuleFilter::True => Filter::TrueFilter(v1::TrueRuleFilter {}),
        RuleFilter::False => Filter::FalseFilter(v1::FalseRuleFilter {}),
        RuleFilter::Sql(filter) => Filter::SqlFilter(v1::SqlRuleFilter {
            expression: filter.expression().to_owned(),
            semantic_version: Some(filter.semantic_version()),
        }),
        RuleFilter::Correlation(filter) => Filter::CorrelationFilter(v1::CorrelationRuleFilter {
            correlation_id: filter.correlation_id.clone(),
            message_id: filter.message_id.clone(),
            to: filter.to.clone(),
            reply_to: filter.reply_to.clone(),
            subject: filter.subject.clone(),
            session_id: filter.session_id.clone(),
            reply_to_session_id: filter.reply_to_session_id.clone(),
            content_type: filter.content_type.clone(),
            properties: filter
                .properties
                .iter()
                .map(|(name, value)| {
                    Ok(v1::CorrelationProperty {
                        name: name.clone(),
                        value: Some(scalar::write(value)?),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?,
        }),
    };
    Ok(v1::RuleFilter {
        filter: Some(filter),
    })
}
