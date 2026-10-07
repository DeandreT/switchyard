use std::collections::BTreeSet;

use domain::SubscriptionConfig;
use quick_xml::{
    Reader, XmlVersion,
    events::{BytesRef, BytesStart, Event},
    name::{Namespace, NamespaceError, NamespaceResolver, PrefixDeclaration, QName, ResolveResult},
};

use super::super::{
    ATOM_NS, Budget, MAX_BODY_BYTES, MAX_NAMESPACE_BINDINGS, SERVICE_BUS_NS, XML_NS, XSI_NS,
    bounded_add, duration, lexical,
};
use super::{SubscriptionXmlError, validate_config};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NamespaceKind {
    Atom,
    ServiceBus,
    Xsi,
    Xml,
}

fn namespace_kind(uri: &str) -> Result<NamespaceKind, SubscriptionXmlError> {
    match uri {
        ATOM_NS => Ok(NamespaceKind::Atom),
        SERVICE_BUS_NS => Ok(NamespaceKind::ServiceBus),
        XSI_NS => Ok(NamespaceKind::Xsi),
        XML_NS => Ok(NamespaceKind::Xml),
        _ => Err(SubscriptionXmlError::Malformed),
    }
}

fn resolved_namespace(
    result: ResolveResult<'_>,
) -> Result<Option<NamespaceKind>, SubscriptionXmlError> {
    match result {
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Bound(uri) => namespace_kind(uri.as_ref()).map(Some),
        ResolveResult::Unknown(_) => Err(SubscriptionXmlError::Malformed),
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Property {
    Lock,
    Session,
    Ttl,
    ExpiryDeadLetter,
    FilterDeadLetter,
    Deliveries,
    Batched,
    Status,
    DefaultRule,
}

impl Property {
    fn parse(name: &str) -> Result<Self, SubscriptionXmlError> {
        match name {
            "LockDuration" => Ok(Self::Lock),
            "RequiresSession" => Ok(Self::Session),
            "DefaultMessageTimeToLive" => Ok(Self::Ttl),
            "DeadLetteringOnMessageExpiration" => Ok(Self::ExpiryDeadLetter),
            "DeadLetteringOnFilterEvaluationExceptions" => Ok(Self::FilterDeadLetter),
            "MaxDeliveryCount" => Ok(Self::Deliveries),
            "EnableBatchedOperations" => Ok(Self::Batched),
            "Status" => Ok(Self::Status),
            "DefaultRuleDescription" => Ok(Self::DefaultRule),
            _ => Err(SubscriptionXmlError::UnsupportedDefinition),
        }
    }
}

#[derive(Clone, Copy)]
enum Node {
    Entry,
    Content,
    Description,
    Property(Property),
    Rule,
    Filter,
    RuleName,
    SqlExpression,
    Parameters,
}

impl Node {
    fn scalar(self) -> bool {
        matches!(
            self,
            Self::Property(_) | Self::RuleName | Self::SqlExpression
        )
    }
}

struct Frame {
    raw_name: String,
    node: Node,
    child_seen: bool,
    scalar: String,
}

struct CheckedAttribute {
    name: String,
    value: String,
}

#[derive(Default)]
struct DefaultRule {
    name: bool,
    filter: bool,
    expression: bool,
    parameters: bool,
}

struct Parser {
    resolver: NamespaceResolver,
    budget: Budget,
    frames: Vec<Frame>,
    properties: BTreeSet<Property>,
    config: SubscriptionConfig,
    rule: DefaultRule,
    root_seen: bool,
    complete: bool,
}

impl Parser {
    fn new() -> Self {
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(MAX_NAMESPACE_BINDINGS);
        Self {
            resolver,
            budget: Budget::default(),
            frames: Vec::new(),
            properties: BTreeSet::new(),
            config: SubscriptionConfig::default(),
            rule: DefaultRule::default(),
            root_seen: false,
            complete: false,
        }
    }

    fn attributes(
        &mut self,
        start: &BytesStart<'_>,
    ) -> Result<Vec<CheckedAttribute>, SubscriptionXmlError> {
        let raw_name = start.name();
        lexical::qname(raw_name.as_ref())?;
        lexical::attribute_tail(&start.as_ref()[raw_name.as_ref().len()..])?;
        let mut attributes = Vec::new();
        for attribute in start.attributes().with_checks(true) {
            Budget::attributes(attributes.len() + 1)?;
            let attribute = attribute.map_err(|_| SubscriptionXmlError::Malformed)?;
            lexical::qname(attribute.key.as_ref())?;
            let value = attribute
                .normalized_value_with(
                    XmlVersion::Explicit1_0,
                    1,
                    quick_xml::escape::resolve_xml_entity,
                )
                .map_err(|_| SubscriptionXmlError::Malformed)?;
            if !lexical::legal_chars(&value) {
                return Err(SubscriptionXmlError::Malformed);
            }
            self.budget.decoded(value.len())?;
            attributes.push(CheckedAttribute {
                name: attribute.key.as_ref().to_owned(),
                value: value.into_owned(),
            });
        }
        Ok(attributes)
    }

    fn open(&mut self, start: &BytesStart<'_>) -> Result<(), SubscriptionXmlError> {
        let depth = self.frames.len() + 1;
        Budget::depth(depth)?;
        let attributes = self.attributes(start)?;
        let mut declarations = Vec::new();
        for attribute in &attributes {
            let prefix = match attribute.name.as_str() {
                "xmlns" => Some(PrefixDeclaration::Default),
                name if name.starts_with("xmlns:") => Some(PrefixDeclaration::Named(&name[6..])),
                _ => None,
            };
            if let Some(prefix) = prefix {
                declarations.push((prefix, checked_declaration(prefix, &attribute.value)?));
            }
        }
        self.resolver.set_level(depth as u16);
        for (prefix, uri) in declarations {
            self.resolver
                .add(prefix, Namespace(uri))
                .map_err(|error| match error {
                    NamespaceError::TooManyBindings(_) => SubscriptionXmlError::WorkLimitExceeded,
                    _ => SubscriptionXmlError::Malformed,
                })?;
        }
        let raw_name = start.name();
        if raw_name.as_ref().starts_with("xmlns:") {
            return Err(SubscriptionXmlError::Malformed);
        }
        let (namespace, local) = self.resolver.resolve_element(raw_name);
        let namespace = resolved_namespace(namespace)?;
        let local = local.as_ref();
        let node = match self.frames.last_mut() {
            None if !self.root_seen
                && namespace == Some(NamespaceKind::Atom)
                && local == "entry" =>
            {
                self.root_seen = true;
                Node::Entry
            }
            Some(parent)
                if matches!(parent.node, Node::Entry)
                    && !parent.child_seen
                    && namespace == Some(NamespaceKind::Atom)
                    && local == "content" =>
            {
                parent.child_seen = true;
                Node::Content
            }
            Some(parent)
                if matches!(parent.node, Node::Content)
                    && !parent.child_seen
                    && namespace == Some(NamespaceKind::ServiceBus)
                    && local == "SubscriptionDescription" =>
            {
                parent.child_seen = true;
                Node::Description
            }
            Some(parent)
                if matches!(parent.node, Node::Description)
                    && namespace == Some(NamespaceKind::ServiceBus) =>
            {
                self.budget.property()?;
                let property = Property::parse(local)?;
                if !self.properties.insert(property) {
                    return Err(SubscriptionXmlError::Malformed);
                }
                if property == Property::DefaultRule {
                    Node::Rule
                } else {
                    Node::Property(property)
                }
            }
            Some(parent)
                if matches!(parent.node, Node::Rule)
                    && namespace == Some(NamespaceKind::ServiceBus) =>
            {
                self.budget.property()?;
                match local {
                    "Name" if !self.rule.name => {
                        self.rule.name = true;
                        Node::RuleName
                    }
                    "Filter" if !self.rule.filter => {
                        self.rule.filter = true;
                        Node::Filter
                    }
                    "Name" | "Filter" => return Err(SubscriptionXmlError::Malformed),
                    _ => return Err(SubscriptionXmlError::UnsupportedDefinition),
                }
            }
            Some(parent)
                if matches!(parent.node, Node::Filter)
                    && namespace == Some(NamespaceKind::ServiceBus) =>
            {
                self.budget.property()?;
                match local {
                    "SqlExpression" if !self.rule.expression => {
                        self.rule.expression = true;
                        Node::SqlExpression
                    }
                    "Parameters" if !self.rule.parameters => {
                        self.rule.parameters = true;
                        Node::Parameters
                    }
                    "SqlExpression" | "Parameters" => return Err(SubscriptionXmlError::Malformed),
                    _ => return Err(SubscriptionXmlError::UnsupportedDefinition),
                }
            }
            Some(parent) if matches!(parent.node, Node::Parameters) => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            _ => return Err(SubscriptionXmlError::Malformed),
        };
        let mut expanded = BTreeSet::new();
        let mut content_type = false;
        let mut true_filter = false;
        for attribute in &attributes {
            if attribute.name == "xmlns" || attribute.name.starts_with("xmlns:") {
                continue;
            }
            let (namespace, local) = self.resolver.resolve_attribute(QName(&attribute.name));
            let namespace = resolved_namespace(namespace)?;
            let local = local.as_ref();
            if !expanded.insert((namespace, local.to_owned())) {
                return Err(SubscriptionXmlError::Malformed);
            }
            if matches!(node, Node::Content)
                && namespace.is_none()
                && local == "type"
                && attribute.value == "application/xml"
            {
                content_type = true;
            } else if matches!(node, Node::Filter)
                && namespace == Some(NamespaceKind::Xsi)
                && local == "type"
            {
                if attribute.value != "TrueFilter" {
                    return Err(SubscriptionXmlError::UnsupportedDefinition);
                }
                true_filter = true;
            } else {
                return Err(SubscriptionXmlError::Malformed);
            }
        }
        if matches!(node, Node::Content) && !content_type {
            return Err(SubscriptionXmlError::Malformed);
        }
        if matches!(node, Node::Filter) && !true_filter {
            return Err(SubscriptionXmlError::UnsupportedDefinition);
        }
        self.frames.push(Frame {
            raw_name: raw_name.as_ref().to_owned(),
            node,
            child_seen: false,
            scalar: String::new(),
        });
        Ok(())
    }

    fn close(&mut self, raw_name: &str) -> Result<(), SubscriptionXmlError> {
        lexical::qname(raw_name)?;
        let frame = self.frames.last().ok_or(SubscriptionXmlError::Malformed)?;
        if frame.raw_name != raw_name {
            return Err(SubscriptionXmlError::Malformed);
        }
        let (namespace, _) = self.resolver.resolve_element(QName(raw_name));
        let expected = match frame.node {
            Node::Entry | Node::Content => NamespaceKind::Atom,
            _ => NamespaceKind::ServiceBus,
        };
        if resolved_namespace(namespace)? != Some(expected) {
            return Err(SubscriptionXmlError::Malformed);
        }
        let frame = self.frames.pop().ok_or(SubscriptionXmlError::Malformed)?;
        match frame.node {
            Node::Entry | Node::Content if !frame.child_seen => {
                return Err(SubscriptionXmlError::Malformed);
            }
            Node::Rule if !self.rule.name || !self.rule.filter => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Node::Filter if !self.rule.expression => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Node::RuleName if frame.scalar != "$Default" => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Node::SqlExpression if frame.scalar != "1=1" => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Node::Property(property) => self.property(property, &frame.scalar)?,
            _ => {}
        }
        self.resolver.pop();
        if matches!(frame.node, Node::Entry) {
            self.complete = true;
        }
        Ok(())
    }

    fn text(&mut self, text: &str, reference: bool) -> Result<(), SubscriptionXmlError> {
        if !lexical::legal_chars(text) {
            return Err(SubscriptionXmlError::Malformed);
        }
        self.budget.decoded(text.len())?;
        let Some(frame) = self.frames.last_mut() else {
            return if !reference && text.chars().all(lexical::xml_space) {
                Ok(())
            } else {
                Err(SubscriptionXmlError::Malformed)
            };
        };
        if frame.node.scalar() {
            let mut length = frame.scalar.len();
            bounded_add(&mut length, text.len(), MAX_BODY_BYTES)?;
            frame.scalar.push_str(text);
        } else if reference || !text.chars().all(lexical::xml_space) {
            return Err(SubscriptionXmlError::Malformed);
        }
        Ok(())
    }

    fn reference(&mut self, reference: &BytesRef<'_>) -> Result<(), SubscriptionXmlError> {
        if let Some(value) = reference
            .resolve_char_ref()
            .map_err(|_| SubscriptionXmlError::Malformed)?
        {
            let mut bytes = [0_u8; 4];
            self.text(value.encode_utf8(&mut bytes), true)
        } else {
            let value = quick_xml::escape::resolve_xml_entity(reference.as_ref())
                .ok_or(SubscriptionXmlError::Malformed)?;
            self.text(value, true)
        }
    }

    fn property(&mut self, property: Property, scalar: &str) -> Result<(), SubscriptionXmlError> {
        let value = lexical::trim(scalar);
        match property {
            Property::Lock => self.config.lock_duration_millis = duration::parse(value)?,
            Property::Session if boolean(value)? => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Property::Session => self.config.requires_session = false,
            Property::Ttl => {
                self.config.default_time_to_live_millis = Some(duration::parse(value)?)
            }
            Property::ExpiryDeadLetter => {
                self.config.dead_lettering_on_message_expiration = boolean(value)?
            }
            Property::FilterDeadLetter => {
                self.config.dead_lettering_on_filter_evaluation_exceptions = boolean(value)?
            }
            Property::Deliveries => {
                let count = duration::integer(value)?;
                if !(1..=i32::MAX as u64).contains(&count) {
                    return Err(SubscriptionXmlError::InvalidDefinition);
                }
                self.config.max_delivery_count = count as u32;
            }
            Property::Batched if boolean(value)? => {}
            Property::Status if value == "Active" => {}
            Property::Batched | Property::Status => {
                return Err(SubscriptionXmlError::UnsupportedDefinition);
            }
            Property::DefaultRule => return Err(SubscriptionXmlError::Malformed),
        }
        Ok(())
    }
}

fn boolean(value: &str) -> Result<bool, SubscriptionXmlError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(SubscriptionXmlError::InvalidDefinition),
    }
}

fn checked_declaration<'a>(
    prefix: PrefixDeclaration<'_>,
    value: &'a str,
) -> Result<&'a str, SubscriptionXmlError> {
    match prefix {
        PrefixDeclaration::Named("xmlns") => Err(SubscriptionXmlError::Malformed),
        PrefixDeclaration::Named("xml") if value == XML_NS => Ok(XML_NS),
        PrefixDeclaration::Named("xml") => Err(SubscriptionXmlError::Malformed),
        PrefixDeclaration::Default if value.is_empty() => Ok(""),
        _ => match value {
            ATOM_NS => Ok(ATOM_NS),
            SERVICE_BUS_NS => Ok(SERVICE_BUS_NS),
            XSI_NS => Ok(XSI_NS),
            _ => Err(SubscriptionXmlError::Malformed),
        },
    }
}

fn declaration(raw: &str, budget: &mut Budget) -> Result<(), SubscriptionXmlError> {
    let tail = raw
        .strip_prefix("xml")
        .ok_or(SubscriptionXmlError::Malformed)?;
    if !tail.chars().next().is_some_and(lexical::xml_space) {
        return Err(SubscriptionXmlError::Malformed);
    }
    lexical::attribute_tail(tail)?;
    let start = BytesStart::from_content(raw, 3);
    let mut position = 0;
    let mut encoding_seen = false;
    let mut standalone_seen = false;
    for attribute in start.attributes().with_checks(true) {
        Budget::attributes(position + 1)?;
        let attribute = attribute.map_err(|_| SubscriptionXmlError::Malformed)?;
        budget.decoded(attribute.value.len())?;
        let key = attribute.key.as_ref();
        let value = attribute.value.as_ref();
        match (position, key) {
            (0, "version") if value == "1.0" => {}
            (_, "encoding")
                if position == 1
                    && !encoding_seen
                    && !standalone_seen
                    && value.eq_ignore_ascii_case("UTF-8") =>
            {
                encoding_seen = true
            }
            (_, "standalone")
                if position >= 1 && !standalone_seen && matches!(value, "yes" | "no") =>
            {
                standalone_seen = true
            }
            _ => return Err(SubscriptionXmlError::Malformed),
        }
        position += 1;
    }
    if position == 0 {
        return Err(SubscriptionXmlError::Malformed);
    }
    Ok(())
}

pub(crate) fn decode_definition(body: &[u8]) -> Result<SubscriptionConfig, SubscriptionXmlError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(SubscriptionXmlError::WorkLimitExceeded);
    }
    let document = std::str::from_utf8(body).map_err(|_| SubscriptionXmlError::Malformed)?;
    let document = document.strip_prefix('\u{FEFF}').unwrap_or(document);
    if document.starts_with('\u{FEFF}') || !lexical::legal_chars(document) {
        return Err(SubscriptionXmlError::Malformed);
    }
    let mut reader = Reader::from_str(document);
    let config = reader.config_mut();
    config.allow_dangling_amp = false;
    config.allow_unmatched_ends = false;
    config.check_comments = true;
    config.check_end_names = true;
    config.expand_empty_elements = false;
    config.trim_markup_names_in_closing_tags = true;
    config.trim_text(false);
    let mut parser = Parser::new();
    loop {
        parser.budget.event()?;
        match reader
            .read_event()
            .map_err(|_| SubscriptionXmlError::Malformed)?
        {
            Event::Start(start) => parser.open(&start)?,
            Event::Empty(start) => {
                parser.open(&start)?;
                parser.close(start.name().as_ref())?;
            }
            Event::End(end) => parser.close(end.name().as_ref())?,
            Event::Text(text) => {
                if text.as_ref().contains("]]>") {
                    return Err(SubscriptionXmlError::Malformed);
                }
                parser.text(&text.xml10_content(), false)?;
            }
            Event::GeneralRef(reference) => parser.reference(&reference)?,
            Event::Decl(decl) if parser.budget.events == 1 => {
                declaration(decl.as_ref(), &mut parser.budget)?
            }
            Event::Eof
                if parser.complete && parser.frames.is_empty() && parser.resolver.level() == 0 =>
            {
                validate_config(&parser.config)?;
                return Ok(parser.config);
            }
            _ => return Err(SubscriptionXmlError::Malformed),
        }
    }
}
