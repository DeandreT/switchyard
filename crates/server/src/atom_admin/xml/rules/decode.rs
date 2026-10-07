use std::collections::BTreeSet;

use domain::{RuleFilter, RuleName};
use quick_xml::{
    Reader, XmlVersion,
    events::{BytesRef, BytesStart, Event},
    name::{Namespace, NamespaceError, NamespaceResolver, PrefixDeclaration, QName, ResolveResult},
};

use super::super::{
    ATOM_NS, Budget, MAX_BODY_BYTES, MAX_NAMESPACE_BINDINGS, SERVICE_BUS_NS, XML_NS, XSI_NS,
    bounded_add, lexical,
};
use super::{RuleXmlError, validate_definition};
use crate::AtomRuleDefinition;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NamespaceKind {
    Atom,
    ServiceBus,
    Xsi,
    Xml,
}

fn namespace_kind(uri: &str) -> Result<NamespaceKind, RuleXmlError> {
    match uri {
        ATOM_NS => Ok(NamespaceKind::Atom),
        SERVICE_BUS_NS => Ok(NamespaceKind::ServiceBus),
        XSI_NS => Ok(NamespaceKind::Xsi),
        XML_NS => Ok(NamespaceKind::Xml),
        _ => Err(RuleXmlError::Malformed),
    }
}

fn resolved_namespace(result: ResolveResult<'_>) -> Result<Option<NamespaceKind>, RuleXmlError> {
    match result {
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Bound(uri) => namespace_kind(uri.as_ref()).map(Some),
        ResolveResult::Unknown(_) => Err(RuleXmlError::Malformed),
    }
}

#[derive(Clone, Copy)]
enum Node {
    Entry,
    Content,
    Description,
    Name,
    Filter,
    SqlExpression,
    Parameters,
}

impl Node {
    fn scalar(self) -> bool {
        matches!(self, Self::Name | Self::SqlExpression)
    }
}

#[derive(Clone, Copy)]
enum FilterKind {
    True,
    False,
}

impl FilterKind {
    fn expression(self) -> &'static str {
        match self {
            Self::True => "1=1",
            Self::False => "1=0",
        }
    }

    fn filter(self) -> RuleFilter {
        match self {
            Self::True => RuleFilter::True,
            Self::False => RuleFilter::False,
        }
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

struct Parser {
    resolver: NamespaceResolver,
    budget: Budget,
    frames: Vec<Frame>,
    root_seen: bool,
    complete: bool,
    name_seen: bool,
    filter_seen: bool,
    expression_seen: bool,
    parameters_seen: bool,
    name: Option<RuleName>,
    filter: Option<FilterKind>,
}

impl Parser {
    fn new() -> Self {
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(MAX_NAMESPACE_BINDINGS);
        Self {
            resolver,
            budget: Budget::default(),
            frames: Vec::new(),
            root_seen: false,
            complete: false,
            name_seen: false,
            filter_seen: false,
            expression_seen: false,
            parameters_seen: false,
            name: None,
            filter: None,
        }
    }

    fn attributes(
        &mut self,
        start: &BytesStart<'_>,
    ) -> Result<Vec<CheckedAttribute>, RuleXmlError> {
        let raw_name = start.name();
        lexical::qname(raw_name.as_ref())?;
        lexical::attribute_tail(&start.as_ref()[raw_name.as_ref().len()..])?;
        let mut attributes = Vec::new();
        for attribute in start.attributes().with_checks(true) {
            Budget::attributes(attributes.len() + 1)?;
            let attribute = attribute.map_err(|_| RuleXmlError::Malformed)?;
            lexical::qname(attribute.key.as_ref())?;
            let value = attribute
                .normalized_value_with(
                    XmlVersion::Explicit1_0,
                    1,
                    quick_xml::escape::resolve_xml_entity,
                )
                .map_err(|_| RuleXmlError::Malformed)?;
            if !lexical::legal_chars(&value) {
                return Err(RuleXmlError::Malformed);
            }
            self.budget.decoded(value.len())?;
            attributes.push(CheckedAttribute {
                name: attribute.key.as_ref().to_owned(),
                value: value.into_owned(),
            });
        }
        Ok(attributes)
    }

    fn open(&mut self, start: &BytesStart<'_>) -> Result<(), RuleXmlError> {
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
                    NamespaceError::TooManyBindings(_) => RuleXmlError::WorkLimitExceeded,
                    _ => RuleXmlError::Malformed,
                })?;
        }
        let raw_name = start.name();
        if raw_name.as_ref().starts_with("xmlns:") {
            return Err(RuleXmlError::Malformed);
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
                    && local == "RuleDescription" =>
            {
                parent.child_seen = true;
                Node::Description
            }
            Some(parent)
                if matches!(parent.node, Node::Description)
                    && namespace == Some(NamespaceKind::ServiceBus) =>
            {
                self.budget.property()?;
                match local {
                    "Name" if !self.name_seen => {
                        self.name_seen = true;
                        Node::Name
                    }
                    "Filter" if !self.filter_seen => {
                        self.filter_seen = true;
                        Node::Filter
                    }
                    "Name" | "Filter" => return Err(RuleXmlError::Malformed),
                    _ => return Err(RuleXmlError::UnsupportedDefinition),
                }
            }
            Some(parent)
                if matches!(parent.node, Node::Filter)
                    && namespace == Some(NamespaceKind::ServiceBus) =>
            {
                self.budget.property()?;
                match local {
                    "SqlExpression" if !self.expression_seen => {
                        self.expression_seen = true;
                        Node::SqlExpression
                    }
                    "Parameters" if !self.parameters_seen => {
                        self.parameters_seen = true;
                        Node::Parameters
                    }
                    "SqlExpression" | "Parameters" => return Err(RuleXmlError::Malformed),
                    _ => return Err(RuleXmlError::UnsupportedDefinition),
                }
            }
            Some(parent) if matches!(parent.node, Node::Parameters) => {
                return Err(RuleXmlError::UnsupportedDefinition);
            }
            _ => return Err(RuleXmlError::Malformed),
        };
        let mut expanded = BTreeSet::new();
        let mut content_type = false;
        let mut filter_type = None;
        for attribute in &attributes {
            if attribute.name == "xmlns" || attribute.name.starts_with("xmlns:") {
                continue;
            }
            let (namespace, local) = self.resolver.resolve_attribute(QName(&attribute.name));
            let namespace = resolved_namespace(namespace)?;
            let local = local.as_ref();
            if !expanded.insert((namespace, local.to_owned())) {
                return Err(RuleXmlError::Malformed);
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
                let kind = match attribute.value.as_str() {
                    "TrueFilter" => FilterKind::True,
                    "FalseFilter" => FilterKind::False,
                    _ => return Err(RuleXmlError::UnsupportedDefinition),
                };
                let (namespace, _) = self.resolver.resolve_element(QName(&attribute.value));
                if resolved_namespace(namespace)? != Some(NamespaceKind::ServiceBus) {
                    return Err(RuleXmlError::Malformed);
                }
                filter_type = Some(kind);
            } else {
                return Err(RuleXmlError::Malformed);
            }
        }
        if matches!(node, Node::Content) && !content_type {
            return Err(RuleXmlError::Malformed);
        }
        if matches!(node, Node::Filter) {
            self.filter = Some(filter_type.ok_or(RuleXmlError::InvalidDefinition)?);
        }
        self.frames.push(Frame {
            raw_name: raw_name.as_ref().to_owned(),
            node,
            child_seen: false,
            scalar: String::new(),
        });
        Ok(())
    }

    fn close(&mut self, raw_name: &str) -> Result<(), RuleXmlError> {
        lexical::qname(raw_name)?;
        let frame = self.frames.last().ok_or(RuleXmlError::Malformed)?;
        if frame.raw_name != raw_name {
            return Err(RuleXmlError::Malformed);
        }
        let (namespace, _) = self.resolver.resolve_element(QName(raw_name));
        let expected = match frame.node {
            Node::Entry | Node::Content => NamespaceKind::Atom,
            _ => NamespaceKind::ServiceBus,
        };
        if resolved_namespace(namespace)? != Some(expected) {
            return Err(RuleXmlError::Malformed);
        }
        let frame = self.frames.pop().ok_or(RuleXmlError::Malformed)?;
        match frame.node {
            Node::Entry | Node::Content if !frame.child_seen => {
                return Err(RuleXmlError::Malformed);
            }
            Node::Description if !self.name_seen || !self.filter_seen => {
                return Err(RuleXmlError::InvalidDefinition);
            }
            Node::Filter if !self.expression_seen => {
                return Err(RuleXmlError::InvalidDefinition);
            }
            Node::Name => {
                self.name =
                    Some(RuleName::new(frame.scalar).map_err(|_| RuleXmlError::InvalidDefinition)?);
            }
            Node::SqlExpression
                if frame.scalar
                    != self
                        .filter
                        .ok_or(RuleXmlError::InvalidDefinition)?
                        .expression() =>
            {
                return Err(RuleXmlError::InvalidDefinition);
            }
            _ => {}
        }
        self.resolver.pop();
        if matches!(frame.node, Node::Entry) {
            self.complete = true;
        }
        Ok(())
    }

    fn text(&mut self, text: &str, reference: bool) -> Result<(), RuleXmlError> {
        if !lexical::legal_chars(text) {
            return Err(RuleXmlError::Malformed);
        }
        self.budget.decoded(text.len())?;
        let Some(frame) = self.frames.last_mut() else {
            return if !reference && text.chars().all(lexical::xml_space) {
                Ok(())
            } else {
                Err(RuleXmlError::Malformed)
            };
        };
        if frame.node.scalar() {
            let mut length = frame.scalar.len();
            bounded_add(&mut length, text.len(), MAX_BODY_BYTES)?;
            frame.scalar.push_str(text);
        } else if reference || !text.chars().all(lexical::xml_space) {
            return Err(RuleXmlError::Malformed);
        }
        Ok(())
    }

    fn reference(&mut self, reference: &BytesRef<'_>) -> Result<(), RuleXmlError> {
        if let Some(value) = reference
            .resolve_char_ref()
            .map_err(|_| RuleXmlError::Malformed)?
        {
            let mut bytes = [0_u8; 4];
            self.text(value.encode_utf8(&mut bytes), true)
        } else {
            let value = quick_xml::escape::resolve_xml_entity(reference.as_ref())
                .ok_or(RuleXmlError::Malformed)?;
            self.text(value, true)
        }
    }
}

fn checked_declaration<'a>(
    prefix: PrefixDeclaration<'_>,
    value: &'a str,
) -> Result<&'a str, RuleXmlError> {
    match prefix {
        PrefixDeclaration::Named("xmlns") => Err(RuleXmlError::Malformed),
        PrefixDeclaration::Named("xml") if value == XML_NS => Ok(XML_NS),
        PrefixDeclaration::Named("xml") => Err(RuleXmlError::Malformed),
        PrefixDeclaration::Default if value.is_empty() => Ok(""),
        _ => match value {
            ATOM_NS => Ok(ATOM_NS),
            SERVICE_BUS_NS => Ok(SERVICE_BUS_NS),
            XSI_NS => Ok(XSI_NS),
            _ => Err(RuleXmlError::Malformed),
        },
    }
}

fn declaration(raw: &str, budget: &mut Budget) -> Result<(), RuleXmlError> {
    let tail = raw.strip_prefix("xml").ok_or(RuleXmlError::Malformed)?;
    if !tail.chars().next().is_some_and(lexical::xml_space) {
        return Err(RuleXmlError::Malformed);
    }
    lexical::attribute_tail(tail)?;
    let start = BytesStart::from_content(raw, 3);
    let mut position = 0;
    let mut encoding_seen = false;
    let mut standalone_seen = false;
    for attribute in start.attributes().with_checks(true) {
        Budget::attributes(position + 1)?;
        let attribute = attribute.map_err(|_| RuleXmlError::Malformed)?;
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
            _ => return Err(RuleXmlError::Malformed),
        }
        position += 1;
    }
    if position == 0 {
        return Err(RuleXmlError::Malformed);
    }
    Ok(())
}

pub(crate) fn decode_definition(body: &[u8]) -> Result<AtomRuleDefinition, RuleXmlError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(RuleXmlError::WorkLimitExceeded);
    }
    let document = std::str::from_utf8(body).map_err(|_| RuleXmlError::Malformed)?;
    let document = document.strip_prefix('\u{FEFF}').unwrap_or(document);
    if document.starts_with('\u{FEFF}') || !lexical::legal_chars(document) {
        return Err(RuleXmlError::Malformed);
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
        match reader.read_event().map_err(|_| RuleXmlError::Malformed)? {
            Event::Start(start) => parser.open(&start)?,
            Event::Empty(start) => {
                parser.open(&start)?;
                parser.close(start.name().as_ref())?;
            }
            Event::End(end) => parser.close(end.name().as_ref())?,
            Event::Text(text) => {
                if text.as_ref().contains("]]>") {
                    return Err(RuleXmlError::Malformed);
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
                let definition = AtomRuleDefinition {
                    name: parser.name.ok_or(RuleXmlError::InvalidDefinition)?,
                    filter: parser
                        .filter
                        .ok_or(RuleXmlError::InvalidDefinition)?
                        .filter(),
                };
                validate_definition(&definition)?;
                return Ok(definition);
            }
            _ => return Err(RuleXmlError::Malformed),
        }
    }
}
