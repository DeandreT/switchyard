use std::collections::BTreeSet;

use domain::{FiniteQueueCapacity, QueueConfig};
use quick_xml::{
    Reader, XmlVersion,
    events::{BytesRef, BytesStart, Event},
    name::{Namespace, NamespaceError, NamespaceResolver, PrefixDeclaration, QName, ResolveResult},
};

use super::{
    ATOM_NS, AtomQueueDefinition, AtomXmlError, Budget, MAX_BODY_BYTES, MAX_NAMESPACE_BINDINGS,
    MIB, SERVICE_BUS_NS, XML_NS, XSI_NS, bounded_add, duration, lexical, validate_definition,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NamespaceKind {
    Atom,
    ServiceBus,
    Xsi,
    Xml,
}

fn namespace_kind(uri: &str) -> Result<NamespaceKind, AtomXmlError> {
    match uri {
        ATOM_NS => Ok(NamespaceKind::Atom),
        SERVICE_BUS_NS => Ok(NamespaceKind::ServiceBus),
        XSI_NS => Ok(NamespaceKind::Xsi),
        XML_NS => Ok(NamespaceKind::Xml),
        _ => Err(AtomXmlError::Malformed),
    }
}

fn resolved_namespace(result: ResolveResult<'_>) -> Result<Option<NamespaceKind>, AtomXmlError> {
    match result {
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Bound(uri) => namespace_kind(uri.as_ref()).map(Some),
        ResolveResult::Unknown(_) => Err(AtomXmlError::Malformed),
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Property {
    MaxSize,
    Lock,
    Ttl,
    Deliveries,
    ExpiryDeadLetter,
    Session,
    Duplicate,
    DuplicateWindow,
    MessageSize,
    Batched,
    Status,
    Anonymous,
    Partitioning,
    Express,
    Ordering,
    Rules,
    Forward,
    ForwardDeadLetter,
    Metadata,
    AutoDelete,
}

impl Property {
    fn parse(name: &str) -> Result<Self, AtomXmlError> {
        match name {
            "MaxSizeInMegabytes" => Ok(Self::MaxSize),
            "LockDuration" => Ok(Self::Lock),
            "DefaultMessageTimeToLive" => Ok(Self::Ttl),
            "MaxDeliveryCount" => Ok(Self::Deliveries),
            "DeadLetteringOnMessageExpiration" => Ok(Self::ExpiryDeadLetter),
            "RequiresSession" => Ok(Self::Session),
            "RequiresDuplicateDetection" => Ok(Self::Duplicate),
            "DuplicateDetectionHistoryTimeWindow" => Ok(Self::DuplicateWindow),
            "MaxMessageSizeInKilobytes" => Ok(Self::MessageSize),
            "EnableBatchedOperations" => Ok(Self::Batched),
            "Status" => Ok(Self::Status),
            "IsAnonymousAccessible" => Ok(Self::Anonymous),
            "EnablePartitioning" => Ok(Self::Partitioning),
            "EnableExpress" => Ok(Self::Express),
            "SupportOrdering" => Ok(Self::Ordering),
            "AuthorizationRules" => Ok(Self::Rules),
            "ForwardTo" => Ok(Self::Forward),
            "ForwardDeadLetteredMessagesTo" => Ok(Self::ForwardDeadLetter),
            "UserMetadata" => Ok(Self::Metadata),
            "AutoDeleteOnIdle" => Ok(Self::AutoDelete),
            _ => Err(AtomXmlError::UnsupportedDefinition),
        }
    }
}

#[derive(Clone, Copy)]
enum Node {
    Entry,
    Content,
    Description,
    Property(Property),
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
    properties: BTreeSet<Property>,
    definition: AtomQueueDefinition,
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
            definition: AtomQueueDefinition {
                config: QueueConfig {
                    duplicate_detection_history_time_window_millis: 60_000,
                    ..QueueConfig::default()
                },
                limit: FiniteQueueCapacity::new(1_024 * MIB)
                    .expect("the static default is nonzero"),
            },
            root_seen: false,
            complete: false,
        }
    }

    fn attributes(
        &mut self,
        start: &BytesStart<'_>,
    ) -> Result<Vec<CheckedAttribute>, AtomXmlError> {
        let raw_name = start.name();
        lexical::qname(raw_name.as_ref())?;
        lexical::attribute_tail(&start.as_ref()[raw_name.as_ref().len()..])?;
        let mut attributes = Vec::new();
        for attribute in start.attributes().with_checks(true) {
            Budget::attributes(attributes.len() + 1)?;
            let attribute = attribute.map_err(|_| AtomXmlError::Malformed)?;
            lexical::qname(attribute.key.as_ref())?;
            let value = attribute
                .normalized_value_with(
                    XmlVersion::Explicit1_0,
                    1,
                    quick_xml::escape::resolve_xml_entity,
                )
                .map_err(|_| AtomXmlError::Malformed)?;
            if !lexical::legal_chars(&value) {
                return Err(AtomXmlError::Malformed);
            }
            self.budget.decoded(value.len())?;
            attributes.push(CheckedAttribute {
                name: attribute.key.as_ref().to_owned(),
                value: value.into_owned(),
            });
        }
        Ok(attributes)
    }

    fn open(&mut self, start: &BytesStart<'_>) -> Result<(), AtomXmlError> {
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
                let uri = checked_declaration(prefix, &attribute.value)?;
                declarations.push((prefix, uri));
            }
        }
        self.resolver.set_level(depth as u16);
        for (prefix, uri) in declarations {
            self.resolver
                .add(prefix, Namespace(uri))
                .map_err(|error| match error {
                    NamespaceError::TooManyBindings(_) => AtomXmlError::WorkLimitExceeded,
                    _ => AtomXmlError::Malformed,
                })?;
        }
        let raw_name = start.name();
        if raw_name.as_ref().starts_with("xmlns:") {
            return Err(AtomXmlError::Malformed);
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
                    && local == "QueueDescription" =>
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
                    return Err(AtomXmlError::Malformed);
                }
                Node::Property(property)
            }
            _ => return Err(AtomXmlError::Malformed),
        };
        let mut expanded = BTreeSet::new();
        let mut content_type = false;
        for attribute in &attributes {
            if attribute.name == "xmlns" || attribute.name.starts_with("xmlns:") {
                continue;
            }
            let (namespace, local) = self.resolver.resolve_attribute(QName(&attribute.name));
            let namespace = resolved_namespace(namespace)?;
            let local = local.as_ref();
            if !expanded.insert((namespace, local.to_owned())) {
                return Err(AtomXmlError::Malformed);
            }
            if matches!(node, Node::Content)
                && namespace.is_none()
                && local == "type"
                && attribute.value == "application/xml"
            {
                content_type = true;
            } else {
                return Err(AtomXmlError::Malformed);
            }
        }
        if matches!(node, Node::Content) && !content_type {
            return Err(AtomXmlError::Malformed);
        }
        self.frames.push(Frame {
            raw_name: raw_name.as_ref().to_owned(),
            node,
            child_seen: false,
            scalar: String::new(),
        });
        Ok(())
    }

    fn close(&mut self, raw_name: &str) -> Result<(), AtomXmlError> {
        lexical::qname(raw_name)?;
        let frame = self.frames.last().ok_or(AtomXmlError::Malformed)?;
        if frame.raw_name != raw_name {
            return Err(AtomXmlError::Malformed);
        }
        let (namespace, _) = self.resolver.resolve_element(QName(raw_name));
        let expected = match frame.node {
            Node::Entry | Node::Content => NamespaceKind::Atom,
            Node::Description | Node::Property(_) => NamespaceKind::ServiceBus,
        };
        if resolved_namespace(namespace)? != Some(expected) {
            return Err(AtomXmlError::Malformed);
        }
        let frame = self.frames.pop().ok_or(AtomXmlError::Malformed)?;
        match frame.node {
            Node::Entry | Node::Content if !frame.child_seen => {
                return Err(AtomXmlError::Malformed);
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

    fn text(&mut self, text: &str, reference: bool) -> Result<(), AtomXmlError> {
        if !lexical::legal_chars(text) {
            return Err(AtomXmlError::Malformed);
        }
        self.budget.decoded(text.len())?;
        let Some(frame) = self.frames.last_mut() else {
            return if !reference && text.chars().all(lexical::xml_space) {
                Ok(())
            } else {
                Err(AtomXmlError::Malformed)
            };
        };
        if matches!(frame.node, Node::Property(property) if property != Property::Rules) {
            let mut length = frame.scalar.len();
            bounded_add(&mut length, text.len(), MAX_BODY_BYTES)?;
            frame.scalar.push_str(text);
        } else if reference || !text.chars().all(lexical::xml_space) {
            return Err(AtomXmlError::Malformed);
        }
        Ok(())
    }

    fn reference(&mut self, reference: &BytesRef<'_>) -> Result<(), AtomXmlError> {
        if let Some(value) = reference
            .resolve_char_ref()
            .map_err(|_| AtomXmlError::Malformed)?
        {
            let mut bytes = [0_u8; 4];
            self.text(value.encode_utf8(&mut bytes), true)
        } else {
            let value = quick_xml::escape::resolve_xml_entity(reference.as_ref())
                .ok_or(AtomXmlError::Malformed)?;
            self.text(value, true)
        }
    }

    fn property(&mut self, property: Property, scalar: &str) -> Result<(), AtomXmlError> {
        let value = lexical::trim(scalar);
        let config = &mut self.definition.config;
        match property {
            Property::MaxSize => {
                let mib = positive_i32(value)?;
                self.definition.limit = FiniteQueueCapacity::new(
                    mib.checked_mul(MIB)
                        .ok_or(AtomXmlError::InvalidDefinition)?,
                )
                .map_err(|_| AtomXmlError::InvalidDefinition)?;
            }
            Property::Lock => config.lock_duration_millis = duration::parse(value)?,
            Property::Ttl => config.default_time_to_live_millis = Some(duration::parse(value)?),
            Property::Deliveries => config.max_delivery_count = positive_i32(value)? as u32,
            Property::ExpiryDeadLetter => {
                config.dead_lettering_on_message_expiration = boolean(value)?
            }
            Property::Session => config.requires_session = supported_false(value)?,
            Property::Duplicate => config.requires_duplicate_detection = supported_false(value)?,
            Property::DuplicateWindow => {
                config.duplicate_detection_history_time_window_millis = duration::parse(value)?
            }
            Property::MessageSize => {
                let kib = duration::integer(value)?;
                if !(1..=256).contains(&kib) {
                    return Err(AtomXmlError::InvalidDefinition);
                }
                config.max_message_bytes = kib as usize * 1_024;
            }
            Property::Batched if boolean(value)? => {}
            Property::Batched => return Err(AtomXmlError::UnsupportedDefinition),
            Property::Status if value == "Active" => {}
            Property::Status => return Err(AtomXmlError::UnsupportedDefinition),
            Property::Anonymous
            | Property::Partitioning
            | Property::Express
            | Property::Ordering => {
                supported_false(value)?;
            }
            Property::Rules => {}
            Property::Forward | Property::ForwardDeadLetter | Property::Metadata
                if scalar.is_empty() => {}
            Property::Forward
            | Property::ForwardDeadLetter
            | Property::Metadata
            | Property::AutoDelete => {
                return Err(AtomXmlError::UnsupportedDefinition);
            }
        }
        Ok(())
    }
}

fn positive_i32(value: &str) -> Result<u64, AtomXmlError> {
    let value = duration::integer(value)?;
    if !(1..=i32::MAX as u64).contains(&value) {
        return Err(AtomXmlError::InvalidDefinition);
    }
    Ok(value)
}

fn boolean(value: &str) -> Result<bool, AtomXmlError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(AtomXmlError::InvalidDefinition),
    }
}

fn supported_false(value: &str) -> Result<bool, AtomXmlError> {
    if boolean(value)? {
        return Err(AtomXmlError::UnsupportedDefinition);
    }
    Ok(false)
}

fn checked_declaration(
    prefix: PrefixDeclaration<'_>,
    value: &str,
) -> Result<&'static str, AtomXmlError> {
    match prefix {
        PrefixDeclaration::Named("xmlns") => Err(AtomXmlError::Malformed),
        PrefixDeclaration::Named("xml") if value == XML_NS => Ok(XML_NS),
        PrefixDeclaration::Named("xml") => Err(AtomXmlError::Malformed),
        PrefixDeclaration::Default if value.is_empty() => Ok(""),
        _ => match value {
            ATOM_NS => Ok(ATOM_NS),
            SERVICE_BUS_NS => Ok(SERVICE_BUS_NS),
            XSI_NS => Ok(XSI_NS),
            _ => Err(AtomXmlError::Malformed),
        },
    }
}

fn declaration(raw: &str, budget: &mut Budget) -> Result<(), AtomXmlError> {
    let tail = raw.strip_prefix("xml").ok_or(AtomXmlError::Malformed)?;
    if !tail.chars().next().is_some_and(lexical::xml_space) {
        return Err(AtomXmlError::Malformed);
    }
    lexical::attribute_tail(tail)?;
    let start = BytesStart::from_content(raw, 3);
    let mut position = 0;
    let mut encoding_seen = false;
    let mut standalone_seen = false;
    for attribute in start.attributes().with_checks(true) {
        Budget::attributes(position + 1)?;
        let attribute = attribute.map_err(|_| AtomXmlError::Malformed)?;
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
                standalone_seen = true;
            }
            _ => return Err(AtomXmlError::Malformed),
        }
        position += 1;
    }
    if position == 0 {
        return Err(AtomXmlError::Malformed);
    }
    Ok(())
}

pub(crate) fn decode_definition(body: &[u8]) -> Result<AtomQueueDefinition, AtomXmlError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(AtomXmlError::WorkLimitExceeded);
    }
    let document = std::str::from_utf8(body).map_err(|_| AtomXmlError::Malformed)?;
    let document = document.strip_prefix('\u{FEFF}').unwrap_or(document);
    // Reader also strips a leading BOM; do not let it consume a second one.
    if document.starts_with('\u{FEFF}') || !lexical::legal_chars(document) {
        return Err(AtomXmlError::Malformed);
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
        let event = reader.read_event().map_err(|_| AtomXmlError::Malformed)?;
        match event {
            Event::Start(start) => parser.open(&start)?,
            Event::Empty(start) => {
                parser.open(&start)?;
                parser.close(start.name().as_ref())?;
            }
            Event::End(end) => parser.close(end.name().as_ref())?,
            Event::Text(text) => {
                if text.as_ref().contains("]]>") {
                    return Err(AtomXmlError::Malformed);
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
                validate_definition(parser.definition)?;
                return Ok(parser.definition);
            }
            _ => return Err(AtomXmlError::Malformed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn captured_scalar(fragment: &str) -> Result<String, AtomXmlError> {
        let mut parser = Parser::new();
        parser.frames.push(Frame {
            raw_name: String::from("UserMetadata"),
            node: Node::Property(Property::Metadata),
            child_seen: false,
            scalar: String::new(),
        });
        let mut reader = Reader::from_str(fragment);
        loop {
            match reader.read_event().map_err(|_| AtomXmlError::Malformed)? {
                Event::Text(text) => parser.text(&text.xml10_content(), false)?,
                Event::GeneralRef(reference) => parser.reference(&reference)?,
                Event::Eof => return Ok(parser.frames.pop().unwrap().scalar),
                _ => return Err(AtomXmlError::Malformed),
            }
        }
    }

    #[test]
    fn scalar_observation_decodes_once_and_does_not_renormalize_referenced_cr() {
        assert_eq!(captured_scalar("&amp;lt;"), Ok(String::from("&lt;")));
        assert_eq!(
            captured_scalar("&lt;&gt;&amp;&apos;&quot;"),
            Ok(String::from("<>&'\""))
        );
        assert_eq!(
            captured_scalar("literal\r\nline\rref&#13;"),
            Ok(String::from("literal\nline\nref\r"))
        );
        assert_eq!(captured_scalar("]]&gt;"), Ok(String::from("]]>")));
        assert_eq!(captured_scalar("&#xFFFE;"), Err(AtomXmlError::Malformed));
    }

    #[test]
    fn scalar_scratch_and_decoded_budgets_are_checked_before_append() {
        let mut parser = Parser::new();
        parser.frames.push(Frame {
            raw_name: String::from("UserMetadata"),
            node: Node::Property(Property::Metadata),
            child_seen: false,
            scalar: String::new(),
        });
        assert_eq!(parser.text(&"x".repeat(MAX_BODY_BYTES), false), Ok(()));
        let original = parser.frames[0].scalar.clone();
        assert_eq!(
            parser.text("x", false),
            Err(AtomXmlError::WorkLimitExceeded)
        );
        assert_eq!(parser.frames[0].scalar, original);
        // Independently reach the scratch bound without consuming decoded credit.
        parser.budget.decoded = 0;
        assert_eq!(
            parser.text("x", false),
            Err(AtomXmlError::WorkLimitExceeded)
        );
        assert_eq!(parser.frames[0].scalar, original);
    }

    #[test]
    fn cumulative_decoded_text_includes_structural_whitespace_and_scalars_once() {
        let mut parser = Parser::new();
        assert_eq!(parser.text(" \n", false), Ok(()));
        assert_eq!(parser.budget.decoded, 2);
        parser.frames.push(Frame {
            raw_name: String::from("entry"),
            node: Node::Entry,
            child_seen: false,
            scalar: String::new(),
        });
        assert_eq!(parser.text("\t", false), Ok(()));
        assert_eq!(parser.budget.decoded, 3);
        parser.frames.push(Frame {
            raw_name: String::from("UserMetadata"),
            node: Node::Property(Property::Metadata),
            child_seen: false,
            scalar: String::new(),
        });
        assert_eq!(parser.text("x", false), Ok(()));
        assert_eq!(parser.budget.decoded, 4);
        assert_eq!(parser.frames[1].scalar, "x");
        parser.budget.decoded = MAX_BODY_BYTES;
        assert_eq!(
            parser.text(" ", false),
            Err(AtomXmlError::WorkLimitExceeded)
        );
        assert_eq!(parser.frames[1].scalar, "x");
    }
}
