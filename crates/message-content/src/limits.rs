use std::fmt;

/// Additional implementation ceiling, including descriptors and array constructors.
pub const MAX_DEPTH: usize = 128;

/// Caller-selected per-operation limits. There is deliberately no default.
///
/// `max_bytes` bounds source/output bytes, not a Rust allocator's capacity overhead.
/// `max_nodes` bounds values, descriptors and array-constructor nodes; implicit array
/// elements count even when their constructor has zero payload bytes. `max_work` bounds
/// bytes consumed/emitted plus node visits, described-array element traversal and
/// payload bytes moved when a compact nested container must widen. Counts
/// are checked against remaining nodes/work before iteration. The root has depth one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_work: usize,
}

/// Bounded, non-recursive errors: no user-provided tree or payload is formatted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ByteLimit,
    DepthLimit,
    NodeLimit,
    WorkLimit,
    Overflow,
    Truncated,
    InvalidConstructor,
    InvalidDescriptor,
    InvalidValue,
    InvalidUtf8,
    InvalidLength,
    OddMapCount,
    TrailingBytes,
    InvalidSection,
    SectionOrder,
    DuplicateSection,
    MixedBody,
    InvalidField,
    InvalidAnnotationKey,
    InvalidApplicationKey,
    AbsentBeforePresent,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ByteLimit => "AMQP byte limit exceeded",
            Self::DepthLimit => "AMQP nesting limit exceeded",
            Self::NodeLimit => "AMQP node/element limit exceeded",
            Self::WorkLimit => "AMQP work limit exceeded",
            Self::Overflow => "AMQP size arithmetic overflow",
            Self::Truncated => "truncated AMQP value",
            Self::InvalidConstructor => "invalid AMQP constructor",
            Self::InvalidDescriptor => "AMQP descriptor must be ulong or symbol",
            Self::InvalidValue => "value does not match its AMQP constructor",
            Self::InvalidUtf8 => "invalid AMQP UTF-8 text",
            Self::InvalidLength => "invalid AMQP container length/count",
            Self::OddMapCount => "AMQP map has odd element count",
            Self::TrailingBytes => "unconsumed AMQP bytes",
            Self::InvalidSection => "invalid AMQP message section",
            Self::SectionOrder => "AMQP sections are out of order",
            Self::DuplicateSection => "duplicate AMQP section",
            Self::MixedBody => "duplicate or mixed AMQP body sections",
            Self::InvalidField => "invalid AMQP header/property field",
            Self::InvalidAnnotationKey => "annotation key must be symbol or ulong",
            Self::InvalidApplicationKey => "application-property key must be string",
            Self::AbsentBeforePresent => "absent list field precedes a present field",
        })
    }
}

impl std::error::Error for Error {}

pub(crate) struct Budget {
    pub(crate) limits: Limits,
    nodes: usize,
    work: usize,
}

impl Budget {
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            limits,
            nodes: 0,
            work: 0,
        }
    }

    pub(crate) fn depth(&self, depth: usize) -> Result<(), Error> {
        if depth > self.limits.max_depth.min(MAX_DEPTH) {
            return Err(Error::DepthLimit);
        }
        Ok(())
    }

    pub(crate) fn node(&mut self, depth: usize) -> Result<(), Error> {
        self.depth(depth)?;
        self.children(1)?;
        self.nodes = self.nodes.checked_add(1).ok_or(Error::Overflow)?;
        self.work(1)
    }

    /// Lower-bound preflight, without reservation/double counting. Every actual child
    /// subsequently spends its node/work from this same budget.
    pub(crate) fn children(&self, count: usize) -> Result<(), Error> {
        if count > self.limits.max_nodes.saturating_sub(self.nodes) {
            return Err(Error::NodeLimit);
        }
        if count > self.limits.max_work.saturating_sub(self.work) {
            return Err(Error::WorkLimit);
        }
        Ok(())
    }

    pub(crate) fn work(&mut self, amount: usize) -> Result<(), Error> {
        let next = self.work.checked_add(amount).ok_or(Error::Overflow)?;
        if next > self.limits.max_work {
            return Err(Error::WorkLimit);
        }
        self.work = next;
        Ok(())
    }
}
