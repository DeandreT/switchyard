use super::{
    numeric::{Kind, Number},
    *,
};

#[derive(Clone, Copy)]
enum Value<'a> {
    Unknown,
    Null,
    Bool(bool),
    Number(Number),
    String(&'a str),
}

impl Value<'_> {
    fn unknown(self) -> bool {
        matches!(self, Self::Unknown | Self::Null)
    }
    fn truth(self) -> Result<SqlTruth, SqlEvaluationError> {
        match self {
            Self::Unknown | Self::Null => Ok(SqlTruth::Unknown),
            Self::Bool(true) => Ok(SqlTruth::True),
            Self::Bool(false) => Ok(SqlTruth::False),
            _ => Err(SqlEvaluationError::NonPredicate),
        }
    }
    fn from_truth(truth: SqlTruth) -> Self {
        match truth {
            SqlTruth::True => Self::Bool(true),
            SqlTruth::False => Self::Bool(false),
            SqlTruth::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy)]
struct Slot<'a> {
    value: Result<Value<'a>, SqlEvaluationError>,
    string_bound: usize,
}

impl<'a> Slot<'a> {
    fn new(value: Result<Value<'a>, SqlEvaluationError>, failed_bound: usize) -> Self {
        let string_bound = match value {
            Ok(Value::String(value)) => value.len(),
            Err(_) => failed_bound,
            _ => 0,
        };
        Self {
            value,
            string_bound,
        }
    }
}

pub(super) fn evaluate<'a>(
    program: &'a SqlProgram,
    message: SqlMessageContext<'a>,
    budget: &mut SqlEvaluationBudget,
) -> Result<SqlTruth, SqlEvaluationError> {
    budget.work(program.nodes.len())?;
    budget.work(
        message
            .application_properties
            .len()
            .saturating_add(message.system_properties.len()),
    )?;
    let key_bytes = message
        .application_properties
        .iter()
        .fold(0_usize, |total, entry| {
            total.saturating_add(entry.name.len())
        });
    budget.bytes(key_bytes.saturating_mul(3))?;
    // Precharge all lookups before allocating slots or reading input values.
    for node in &program.nodes {
        if let Node::Property(property) | Node::Exists(property) = node {
            match property {
                Property::User(name) => {
                    let bytes = name
                        .len()
                        .saturating_mul(message.application_properties.len())
                        .saturating_add(key_bytes)
                        .saturating_mul(3);
                    budget.work(
                        message
                            .application_properties
                            .len()
                            .saturating_add(1)
                            .saturating_add(bytes),
                    )?;
                    budget.bytes(bytes)?;
                }
                Property::System(_) => {
                    budget.work(message.system_properties.len().saturating_add(1))?
                }
            }
        }
    }
    // The bound pass repeats scalar lookups; charge every scan before any runs.
    for node in &program.nodes {
        match node {
            Node::InList {
                input,
                operands,
                len,
                ..
            } => {
                charge_scalar_lookup(program, message, *input, key_bytes, budget)?;
                for operand in &operands[..usize::from(*len)] {
                    charge_scalar_lookup(program, message, *operand, key_bytes, budget)?;
                }
            }
            Node::Like {
                input,
                pattern,
                escape,
                ..
            } => {
                charge_scalar_lookup(program, message, *input, key_bytes, budget)?;
                charge_scalar_lookup(program, message, *pattern, key_bytes, budget)?;
                if let Some(escape) = escape {
                    charge_scalar_lookup(program, message, *escape, key_bytes, budget)?;
                }
            }
            _ => {}
        }
    }
    // New scalar predicates reserve every operand and regex translation before
    // allocating slots, including failed or Boolean-hidden branches.
    for node in &program.nodes {
        match node {
            Node::InList {
                input,
                operands,
                len,
                ..
            } => {
                let input = scalar_bound(program, message, *input);
                let mut bytes = input.saturating_mul(usize::from(*len));
                for operand in &operands[..usize::from(*len)] {
                    bytes = bytes.saturating_add(scalar_bound(program, message, *operand));
                }
                budget.work(usize::from(*len))?;
                budget.bytes(bytes)?;
            }
            Node::Like {
                input,
                pattern,
                escape,
                ..
            } => {
                let input = scalar_bound(program, message, *input);
                let pattern = scalar_bound(program, message, *pattern);
                let escape = escape.map_or(0, |index| scalar_bound(program, message, index));
                let bytes = input.saturating_add(pattern).saturating_add(escape);
                budget.work(bytes.saturating_add(1))?;
                budget.bytes(bytes)?;
                if pattern > MAX_SQL_LIKE_PATTERN_BYTES {
                    return Err(SqlEvaluationError::Limit {
                        kind: SqlEvaluationLimit::LikePatternBytes,
                        maximum: MAX_SQL_LIKE_PATTERN_BYTES,
                    });
                }
                budget.bytes(
                    super::pattern::allocation_bound(pattern).saturating_add(MAX_SQL_REGEX_BYTES),
                )?;
                budget.work(super::pattern::build_work_bound(pattern))?;
            }
            _ => {}
        }
    }
    let mut slots: Vec<Slot<'a>> = Vec::with_capacity(program.nodes.len());
    let mut first_error = None;
    for node in &program.nodes {
        let slot = match node {
            Node::Literal(literal) => Slot::new(
                Ok(match literal {
                    Literal::Null => Value::Null,
                    Literal::Bool(value) => Value::Bool(*value),
                    Literal::Integer(value) => Value::Number(Number {
                        kind: Kind::I64(*value),
                        constant: true,
                    }),
                    Literal::Double(value) => Value::Number(Number {
                        kind: Kind::F64(*value),
                        constant: true,
                    }),
                    Literal::String(value) => Value::String(value),
                }),
                0,
            ),
            Node::Property(property) | Node::Exists(property) => {
                let (exists, slot) = lookup(message, property);
                if matches!(node, Node::Exists(_)) {
                    Slot::new(slot.value.map(|_| Value::Bool(exists)), slot.string_bound)
                } else {
                    slot
                }
            }
            Node::Not(input) => {
                let input = slots[usize::from(*input)];
                Slot::new(
                    input.value.and_then(operand_truth).map(|truth| {
                        Value::from_truth(match truth {
                            SqlTruth::True => SqlTruth::False,
                            SqlTruth::False => SqlTruth::True,
                            SqlTruth::Unknown => SqlTruth::Unknown,
                        })
                    }),
                    input.string_bound,
                )
            }
            Node::IsNull { input, negated } => {
                let input = slots[usize::from(*input)];
                Slot::new(
                    input
                        .value
                        .map(|value| Value::Bool(value.unknown() != *negated)),
                    input.string_bound,
                )
            }
            Node::Binary { op, left, right } => {
                let left = slots[usize::from(*left)];
                let right = slots[usize::from(*right)];
                Slot::new(
                    binary(*op, left, right, budget),
                    left.string_bound.max(right.string_bound),
                )
            }
            Node::InList {
                input,
                operands,
                len,
                negated,
            } => {
                let input = slots[usize::from(*input)];
                let mut truth = SqlTruth::False;
                let mut error = None;
                let mut bound = input.string_bound;
                for operand in &operands[..usize::from(*len)] {
                    let operand = slots[usize::from(*operand)];
                    bound = bound.max(operand.string_bound);
                    match compare(Binary::Eq, input, operand).and_then(operand_truth) {
                        Ok(SqlTruth::True) => truth = SqlTruth::True,
                        Ok(SqlTruth::Unknown) if truth == SqlTruth::False => {
                            truth = SqlTruth::Unknown
                        }
                        Ok(_) => {}
                        Err(found) => {
                            error.get_or_insert(found);
                        }
                    }
                }
                Slot::new(
                    match error {
                        Some(error) => Err(error),
                        None => Ok(Value::from_truth(negate(truth, *negated))),
                    },
                    bound,
                )
            }
            Node::Like {
                input,
                pattern,
                escape,
                negated,
            } => {
                let input = slots[usize::from(*input)];
                let pattern = slots[usize::from(*pattern)];
                let escape = escape.map(|index| slots[usize::from(index)]);
                Slot::new(
                    like(input, pattern, escape, *negated, budget),
                    input.string_bound.max(pattern.string_bound),
                )
            }
        };
        if let Err(error) = slot.value {
            if matches!(error, SqlEvaluationError::Limit { .. }) {
                return Err(error);
            }
            first_error.get_or_insert(error);
        }
        slots.push(slot);
    }
    match first_error {
        Some(error) => Err(error),
        None => slots[usize::from(program.root)].value?.truth(),
    }
}

fn scalar_bound(program: &SqlProgram, message: SqlMessageContext<'_>, index: u16) -> usize {
    match &program.nodes[usize::from(index)] {
        Node::Literal(Literal::String(value)) => value.len(),
        Node::Property(property) => lookup(message, property).1.string_bound,
        _ => 0,
    }
}

fn charge_scalar_lookup(
    program: &SqlProgram,
    message: SqlMessageContext<'_>,
    index: u16,
    key_bytes: usize,
    budget: &mut SqlEvaluationBudget,
) -> Result<(), SqlEvaluationError> {
    match &program.nodes[usize::from(index)] {
        Node::Property(Property::User(name)) => {
            let bytes = name
                .len()
                .saturating_mul(message.application_properties.len())
                .saturating_add(key_bytes);
            budget.work(message.application_properties.len().saturating_add(bytes))?;
            budget.bytes(bytes)
        }
        Node::Property(Property::System(_)) => budget.work(message.system_properties.len()),
        _ => Ok(()),
    }
}

fn negate(truth: SqlTruth, negated: bool) -> SqlTruth {
    match (negated, truth) {
        (true, SqlTruth::True) => SqlTruth::False,
        (true, SqlTruth::False) => SqlTruth::True,
        _ => truth,
    }
}

fn like<'a>(
    input: Slot<'a>,
    pattern: Slot<'a>,
    escape: Option<Slot<'a>>,
    negated: bool,
    budget: &mut SqlEvaluationBudget,
) -> Result<Value<'a>, SqlEvaluationError> {
    let escape = match escape.map(|slot| slot.value).transpose()? {
        None => None,
        Some(Value::String(text)) => {
            Some(super::pattern::escape(text).ok_or(SqlEvaluationError::InvalidLikeEscape)?)
        }
        Some(value) if value.unknown() => return Ok(Value::Unknown),
        Some(_) => return Err(SqlEvaluationError::TypeMismatch),
    };
    let pattern = match pattern.value? {
        Value::String(text) => text,
        value if value.unknown() => return input.value.map(|_| Value::Unknown),
        _ => return Err(SqlEvaluationError::TypeMismatch),
    };
    // Build even with null, incompatible or unsupported input: those values
    // cannot conceal a regex limit. Matching work is charged before its cache.
    let regex = super::pattern::compile(pattern, escape)?;
    budget.work(
        input
            .string_bound
            .saturating_add(1)
            .saturating_mul(regex.get_nfa().states().len()),
    )?;
    let input = match input.value? {
        Value::String(text) => text,
        value if value.unknown() => return Ok(Value::Unknown),
        _ => return Err(SqlEvaluationError::TypeMismatch),
    };
    // Capture-free PikeVM sparse sets and epsilon stack are linear in states.
    budget.bytes(
        regex
            .get_nfa()
            .states()
            .len()
            .saturating_mul(64)
            .saturating_add(32),
    )?;
    let mut cache = regex.create_cache();
    Ok(Value::Bool(regex.is_match(&mut cache, input) != negated))
}

fn lookup<'a>(message: SqlMessageContext<'a>, property: &Property) -> (bool, Slot<'a>) {
    let mut found = None;
    let mut ambiguous = false;
    let mut bound = 0;
    let mut accept = |value| {
        ambiguous |= found.is_some();
        if let SqlValue::String(value) = value {
            bound = bound.max(value.len());
        }
        found = Some(value);
    };
    match property {
        Property::User(name) => {
            for entry in message.application_properties {
                if entry.name.eq_ignore_ascii_case(name) {
                    accept(entry.value);
                }
            }
        }
        Property::System(property) => {
            for entry in message.system_properties {
                if entry.property == *property {
                    accept(entry.value);
                }
            }
        }
    }
    let value = if ambiguous {
        Err(SqlEvaluationError::AmbiguousProperty)
    } else if let Some(value) = found {
        input_value(value)
    } else if matches!(property, Property::System(_)) {
        Err(SqlEvaluationError::MissingSystemProperty)
    } else {
        Ok(Value::Unknown)
    };
    (found.is_some(), Slot::new(value, bound))
}

fn input_value(value: SqlValue<'_>) -> Result<Value<'_>, SqlEvaluationError> {
    let kind = match value {
        SqlValue::Null => return Ok(Value::Null),
        SqlValue::Bool(value) => return Ok(Value::Bool(value)),
        SqlValue::String(value) => return Ok(Value::String(value)),
        SqlValue::Unsupported => return Err(SqlEvaluationError::UnsupportedValue),
        SqlValue::Byte(value) => Kind::I8(value),
        SqlValue::Ubyte(value) => Kind::U8(value),
        SqlValue::Short(value) => Kind::I16(value),
        SqlValue::Ushort(value) => Kind::U16(value),
        SqlValue::Int(value) => Kind::I32(value),
        SqlValue::Uint(value) => Kind::U32(value),
        SqlValue::Long(value) => Kind::I64(value),
        SqlValue::Ulong(value) => Kind::U64(value),
        SqlValue::Float(value) => Kind::F32(value),
        SqlValue::Double(value) => Kind::F64(value),
    };
    Ok(Value::Number(Number {
        kind,
        constant: false,
    }))
}

fn operand_truth(value: Value<'_>) -> Result<SqlTruth, SqlEvaluationError> {
    value.truth().map_err(|error| {
        if error == SqlEvaluationError::NonPredicate {
            SqlEvaluationError::TypeMismatch
        } else {
            error
        }
    })
}

fn binary<'a>(
    op: Binary,
    left: Slot<'a>,
    right: Slot<'a>,
    budget: &mut SqlEvaluationBudget,
) -> Result<Value<'a>, SqlEvaluationError> {
    if matches!(op, Binary::And | Binary::Or) {
        let left = operand_truth(left.value?)?;
        let right = operand_truth(right.value?)?;
        return Ok(Value::from_truth(match (op, left, right) {
            (Binary::And, SqlTruth::False, _) | (Binary::And, _, SqlTruth::False) => {
                SqlTruth::False
            }
            (Binary::And, SqlTruth::True, SqlTruth::True) => SqlTruth::True,
            (Binary::Or, SqlTruth::True, _) | (Binary::Or, _, SqlTruth::True) => SqlTruth::True,
            (Binary::Or, SqlTruth::False, SqlTruth::False) => SqlTruth::False,
            _ => SqlTruth::Unknown,
        }));
    }
    budget.work(1)?;
    budget.bytes(left.string_bound.saturating_add(right.string_bound))?;
    compare(op, left, right)
}

fn compare<'a>(
    op: Binary,
    left: Slot<'a>,
    right: Slot<'a>,
) -> Result<Value<'a>, SqlEvaluationError> {
    let left = left.value?;
    let right = right.value?;
    if left.unknown() || right.unknown() {
        return Ok(Value::Unknown);
    }
    let result = match (left, right) {
        (Value::Number(left), Value::Number(right)) => numeric::compare(op, left, right)?,
        (Value::Bool(left), Value::Bool(right)) => match op {
            Binary::Eq => left == right,
            Binary::Ne => left != right,
            _ => return Err(SqlEvaluationError::TypeMismatch),
        },
        (Value::String(left), Value::String(right)) => match op {
            Binary::Eq => left == right,
            Binary::Ne => left != right,
            _ => return Err(SqlEvaluationError::StringOrderingUnsupported),
        },
        _ => return Err(SqlEvaluationError::TypeMismatch),
    };
    Ok(Value::Bool(result))
}
