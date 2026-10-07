use std::fmt::Write as _;

use super::AtomXmlError;

pub(super) const MAX_DURATION_MILLIS: u64 = 922_337_203_685_477;

pub(super) fn integer(value: &str) -> Result<u64, AtomXmlError> {
    if value.is_empty() || !value.bytes().all(|value| value.is_ascii_digit()) {
        return Err(AtomXmlError::InvalidDefinition);
    }
    value.bytes().try_fold(0_u64, |total, digit| {
        total
            .checked_mul(10)
            .and_then(|total| total.checked_add(u64::from(digit - b'0')))
            .ok_or(AtomXmlError::InvalidDefinition)
    })
}

pub(super) fn parse(value: &str) -> Result<u64, AtomXmlError> {
    let mut rest = value
        .strip_prefix('P')
        .ok_or(AtomXmlError::InvalidDefinition)?;
    let mut time = false;
    let mut last = 0;
    let mut components = 0;
    let mut time_components = 0;
    let mut total = 0_u64;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix('T') {
            if time {
                return Err(AtomXmlError::InvalidDefinition);
            }
            time = true;
            rest = next;
            continue;
        }
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(AtomXmlError::InvalidDefinition);
        }
        let whole = integer(&rest[..digits])?;
        rest = &rest[digits..];
        let mut fraction = None;
        if let Some(next) = rest.strip_prefix('.') {
            let digits = next.bytes().take_while(u8::is_ascii_digit).count();
            if !(1..=7).contains(&digits)
                || next.as_bytes()[3.min(digits)..digits]
                    .iter()
                    .any(|b| *b != b'0')
            {
                return Err(AtomXmlError::InvalidDefinition);
            }
            let significant = &next[..3.min(digits)];
            fraction = Some(integer(significant)? * 10_u64.pow((3 - significant.len()) as u32));
            rest = &next[digits..];
        }
        let unit = rest
            .as_bytes()
            .first()
            .copied()
            .ok_or(AtomXmlError::InvalidDefinition)?;
        let (order, scale) = match (time, unit) {
            (false, b'D') => (1, 86_400_000_u64),
            (true, b'H') => (2, 3_600_000),
            (true, b'M') => (3, 60_000),
            (true, b'S') => (4, 1_000),
            _ => return Err(AtomXmlError::InvalidDefinition),
        };
        if order <= last || (fraction.is_some() && unit != b'S') {
            return Err(AtomXmlError::InvalidDefinition);
        }
        last = order;
        components += 1;
        time_components += usize::from(time);
        total = whole
            .checked_mul(scale)
            .and_then(|value| value.checked_add(fraction.unwrap_or(0)))
            .and_then(|value| total.checked_add(value))
            .filter(|value| *value <= MAX_DURATION_MILLIS)
            .ok_or(AtomXmlError::InvalidDefinition)?;
        rest = &rest[1..];
    }
    if components == 0 || (time && time_components == 0) {
        return Err(AtomXmlError::InvalidDefinition);
    }
    Ok(total)
}

pub(super) fn format(millis: u64) -> String {
    let days = millis / 86_400_000;
    let hours = millis / 3_600_000 % 24;
    let minutes = millis / 60_000 % 60;
    let seconds = millis / 1_000 % 60;
    let fraction = millis % 1_000;
    let mut output = String::from("P");
    if days != 0 {
        write!(output, "{days}D").expect("writing to a string cannot fail");
    }
    if hours != 0 || minutes != 0 || seconds != 0 || fraction != 0 || days == 0 {
        output.push('T');
        if hours != 0 {
            write!(output, "{hours}H").expect("writing to a string cannot fail");
        }
        if minutes != 0 {
            write!(output, "{minutes}M").expect("writing to a string cannot fail");
        }
        if fraction != 0 {
            let digits = format!("{fraction:03}");
            write!(output, "{seconds}.{}S", digits.trim_end_matches('0'))
                .expect("writing to a string cannot fail");
        } else if seconds != 0 || (hours == 0 && minutes == 0) {
            write!(output, "{seconds}S").expect("writing to a string cannot fail");
        }
    }
    output
}
