use super::AtomXmlError;

pub(super) fn xml_space(value: char) -> bool {
    matches!(value, ' ' | '\t' | '\r' | '\n')
}

pub(super) fn trim(value: &str) -> &str {
    value.trim_matches(xml_space)
}

pub(super) fn legal_chars(value: &str) -> bool {
    value.chars().all(|value| {
        matches!(value, '\t' | '\n' | '\r')
            || matches!(value as u32, 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF)
    })
}

fn name_start(value: char) -> bool {
    matches!(value, 'A'..='Z' | '_' | 'a'..='z')
        || matches!(value as u32,
            0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0x2FF | 0x370..=0x37D
            | 0x37F..=0x1FFF | 0x200C..=0x200D | 0x2070..=0x218F
            | 0x2C00..=0x2FEF | 0x3001..=0xD7FF | 0xF900..=0xFDCF
            | 0xFDF0..=0xFFFD | 0x10000..=0xEFFFF)
}

fn name_char(value: char) -> bool {
    name_start(value)
        || matches!(value, '-' | '.' | '0'..='9' | '\u{B7}')
        || matches!(value as u32, 0x300..=0x36F | 0x203F..=0x2040)
}

fn ncname(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(name_start) && chars.all(name_char)
}

pub(super) fn qname(value: &str) -> Result<(), AtomXmlError> {
    let valid = match value.split_once(':') {
        Some((prefix, local)) => ncname(prefix) && ncname(local),
        None => ncname(value),
    };
    if !valid {
        return Err(AtomXmlError::Malformed);
    }
    Ok(())
}

// The library owns attribute parsing; this guard closes its whitespace and
// literal-less-than lexical gaps before any normalized value is retained.
pub(super) fn attribute_tail(mut tail: &str) -> Result<(), AtomXmlError> {
    while !tail.is_empty() {
        if !tail.chars().next().is_some_and(xml_space) {
            return Err(AtomXmlError::Malformed);
        }
        tail = tail.trim_start_matches(xml_space);
        if tail.is_empty() {
            break;
        }
        let name_end = tail
            .find(|value: char| xml_space(value) || value == '=')
            .ok_or(AtomXmlError::Malformed)?;
        qname(&tail[..name_end])?;
        tail = tail[name_end..].trim_start_matches(xml_space);
        tail = tail.strip_prefix('=').ok_or(AtomXmlError::Malformed)?;
        tail = tail.trim_start_matches(xml_space);
        let quote = tail
            .chars()
            .next()
            .filter(|value| matches!(value, '\'' | '"'))
            .ok_or(AtomXmlError::Malformed)?;
        tail = &tail[1..];
        let end = tail.find(quote).ok_or(AtomXmlError::Malformed)?;
        if tail[..end].contains('<') {
            return Err(AtomXmlError::Malformed);
        }
        tail = &tail[end + 1..];
    }
    Ok(())
}
