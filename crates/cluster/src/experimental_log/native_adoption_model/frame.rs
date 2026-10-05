use std::io::{self, Write};

use serde::{Deserialize, Serialize};

use super::*;

const PREFIX: &[u8; 6] = b"SWAI\0\x01";
const HEADER: usize = 12;
const FOOTER: usize = 32;

pub(super) fn encode<T: Serialize>(
    kind: Kind,
    role: Role,
    value: &T,
    cap: usize,
) -> Result<Vec<u8>> {
    if cap > MAX_INTENT {
        return Err(ModelCodecError::Limit);
    }
    let payload_cap = cap
        .checked_sub(HEADER + FOOTER)
        .ok_or(ModelCodecError::Limit)?;
    let payload_len = count(value, payload_cap)?;
    let total = HEADER + payload_len + FOOTER;
    let mut output = Vec::new();
    output
        .try_reserve_exact(total)
        .map_err(|_| ModelCodecError::Allocation)?;
    output.extend_from_slice(PREFIX);
    output.push(kind as u8);
    output.push(role as u8);
    let wire_len = u32::try_from(payload_len).map_err(|_| ModelCodecError::Limit)?;
    output.extend_from_slice(&wire_len.to_be_bytes());
    output.resize(HEADER + payload_len, 0);
    let encoded =
        postcard::to_slice(value, &mut output[HEADER..]).map_err(|_| ModelCodecError::Format)?;
    if encoded.len() != payload_len {
        return Err(ModelCodecError::Format);
    }
    let checksum = digest(&output);
    output.extend_from_slice(&checksum);
    Ok(output)
}

pub(super) fn decode<'a, T: Deserialize<'a> + Serialize>(
    kind: Kind,
    role: Role,
    bytes: &'a [u8],
    cap: usize,
) -> Result<T> {
    if bytes.len() > cap {
        return Err(ModelCodecError::Limit);
    }
    if bytes.len() < HEADER + FOOTER
        || &bytes[..6] != PREFIX
        || bytes[6] != kind as u8
        || bytes[7] != role as u8
    {
        return Err(ModelCodecError::Format);
    }
    let length = u32::from_be_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| ModelCodecError::Format)?,
    ) as usize;
    let end = HEADER
        .checked_add(length)
        .filter(|end| end.checked_add(FOOTER) == Some(bytes.len()))
        .ok_or(ModelCodecError::Format)?;
    if digest(&bytes[..end]).as_slice() != &bytes[end..] {
        return Err(ModelCodecError::Format);
    }
    decode_payload(&bytes[HEADER..end], cap - HEADER - FOOTER)
}

pub(super) fn encode_payload<T: Serialize>(value: &T, cap: usize) -> Result<Vec<u8>> {
    let size = count(value, cap)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| ModelCodecError::Allocation)?;
    bytes.resize(size, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| ModelCodecError::Format)?;
    Ok(bytes)
}

pub(super) fn decode_payload<'a, T: Deserialize<'a> + Serialize>(
    bytes: &'a [u8],
    cap: usize,
) -> Result<T> {
    if bytes.len() > cap {
        return Err(ModelCodecError::Limit);
    }
    let (value, remaining): (T, _) =
        postcard::take_from_bytes(bytes).map_err(|_| ModelCodecError::Format)?;
    if !remaining.is_empty() {
        return Err(ModelCodecError::Format);
    }
    let mut comparison = Comparison { bytes, offset: 0 };
    if postcard::to_io(&value, &mut comparison).is_err() || comparison.offset != bytes.len() {
        return Err(ModelCodecError::Format);
    }
    Ok(value)
}

fn count<T: Serialize>(value: &T, cap: usize) -> Result<usize> {
    let mut writer = Counter {
        bytes: 0,
        cap,
        exceeded: false,
    };
    if postcard::to_io(value, &mut writer).is_err() {
        return Err(if writer.exceeded {
            ModelCodecError::Limit
        } else {
            ModelCodecError::Format
        });
    }
    Ok(writer.bytes)
}

struct Counter {
    bytes: usize,
    cap: usize,
    exceeded: bool,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(total) = self
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= self.cap)
        else {
            self.exceeded = true;
            return Err(io::ErrorKind::WriteZero.into());
        };
        self.bytes = total;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Comparison<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl Write for Comparison<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or(io::ErrorKind::InvalidData)?;
        if self.bytes.get(self.offset..end) != Some(bytes) {
            return Err(io::ErrorKind::InvalidData.into());
        }
        self.offset = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
