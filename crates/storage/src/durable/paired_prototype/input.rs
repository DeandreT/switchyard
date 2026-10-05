use super::inventory::{MAX_IMAGE, business_shape, log_shape};
use super::{BorrowedRow, Error, Result};

pub(super) struct SeedStateParts<'a> {
    pub(super) header: &'a [u8],
    pub(super) fence: &'a [u8],
    pub(super) business: &'a [BorrowedRow<'a>],
    pub(super) live: Option<(&'a [u8], &'a [u8])>,
}

pub(super) struct SeedLogParts<'a> {
    pub(super) header: &'a [u8],
    pub(super) progress: &'a [u8],
    pub(super) baseline: &'a [u8],
    pub(super) manifest: &'a [u8],
    pub(super) entries: &'a [BorrowedRow<'a>],
}

pub(super) struct CompositeFixtureParts<'a> {
    pub(super) business: &'a [BorrowedRow<'a>],
    pub(super) live: (&'a [u8], &'a [u8]),
    pub(super) fence: &'a [u8],
}

pub(super) fn bounded(bytes: &[u8], max: usize) -> Result<()> {
    if bytes.len() > max {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}

pub(super) fn live(parts: (&[u8], &[u8])) -> Result<()> {
    bounded(parts.0, 8192)?;
    bounded(parts.1, MAX_IMAGE)
}

impl SeedStateParts<'_> {
    pub(super) fn validate(&self) -> Result<()> {
        bounded(self.header, 256)?;
        bounded(self.fence, 256)?;
        business_shape(self.business.iter().copied())?;
        if let Some(parts) = self.live {
            live(parts)?;
        }
        Ok(())
    }
}

impl SeedLogParts<'_> {
    pub(super) fn validate(&self) -> Result<()> {
        bounded(self.header, 256)?;
        bounded(self.progress, 256)?;
        bounded(self.baseline, 16384)?;
        bounded(self.manifest, 131072)?;
        log_shape(self.entries.iter().copied())
    }
}

impl CompositeFixtureParts<'_> {
    pub(super) fn validate(&self) -> Result<()> {
        business_shape(self.business.iter().copied())?;
        live(self.live)?;
        bounded(self.fence, 256)
    }
}
