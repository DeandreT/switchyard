use std::fmt::{self, Write};

use super::{
    DiagnosticLossSummary, DiagnosticRecord, DiagnosticRefusal, MAX_SERVER_DIAGNOSTIC_EVENTS,
};

pub(super) const MAX_RECORD_BYTES: usize = 224;
pub(super) const MAX_SUMMARY_BYTES: usize = 512;
pub const MAX_SERVER_DIAGNOSTIC_FORMAT_BYTES: usize =
    MAX_SERVER_DIAGNOSTIC_EVENTS * MAX_RECORD_BYTES + MAX_SUMMARY_BYTES;

/// Immutable owned rows/counters, with no source resources or coverage guarantee.
///
/// ```compile_fail
/// fn edit(capture: &mut amqp::DiagnosticCapture) {
///     capture.records().clear();
/// }
/// ```
///
/// ```compile_fail
/// fn extract(capture: amqp::DiagnosticCapture) { let _ = capture.records; }
/// ```
pub struct DiagnosticCapture {
    records: Vec<DiagnosticRecord>,
    losses: DiagnosticLossSummary,
}

impl DiagnosticCapture {
    pub(super) fn new(records: Vec<DiagnosticRecord>, losses: DiagnosticLossSummary) -> Self {
        Self { records, losses }
    }

    pub fn records(&self) -> &[DiagnosticRecord] {
        &self.records
    }
    pub fn losses(&self) -> DiagnosticLossSummary {
        self.losses
    }

    /// Format fixed labels/numbers into a fallibly reserved bounded private text.
    ///
    /// No recorder lock is retained. Schema rows are limited to 224 ASCII bytes,
    /// the summary to 512, and the whole output to 918016 bytes. This renders no
    /// raw error/identity/payload and never invokes a caller's Display callback.
    pub fn format_bounded(&self) -> Result<String, DiagnosticRefusal> {
        self.format_with_limit(MAX_SERVER_DIAGNOSTIC_FORMAT_BYTES)
    }

    pub(super) fn format_with_limit(&self, maximum: usize) -> Result<String, DiagnosticRefusal> {
        if self.records.len() > MAX_SERVER_DIAGNOSTIC_EVENTS {
            return Err(DiagnosticRefusal::FormatLimit);
        }
        let needed = self
            .records
            .len()
            .checked_mul(MAX_RECORD_BYTES)
            .and_then(|rows| rows.checked_add(MAX_SUMMARY_BYTES))
            .filter(|needed| *needed <= maximum)
            .ok_or(DiagnosticRefusal::FormatLimit)?;
        let mut output = String::new();
        output
            .try_reserve_exact(needed)
            .map_err(|_| DiagnosticRefusal::Allocation)?;
        for record in &self.records {
            let mut row = FixedLine::<MAX_RECORD_BYTES>::new();
            writeln!(
                row,
                "sequence={} scope={} parent={} kind={} elapsed_ms={} event={}",
                record.sequence,
                record.scope,
                record.parent.unwrap_or(0),
                record.kind.label(),
                record.elapsed_millis,
                record.event.label(),
            )
            .map_err(|_| DiagnosticRefusal::FormatLimit)?;
            output.push_str(row.as_str()?);
        }
        let mut summary = FixedLine::<MAX_SUMMARY_BYTES>::new();
        writeln!(
            summary,
            "rows={} buffer_full={} contention={} poisoned={} ordinal_exhausted={} foreign_scope={} capture_allocation={}",
            self.records.len(), self.losses.buffer_full, self.losses.contention, self.losses.poisoned,
            self.losses.ordinal_exhausted, self.losses.foreign_scope, self.losses.capture_allocation,
        ).map_err(|_| DiagnosticRefusal::FormatLimit)?;
        output.push_str(summary.as_str()?);
        if output.len() > maximum {
            return Err(DiagnosticRefusal::FormatLimit);
        }
        Ok(output)
    }
}

// Refuse a schema expansion before it can grow the pre-reserved String.
pub(super) struct FixedLine<const N: usize> {
    bytes: [u8; N],
    length: usize,
}

impl<const N: usize> FixedLine<N> {
    pub(super) fn new() -> Self {
        Self {
            bytes: [0; N],
            length: 0,
        }
    }

    pub(super) fn as_str(&self) -> Result<&str, DiagnosticRefusal> {
        std::str::from_utf8(&self.bytes[..self.length]).map_err(|_| DiagnosticRefusal::FormatLimit)
    }
}

impl<const N: usize> Write for FixedLine<N> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self
            .length
            .checked_add(text.len())
            .filter(|end| *end <= N)
            .ok_or(fmt::Error)?;
        self.bytes[self.length..end].copy_from_slice(text.as_bytes());
        self.length = end;
        Ok(())
    }
}

impl fmt::Debug for DiagnosticCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiagnosticCapture")
            .field("rows", &self.records.len())
            .field("losses", &self.losses)
            .finish_non_exhaustive()
    }
}
