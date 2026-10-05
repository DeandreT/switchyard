use tokio::time::Instant;

use super::super::diagnostics::{
    DiagnosticEvent, DiagnosticScope, DiagnosticScopeKind, DiagnosticWriterPhase as Phase,
    ServerDiagnosticRecorder,
};

pub(super) struct Binding {
    recorder: ServerDiagnosticRecorder,
    parent: Option<DiagnosticScope>,
    origin: Instant,
}

impl Binding {
    #[cfg(test)]
    pub(super) fn new(recorder: ServerDiagnosticRecorder, parent: Option<DiagnosticScope>) -> Self {
        Self {
            recorder,
            parent,
            origin: Instant::now(),
        }
    }

    fn start(&self) -> Option<Attempt> {
        let scope = self
            .recorder
            .scope(DiagnosticScopeKind::Write, self.parent.as_ref())
            .ok()?;
        Some(Attempt {
            recorder: self.recorder.clone(),
            scope,
            origin: self.origin,
        })
    }
}

struct Attempt {
    recorder: ServerDiagnosticRecorder,
    scope: DiagnosticScope,
    origin: Instant,
}

impl Attempt {
    fn record(&self, phase: Phase) {
        let elapsed = self.origin.elapsed();
        let _ = self
            .recorder
            .record(&self.scope, DiagnosticEvent::Writer(phase), elapsed);
    }
}

enum Stage {
    Preflight,
    Write,
    Flush,
    Flushed,
    Finished,
}

pub(super) struct Observation {
    attempt: Option<Attempt>,
    stage: Stage,
}

impl Observation {
    pub(super) fn start(binding: Option<&Binding>) -> Self {
        let observation = Self {
            attempt: binding.and_then(Binding::start),
            stage: Stage::Preflight,
        };
        observation.record(Phase::PreflightStarted);
        observation
    }

    fn record(&self, phase: Phase) {
        if let Some(attempt) = &self.attempt {
            attempt.record(phase);
        }
    }

    pub(super) fn writing(&mut self) {
        self.stage = Stage::Write;
        self.record(Phase::WriteStarted);
    }

    pub(super) fn wrote(&mut self) {
        self.record(Phase::WriteAllDone);
        self.stage = Stage::Flush;
    }

    pub(super) fn flushed(&mut self) {
        self.stage = Stage::Flushed;
        self.record(Phase::FlushDone);
    }

    pub(super) fn timeout_phase(&self) -> Phase {
        match self.stage {
            Stage::Write => Phase::TimeoutWrite,
            _ => Phase::TimeoutFlush,
        }
    }

    pub(super) fn error_phase(&self) -> Phase {
        match self.stage {
            Stage::Write => Phase::ErrorWrite,
            _ => Phase::ErrorFlush,
        }
    }

    pub(super) fn finish(&mut self, phase: Phase) {
        self.stage = Stage::Finished;
        self.record(phase);
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        match self.stage {
            Stage::Write => self.record(Phase::DroppedWrite),
            Stage::Flush => self.record(Phase::DroppedFlush),
            Stage::Preflight | Stage::Flushed | Stage::Finished => {}
        }
    }
}
