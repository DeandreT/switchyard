use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Clone, Copy)]
enum Fault {
    Full,
    Contended,
    Poisoned,
    Exhausted,
    Foreign,
}
const FAULTS: [Fault; 5] = [
    Fault::Full,
    Fault::Contended,
    Fault::Poisoned,
    Fault::Exhausted,
    Fault::Foreign,
];
const ACTIONS: [(Mode, bool); 6] = [
    (Mode::Healthy, false),
    (Mode::Healthy, true),
    (Mode::ErrorWrite(io::ErrorKind::TimedOut), false),
    (Mode::ErrorFlush(io::ErrorKind::TimedOut), false),
    (Mode::PendingWrite, false),
    (Mode::PendingFlush, false),
];

#[derive(Debug, Eq, PartialEq)]
struct ResultView {
    wire: WireSnapshot,
    error: Option<io::ErrorKind>,
    overflow: Option<FrameWriteError>,
    tainted: bool,
    closing: bool,
    timeout: Option<ActivityTimeout>,
}

fn exercise(
    mode: Mode,
    oversized: bool,
    recorder: Option<&ServerDiagnosticRecorder>,
    parent: Option<&DiagnosticScope>,
) -> Result<ResultView, io::Error> {
    let (mut output, control, activity) = writer(mode, recorder, parent)?;
    let frame = if oversized {
        sized_transfer(513)
    } else {
        heartbeat()
    };
    let mut writing = Box::pin(output.write_frame(&frame));
    let (error, overflow) = match poll_once(writing.as_mut()) {
        Poll::Ready(result) => match result {
            Ok(()) => {
                assert!(matches!(mode, Mode::Healthy));
                assert!(!oversized);
                (None, None)
            }
            Err(error) => {
                if oversized {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                } else if let Mode::ErrorWrite(kind) | Mode::ErrorFlush(kind) = mode {
                    assert_native_error(&error, kind, &control);
                } else {
                    panic!("unexpected controlled I/O result");
                }
                let overflow = error
                    .get_ref()
                    .and_then(|cause| cause.downcast_ref::<FrameWriteError>())
                    .copied();
                (Some(error.kind()), overflow)
            }
        },
        Poll::Pending => {
            assert!(matches!(mode, Mode::PendingWrite | Mode::PendingFlush));
            (None, None)
        }
    };
    drop(writing);
    let mut timeout =
        Box::pin(activity.timeout(ConnectionOptions::default().idle_timeout_millis(0), 0));
    let timeout = match poll_once(timeout.as_mut()) {
        Poll::Ready(reason) => Some(reason),
        Poll::Pending => None,
    };
    Ok(ResultView {
        wire: control.snapshot(),
        error,
        overflow,
        tainted: activity.is_tainted(),
        closing: activity.is_closing(),
        timeout,
    })
}

fn counter(recorder: &ServerDiagnosticRecorder, fault: Fault) -> &AtomicU64 {
    match fault {
        Fault::Full => &recorder.0.losses.buffer_full,
        Fault::Contended => &recorder.0.losses.contention,
        Fault::Poisoned => &recorder.0.losses.poisoned,
        Fault::Exhausted => &recorder.0.losses.ordinal_exhausted,
        Fault::Foreign => &recorder.0.losses.foreign_scope,
    }
}

fn refusal(fault: Fault) -> DiagnosticRefusal {
    match fault {
        Fault::Full => DiagnosticRefusal::Full,
        Fault::Contended => DiagnosticRefusal::Contended,
        Fault::Poisoned => DiagnosticRefusal::Poisoned,
        Fault::Exhausted => DiagnosticRefusal::OrdinalExhausted,
        Fault::Foreign => DiagnosticRefusal::ForeignScope,
    }
}

fn run_fault(fault: Fault, saturated: bool) -> TestResult {
    let recorder = ServerDiagnosticRecorder::try_new()?;
    let parent = if matches!(fault, Fault::Foreign) {
        Some(ServerDiagnosticRecorder::try_new()?.scope(DiagnosticScopeKind::Fixture, None)?)
    } else {
        None
    };
    if matches!(fault, Fault::Full) {
        let scope = recorder.scope(DiagnosticScopeKind::Fixture, None)?;
        for _ in 0..MAX_SERVER_DIAGNOSTIC_EVENTS {
            recorder.record(&scope, EVENT, Duration::ZERO)?;
        }
    }
    if matches!(fault, Fault::Poisoned) {
        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _held = recorder.0.records.lock().unwrap();
            panic!("synthetic recorder poison");
        }));
        assert!(poisoned.is_err());
    }
    if matches!(fault, Fault::Exhausted) {
        recorder.0.last_scope.store(u64::MAX, Ordering::Relaxed);
    }
    if saturated {
        counter(&recorder, fault).store(u64::MAX - 1, Ordering::Relaxed);
    }
    // Poll synchronously while the independent test control occupies storage;
    // publication must refuse rather than acquire that lock or await it.
    let held = if matches!(fault, Fault::Contended) {
        Some(recorder.0.records.lock().unwrap())
    } else {
        None
    };
    for (mode, oversized) in ACTIONS {
        let original = exercise(mode, oversized, None, None)?;
        let observed = exercise(mode, oversized, Some(&recorder), parent.as_ref())?;
        assert_eq!(original, observed);
    }
    drop(held);
    let count = counter(&recorder, fault).load(Ordering::Relaxed);
    assert_eq!(
        count,
        if saturated {
            u64::MAX
        } else if matches!(fault, Fault::Exhausted | Fault::Foreign) {
            6
        } else {
            21
        }
    );
    if matches!(fault, Fault::Poisoned) {
        assert_eq!(recorder.capture().unwrap_err(), refusal(fault));
    } else {
        let capture = recorder.capture()?;
        assert_eq!(
            capture.records().len(),
            if matches!(fault, Fault::Full) {
                MAX_SERVER_DIAGNOSTIC_EVENTS
            } else {
                0
            }
        );
    }
    if matches!(fault, Fault::Exhausted) {
        assert_eq!(recorder.0.last_scope.load(Ordering::Relaxed), u64::MAX);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn recording_refusals_never_replace_controlled_io_or_drop_behavior() -> TestResult {
    for fault in FAULTS {
        run_fault(fault, false)?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn writer_recording_losses_saturate_without_resuming_or_changing_io() -> TestResult {
    for fault in FAULTS {
        run_fault(fault, true)?;
    }
    Ok(())
}
