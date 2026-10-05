//! A stale description of one owner-turn clock check, not admission authority.

/// Whether the existing stamping rule accepted one owner's sampled clock/floor.
///
/// This is not whole-node readiness, a wall-time certificate, timer progress,
/// storage health, a lease or permission to submit work. Subsequent commands
/// stamp independently. `Ready` is intentionally publicly constructible data.
///
/// ```compile_fail
/// fn timestamp(value: server::MaintenanceClockAssessment) {
///     let _ = value.now;
/// }
/// ```
///
/// ```compile_fail
/// fn mutate(value: server::MaintenanceClockAssessment) {
///     value.commit();
/// }
/// ```
///
/// ```no_run
/// async fn observe(handle: &server::BrokerHandle) {
///     use server::MaintenanceClockAssessment;
///     match handle.maintenance_clock_assessment().await {
///         MaintenanceClockAssessment::Unknown => {},
///         MaintenanceClockAssessment::Ready => {},
///         MaintenanceClockAssessment::Unsafe => {},
///         MaintenanceClockAssessment::Unavailable => {},
///         MaintenanceClockAssessment::Stopped => {},
///     }
///     // Subsequent commands still validate and stamp independently.
/// }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MaintenanceClockAssessment {
    /// No assessment was observed; a default is never readiness evidence.
    #[default]
    Unknown,
    /// The unchanged stamping rule returned success during this owner turn.
    Ready,
    /// Clock regression exceeded the existing configured allowance.
    Unsafe,
    /// The floor read or another non-clock probe step returned an error.
    Unavailable,
    /// Admission or reply disconnected; this does not certify orderly shutdown.
    Stopped,
}

impl std::fmt::Display for MaintenanceClockAssessment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unknown => "Unknown",
            Self::Ready => "Ready",
            Self::Unsafe => "Unsafe",
            Self::Unavailable => "Unavailable",
            Self::Stopped => "Stopped",
        })
    }
}
