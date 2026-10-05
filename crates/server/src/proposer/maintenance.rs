use super::*;
use crate::MaintenanceClockAssessment;

impl<S: StateStore, C: Clock> LocalProposer<S, C> {
    pub(crate) fn maintenance_clock_assessment(&self) -> MaintenanceClockAssessment {
        match self.stamp() {
            Ok(_) => MaintenanceClockAssessment::Ready,
            Err(ProposeError::ClockWentBackward { .. }) => MaintenanceClockAssessment::Unsafe,
            Err(_) => MaintenanceClockAssessment::Unavailable,
        }
    }
}
