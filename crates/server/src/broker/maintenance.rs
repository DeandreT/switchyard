use super::*;
use crate::MaintenanceClockAssessment;

impl BrokerHandle {
    /// Queues one read-only assessment on the existing owner and blocks for it.
    /// The returned description can already be stale and grants no authority.
    pub fn maintenance_clock_assessment_blocking(&self) -> MaintenanceClockAssessment {
        let (reply, observed) = flume::bounded(1);
        if self
            .requests
            .send(Request::MaintenanceClockAssessment { reply })
            .is_err()
        {
            return MaintenanceClockAssessment::Stopped;
        }
        observed
            .recv()
            .unwrap_or(MaintenanceClockAssessment::Stopped)
    }

    /// Observes the same owner turn without blocking an executor thread.
    /// No admission occurs until first poll. Losing an admitted observer does
    /// not remove the accepted probe from its FIFO position.
    pub async fn maintenance_clock_assessment(&self) -> MaintenanceClockAssessment {
        let (reply, observed) = flume::bounded(1);
        if self
            .requests
            .send_async(Request::MaintenanceClockAssessment { reply })
            .await
            .is_err()
        {
            return MaintenanceClockAssessment::Stopped;
        }
        observed
            .recv_async()
            .await
            .unwrap_or(MaintenanceClockAssessment::Stopped)
    }
}

#[cfg(test)]
mod tests;
