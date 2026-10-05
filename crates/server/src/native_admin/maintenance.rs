use admin_api::v1::{
    ClockReadinessResponse, GetClockReadinessRequest, MaintenanceClockState,
    maintenance_service_server::{MaintenanceService, MaintenanceServiceServer},
};

use super::*;
use crate::{MaintenanceClockAssessment, NATIVE_ADMIN_REQUEST_LIMIT, NATIVE_ADMIN_RESPONSE_LIMIT};

impl NativeAdminService {
    /// Enables this descriptive development probe on the existing admin route.
    /// Embedders retain their deployment-policy obligation; only the binary
    /// enforces its explicit Development flag and admin-listener prerequisite.
    pub fn with_development_maintenance_readiness(mut self) -> Self {
        self.development_maintenance_readiness = true;
        self
    }

    pub(crate) fn development_maintenance_service(&self) -> Option<MaintenanceServiceServer<Self>> {
        self.development_maintenance_readiness.then(|| {
            MaintenanceServiceServer::new(self.clone())
                .max_decoding_message_size(NATIVE_ADMIN_REQUEST_LIMIT)
                .max_encoding_message_size(NATIVE_ADMIN_RESPONSE_LIMIT)
        })
    }
}

#[tonic::async_trait]
impl MaintenanceService for NativeAdminService {
    async fn get_clock_readiness(
        &self,
        request: Request<GetClockReadinessRequest>,
    ) -> Result<Response<ClockReadinessResponse>, Status> {
        let input = request.get_ref();
        let _permit = self.begin_request(&request, &input.namespace, None)?;
        let state = match self.broker.maintenance_clock_assessment().await {
            MaintenanceClockAssessment::Unknown => MaintenanceClockState::Unknown,
            MaintenanceClockAssessment::Ready => MaintenanceClockState::Ready,
            MaintenanceClockAssessment::Unsafe => MaintenanceClockState::Unsafe,
            MaintenanceClockAssessment::Unavailable => MaintenanceClockState::Unavailable,
            MaintenanceClockAssessment::Stopped => MaintenanceClockState::Stopped,
        };
        Ok(Response::new(ClockReadinessResponse {
            state: state as i32,
        }))
    }
}
