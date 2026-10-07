use admin_api::v1::{
    CreateFiniteQueueRequest, FiniteQueue, GetFiniteQueueRequest, QueueConfiguration,
    SetFiniteQueueDefinitionRequest, finite_queue_service_server::FiniteQueueService,
    queue_configuration::DefaultTimeToLive,
};
use domain::{
    EntityBinding, EntityIncarnationKind, FiniteQueueCapacity, QueueCapacityStatus,
    QueueCapacityView, QueueConfig,
};
use tonic::{Request, Response, Status};

use super::{NativeAdminService, entity_response, read_status, submit_status, topology};

#[tonic::async_trait]
impl FiniteQueueService for NativeAdminService {
    async fn create_finite_queue(
        &self,
        request: Request<CreateFiniteQueueRequest>,
    ) -> Result<Response<FiniteQueue>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let path = self.entity_path(&input.path)?;
        let config = full_configuration(input.config.as_ref())?;
        let limit = reservation_limit(input.reservation_limit_bytes)?;
        let view = self
            .broker
            .create_finite_queue(self.namespace.clone(), path, config, limit)
            .await
            .map_err(submit_status)?;
        Ok(Response::new(finite_response(view)?))
    }

    async fn get_finite_queue(
        &self,
        request: Request<GetFiniteQueueRequest>,
    ) -> Result<Response<FiniteQueue>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let path = self.entity_path(&input.path)?;
        let view = self
            .broker
            .describe_queue_capacity(self.namespace.clone(), path)
            .await
            .map_err(read_status)?
            .ok_or_else(|| Status::not_found("queue does not exist"))?;
        Ok(Response::new(finite_response(view)?))
    }

    async fn set_finite_queue_definition(
        &self,
        request: Request<SetFiniteQueueDefinitionRequest>,
    ) -> Result<Response<FiniteQueue>, Status> {
        let input = request.get_ref();
        let resource = topology::requested_resource(&input.path);
        let _permit = self.begin_request(&request, &input.namespace, Some(&resource))?;
        let path = self.entity_path(&input.path)?;
        let generation = expected_generation(input.expected_generation)?;
        let config = full_configuration(input.config.as_ref())?;
        let limit = reservation_limit(input.reservation_limit_bytes)?;
        let binding = EntityBinding::new(
            self.namespace.clone(),
            path.clone(),
            path,
            EntityIncarnationKind::Queue,
            generation,
        )
        .map_err(|_| Status::invalid_argument("invalid queue owner identity"))?;
        let view = self
            .broker
            .set_finite_queue_definition_fenced(binding, config, limit)
            .await
            .map_err(submit_status)?;
        Ok(Response::new(finite_response(view)?))
    }
}

fn required_field<T>(value: Option<T>) -> Result<T, Status> {
    value.ok_or_else(|| Status::invalid_argument("all queue configuration fields are required"))
}

fn full_configuration(input: Option<&QueueConfiguration>) -> Result<QueueConfig, Status> {
    let input = input
        .ok_or_else(|| Status::invalid_argument("complete queue configuration is required"))?;
    let default_time_to_live_millis = match required_field(input.default_time_to_live.as_ref())? {
        DefaultTimeToLive::DefaultTtlMillis(millis) => Some(*millis),
        DefaultTimeToLive::DefaultTtlUnlimited(_) => None,
    };
    // Numeric values remain unchanged so the owner retains authoritative failure priority.
    Ok(QueueConfig {
        lock_duration_millis: required_field(input.lock_duration_millis)?,
        max_delivery_count: required_field(input.max_delivery_count)?,
        default_time_to_live_millis,
        max_message_bytes: usize::try_from(required_field(input.max_message_bytes)?)
            .map_err(|_| Status::invalid_argument("message limit exceeds this platform"))?,
        requires_session: required_field(input.requires_session)?,
        requires_duplicate_detection: required_field(input.requires_duplicate_detection)?,
        duplicate_detection_history_time_window_millis: required_field(
            input.duplicate_detection_history_time_window_millis,
        )?,
        dead_lettering_on_message_expiration: required_field(
            input.dead_lettering_on_message_expiration,
        )?,
    })
}

fn reservation_limit(input: Option<u64>) -> Result<FiniteQueueCapacity, Status> {
    let bytes = input.ok_or_else(|| Status::invalid_argument("reservation limit is required"))?;
    FiniteQueueCapacity::new(bytes)
        .map_err(|_| Status::invalid_argument("reservation limit must be positive"))
}

fn expected_generation(input: Option<u64>) -> Result<u64, Status> {
    input
        .filter(|generation| *generation != 0)
        .ok_or_else(|| Status::invalid_argument("positive expected generation is required"))
}

fn finite_response(view: QueueCapacityView) -> Result<FiniteQueue, Status> {
    let QueueCapacityStatus::FiniteV1 {
        limit,
        reserved_bytes,
        message_count,
    } = view.capacity
    else {
        return Err(Status::failed_precondition(
            "queue does not have a finite reservation limit",
        ));
    };
    Ok(FiniteQueue {
        namespace: view.binding.namespace().as_str().to_owned(),
        path: view.binding.target().as_str().to_owned(),
        generation: view.binding.generation(),
        config: entity_response(view.binding.namespace(), view.binding.target(), view.config)
            .queue_config,
        reservation_limit_bytes: limit.bytes(),
        reserved_logical_bytes: reserved_bytes,
        retained_message_count: message_count,
    })
}

#[cfg(test)]
mod tests {
    use domain::{EntityPath, NamespaceName};
    use prost::Message;

    use super::*;

    fn complete_configuration() -> QueueConfiguration {
        QueueConfiguration {
            lock_duration_millis: Some(1_000),
            max_delivery_count: Some(2),
            default_time_to_live: Some(DefaultTimeToLive::DefaultTtlUnlimited(
                admin_api::v1::UnlimitedTimeToLive {},
            )),
            max_message_bytes: Some(4_096),
            requires_session: Some(false),
            requires_duplicate_detection: Some(false),
            duplicate_detection_history_time_window_millis: Some(20_000),
            dead_lettering_on_message_expiration: Some(false),
        }
    }

    #[test]
    fn complete_configuration_preserves_false_and_explicit_unlimited() {
        let wire = complete_configuration();
        let decoded = QueueConfiguration::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, wire);
        assert_eq!(
            full_configuration(Some(&decoded)).unwrap(),
            QueueConfig {
                lock_duration_millis: 1_000,
                max_delivery_count: 2,
                default_time_to_live_millis: None,
                max_message_bytes: 4_096,
                requires_session: false,
                requires_duplicate_detection: false,
                duplicate_detection_history_time_window_millis: 20_000,
                dead_lettering_on_message_expiration: false,
            }
        );
    }

    #[test]
    fn every_missing_full_field_is_refused_without_creation_defaults() {
        assert_eq!(
            full_configuration(None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        for missing in 0..8 {
            let mut wire = complete_configuration();
            match missing {
                0 => wire.lock_duration_millis = None,
                1 => wire.max_delivery_count = None,
                2 => wire.default_time_to_live = None,
                3 => wire.max_message_bytes = None,
                4 => wire.requires_session = None,
                5 => wire.requires_duplicate_detection = None,
                6 => wire.duplicate_detection_history_time_window_millis = None,
                7 => wire.dead_lettering_on_message_expiration = None,
                _ => unreachable!(),
            }
            let decoded = QueueConfiguration::decode(wire.encode_to_vec().as_slice()).unwrap();
            assert_eq!(
                full_configuration(Some(&decoded)).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn zero_numeric_configuration_is_preserved_for_domain_priority() {
        let wire = QueueConfiguration {
            lock_duration_millis: Some(0),
            max_delivery_count: Some(0),
            default_time_to_live: Some(DefaultTimeToLive::DefaultTtlMillis(0)),
            max_message_bytes: Some(0),
            requires_session: Some(true),
            requires_duplicate_detection: Some(true),
            duplicate_detection_history_time_window_millis: Some(0),
            dead_lettering_on_message_expiration: Some(false),
        };
        let decoded = QueueConfiguration::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, wire);
        assert_eq!(
            full_configuration(Some(&decoded)).unwrap(),
            QueueConfig {
                lock_duration_millis: 0,
                max_delivery_count: 0,
                default_time_to_live_millis: Some(0),
                max_message_bytes: 0,
                requires_session: true,
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis: 0,
                dead_lettering_on_message_expiration: false,
            }
        );
    }

    #[test]
    fn dedicated_messages_preserve_unsigned_capacity_and_generation_presence() {
        let create = CreateFiniteQueueRequest {
            namespace: "n".to_owned(),
            path: "q".to_owned(),
            config: Some(complete_configuration()),
            reservation_limit_bytes: Some(u64::MAX),
        };
        assert_eq!(
            CreateFiniteQueueRequest::decode(create.encode_to_vec().as_slice()).unwrap(),
            create
        );
        let update = SetFiniteQueueDefinitionRequest {
            namespace: "n".to_owned(),
            path: "q".to_owned(),
            expected_generation: Some(u64::MAX),
            config: Some(complete_configuration()),
            reservation_limit_bytes: Some(u64::MAX),
        };
        assert_eq!(
            SetFiniteQueueDefinitionRequest::decode(update.encode_to_vec().as_slice()).unwrap(),
            update
        );
        let get = GetFiniteQueueRequest {
            namespace: "n".to_owned(),
            path: "q".to_owned(),
        };
        assert_eq!(
            GetFiniteQueueRequest::decode(get.encode_to_vec().as_slice()).unwrap(),
            get
        );
        for value in [1, u64::MAX] {
            assert_eq!(reservation_limit(Some(value)).unwrap().bytes(), value);
            assert_eq!(expected_generation(Some(value)).unwrap(), value);
        }
        for value in [None, Some(0)] {
            let create = CreateFiniteQueueRequest {
                reservation_limit_bytes: value,
                ..create.clone()
            };
            let decoded =
                CreateFiniteQueueRequest::decode(create.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded.reservation_limit_bytes, value);
            assert_eq!(
                reservation_limit(decoded.reservation_limit_bytes)
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
            let update = SetFiniteQueueDefinitionRequest {
                expected_generation: value,
                reservation_limit_bytes: value,
                ..update.clone()
            };
            let decoded =
                SetFiniteQueueDefinitionRequest::decode(update.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded.expected_generation, value);
            assert_eq!(decoded.reservation_limit_bytes, value);
            assert_eq!(
                expected_generation(decoded.expected_generation)
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        let mut wire = complete_configuration();
        wire.max_message_bytes = Some(u64::MAX);
        match usize::try_from(u64::MAX) {
            Ok(bytes) => assert_eq!(
                full_configuration(Some(&wire)).unwrap().max_message_bytes,
                bytes
            ),
            Err(_) => assert_eq!(
                full_configuration(Some(&wire)).unwrap_err().code(),
                tonic::Code::InvalidArgument
            ),
        }
    }

    #[test]
    fn finite_response_preserves_prepared_identity_and_aggregate_accounting() {
        let wire = complete_configuration();
        let config = full_configuration(Some(&wire)).unwrap();
        let path = EntityPath::new("q").unwrap();
        let view = QueueCapacityView {
            binding: EntityBinding::new(
                NamespaceName::new("n").unwrap(),
                path.clone(),
                path,
                EntityIncarnationKind::Queue,
                u64::MAX,
            )
            .unwrap(),
            config,
            capacity: QueueCapacityStatus::FiniteV1 {
                limit: FiniteQueueCapacity::new(u64::MAX).unwrap(),
                reserved_bytes: u64::MAX - 1,
                message_count: u64::MAX,
            },
        };
        let response = finite_response(view.clone()).unwrap();
        assert_eq!(
            response,
            FiniteQueue {
                namespace: "n".to_owned(),
                path: "q".to_owned(),
                generation: u64::MAX,
                config: Some(wire),
                reservation_limit_bytes: u64::MAX,
                reserved_logical_bytes: u64::MAX - 1,
                retained_message_count: u64::MAX,
            }
        );
        assert_eq!(
            FiniteQueue::decode(response.encode_to_vec().as_slice()).unwrap(),
            response
        );
        assert_eq!(
            finite_response(QueueCapacityView {
                capacity: QueueCapacityStatus::NonFinite,
                ..view
            })
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
