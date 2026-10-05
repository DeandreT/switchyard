use admin_api::{FILE_DESCRIPTOR_SET, PROTOBUF_PACKAGE, v1};
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet};

// Frozen complete pre-increment services, message fields, oneofs and enum values.
const ORIGINAL: &str = r"E|EntityKind
F|AuditRecord|actor|3|1|9|-|false|-
F|AuditRecord|entity|5|1|9|-|false|-
F|AuditRecord|namespace|4|1|9|-|false|-
F|AuditRecord|operation|6|1|9|-|false|-
F|AuditRecord|previous_hash|8|1|12|-|false|-
F|AuditRecord|record_hash|9|1|12|-|false|-
F|AuditRecord|result|7|1|9|-|false|-
F|AuditRecord|sequence|1|1|4|-|false|-
F|AuditRecord|unix_millis|2|1|4|-|false|-
F|Cluster|cluster_id|1|1|9|-|false|-
F|Cluster|version|2|1|9|-|false|-
F|Cluster|voters|3|1|13|-|false|-
F|CorrelationProperty|name|1|1|9|-|false|-
F|CorrelationProperty|value|2|1|11|.switchyard.admin.v1.RuleScalarValue|false|-
F|CorrelationRuleFilter|content_type|8|1|9|-|true|_content_type
F|CorrelationRuleFilter|correlation_id|1|1|9|-|true|_correlation_id
F|CorrelationRuleFilter|message_id|2|1|9|-|true|_message_id
F|CorrelationRuleFilter|properties|9|3|11|.switchyard.admin.v1.CorrelationProperty|false|-
F|CorrelationRuleFilter|reply_to_session_id|7|1|9|-|true|_reply_to_session_id
F|CorrelationRuleFilter|reply_to|4|1|9|-|true|_reply_to
F|CorrelationRuleFilter|session_id|6|1|9|-|true|_session_id
F|CorrelationRuleFilter|subject|5|1|9|-|true|_subject
F|CorrelationRuleFilter|to|3|1|9|-|true|_to
F|CreateEntityRequest|default_ttl_millis|6|1|4|-|false|-
F|CreateEntityRequest|kind|3|1|14|.switchyard.admin.v1.EntityKind|false|-
F|CreateEntityRequest|lock_duration_millis|7|1|4|-|false|-
F|CreateEntityRequest|max_delivery_count|8|1|13|-|false|-
F|CreateEntityRequest|max_size_bytes|5|1|4|-|false|-
F|CreateEntityRequest|namespace|1|1|9|-|false|-
F|CreateEntityRequest|path|2|1|9|-|false|-
F|CreateEntityRequest|placement_group_id|4|1|9|-|false|-
F|CreateEntityRequest|queue_config|10|1|11|.switchyard.admin.v1.QueueConfiguration|false|-
F|CreateEntityRequest|requires_session|9|1|8|-|false|-
F|CreateEntityRequest|subscription_config|12|1|11|.switchyard.admin.v1.SubscriptionConfiguration|false|-
F|CreateEntityRequest|topic_config|11|1|11|.switchyard.admin.v1.TopicConfiguration|false|-
F|CreateNamespaceRequest|key_provider|3|1|9|-|false|-
F|CreateNamespaceRequest|key_reference|4|1|9|-|false|-
F|CreateNamespaceRequest|name|1|1|9|-|false|-
F|CreateNamespaceRequest|storage_quota_bytes|2|1|4|-|false|-
F|CreateRuleRequest|filter|4|1|11|.switchyard.admin.v1.RuleFilter|false|-
F|CreateRuleRequest|namespace|1|1|9|-|false|-
F|CreateRuleRequest|name|3|1|9|-|false|-
F|CreateRuleRequest|subscription_path|2|1|9|-|false|-
F|CreateRuleWithActionRequest|action|5|1|11|.switchyard.admin.v1.SqlRuleAction|false|-
F|CreateRuleWithActionRequest|filter|4|1|11|.switchyard.admin.v1.RuleFilter|false|-
F|CreateRuleWithActionRequest|namespace|1|1|9|-|false|-
F|CreateRuleWithActionRequest|name|3|1|9|-|false|-
F|CreateRuleWithActionRequest|subscription_path|2|1|9|-|false|-
F|DeleteEntityRequest|kind|3|1|14|.switchyard.admin.v1.EntityKind|false|-
F|DeleteEntityRequest|namespace|1|1|9|-|false|-
F|DeleteEntityRequest|path|2|1|9|-|false|-
F|DeleteNamespaceRequest|name|1|1|9|-|false|-
F|DeleteRuleRequest|namespace|1|1|9|-|false|-
F|DeleteRuleRequest|name|3|1|9|-|false|-
F|DeleteRuleRequest|subscription_path|2|1|9|-|false|-
F|Entity|kind|3|1|14|.switchyard.admin.v1.EntityKind|false|-
F|Entity|max_size_bytes|5|1|4|-|true|_max_size_bytes
F|Entity|namespace|1|1|9|-|false|-
F|Entity|path|2|1|9|-|false|-
F|Entity|placement_group_id|4|1|9|-|false|-
F|Entity|queue_config|7|1|11|.switchyard.admin.v1.QueueConfiguration|false|-
F|Entity|subscription_config|9|1|11|.switchyard.admin.v1.SubscriptionConfiguration|false|-
F|Entity|topic_config|8|1|11|.switchyard.admin.v1.TopicConfiguration|false|-
F|Entity|used_logical_bytes|6|1|4|-|true|_used_logical_bytes
F|GetEntityRequest|namespace|1|1|9|-|false|-
F|GetEntityRequest|path|2|1|9|-|false|-
F|GetNamespaceRequest|name|1|1|9|-|false|-
F|GetOperationRequest|operation_id|1|1|9|-|false|-
F|GetRuleRequest|include_actions|4|1|8|-|false|-
F|GetRuleRequest|namespace|1|1|9|-|false|-
F|GetRuleRequest|name|3|1|9|-|false|-
F|GetRuleRequest|subscription_path|2|1|9|-|false|-
F|ListEntitiesRequest|kind|4|1|14|.switchyard.admin.v1.EntityKind|false|-
F|ListEntitiesRequest|namespace|1|1|9|-|false|-
F|ListEntitiesRequest|page_size|3|1|13|-|false|-
F|ListEntitiesRequest|page_token|2|1|9|-|false|-
F|ListEntitiesRequest|parent_topic|5|1|9|-|false|-
F|ListEntitiesResponse|entities|1|3|11|.switchyard.admin.v1.Entity|false|-
F|ListEntitiesResponse|next_page_token|2|1|9|-|false|-
F|ListNamespacesRequest|page_size|2|1|13|-|false|-
F|ListNamespacesRequest|page_token|1|1|9|-|false|-
F|ListNamespacesResponse|namespaces|1|3|11|.switchyard.admin.v1.Namespace|false|-
F|ListNamespacesResponse|next_page_token|2|1|9|-|false|-
F|ListNodesResponse|nodes|1|3|11|.switchyard.admin.v1.Node|false|-
F|ListRulesRequest|include_actions|3|1|8|-|false|-
F|ListRulesRequest|namespace|1|1|9|-|false|-
F|ListRulesRequest|subscription_path|2|1|9|-|false|-
F|ListRulesResponse|rules|1|3|11|.switchyard.admin.v1.Rule|false|-
F|Namespace|key_provider|4|1|9|-|false|-
F|Namespace|name|1|1|9|-|false|-
F|Namespace|storage_quota_bytes|2|1|4|-|false|-
F|Namespace|used_logical_bytes|3|1|4|-|false|-
F|Node|address|2|1|9|-|false|-
F|Node|availability_zone|3|1|9|-|false|-
F|Node|node_id|1|1|9|-|false|-
F|Node|state|4|1|9|-|false|-
F|Operation|error|3|1|9|-|false|-
F|Operation|operation_id|1|1|9|-|false|-
F|Operation|state|2|1|9|-|false|-
F|QueueConfiguration|dead_lettering_on_message_expiration|8|1|8|-|true|_dead_lettering_on_message_expiration
F|QueueConfiguration|default_ttl_millis|3|1|4|-|false|default_time_to_live
F|QueueConfiguration|default_ttl_unlimited|9|1|11|.switchyard.admin.v1.UnlimitedTimeToLive|false|default_time_to_live
F|QueueConfiguration|duplicate_detection_history_time_window_millis|7|1|4|-|true|_duplicate_detection_history_time_window_millis
F|QueueConfiguration|lock_duration_millis|1|1|4|-|true|_lock_duration_millis
F|QueueConfiguration|max_delivery_count|2|1|13|-|true|_max_delivery_count
F|QueueConfiguration|max_message_bytes|4|1|4|-|true|_max_message_bytes
F|QueueConfiguration|requires_duplicate_detection|6|1|8|-|true|_requires_duplicate_detection
F|QueueConfiguration|requires_session|5|1|8|-|true|_requires_session
F|RuleFilter|correlation_filter|3|1|11|.switchyard.admin.v1.CorrelationRuleFilter|false|filter
F|RuleFilter|false_filter|2|1|11|.switchyard.admin.v1.FalseRuleFilter|false|filter
F|RuleFilter|sql_filter|4|1|11|.switchyard.admin.v1.SqlRuleFilter|false|filter
F|RuleFilter|true_filter|1|1|11|.switchyard.admin.v1.TrueRuleFilter|false|filter
F|RuleScalarValue|binary_value|19|1|12|-|false|value
F|RuleScalarValue|bool_value|2|1|8|-|false|value
F|RuleScalarValue|byte_value|7|1|17|-|false|value
F|RuleScalarValue|char_codepoint|16|1|13|-|false|value
F|RuleScalarValue|decimal128_bytes|15|1|12|-|false|value
F|RuleScalarValue|decimal32_bytes|13|1|12|-|false|value
F|RuleScalarValue|decimal64_bytes|14|1|12|-|false|value
F|RuleScalarValue|double_bits|12|1|6|-|false|value
F|RuleScalarValue|float_bits|11|1|7|-|false|value
F|RuleScalarValue|int_value|9|1|17|-|false|value
F|RuleScalarValue|long_value|10|1|18|-|false|value
F|RuleScalarValue|null_value|1|1|11|.switchyard.admin.v1.RuleNullValue|false|value
F|RuleScalarValue|short_value|8|1|17|-|false|value
F|RuleScalarValue|string_value|20|1|9|-|false|value
F|RuleScalarValue|symbol_value|21|1|9|-|false|value
F|RuleScalarValue|timestamp_millis|17|1|18|-|false|value
F|RuleScalarValue|ubyte_value|3|1|13|-|false|value
F|RuleScalarValue|uint_value|5|1|13|-|false|value
F|RuleScalarValue|ulong_value|6|1|4|-|false|value
F|RuleScalarValue|ushort_value|4|1|13|-|false|value
F|RuleScalarValue|uuid_bytes|18|1|12|-|false|value
F|Rule|action|6|1|11|.switchyard.admin.v1.SqlRuleAction|false|-
F|Rule|created_at_unix_millis|5|1|4|-|false|-
F|Rule|filter|4|1|11|.switchyard.admin.v1.RuleFilter|false|-
F|Rule|namespace|1|1|9|-|false|-
F|Rule|name|3|1|9|-|false|-
F|Rule|subscription_path|2|1|9|-|false|-
F|SqlRuleAction|expression|1|1|9|-|false|-
F|SqlRuleAction|semantic_version|2|1|13|-|true|_semantic_version
F|SqlRuleFilter|expression|1|1|9|-|false|-
F|SqlRuleFilter|semantic_version|2|1|13|-|true|_semantic_version
F|StartBackupRequest|destination|1|1|9|-|false|-
F|StreamAuditRequest|after_sequence|2|1|4|-|false|-
F|StreamAuditRequest|namespace|1|1|9|-|false|-
F|SubscriptionConfiguration|dead_lettering_on_filter_evaluation_exceptions|8|1|8|-|true|_dead_lettering_on_filter_evaluation_exceptions
F|SubscriptionConfiguration|dead_lettering_on_message_expiration|6|1|8|-|true|_dead_lettering_on_message_expiration
F|SubscriptionConfiguration|default_ttl_millis|3|1|4|-|false|default_time_to_live
F|SubscriptionConfiguration|default_ttl_unlimited|7|1|11|.switchyard.admin.v1.UnlimitedTimeToLive|false|default_time_to_live
F|SubscriptionConfiguration|lock_duration_millis|1|1|4|-|true|_lock_duration_millis
F|SubscriptionConfiguration|max_delivery_count|2|1|13|-|true|_max_delivery_count
F|SubscriptionConfiguration|max_message_bytes|4|1|4|-|true|_max_message_bytes
F|SubscriptionConfiguration|requires_session|5|1|8|-|true|_requires_session
F|TopicConfiguration|default_ttl_millis|1|1|4|-|false|default_time_to_live
F|TopicConfiguration|default_ttl_unlimited|5|1|11|.switchyard.admin.v1.UnlimitedTimeToLive|false|default_time_to_live
F|TopicConfiguration|duplicate_detection_history_time_window_millis|4|1|4|-|true|_duplicate_detection_history_time_window_millis
F|TopicConfiguration|max_message_bytes|2|1|4|-|true|_max_message_bytes
F|TopicConfiguration|requires_duplicate_detection|3|1|8|-|true|_requires_duplicate_detection
F|UpdateEntityRequest|namespace|1|1|9|-|false|-
F|UpdateEntityRequest|path|2|1|9|-|false|-
F|UpdateEntityRequest|queue_config|3|1|11|.switchyard.admin.v1.QueueConfiguration|false|-
F|UpdateEntityRequest|subscription_config|5|1|11|.switchyard.admin.v1.SubscriptionConfiguration|false|-
F|UpdateEntityRequest|topic_config|4|1|11|.switchyard.admin.v1.TopicConfiguration|false|-
M|AuditRecord
M|Cluster
M|CorrelationProperty
M|CorrelationRuleFilter
M|CreateEntityRequest
M|CreateNamespaceRequest
M|CreateRuleRequest
M|CreateRuleWithActionRequest
M|DeleteEntityRequest
M|DeleteNamespaceRequest
M|DeleteRuleRequest
M|Entity
M|FalseRuleFilter
M|GetClusterRequest
M|GetEntityRequest
M|GetNamespaceRequest
M|GetOperationRequest
M|GetRuleRequest
M|ListEntitiesRequest
M|ListEntitiesResponse
M|ListNamespacesRequest
M|ListNamespacesResponse
M|ListNodesRequest
M|ListNodesResponse
M|ListRulesRequest
M|ListRulesResponse
M|Namespace
M|Node
M|Operation
M|QueueConfiguration
M|Rule
M|RuleFilter
M|RuleMutationResponse
M|RuleNullValue
M|RuleScalarValue
M|SqlRuleAction
M|SqlRuleFilter
M|StartBackupRequest
M|StreamAuditRequest
M|SubscriptionConfiguration
M|TopicConfiguration
M|TrueRuleFilter
M|UnlimitedTimeToLive
M|UpdateEntityRequest
O|CorrelationRuleFilter|_content_type
O|CorrelationRuleFilter|_correlation_id
O|CorrelationRuleFilter|_message_id
O|CorrelationRuleFilter|_reply_to
O|CorrelationRuleFilter|_reply_to_session_id
O|CorrelationRuleFilter|_session_id
O|CorrelationRuleFilter|_subject
O|CorrelationRuleFilter|_to
O|Entity|_max_size_bytes
O|Entity|_used_logical_bytes
O|QueueConfiguration|_dead_lettering_on_message_expiration
O|QueueConfiguration|_duplicate_detection_history_time_window_millis
O|QueueConfiguration|_lock_duration_millis
O|QueueConfiguration|_max_delivery_count
O|QueueConfiguration|_max_message_bytes
O|QueueConfiguration|_requires_duplicate_detection
O|QueueConfiguration|_requires_session
O|QueueConfiguration|default_time_to_live
O|RuleFilter|filter
O|RuleScalarValue|value
O|SqlRuleAction|_semantic_version
O|SqlRuleFilter|_semantic_version
O|SubscriptionConfiguration|_dead_lettering_on_filter_evaluation_exceptions
O|SubscriptionConfiguration|_dead_lettering_on_message_expiration
O|SubscriptionConfiguration|_lock_duration_millis
O|SubscriptionConfiguration|_max_delivery_count
O|SubscriptionConfiguration|_max_message_bytes
O|SubscriptionConfiguration|_requires_session
O|SubscriptionConfiguration|default_time_to_live
O|TopicConfiguration|_duplicate_detection_history_time_window_millis
O|TopicConfiguration|_max_message_bytes
O|TopicConfiguration|_requires_duplicate_detection
O|TopicConfiguration|default_time_to_live
R|AuditService|StreamAudit|.switchyard.admin.v1.StreamAuditRequest|.switchyard.admin.v1.AuditRecord|false|true
R|BackupService|GetOperation|.switchyard.admin.v1.GetOperationRequest|.switchyard.admin.v1.Operation|false|false
R|BackupService|StartBackup|.switchyard.admin.v1.StartBackupRequest|.switchyard.admin.v1.Operation|false|false
R|ClusterService|GetCluster|.switchyard.admin.v1.GetClusterRequest|.switchyard.admin.v1.Cluster|false|false
R|ClusterService|ListNodes|.switchyard.admin.v1.ListNodesRequest|.switchyard.admin.v1.ListNodesResponse|false|false
R|EntityService|CreateEntity|.switchyard.admin.v1.CreateEntityRequest|.switchyard.admin.v1.Entity|false|false
R|EntityService|DeleteEntity|.switchyard.admin.v1.DeleteEntityRequest|.switchyard.admin.v1.Operation|false|false
R|EntityService|GetEntity|.switchyard.admin.v1.GetEntityRequest|.switchyard.admin.v1.Entity|false|false
R|EntityService|ListEntities|.switchyard.admin.v1.ListEntitiesRequest|.switchyard.admin.v1.ListEntitiesResponse|false|false
R|EntityService|UpdateEntity|.switchyard.admin.v1.UpdateEntityRequest|.switchyard.admin.v1.Entity|false|false
R|NamespaceService|CreateNamespace|.switchyard.admin.v1.CreateNamespaceRequest|.switchyard.admin.v1.Namespace|false|false
R|NamespaceService|DeleteNamespace|.switchyard.admin.v1.DeleteNamespaceRequest|.switchyard.admin.v1.Operation|false|false
R|NamespaceService|GetNamespace|.switchyard.admin.v1.GetNamespaceRequest|.switchyard.admin.v1.Namespace|false|false
R|NamespaceService|ListNamespaces|.switchyard.admin.v1.ListNamespacesRequest|.switchyard.admin.v1.ListNamespacesResponse|false|false
R|RuleService|CreateRuleWithAction|.switchyard.admin.v1.CreateRuleWithActionRequest|.switchyard.admin.v1.RuleMutationResponse|false|false
R|RuleService|CreateRule|.switchyard.admin.v1.CreateRuleRequest|.switchyard.admin.v1.RuleMutationResponse|false|false
R|RuleService|DeleteRule|.switchyard.admin.v1.DeleteRuleRequest|.switchyard.admin.v1.RuleMutationResponse|false|false
R|RuleService|GetRule|.switchyard.admin.v1.GetRuleRequest|.switchyard.admin.v1.Rule|false|false
R|RuleService|ListRules|.switchyard.admin.v1.ListRulesRequest|.switchyard.admin.v1.ListRulesResponse|false|false
S|AuditService
S|BackupService
S|ClusterService
S|EntityService
S|NamespaceService
S|RuleService
V|EntityKind|ENTITY_KIND_QUEUE|1
V|EntityKind|ENTITY_KIND_SUBSCRIPTION|3
V|EntityKind|ENTITY_KIND_TOPIC|2
V|EntityKind|ENTITY_KIND_UNSPECIFIED|0";
fn descriptor() -> FileDescriptorProto {
    FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
        .expect("generated descriptor")
        .file
        .into_iter()
        .find(|file| file.package.as_deref() == Some(PROTOBUF_PACKAGE))
        .expect("native administration package")
}

fn records(file: &FileDescriptorProto) -> Vec<String> {
    let mut rows = Vec::new();
    for service in &file.service {
        let name = service.name.as_deref().expect("service name");
        rows.push(format!("S|{name}"));
        for method in &service.method {
            rows.push(format!(
                "R|{name}|{}|{}|{}|{}|{}",
                method.name.as_deref().expect("method"),
                method.input_type.as_deref().expect("input"),
                method.output_type.as_deref().expect("output"),
                method.client_streaming.unwrap_or(false),
                method.server_streaming.unwrap_or(false)
            ));
        }
    }
    for message in &file.message_type {
        let name = message.name.as_deref().expect("message name");
        rows.push(format!("M|{name}"));
        assert!(
            message.nested_type.is_empty()
                && message.enum_type.is_empty()
                && message.extension.is_empty()
        );
        assert!(message.reserved_range.is_empty() && message.reserved_name.is_empty());
        for oneof in &message.oneof_decl {
            rows.push(format!(
                "O|{name}|{}",
                oneof.name.as_deref().expect("oneof")
            ));
        }
        for field in &message.field {
            let oneof = field
                .oneof_index
                .map(|index| {
                    message.oneof_decl[index as usize]
                        .name
                        .as_deref()
                        .expect("oneof")
                })
                .unwrap_or("-");
            rows.push(format!(
                "F|{name}|{}|{}|{}|{}|{}|{}|{oneof}",
                field.name.as_deref().expect("field"),
                field.number.expect("number"),
                field.label.expect("label"),
                field.r#type.expect("type"),
                field.type_name.as_deref().unwrap_or("-"),
                field.proto3_optional.unwrap_or(false)
            ));
            assert!(field.extendee.is_none() && field.default_value.is_none());
        }
    }
    for enumeration in &file.enum_type {
        let name = enumeration.name.as_deref().expect("enum name");
        rows.push(format!("E|{name}"));
        for value in &enumeration.value {
            rows.push(format!(
                "V|{name}|{}|{}",
                value.name.as_deref().expect("value"),
                value.number.expect("number")
            ));
        }
    }
    rows.sort();
    rows
}

#[test]
fn additive_descriptor_preserves_old_admin_contracts() {
    let file = descriptor();
    assert_eq!(file.syntax.as_deref(), Some("proto3"));
    let mut expected: Vec<String> = ORIGINAL.lines().map(str::to_owned).collect();
    expected.extend([
        "S|MaintenanceService",
        "R|MaintenanceService|GetClockReadiness|.switchyard.admin.v1.GetClockReadinessRequest|.switchyard.admin.v1.ClockReadinessResponse|false|false",
        "M|GetClockReadinessRequest", "F|GetClockReadinessRequest|namespace|1|1|9|-|false|-",
        "M|ClockReadinessResponse", "F|ClockReadinessResponse|state|1|1|14|.switchyard.admin.v1.MaintenanceClockState|false|-",
        "E|MaintenanceClockState",
        "V|MaintenanceClockState|MAINTENANCE_CLOCK_STATE_UNKNOWN|0",
        "V|MaintenanceClockState|MAINTENANCE_CLOCK_STATE_READY|1",
        "V|MaintenanceClockState|MAINTENANCE_CLOCK_STATE_UNSAFE|2",
        "V|MaintenanceClockState|MAINTENANCE_CLOCK_STATE_UNAVAILABLE|3",
        "V|MaintenanceClockState|MAINTENANCE_CLOCK_STATE_STOPPED|4",
    ].into_iter().map(str::to_owned));
    expected.sort();
    assert_eq!(records(&file), expected);
}

#[test]
fn default_and_five_states_have_exact_wire_encodings() {
    assert_eq!(
        v1::MaintenanceClockState::default(),
        v1::MaintenanceClockState::Unknown
    );
    assert_eq!(v1::ClockReadinessResponse::default().state, 0);
    for (state, expected) in [
        (0, vec![]),
        (1, vec![8, 1]),
        (2, vec![8, 2]),
        (3, vec![8, 3]),
        (4, vec![8, 4]),
    ] {
        let response = v1::ClockReadinessResponse { state };
        assert_eq!(response.encode_to_vec(), expected);
        assert!(response.encoded_len() <= 2);
        assert_eq!(
            v1::ClockReadinessResponse::decode(expected.as_slice()).expect("known state"),
            response
        );
        assert_eq!(
            v1::MaintenanceClockState::try_from(state).expect("known enum") as i32,
            state
        );
    }
    let unknown = v1::ClockReadinessResponse::decode([8, 5].as_slice())
        .expect("protobuf preserves numeric unknown");
    assert_eq!(unknown.state, 5);
    assert!(v1::MaintenanceClockState::try_from(unknown.state).is_err());
    assert_eq!(unknown.encode_to_vec(), [8, 5]);
    let request = v1::GetClockReadinessRequest {
        namespace: "tenant".into(),
    };
    assert_eq!(
        request.encode_to_vec(),
        [10, 6, b't', b'e', b'n', b'a', b'n', b't']
    );
}
