use super::*;
use domain::{QueueConfig, SubscriptionConfig};

#[path = "../../../tests/support/subscription_xml.rs"]
mod validate;

fn definition(config: SubscriptionConfig) -> EntityDefinition {
    EntityDefinition {
        namespace: NamespaceName::new("tenant").unwrap(),
        path: EntityPath::new("topic/subscriptions/sub").unwrap(),
        profile: EntityProfile::Subscription(config),
        placement_group_id: None,
        max_size_bytes: None,
        used_logical_bytes: None,
    }
}
fn projected(definition: EntityDefinition) -> Result<Vec<u8>, Failure> {
    project(
        definition,
        &NamespaceName::new("tenant").unwrap(),
        &EntityPath::new("topic").unwrap(),
        &SubscriptionName::new("sub").unwrap(),
    )
}

#[test]
fn exact_fields_boundaries_and_unavailable_extensions() {
    let normal = SubscriptionConfig {
        default_time_to_live_millis: Some(3600001),
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    };
    let bytes = projected(definition(normal)).unwrap();
    assert_eq!(
        validate::entry(&bytes).unwrap(),
        [
            "sub",
            "P0DT0H1M0.000S",
            "false",
            "P0DT1H0M0.001S",
            "true",
            "false",
            "10"
        ]
    );
    let unlimited = projected(definition(SubscriptionConfig::default())).unwrap();
    assert_eq!(
        validate::entry(&unlimited).unwrap()[3],
        "P10675199DT2H48M5.4775807S"
    );
    for config in [
        SubscriptionConfig {
            lock_duration_millis: 5000,
            default_time_to_live_millis: Some(1000),
            max_delivery_count: 1,
            dead_lettering_on_message_expiration: false,
            ..normal
        },
        SubscriptionConfig {
            lock_duration_millis: 300000,
            default_time_to_live_millis: Some(xml::MAX_TTL_MILLIS),
            max_delivery_count: i32::MAX as u32,
            ..normal
        },
    ] {
        validate::entry(&projected(definition(config)).unwrap()).unwrap();
    }
    for config in [
        SubscriptionConfig {
            lock_duration_millis: 4999,
            ..normal
        },
        SubscriptionConfig {
            default_time_to_live_millis: Some(999),
            ..normal
        },
        SubscriptionConfig {
            default_time_to_live_millis: Some(xml::MAX_TTL_MILLIS + 1),
            ..normal
        },
        SubscriptionConfig {
            default_time_to_live_millis: Some(u64::MAX),
            ..normal
        },
        SubscriptionConfig {
            max_delivery_count: i32::MAX as u32 + 1,
            ..normal
        },
    ] {
        assert!(config.validate().is_ok());
        assert_eq!(projected(definition(config)), Err(Failure::Unsupported));
    }
    for config in [
        SubscriptionConfig {
            lock_duration_millis: 0,
            ..normal
        },
        SubscriptionConfig {
            max_delivery_count: 0,
            ..normal
        },
        SubscriptionConfig {
            default_time_to_live_millis: Some(0),
            ..normal
        },
        SubscriptionConfig {
            lock_duration_millis: 300001,
            ..normal
        },
    ] {
        assert_eq!(projected(definition(config)), Err(Failure::Internal));
    }
    for field in 0..3 {
        let mut d = definition(normal);
        match field {
            0 => d.placement_group_id = Some(domain::PlacementGroupId::new("group").unwrap()),
            1 => d.max_size_bytes = Some(1),
            _ => d.used_logical_bytes = Some(0),
        }
        assert_eq!(projected(d), Err(Failure::Unsupported));
    }
    let mut d = definition(normal);
    d.profile = EntityProfile::Queue(QueueConfig::default());
    assert_eq!(projected(d), Err(Failure::Internal));
    let mut d = definition(normal);
    d.namespace = NamespaceName::new("other").unwrap();
    assert_eq!(projected(d), Err(Failure::Internal));
    let mut d = definition(normal);
    d.path = EntityPath::new("topic/subscriptions/sibling").unwrap();
    assert_eq!(projected(d), Err(Failure::Internal));
}

#[test]
fn closed_output_validator_rejects_extensions_namespaces_and_resource_abuse() {
    let bytes = projected(definition(SubscriptionConfig::default())).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    for bad in [
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession>false</RequiresSession><FutureFeature>1</FutureFeature>"),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession>false</RequiresSession><RequiresSession>false</RequiresSession>"),
        text.replace(xml::CONNECT, "urn:foreign"), text.replace(xml::ATOM, "urn:foreign"),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" i:nil=\"true\"/>"),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession><nested>false</nested></RequiresSession>"),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession><![CDATA[false]]></RequiresSession>"),
        text.replace("<entry ", "<!DOCTYPE entry [<!ENTITY injected 'false'>]><entry "),
        text.replace("<entry ", "<!DOCTYPE entry SYSTEM 'file:///not-read'><entry "),
        text.replace("<entry ", "<!DOCTYPE entry SYSTEM 'https://not-fetched.invalid/xml'><entry "),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession extra=\"1\">false</RequiresSession>"),
        text.replace("<RequiresSession>false</RequiresSession>", "<RequiresSession>".repeat(70).as_str()),
        " ".repeat(4097),
    ] { assert!(validate::entry(bad.as_bytes()).is_err()); }
    assert!(validate::entry(&[0xff]).is_err());
}

#[test]
fn error_bodies_are_bounded_namespace_free_and_fixed() {
    for error in [
        Failure::Invalid,
        Failure::Forbidden,
        Failure::NotFound,
        Failure::Unsupported,
        Failure::Unavailable,
        Failure::Internal,
    ] {
        let bytes = xml::error(error).unwrap();
        assert!(bytes.len() <= xml::XML_BYTES);
        let (code, detail) = validate::error(&bytes).unwrap();
        assert_eq!(code, error.status().as_u16());
        assert_eq!(detail, error.message());
    }
}
