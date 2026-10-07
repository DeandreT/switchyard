use domain::{EntityPath, NamespaceName};

use super::*;

mod decode;
mod duration;
mod encode;
mod limits;

fn document(properties: &str) -> String {
    format!(
        "<entry xmlns=\"{ATOM_NS}\"><content type=\"application/xml\"><QueueDescription xmlns=\"{SERVICE_BUS_NS}\">{properties}</QueueDescription></content></entry>"
    )
}

fn parse(properties: &str) -> Result<AtomQueueDefinition, AtomXmlError> {
    decode_definition(document(properties).as_bytes())
}

fn property(name: &str, value: &str) -> String {
    format!("<{name}>{value}</{name}>")
}

fn default_definition() -> AtomQueueDefinition {
    AtomQueueDefinition {
        config: QueueConfig {
            duplicate_detection_history_time_window_millis: 60_000,
            ..QueueConfig::default()
        },
        limit: FiniteQueueCapacity::new(1_024 * MIB).unwrap(),
    }
}

fn view(path: &str) -> QueueCapacityView {
    let target = EntityPath::new(path).unwrap();
    let definition = default_definition();
    QueueCapacityView {
        binding: EntityBinding::new(
            NamespaceName::new("test").unwrap(),
            target.clone(),
            target,
            EntityIncarnationKind::Queue,
            1,
        )
        .unwrap(),
        config: definition.config,
        capacity: QueueCapacityStatus::FiniteV1 {
            limit: definition.limit,
            reserved_bytes: 0,
            message_count: 0,
        },
    }
}
