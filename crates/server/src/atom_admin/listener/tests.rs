use super::*;

fn broker() -> crate::Broker {
    crate::Broker::spawn(crate::LocalProposer::new(
        domain::StateMachine::new(storage::MemoryStore::default()),
        crate::ManualClock::at(1_000),
    ))
}

fn tls() -> rustls::ServerConfig {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    protocol_amqp::tls_server_config(cert.pem().as_bytes(), key_pair.serialize_pem().as_bytes())
        .unwrap()
}

#[test]
fn the_fixed_audience_must_be_a_namespace_scope() {
    let broker = broker();
    let policy = SharedAccessPolicy::new([]).unwrap();
    let result = AtomAdminListener::new(
        broker.handle(),
        NamespaceName::new("tenant").unwrap(),
        policy,
        ResourceScope::entity("tenant.servicebus.windows.net", "orders").unwrap(),
        tls(),
    );
    assert!(matches!(result, Err(AtomAdminError::InvalidAudience)));
}

#[test]
fn a_deserialized_namespace_is_revalidated_before_listener_construction() {
    let malformed: NamespaceName =
        domain::codec::decode(&domain::codec::encode(&"").unwrap()).unwrap();
    let broker = broker();
    let result = AtomAdminListener::new(
        broker.handle(),
        malformed,
        SharedAccessPolicy::new([]).unwrap(),
        ResourceScope::namespace("tenant.servicebus.windows.net").unwrap(),
        tls(),
    );
    assert!(matches!(result, Err(AtomAdminError::InvalidNamespace)));
}
