use super::*;

#[test]
fn incarnation_construction_and_encoding_preserve_exhausted_retirement() -> Result<(), BrokerError>
{
    assert_eq!(
        EntityIncarnation::new(0, EntityIncarnationKind::Queue, false),
        Err(BrokerError::DanglingEntityMetadata)
    );
    for kind in [
        EntityIncarnationKind::Queue,
        EntityIncarnationKind::Topic,
        EntityIncarnationKind::Subscription,
    ] {
        let record = EntityIncarnation::new(u64::MAX, kind, true)?;
        assert_eq!(record.generation(), u64::MAX);
        assert_eq!(record.kind(), kind);
        assert!(record.is_retired());
        assert_eq!(
            crate::codec::encode(&record)?,
            crate::codec::encode(&(u64::MAX, kind, true))?
        );
        assert_eq!(
            crate::codec::decode::<EntityIncarnation>(&crate::codec::encode(&record)?)?,
            record
        );
    }
    Ok(())
}

#[test]
fn physical_targets_share_only_their_exact_owner() -> Result<(), BrokerError> {
    let namespace = NamespaceName::new("tenant")?;
    let primary = EntityPath::new("orders/Subscriptions")?;
    let child = primary.subscription(&SubscriptionName::new("Subscriptions")?)?;
    for (owner, kind) in [
        (&primary, EntityIncarnationKind::Queue),
        (&child, EntityIncarnationKind::Subscription),
    ] {
        for target in [owner.clone(), owner.dead_letter_queue()?] {
            let binding =
                EntityBinding::new(namespace.clone(), target.clone(), owner.clone(), kind, 7)?;
            assert_eq!(binding.target(), &target);
            assert_eq!(binding.owner(), owner);
            assert_eq!(binding.generation(), 7);
            assert_eq!(
                crate::codec::decode::<EntityBinding>(&crate::codec::encode(&binding)?)?,
                binding
            );
        }
    }
    assert_eq!(
        EntityBinding::new(
            namespace.clone(),
            primary.dead_letter_queue()?,
            primary.clone(),
            EntityIncarnationKind::Topic,
            1
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(
        EntityBinding::new(
            namespace,
            child.clone(),
            primary,
            EntityIncarnationKind::Queue,
            1
        ),
        Err(BrokerError::InvalidEntityBinding)
    );
    Ok(())
}

#[test]
fn decoded_bindings_revalidate_identifiers_and_owner_classes() -> Result<(), BrokerError> {
    let namespace = NamespaceName::new("tenant")?;
    let primary = EntityPath::new("orders")?;
    let valid = EntityBinding::new(
        namespace.clone(),
        primary.clone(),
        primary.clone(),
        EntityIncarnationKind::Queue,
        1,
    )?;
    for binding in [
        EntityBinding {
            generation: 0,
            ..valid.clone()
        },
        EntityBinding {
            namespace: crate::codec::decode(&crate::codec::encode(&"tenant\0other")?)?,
            ..valid.clone()
        },
        EntityBinding {
            owner: primary.dead_letter_queue()?,
            target: primary.dead_letter_queue()?,
            ..valid.clone()
        },
        EntityBinding {
            kind: EntityIncarnationKind::Subscription,
            ..valid.clone()
        },
        EntityBinding {
            owner: EntityPath::new("orders/subscriptions/Alpha/extra")?,
            target: EntityPath::new("orders/subscriptions/Alpha/extra")?,
            kind: EntityIncarnationKind::Subscription,
            ..valid
        },
    ] {
        let decoded = crate::codec::decode::<EntityBinding>(&crate::codec::encode(&binding)?)?;
        assert_eq!(decoded.validate(), Err(BrokerError::InvalidEntityBinding));
    }
    Ok(())
}

#[test]
fn fenced_envelope_does_not_change_legacy_command_bytes() -> Result<(), BrokerError> {
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let command = Command::new(
        namespace.clone(),
        entity.clone(),
        crate::Timestamp::from_millis(9),
        crate::CommandKind::CancelScheduled {
            sequences: Vec::new(),
        },
    );
    let bytes = crate::codec::encode(&command)?;
    assert_eq!(
        bytes,
        crate::codec::encode(&(
            &command.namespace,
            &command.entity,
            command.issued_at,
            &command.kind
        ))?
    );
    let binding = EntityBinding::new(
        namespace,
        entity.clone(),
        entity,
        EntityIncarnationKind::Queue,
        1,
    )?;
    let envelope = FencedCommand::new(binding, command.clone());
    let decoded = crate::codec::decode::<FencedCommand>(&crate::codec::encode(&envelope)?)?;
    assert_eq!(decoded, envelope);
    assert_eq!(crate::codec::encode(&decoded.command)?, bytes);
    Ok(())
}
