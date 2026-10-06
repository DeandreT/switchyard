use super::fixture::*;
use super::*;

#[tokio::test]
async fn planned_permission_denial_never_installs_or_credits_either_role() {
    for role in [Role::Sender, Role::Receiver] {
        for initial in [false, true] {
            let permissions = if role == Role::Sender {
                auth::PermissionSet::LISTEN
            } else {
                auth::PermissionSet::SEND
            };
            let mut fixture = Fixture::new(authorization(permissions, initial)).await;
            fixture.start_loop();
            let observed = caught(async {
                let request = request("denied", 17, role.clone(), "orders");
                fixture.send_attach(&request).await;
                fixture.pair(&request, "amqp:unauthorized-access").await;
                fixture.no_broker_effects();
            })
            .await;
            fixture.finish().await;
            fixture.joined();
            fixture.no_broker_effects();
            rethrow(observed);
        }
    }
}

#[tokio::test]
async fn entity_and_management_plan_errors_use_pending_refusal() {
    for role in [Role::Sender, Role::Receiver] {
        for (address, missing, condition) in [
            ("", false, "amqp:invalid-field"),
            ("missing", true, crate::NOT_FOUND),
            ("/$management", false, "amqp:invalid-field"),
            ("missing/$management", true, crate::NOT_FOUND),
        ] {
            let mut fixture = Fixture::new(authorization(
                auth::PermissionSet::SEND | auth::PermissionSet::LISTEN,
                true,
            ))
            .await;
            fixture
                .broker
                .missing
                .store(missing, std::sync::atomic::Ordering::SeqCst);
            fixture.start_loop();
            let observed = caught(async {
                let request = request("plan-error", 17, role.clone(), address);
                fixture.send_attach(&request).await;
                fixture.pair(&request, condition).await;
                assert_eq!(
                    fixture
                        .broker
                        .binds
                        .load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(missing)
                );
                assert!(fixture.broker.commands.lock().expect("commands").is_empty());
            })
            .await;
            fixture.finish().await;
            fixture.joined();
            assert!(fixture.worker_results.is_empty());
            rethrow(observed);
        }
    }
}
