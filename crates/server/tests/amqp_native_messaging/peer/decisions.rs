use super::*;

impl Peer {
    pub(in super::super) async fn committed(
        &mut self,
        original: &WireDelivery,
        post: u32,
        control: u32,
    ) -> TestResult {
        let expected = [
            (self.local(RECEIVE), Role::Sender, original.id),
            (self.local(POST), Role::Receiver, post),
        ];
        let mut seen = HashSet::new();
        for _ in 0..2 {
            let (channel, frame) = self.control().await?;
            let Performative::Disposition(disposition) = frame else {
                return Err("applied resource disposition missing".into());
            };
            assert!(disposition.settled);
            assert!(disposition.last.is_none());
            assert!(matches!(
                disposition.state,
                Some(DeliveryState::Accepted(_))
            ));
            let index = expected
                .iter()
                .position(|(expected_channel, role, id)| {
                    channel == *expected_channel
                        && disposition.role == *role
                        && disposition.first == *id
                })
                .expect("exact original resource, never control first");
            assert!(seen.insert(index));
        }
        let disposition = self.disposition(CONTROL, Role::Receiver, control).await?;
        assert!(disposition.settled);
        assert!(matches!(
            disposition.state,
            Some(DeliveryState::Accepted(_))
        ));
        Ok(())
    }

    pub(in super::super) async fn rollback_control(
        &mut self,
        original: &WireDelivery,
        transaction: &TransactionId,
        control: u32,
    ) -> TestResult {
        for _ in 0..2 {
            let (channel, frame) = self.control().await?;
            let Performative::Disposition(disposition) = frame else {
                return Err("rollback may not close or resend source".into());
            };
            assert!(disposition.last.is_none());
            if channel == self.local(CONTROL) {
                assert_eq!(disposition.role, Role::Receiver);
                assert_eq!(disposition.first, control);
                assert!(disposition.settled);
                assert!(matches!(
                    disposition.state,
                    Some(DeliveryState::Accepted(_))
                ));
                return Ok(());
            }
            assert_eq!(channel, self.local(RECEIVE));
            assert_eq!(disposition.role, Role::Sender);
            assert_eq!(disposition.first, original.id);
            assert!(!disposition.settled);
            assert!(
                matches!(disposition.state, Some(DeliveryState::Transactional(TransactionalState { txn_id, outcome: Some(Outcome::Accepted(_)) })) if txn_id == *transaction)
            );
        }
        Err("bounded explicit rollback response missing".into())
    }

    pub(in super::super) async fn rejected_mixed(
        &mut self,
        post: u32,
        control: u32,
        source_condition: &str,
    ) -> TestResult {
        let post_outcome = self.disposition(POST, Role::Receiver, post).await?;
        assert!(post_outcome.settled);
        assert!(
            post_outcome.state.is_none(),
            "refused enqueues have no applied outcome"
        );
        let control_outcome = self.disposition(CONTROL, Role::Receiver, control).await?;
        assert!(control_outcome.settled);
        let Some(DeliveryState::Rejected(amqp::Rejected { error: Some(error) })) =
            control_outcome.state
        else {
            return Err("mixed validation must reject the whole transaction".into());
        };
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:transaction:rollback"
        );
        self.detached(RECEIVE, RECEIVE_HANDLE, Some(source_condition), true)
            .await
    }

    pub(in super::super) async fn unknown_mixed(&mut self) -> TestResult {
        let mut seen = HashSet::new();
        for _ in 0..2 {
            let (channel, frame) = self.control().await?;
            let route = if channel == self.local(RECEIVE) {
                (RECEIVE, RECEIVE_HANDLE)
            } else if channel == self.local(CONTROL) {
                (CONTROL, CONTROL_HANDLE)
            } else {
                return Err("indeterminate completion cannot report posting success".into());
            };
            assert!(seen.insert(route.0));
            let Performative::Detach(detach) = frame else {
                return Err("unknown physical outcome has no positive settlement".into());
            };
            assert_eq!(detach.handle, 0);
            assert!(detach.closed);
            let error = detach.error.expect("static indeterminate refusal");
            assert_eq!(error.condition.as_symbol().as_str(), "amqp:internal-error");
            assert!(
                !error
                    .description
                    .as_deref()
                    .unwrap_or("")
                    .contains("DO-NOT-EXPOSE")
            );
            self.request_detach(route.0, route.1).await?;
        }
        Ok(())
    }
}
