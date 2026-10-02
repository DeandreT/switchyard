use super::{
    DeliveryIdentity, IncomingLedger, IncomingLedgerError, LinkIdentity, Phase, SettlementAction,
    Terminal,
};
use crate::ReceiverSettleMode;

impl IncomingLedger {
    pub(in crate::server) fn preflight_transactional_provisional(
        &self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        if !identity.belongs_to(owner) {
            return Err(IncomingLedgerError::WrongOwner);
        }
        let delivery = self.live_delivery(identity)?;
        if delivery.phase != Phase::Complete || delivery.remote_settled {
            return Err(IncomingLedgerError::TransactionalDelivery);
        }
        Ok(())
    }

    pub(in crate::server) fn mark_transactional_provisional(
        &mut self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        self.preflight_transactional_provisional(owner, identity)?;
        self.deliveries
            .get_mut(&identity.id())
            .ok_or(IncomingLedgerError::UnknownDelivery)?
            .phase = Phase::TransactionalProvisional;
        Ok(())
    }

    pub(in crate::server) fn finish_transactional_post(
        &self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<SettlementAction, IncomingLedgerError> {
        if !identity.belongs_to(owner) {
            return Err(IncomingLedgerError::WrongOwner);
        }
        if owner.is_retired()
            || matches!(
                identity.terminal(),
                Terminal::Settled | Terminal::Invalidated
            )
        {
            return Ok(SettlementAction::NoDisposition);
        }
        let delivery = self.live_delivery(identity)?;
        if delivery.phase == Phase::AwaitingSenderAck {
            return Ok(SettlementAction::NoDisposition);
        }
        if delivery.phase != Phase::TransactionalProvisional {
            return Err(IncomingLedgerError::TransactionalDelivery);
        }
        Ok(SettlementAction::SendDisposition {
            settled: delivery.receiver_mode == Some(ReceiverSettleMode::First),
        })
    }

    pub(in crate::server) fn commit_transactional_post(
        &mut self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        match self.finish_transactional_post(owner, identity)? {
            SettlementAction::NoDisposition => {}
            SettlementAction::SendDisposition { settled: true } => {
                self.release(identity, Terminal::Settled);
            }
            SettlementAction::SendDisposition { settled: false } => {
                self.deliveries
                    .get_mut(&identity.id())
                    .ok_or(IncomingLedgerError::UnknownDelivery)?
                    .phase = Phase::AwaitingSenderAck;
            }
        }
        Ok(())
    }

    pub(in crate::server) fn finish_transactional_abort(
        &self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<SettlementAction, IncomingLedgerError> {
        if !identity.belongs_to(owner) {
            return Err(IncomingLedgerError::WrongOwner);
        }
        if owner.is_retired() || identity.terminal() != Terminal::Live {
            return Ok(SettlementAction::NoDisposition);
        }
        let delivery = self.live_delivery(identity)?;
        if delivery.phase == Phase::AwaitingSenderAck {
            return Ok(SettlementAction::NoDisposition);
        }
        if !matches!(
            delivery.phase,
            Phase::Complete | Phase::TransactionalProvisional
        ) {
            return Err(IncomingLedgerError::IncompleteDelivery);
        }
        if delivery.remote_settled {
            return Ok(SettlementAction::NoDisposition);
        }
        Ok(SettlementAction::SendDisposition {
            settled: delivery.receiver_mode == Some(ReceiverSettleMode::First),
        })
    }

    pub(in crate::server) fn commit_transactional_abort(
        &mut self,
        owner: &LinkIdentity,
        identity: &DeliveryIdentity,
    ) -> Result<(), IncomingLedgerError> {
        match self.finish_transactional_abort(owner, identity)? {
            SettlementAction::NoDisposition => {
                if !owner.is_retired() && identity.terminal() == Terminal::Live {
                    let delivery = self.live_delivery(identity)?;
                    if delivery.remote_settled
                        && matches!(
                            delivery.phase,
                            Phase::Complete | Phase::TransactionalProvisional
                        )
                    {
                        self.release(identity, Terminal::Settled);
                    }
                }
            }
            SettlementAction::SendDisposition { settled: true } => {
                self.release(identity, Terminal::Settled);
            }
            SettlementAction::SendDisposition { settled: false } => {
                self.deliveries
                    .get_mut(&identity.id())
                    .ok_or(IncomingLedgerError::UnknownDelivery)?
                    .phase = Phase::AwaitingSenderAck;
            }
        }
        Ok(())
    }
}
