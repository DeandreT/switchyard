use super::group::{ControlData, Controller, DischargeStatus, Group, NativeRoute};
use super::*;

struct ControllerRoute {
    controller: NativeControllerIdentity,
    route: NativeRoute,
}

struct ReceiverRoute {
    route: NativeRoute,
    hook: Arc<NativeRetirementHook>,
}

struct SenderRoute {
    route: NativeRoute,
    hook: Arc<NativeRetirementHook>,
}

struct Terminal {
    id: TransactionId,
    controller: NativeControllerIdentity,
    group: Arc<Group>,
}

enum ControlPreparation {
    Declare,
    Discharge {
        group: Arc<Group>,
        fail: bool,
    },
    TerminalAbort {
        id: TransactionId,
        group: Arc<Group>,
    },
}

pub(in crate::server) struct NativeTransactionBook {
    connection: Option<NativeConnectionIdentity>,
    policy: NativeIngressPolicy,
    controllers: Vec<ControllerRoute>,
    receivers: Vec<ReceiverRoute>,
    senders: Vec<SenderRoute>,
    groups: HashMap<TransactionId, Arc<Group>>,
    terminals: VecDeque<Terminal>,
    cleanup: Arc<tokio::sync::Notify>,
}

impl NativeTransactionBook {
    pub(in crate::server) fn new(
        connection: &NativeConnectionIdentity,
        policy: NativeIngressPolicy,
    ) -> Self {
        Self {
            connection: Some(connection.clone()),
            policy,
            controllers: Vec::new(),
            receivers: Vec::new(),
            senders: Vec::new(),
            groups: HashMap::new(),
            terminals: VecDeque::new(),
            cleanup: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[cfg(any(test, feature = "test-client"))]
    pub(in crate::server) fn disabled() -> Self {
        Self {
            connection: None,
            policy: NativeIngressPolicy::Disabled,
            controllers: Vec::new(),
            receivers: Vec::new(),
            senders: Vec::new(),
            groups: HashMap::new(),
            terminals: VecDeque::new(),
            cleanup: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(in crate::server) fn policy(&self) -> NativeIngressPolicy {
        self.policy
    }

    pub(in crate::server) fn cleanup_notify(&self) -> Arc<tokio::sync::Notify> {
        self.cleanup.clone()
    }

    fn check_owner(&self, owner: &LinkIdentity) -> Result<(), NativeTransactionError> {
        if self.policy == NativeIngressPolicy::Disabled {
            return Err(NativeTransactionError::Disabled);
        }
        let same = self
            .connection
            .as_ref()
            .zip(owner.connection_identity())
            .is_some_and(|(connection, candidate)| {
                connection.same_connection(candidate) && connection.is_active()
            });
        if !same || owner.is_retired() {
            return Err(NativeTransactionError::Retired);
        }
        Ok(())
    }

    fn reap(&mut self) {
        self.controllers
            .retain(|record| record.controller.is_active() && !record.route.owner.is_retired());
        self.receivers
            .retain(|record| !record.route.owner.is_retired());
        self.senders
            .retain(|record| !record.route.owner.is_retired());
        let completed: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| group.is_terminal())
            .map(|(id, _)| id.clone())
            .collect();
        for id in completed {
            if let Some(group) = self.groups.remove(&id) {
                if self.terminals.len() == MAX_NATIVE_TERMINALS {
                    self.terminals.pop_front();
                }
                self.terminals.push_back(Terminal {
                    id,
                    controller: group.controller.clone(),
                    group,
                });
            }
        }
    }

    pub(in crate::server) fn accept_controller(
        &mut self,
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        commands: mpsc::Sender<Command>,
        profile: NativeCoordinatorProfile,
    ) -> Result<NativeControllerIdentity, NativeTransactionError> {
        self.check_owner(&owner)?;
        self.reap();
        if self.controllers.len() >= MAX_LINKS_PER_CONNECTION {
            return Err(NativeTransactionError::Limit);
        }
        let controller = NativeControllerIdentity(Controller::new(owner.clone(), profile));
        let hook = NativeRetirementHook::new();
        hook.controller(&controller.0)?;
        owner
            .install_native_retirement_hook(hook)
            .map_err(|_| NativeTransactionError::Retired)?;
        self.controllers.push(ControllerRoute {
            controller: controller.clone(),
            route: NativeRoute {
                channel,
                handle,
                owner,
                commands,
            },
        });
        Ok(controller)
    }

    pub(in crate::server) fn accept_receiver(
        &mut self,
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        commands: mpsc::Sender<Command>,
    ) -> Result<(), NativeTransactionError> {
        self.check_owner(&owner)?;
        self.reap();
        if self.receivers.len() >= MAX_LINKS_PER_CONNECTION {
            return Err(NativeTransactionError::Limit);
        }
        let hook = NativeRetirementHook::new();
        owner
            .install_native_retirement_hook(hook.clone())
            .map_err(|_| NativeTransactionError::Retired)?;
        self.receivers.push(ReceiverRoute {
            route: NativeRoute {
                channel,
                handle,
                owner,
                commands,
            },
            hook,
        });
        Ok(())
    }

    pub(in crate::server) fn begin_posting(
        &mut self,
        owner: &LinkIdentity,
        identity: DeliveryIdentity,
        state: &TransactionalState,
    ) -> Result<NativePartialPosting, NativeTransactionError> {
        self.check_owner(owner)?;
        self.reap();
        let group = self
            .groups
            .get(&state.txn_id)
            .cloned()
            .ok_or(NativeTransactionError::UnknownTransaction)?;
        if state.outcome.is_some() {
            group.fault(NativeFault::Stage);
            return Err(NativeTransactionError::Unsupported);
        }
        if !group.controller.is_active() {
            return Err(NativeTransactionError::Retired);
        }
        let receiver = self
            .receivers
            .iter()
            .find(|record| record.route.owner.same_link(owner))
            .ok_or(NativeTransactionError::InvalidAttach)?;
        if let Err(error) = receiver.hook.track(&group) {
            group.fault(NativeFault::Stage);
            return Err(error);
        }
        let obligation = match group.reserve(
            receiver.route.channel,
            receiver.route.handle,
            owner.clone(),
            identity,
        ) {
            Ok(obligation) => obligation,
            Err(error) => {
                group.fault(NativeFault::Stage);
                return Err(error);
            }
        };
        Ok(NativePartialPosting::new(group, obligation))
    }

    pub(in crate::server) fn preflight_accept_sender(
        &mut self,
        owner: &LinkIdentity,
    ) -> Result<(), NativeTransactionError> {
        if self.policy != NativeIngressPolicy::PostingAndRetirement {
            return Err(NativeTransactionError::Disabled);
        }
        self.check_owner(owner)?;
        self.reap();
        if self.senders.len() >= MAX_LINKS_PER_CONNECTION {
            return Err(NativeTransactionError::Limit);
        }
        Ok(())
    }

    pub(in crate::server) fn accept_sender(
        &mut self,
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        commands: mpsc::Sender<Command>,
    ) -> Result<(), NativeTransactionError> {
        self.preflight_accept_sender(&owner)?;
        let hook = NativeRetirementHook::new();
        owner
            .install_native_retirement_hook(hook.clone())
            .map_err(|_| NativeTransactionError::Retired)?;
        self.senders.push(SenderRoute {
            route: NativeRoute {
                channel,
                handle,
                owner,
                commands,
            },
            hook,
        });
        Ok(())
    }

    pub(in crate::server) fn preflight_sender(
        &self,
        owner: &LinkIdentity,
    ) -> Result<(), NativeTransactionError> {
        if self.policy != NativeIngressPolicy::PostingAndRetirement {
            return Err(NativeTransactionError::Disabled);
        }
        self.check_owner(owner)?;
        if !self
            .senders
            .iter()
            .any(|record| record.route.owner.same_link(owner))
        {
            return Err(NativeTransactionError::InvalidAttach);
        }
        Ok(())
    }

    pub(in crate::server) fn begin_retirements(
        &mut self,
        state: &TransactionalState,
        candidates: &[NativeRetirementCandidate],
    ) -> Result<Vec<NativeRetirementAttempt>, NativeTransactionError> {
        if self.policy != NativeIngressPolicy::PostingAndRetirement {
            return Err(NativeTransactionError::Disabled);
        }
        self.reap();
        let group = self
            .groups
            .get(&state.txn_id)
            .cloned()
            .ok_or(NativeTransactionError::UnknownTransaction)?;
        if !matches!(&state.outcome, Some(Outcome::Accepted(_))) {
            group.fault(NativeFault::Stage);
            return Err(NativeTransactionError::Unsupported);
        }
        if group.state() != NativeTransactionState::Pending || !group.controller.is_active() {
            return Err(group.error());
        }
        if candidates.len() > MAX_NATIVE_TRANSACTION_POSTINGS {
            group.fault(NativeFault::Stage);
            return Err(NativeTransactionError::Limit);
        }
        for candidate in candidates {
            if let Err(error) = self.preflight_sender(&candidate.owner) {
                group.fault(NativeFault::Stage);
                return Err(error);
            }
            if !candidate
                .delivery_identity
                .owner()
                .same_link(&candidate.owner)
                || !self.senders.iter().any(|record| {
                    record.route.channel == candidate.channel
                        && record.route.handle == candidate.handle
                        && record.route.owner.same_link(&candidate.owner)
                })
            {
                group.fault(NativeFault::Stage);
                return Err(NativeTransactionError::InvalidPreparedSet);
            }
        }
        let attempts = match group.reserve_retirements(candidates) {
            Ok(attempts) => attempts,
            Err(error) => {
                group.fault(NativeFault::Stage);
                return Err(error);
            }
        };
        for candidate in candidates {
            let sender = self
                .senders
                .iter()
                .find(|record| record.route.owner.same_link(&candidate.owner))
                .ok_or(NativeTransactionError::Retired)?;
            if let Err(error) = sender.hook.track(&group) {
                group.fault(NativeFault::Stage);
                return Err(error);
            }
        }
        if group.state() != NativeTransactionState::Pending {
            return Err(group.error());
        }
        Ok(attempts)
    }

    pub(in crate::server) fn fault_retirement(
        &self,
        state: Option<&DeliveryState>,
        fault: NativeFault,
    ) {
        if let Some(DeliveryState::Transactional(state)) = state
            && let Some(group) = self.groups.get(&state.txn_id)
        {
            group.fault(fault);
        }
    }

    pub(in crate::server) fn fault_posting(
        &self,
        state: Option<&DeliveryState>,
        fault: NativeFault,
    ) {
        if let Some(DeliveryState::Transactional(state)) = state
            && let Some(group) = self.groups.get(&state.txn_id)
        {
            group.fault(fault);
        }
    }

    pub(in crate::server) fn publish_posting(
        &mut self,
        partial: NativePartialPosting,
        delivery: Delivery,
        sink: &mpsc::Sender<TransactionalIngress>,
    ) -> Result<(), NativeTransactionError> {
        let route = self
            .receivers
            .iter()
            .find(|record| record.route.owner.same_link(&partial.obligation.owner))
            .map(|record| record.route.clone())
            .ok_or(NativeTransactionError::Retired)?;
        if !partial
            .obligation
            .identity
            .same_delivery(&delivery.identity)
        {
            return Err(NativeTransactionError::InvalidPreparedSet);
        }
        let posting = partial.into_post(route, delivery);
        if let Err(error) = sink.try_send(TransactionalIngress::Posting(posting)) {
            if let TransactionalIngress::Posting(posting) = error.into_inner() {
                posting.data.group.fault(NativeFault::Inbox);
            }
            return Err(NativeTransactionError::Limit);
        }
        Ok(())
    }

    pub(in crate::server) fn publish_control(
        &mut self,
        channel: u16,
        handle: u32,
        owner: &LinkIdentity,
        delivery: Delivery,
        sink: &mpsc::Sender<CoordinatorRequest>,
    ) -> Result<(), Box<NativeControlRefusal>> {
        self.reap();
        let record = self.controllers.iter().find(|record| {
            record.route.channel == channel
                && record.route.handle == handle
                && record.route.owner.same_link(owner)
        });
        let controller = record.map(|record| record.controller.clone());
        let route = record.map(|record| record.route.clone());
        let result = self.check_owner(owner).and_then(|()| {
            let controller = controller.as_ref().ok_or(NativeTransactionError::Retired)?;
            if !controller.is_active() {
                return Err(NativeTransactionError::Retired);
            }
            if delivery.message_format != 0 {
                return Err(NativeTransactionError::Unsupported);
            }
            self.prepare_control(controller, delivery.message())
        });
        let preparation = match result {
            Ok(preparation) => preparation,
            Err(error) => {
                return Err(NativeControlRefusal::new(
                    error,
                    channel,
                    handle,
                    owner.clone(),
                    controller,
                    delivery,
                ));
            }
        };
        let (Some(route), Some(controller)) = (route, controller) else {
            return Err(NativeControlRefusal::new(
                NativeTransactionError::Retired,
                channel,
                handle,
                owner.clone(),
                None,
                delivery,
            ));
        };
        let event = match preparation {
            ControlPreparation::Declare => CoordinatorRequest::Declare(PendingDeclareReceipt {
                data: ControlData::new(route, controller, delivery, None, false),
            }),
            ControlPreparation::Discharge { group, fail } => {
                CoordinatorRequest::Discharge(SealedDischargeReceipt {
                    data: ControlData::new(route, controller, delivery, Some(group.clone()), fail),
                    status: DischargeStatus::Live(group),
                })
            }
            ControlPreparation::TerminalAbort { id, group } => {
                let state = group.state();
                let mut data = ControlData::new(route, controller, delivery, Some(group), true);
                data.terminal_abort = true;
                data.disarm();
                CoordinatorRequest::Discharge(SealedDischargeReceipt {
                    data,
                    status: DischargeStatus::Terminal { id, state },
                })
            }
        };
        if let Err(error) = sink.try_send(event) {
            let data = match error.into_inner() {
                CoordinatorRequest::Declare(receipt) => receipt.data,
                CoordinatorRequest::Discharge(receipt) => receipt.data,
            };
            if let Some(group) = &data.group {
                group.fault(NativeFault::Inbox);
            }
            return Err(NativeControlRefusal::from_data(
                NativeTransactionError::Faulted(NativeFault::Inbox),
                Box::new(data),
                false,
            ));
        }
        Ok(())
    }

    fn prepare_control(
        &self,
        controller: &NativeControllerIdentity,
        message: &Message,
    ) -> Result<ControlPreparation, NativeTransactionError> {
        match control_command(message)? {
            TransactionCommand::Declare(declare) => {
                if declare.global_id.is_some() {
                    return Err(NativeTransactionError::Unsupported);
                }
                Ok(ControlPreparation::Declare)
            }
            TransactionCommand::Discharge(discharge) => {
                let fail = discharge.fail.unwrap_or(false);
                if let Some(group) = self.groups.get(&discharge.txn_id) {
                    if !group.controller.same_controller(controller) {
                        return Err(NativeTransactionError::UnknownTransaction);
                    }
                    if !matches!(
                        group.state(),
                        NativeTransactionState::Pending | NativeTransactionState::Faulted
                    ) {
                        return Err(NativeTransactionError::InvalidDecision);
                    }
                    group.seal(fail)?;
                    return Ok(ControlPreparation::Discharge {
                        group: group.clone(),
                        fail,
                    });
                }
                let terminal = self
                    .terminals
                    .iter()
                    .find(|terminal| {
                        terminal.id == discharge.txn_id
                            && terminal.controller.same_controller(controller)
                    })
                    .ok_or(NativeTransactionError::UnknownTransaction)?;
                if terminal.group.state() == NativeTransactionState::Faulted
                    && terminal.group.error()
                        == NativeTransactionError::Faulted(NativeFault::PartialAtSeal)
                {
                    return Err(NativeTransactionError::Faulted(NativeFault::PartialAtSeal));
                }
                if fail
                    && matches!(
                        terminal.group.state(),
                        NativeTransactionState::Faulted
                            | NativeTransactionState::Aborted
                            | NativeTransactionState::Rejected
                    )
                {
                    terminal.group.known_abort()?;
                    return Ok(ControlPreparation::TerminalAbort {
                        id: terminal.id.clone(),
                        group: terminal.group.clone(),
                    });
                }
                if terminal.group.state() == NativeTransactionState::Faulted {
                    return Err(terminal.group.error());
                }
                Err(NativeTransactionError::InvalidDecision)
            }
        }
    }

    pub(in crate::server) fn register_declare(
        &mut self,
        data: &ControlData,
        id: TransactionId,
    ) -> Result<Arc<Group>, NativeTransactionError> {
        self.check_owner(&data.route.owner)?;
        self.reap();
        if !data.controller.is_active()
            || !self
                .controllers
                .iter()
                .any(|record| record.controller.same_controller(&data.controller))
        {
            return Err(NativeTransactionError::Retired);
        }
        if self.groups.len() >= MAX_NATIVE_TRANSACTIONS {
            return Err(NativeTransactionError::Limit);
        }
        if self.groups.contains_key(&id) || self.terminals.iter().any(|terminal| terminal.id == id)
        {
            return Err(NativeTransactionError::UnknownTransaction);
        }
        let group =
            Group::new_with_cleanup(id.clone(), data.controller.clone(), self.cleanup.clone());
        data.controller.0.register(&group)?;
        self.groups.insert(id, group.clone());
        Ok(group)
    }

    pub(in crate::server) fn close_all(&mut self) {
        for group in self.groups.values() {
            group.fault(NativeFault::Closed);
        }
        for record in &self.controllers {
            record.controller.0.close();
        }
        for record in &self.receivers {
            record.hook.close();
        }
        for record in &self.senders {
            record.hook.close();
        }
    }
}

impl Drop for NativeTransactionBook {
    fn drop(&mut self) {
        self.close_all();
    }
}
