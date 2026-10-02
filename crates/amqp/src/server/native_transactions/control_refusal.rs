use super::group::ControlData;
use super::*;

enum RefusedControl {
    Delivery(Box<RetainedDelivery>),
    Receipt(Box<ControlData>),
}

#[derive(Clone, Copy)]
enum RefusalCause {
    Native(NativeTransactionError),
    Declaration(NativeDeclarationRefusal),
    Staging,
}

pub(in crate::server) struct NativeControlRefusal {
    cause: RefusalCause,
    pub(in crate::server) channel: u16,
    pub(in crate::server) handle: u32,
    pub(in crate::server) owner: LinkIdentity,
    controller: Option<NativeControllerIdentity>,
    pub(in crate::server) published: bool,
    receipt: RefusedControl,
}

impl NativeControlRefusal {
    pub(in crate::server) fn new(
        error: NativeTransactionError,
        channel: u16,
        handle: u32,
        owner: LinkIdentity,
        controller: Option<NativeControllerIdentity>,
        delivery: Delivery,
    ) -> Box<Self> {
        Box::new(Self {
            cause: RefusalCause::Native(error),
            channel,
            handle,
            owner,
            controller,
            published: false,
            receipt: RefusedControl::Delivery(Box::new(RetainedDelivery::new(delivery))),
        })
    }

    pub(in crate::server) fn from_data(
        error: NativeTransactionError,
        data: Box<ControlData>,
        published: bool,
    ) -> Box<Self> {
        Self::from_receipt(RefusalCause::Native(error), data, published)
    }

    pub(in crate::server) fn declaration(
        reason: NativeDeclarationRefusal,
        data: Box<ControlData>,
    ) -> Box<Self> {
        Self::from_receipt(RefusalCause::Declaration(reason), data, true)
    }

    pub(in crate::server) fn staging(data: Box<ControlData>) -> Box<Self> {
        Self::from_receipt(RefusalCause::Staging, data, true)
    }

    fn from_receipt(cause: RefusalCause, data: Box<ControlData>, published: bool) -> Box<Self> {
        Box::new(Self {
            cause,
            channel: data.route.channel,
            handle: data.route.handle,
            owner: data.route.owner.clone(),
            controller: Some(data.controller.clone()),
            published,
            receipt: RefusedControl::Receipt(data),
        })
    }

    pub(in crate::server) fn identity(&self) -> &DeliveryIdentity {
        match &self.receipt {
            RefusedControl::Delivery(delivery) => &delivery.inner().identity,
            RefusedControl::Receipt(data) => &data.delivery.inner().identity,
        }
    }

    pub(in crate::server) fn disarm(&mut self) {
        if let RefusedControl::Receipt(data) = &mut self.receipt {
            data.disarm();
        }
    }

    pub(in crate::server) fn supports_rejected(&self) -> bool {
        self.controller
            .as_ref()
            .is_some_and(|controller| controller.0.profile.outcomes & 2 != 0)
    }

    pub(in crate::server) fn is_partial_at_seal(&self) -> bool {
        matches!(
            self.cause,
            RefusalCause::Native(NativeTransactionError::Faulted(NativeFault::PartialAtSeal))
        )
    }

    pub(in crate::server) fn condition(&self) -> &'static str {
        match self.cause {
            RefusalCause::Native(NativeTransactionError::Limit) => "amqp:transaction:rollback",
            RefusalCause::Native(error) => error.condition(),
            RefusalCause::Declaration(_) | RefusalCause::Staging => "amqp:transaction:rollback",
        }
    }

    pub(in crate::server) fn description(&self) -> &'static str {
        match self.cause {
            RefusalCause::Native(error) => error.description(),
            RefusalCause::Declaration(NativeDeclarationRefusal::ResourceLimit) => {
                "native transaction declaration resource limit reached"
            }
            RefusalCause::Declaration(NativeDeclarationRefusal::Unavailable) => {
                "native transaction declaration is unavailable"
            }
            RefusalCause::Staging => "native transaction staging was refused",
        }
    }
}
