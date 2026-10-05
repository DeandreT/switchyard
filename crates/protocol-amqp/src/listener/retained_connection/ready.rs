use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::Mutex,
    task::Poll,
};

use super::{PrimaryPublisher, RetainedConnectionOutcome, locked};

/// Local data-capability staging only, not another outcome cell or role.
pub(in crate::listener) struct Primary {
    stage: Mutex<Stage>,
}

enum Stage {
    Available(PrimaryPublisher),
    Loaned,
    Published,
}

pub(in crate::listener) enum Attempt<'a> {
    Available(PrimaryLoan<'a>),
    Published,
}

impl Primary {
    pub(in crate::listener) fn new(capability: PrimaryPublisher) -> Self {
        Self {
            stage: Mutex::new(Stage::Available(capability)),
        }
    }

    pub(in crate::listener) fn loan(&self) -> Attempt<'_> {
        let capability = {
            let mut stage = locked(&self.stage);
            match &*stage {
                Stage::Published => return Attempt::Published,
                // Leaves restore before returning Pending. No parent observer
                // loans while polling a nested leaf, and Io never sees this cell.
                Stage::Loaned => None,
                Stage::Available(_) => {
                    let Stage::Available(capability) =
                        std::mem::replace(&mut *stage, Stage::Loaned)
                    else {
                        unreachable!("checked available primary")
                    };
                    Some(capability)
                }
            }
        };
        let capability = capability.expect("overlapping private primary polls");
        Attempt::Available(PrimaryLoan {
            primary: self,
            capability: Some(capability),
        })
    }
}

pub(in crate::listener) struct PrimaryLoan<'a> {
    primary: &'a Primary,
    capability: Option<PrimaryPublisher>,
}

impl PrimaryLoan<'_> {
    pub(in crate::listener) fn publish(mut self, value: RetainedConnectionOutcome) {
        // Every loan was constructed with the unique capability BEFORE raw poll.
        let capability = self.capability.take().expect("live primary loan");
        capability.publish(value);
        *locked(&self.primary.stage) = Stage::Published;
    }
}

impl Drop for PrimaryLoan<'_> {
    fn drop(&mut self) {
        if let Some(capability) = self.capability.take() {
            let previous = {
                let mut stage = locked(&self.primary.stage);
                std::mem::replace(&mut *stage, Stage::Available(capability))
            };
            drop(previous);
        }
    }
}

/// Maps actual Ready inside its poll, BEFORE completed future/capture Drop.
///
/// Pending callbacks are test-only and run outside every custody/data lock.
pub(in crate::listener) async fn observe<F, M, R>(
    future: F,
    mapping: M,
    #[cfg(test)] mut pending: impl FnMut(),
) -> R
where
    F: Future,
    M: FnOnce(F::Output) -> R,
{
    let mut future = pin!(future);
    let mut mapping = Some(mapping);
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(value) => Poll::Ready(mapping.take().expect("single Ready observation")(value)),
        Poll::Pending => {
            #[cfg(test)]
            pending();
            Poll::Pending
        }
    })
    .await
}

/// The loan exists BEFORE poll, so an absent capability never strands a Ready
/// error. Already-published branches do not poll another outcome-producing future.
pub(in crate::listener) async fn with_primary<F, M, R>(
    future: F,
    primary: &Primary,
    already_published: impl FnOnce() -> R,
    mut mapping: M,
    #[cfg(test)] mut pending: impl FnMut(),
) -> R
where
    F: Future,
    M: FnMut(F::Output, PrimaryLoan<'_>) -> R,
{
    let mut future = pin!(future);
    let mut already_published = Some(already_published);
    poll_fn(|cx| {
        let loan = match primary.loan() {
            Attempt::Available(loan) => loan,
            Attempt::Published => {
                return Poll::Ready(already_published.take().expect("single closed observation")());
            }
        };
        match future.as_mut().poll(cx) {
            Poll::Ready(value) => Poll::Ready(mapping(value, loan)),
            Poll::Pending => {
                drop(loan);
                #[cfg(test)]
                pending();
                Poll::Pending
            }
        }
    })
    .await
}
