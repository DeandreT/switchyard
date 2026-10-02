# Native Transaction Refusals

The opt-in [native transactional ingress](native-transactional-ingress.md) API
lets a trusted adapter consume original control receipts when declaration or
message staging cannot proceed. These responses do not enable Service Bus
listener transactions or establish authorization and logical/native
correspondence.

## Declaration

`PendingDeclareReceipt::refuse` consumes the actor-approved declaration with one
`NativeDeclarationRefusal`: ResourceLimit or Unavailable. The reasons have fixed,
bounded descriptions and use `amqp:transaction:rollback`. This is a local error
taxonomy for an unsuccessful declaration, not evidence that an existing or
started group rolled back. No native group or transaction ID is allocated by
this refusal. Native control-side group-limit errors also use this transaction
condition; ordinary data-link resource-limit errors keep their existing condition.

The response uses Rejected only if the original coordinator source advertised
it; otherwise it detaches that coordinator. This follows
[AMQP Part 4, section 4.2](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html).
A successfully flushed Rejected leaves the controller and its other groups
active. A scoped detach retires that controller's pending groups, not unrelated
controllers or sessions. The adapter remains responsible for any logical
reservation it created before declining the native declaration.

## Staging

`SealedDischargeReceipt::refuse_staging` consumes only the original live
Discharge whose fail flag was false. It never accepts that failed commit request
as though the controller had asked to roll back.

The group atomically becomes known Aborted only from Sealed, Ready, or a
non-partial pending fault; an already known Aborted group remains so. The same
state transition competes with native owner acquisition. Pending, OwnerStarted,
Committed, Rejected, Indeterminate, and terminal replay receipts cannot use this
path. In particular, an owner-issued Rejected decision is not relabeled as a
pre-owner abort. These refusals make no storage calls.

Original complete or provisionally acknowledged postings receive no-outcome
cleanup dispositions before the control response is sent. Cleanup uses bounded
original delivery proofs, including their exact link and delivery generations;
reused numeric IDs and tags cannot authorize dispositions against replacements.
First settlement retires the original alias; Second leaves it awaiting sender
acknowledgment. Caller-held posting receipts and prepared postings retain their
original encoded-content charges until they are destroyed.

The control response carries `amqp:transaction:rollback`: negotiated Rejected,
or a scoped coordinator detach if Rejected was not advertised. A partial posting
at discharge still requires coordinator detachment even when Rejected was
advertised, as required by
[AMQP Part 4, sections 4.3 and 4.4.1](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html).
That case does not run incomplete-post settlement or turn partial work into a
prepared group.

## Lifetime And Limits

The actor checks the exact original live, complete, unsettled control delivery
before responding. A locally retired or sender-settled original cannot mint a
successful replacement response. Disposition bookkeeping is committed only
after write and flush. The error-detach path retains its existing eager link
retirement and ledger cleanup before I/O; a failed detach does not undo that
retirement. A write or flush failure terminates the native actor; it is not a
successful wire acknowledgment.

Dropping an unpolled refusal future follows the existing receipt destruction
rules and sends no response. After queue admission, the command can outlive the
waiting caller and complete its response. Caller cancellation therefore is not
proof that no wire operation occurred. Neither path revokes an owner that has
started or resolves an indeterminate physical commit.

These APIs retain the existing bounded group, posting, control-content, and
metadata-history limits. They add no storage format or recovery log. The
[paired broker handoff](native-atomic-owner-handoff.md) still needs a serialized,
authorized connection adapter; faulted owner-completion resources do not gain a
new guaranteed negative wire response from these pre-owner receipt methods.
