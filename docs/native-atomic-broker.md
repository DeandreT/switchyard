# Native Atomic Broker Boundary

`protocol-amqp::NativeAtomicBroker` is an opt-in extension of the ordinary
`Broker` trait for a trusted [paired native/logical submission](native-atomic-owner-handoff.md).
Ordinary broker implementations and mocks do not acquire transaction methods.
The protocol crate does not depend on the server crate, storage backend, or
local proposer error type.

## Owned Submission And Reply

The extension's plain method returns an owned, Send, static future. The server
implementation constructs its existing guarded submission future synchronously,
before wrapping it in asynchronous result conversion. Dropping the wrapper
before its first poll therefore retains the original pending-only cancellation
behavior; it does not postpone arming cancellation until polling.

`NativeAtomicBrokerCompletion` owns an application result and the unique native
resources. It is not clonable. `application()` borrows the result; consuming
`into_parts()` moves both parts out for asynchronous native finalization. Its
trusted `from_owner_parts()` constructor does not verify native/logical work
correspondence or create commit authority.

Known owner errors remain inside a completion with its resources. In particular,
closed queue admission can return an OwnerStopped result while retaining those
resources. The outer `NativeAtomicResponseUnavailable` instead means the owner
completion and its resources were not returned. It does not prove that storage
was untouched, a queued command was canceled, or a transaction rolled back.

## Portable Errors

The portable owner result preserves native claim errors, logical claim errors,
ordinary domain refusals, clock regression, and owner stoppage. Physical storage
errors, unexpected owner outcomes, and unavailable owner work use static
Indeterminate causes. Backend details and unexpected-outcome text are not copied
into that uncertainty result.

This conversion follows the existing owner classification; it does not publish
another decision, retry work, or alter either permit. The recorded native and
logical states remain authoritative. A physical commit can have applied the
whole batch even when its persistence call reports failure, so an indeterminate
result is never a guaranteed rollback or retry-safe absence.

Native resource `finish()` still follows the recorded decision and performs its
own original-delivery checks. A successful broker application is not a flushed
wire reply. Faulted resources can refuse finalization, and losing a reply cannot
reverse a started owner.

This boundary adds no authorization, receiver admission, body conversion,
connection supervision, persistent format, or recovery log. A serialized,
authorized connection adapter is still required; the explicit
[posting-only listener](atomic-posting-ingress.md) provides a narrow one.
Default Service Bus listener
transactions and SDK transaction scopes remain disabled.
