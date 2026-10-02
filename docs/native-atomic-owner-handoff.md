# Paired Native And Logical Owner Handoff

`protocol-amqp::OwnedNativeAtomicMessagingSubmission` pairs one unique
[native ready submission](native-transactional-ingress.md#owner-handoff) with
one [logical registry submission](transaction-registry.md). The server's
`BrokerHandle::submit_native_atomic_messaging_owned` and blocking counterpart
carry that pair through one broker owner job.

This is a trusted Rust ownership boundary, not an enabled Service Bus wire
adapter. Constructing the pair does not prove matching transaction IDs,
controller provenance, message conversion, queue bindings, or authorization.
Service Bus listeners and default native connections still refuse transactions.

## Ownership And Cancellation

The non-clonable wrapper owns the logical pending-abort guard before its native
and logical submissions. Destruction aborts pending logical authority before
destroying the native ticket or original encoded content, then destroys staged
commands and refunds their work reservation. Consuming the wrapper disarms that
guard and transfers both submissions without cloning their payloads.

The asynchronous broker factory arms logical cancellation before the returned
future's first poll. Dropping a waiting caller can abort only pending logical
authority. If its paired job remains queued, the native claim cannot turn that
aborted logical work into a commit. Dropping the queued job destroys both
submissions in the wrapper's order. Dropping observer clones does not cancel;
those clones do not retain the unique payloads.

The owner decomposes both submissions into their tickets and retained resource
scopes before acquiring claims. It claims native authority first and logical
authority second, before broker clock sampling, entity validation, or storage
access. The logical claim checks its existing monotonic deadline. A refused
native claim drops the logical ticket before replying. A refused logical claim
records a known-no-I/O native abort before replying.

Once both claims have started, cancellation or connection retirement cannot
undo the owner operation. On unwind, the logical claim records Indeterminate
before the native claim does, before the retained native resources and logical
work lease are destroyed. Temporary command copies inside application do not
extend this guarantee beyond their existing owner scope.

## Application And Decisions

Bound work uses the existing same-primary-queue atomic application and its
[validation and resource bounds](atomic-queue-operations.md). Unbound empty
registry work commits through the owner without inventing a queue binding,
sampling the broker clock, accessing storage, or waking receivers. An empty
send batch that already bound a queue is not this unbound empty case.

Both successful and refused owner applications publish the logical decision
first and the corresponding native decision second, before receiver wakeups or
reply publication. Only a successful commit wakes affected receivers. The work
reservation remains held through those effects and the reply.

Logical validation or clock refusals record Rejected. Storage errors and
unexpected application outcomes record Indeterminate in both authorities.
A journal batch can survive a reported physical storage error and become
visible after reopen; an error is not proof of rollback or permission to retry.
See [Result Boundaries](atomic-commit-permits.md#result-boundaries).

## Completion And Wire Work

`NativeAtomicMessagingCompletion` is unique and retains the native resources
alongside an application result, including known owner failures. An outer
submission error means that the owner response and its resources are
unavailable. A refused queue admission recovers the original job and returns its
resources in a completion when possible.

The synchronous broker owner performs no native wire I/O. The caller consumes
the completion and finishes its resources asynchronously using the already
recorded decision. Losing the reply does not undo a commit; a failed wire flush
does not change the stored owner decision. A native claim refusal can leave its
resources Faulted, and `finish` currently refuses that state. Retaining those
resources is not a promise that a negative wire response can be sent.

Pairing introduces no new capacity pool: native groups retain their existing
32-group, 100-posting, 4 KiB control, and 32-record metadata-history limits;
logical work retains its shared 32-slot, 8 MiB content, and 131,072-value-item
limits. These remain accounting limits, not process-RSS guarantees.

## Remaining Wire Adapter

A serialized connection adapter must still bind actual native controller and
delivery provenance to the local registry, authorize data links, convert and
stage messages, retain exact prepared receipts, and submit matching native and
logical groups. It must keep processing admitted postings while waiting for
sealed readiness, rather than blocking their provisional acknowledgments.

The [consuming native refusal API](native-transaction-refusals.md) supplies a
pre-owner declaration and staging response, without reversing started or
indeterminate work. Transactional outgoing settlements,
SDK transaction scopes, durable recovery, and new persistent formats are not
enabled by this handoff.
