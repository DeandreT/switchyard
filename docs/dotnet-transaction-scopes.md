# Experimental .NET Transaction Scopes

The explicit [atomic messaging listener](atomic-messaging-ingress.md) has
opt-in end-to-end gates for .NET Service Bus SDK 7.21.0 and 7.20.2 over trusted
raw TLS. Each pin runs against both memory storage and Fjall. This establishes
warmed, same-queue immediate send and held PeekLock Complete, not general
transaction compatibility or production support. Ordinary listeners and the
posting-only listener retain their previous policies.

## Gated Workflows

One experimental client and connection performs each scope on one primary
non-session queue. The gate uses Serializable isolation, async flow, a
30-second scope timeout, zero SDK retries, and receiver prefetch zero.

- A true two-member `ServiceBusMessageBatch` sent in an incomplete scope is
  rolled back without retaining either member.
- Completing the scope commits both batch members exactly once, preserving
  their individual bodies, identifiers, and typed application properties.
- A message acquired outside the scope can be completed alongside a send.
  Rolling that scope back retains the original sequence and held delivery;
  the replacement is absent.
- A second scope reuses the same `ServiceBusReceivedMessage`, receiver, and
  lock token. Committing Complete plus send removes the original and retains
  exactly its replacement. No new receive or replay bridges those scopes.
- The ordinary TLS listener refuses a transactional Send and remains usable
  for ordinary send, receive, and Complete afterward.

The Rust fixture checks canonical records and their exact ready, lock, and
expiry indexes after the client exits and the broker owner joins. Batch-only
outputs remain Ready. A held queue's committed replacement may already be
Locked by the consumer's next fetch; its precise token and index are checked.
The fixture drops the store, reopens Fjall, and repeats the record checks with
an unchanged snapshot. This is message persistence, not transaction-log
recovery or a graceful-shutdown guarantee.

## Authorization And Warmup

Send authorization must exist before opening the coordinator. The SDK enlists
before it opens the producer for a transactional Send, so a cold-first scope is
not supported. The gate opens a batch outside the scope to warm its producer.
For mixed work, sending and receiving the original outside the scope supplies
Send and Listen on the same experimental connection.

Acquisition happens outside ambient scopes. Peek verification uses a separate
ordinary client and occurs outside scopes; the experimental listener does not
serve management operations. Do not put management or renewal calls inside
these transaction scopes. Lock and authorization deadlines remain authoritative.
The listener's [attach-default opt-ins](transaction-attach-defaults.md), including
its narrow coordinator-count deviation, are still required.

## Refusal Proof And Limits

The pinned client can mask a session's `amqp:not-implemented` End error as a
sender-aborted `ServiceTimeout`. The negative gate temporarily observes the
public [AMQP state-transition diagnostic](https://raw.githubusercontent.com/Azure/azure-amqp/v2.7.0/Microsoft.Azure.Amqp/AmqpTrace.cs)
and checks the typed session error before link abortion; that ordering is
visible in the pinned [close-command path](https://raw.githubusercontent.com/Azure/azure-amqp/v2.7.0/Microsoft.Azure.Amqp/Amqp/AmqpObject.cs).
For that translated `ServiceTimeout` symptom, only the exact disabled-ingress
condition and description establish refusal. A timeout or cancellation alone
does not pass. The observer retains only a count, restores the previous provider,
and does not capture frames or credentials.
Unexpected scope-disposal errors and unknown commit outcomes fail the gate.

The existing same-queue, incarnation, live-lock, authorization, and resource
checks remain unchanged. The gate does not establish cold-first authorization,
cross-queue work, transactional acquisition, sessions, topics, dead-letter
sources, other settlement outcomes, WSS transaction scopes, management on the
experimental address, retry idempotency, or durable transaction recovery.
It does not retry an uncertain commit.

## Running The Gate

The opt-in gate requires Linux, .NET 8, NuGet access or a populated package
cache, and the repository's pinned Rust toolchain. It builds each SDK pin once
and runs its two storage backends sequentially:

```sh
CARGO_BUILD_JOBS=2 DOTNET_PROCESSOR_COUNT=2 cargo test -p server \
  --all-features --locked -j 2 --test amqp_dotnet_current atomic_messaging \
  -- --ignored --test-threads=1
```

The fixture uses an isolated trusted localhost CA, bounded output and process
deadlines, and private temporary artifacts. Failure cleanup kills the owned
process group while its leader is unreaped, then reaps the direct child.
This handles pipe-holding build workers; arbitrary detached descendants and
cancellation-time process-tree cleanup are not claimed. The pre-existing four
ordinary TLS and WSS gates are separate and unchanged.
