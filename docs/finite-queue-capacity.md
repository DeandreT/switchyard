# Finite Queue Capacity

Finite capacity is an opt-in logical reservation policy for a primary queue and
its dead-letter shadow. It is not physical disk usage, encoded AMQP size, a
namespace quota, or measured Azure byte accounting. Ordinary creation remains
non-finite; it still writes mandatory generation-bound capacity-mode metadata.

## API And Scope

The separate `QueueCapacityCommandV1` supports `CreateFinite`,
`SetLimitFenced`, and the append-only `SetDefinitionFenced` variant.
`FiniteQueueCapacity` accepts nonzero unsigned 64-bit limits.
`StateMachine::describe_queue_capacity` is clock-free. The broker handle's
asynchronous and blocking describe methods run that read in one serialized owner
turn. The view contains the binding, queue configuration, mode, reserved bytes,
and message count. Capacity mutations stamp their time and prepare their return
view before the storage batch commits, without a post-commit reread.
Describing an owner checks its metadata and aggregate, not its entire message
ledger. Existing command variants, queue configuration, protobuf, and
committed-entry encodings are unchanged.

Finite queues currently exclude session-required queues and duplicate detection.
An ordinary queue may retain an optional session identifier as metadata; that
does not grant session ownership or affinity. Topics, subscriptions, session
state, duplicate-history storage, and namespace accounting are outside this
policy. A dead-letter shadow shares its primary owner's limit and cannot have a
separate capacity mode or aggregate.

Limit and desired-definition changes require the live primary-queue binding.
Stale bindings are refused before the proposer reads its host clock. Deterministic
apply repeats the fence before reading the stored command clock. Lowering a limit
below retained reservations is refused atomically. Restating the existing limit,
or the complete unchanged configuration and limit, is a no-op: no storage batch
applies and the stored command clock does not advance. Proposer time stamping and
stored-clock regression validation still occur for a no-op.

The broker handle's `set_finite_queue_definition_fenced` and
`set_finite_queue_definition_fenced_blocking` methods accept a complete
`QueueConfig` and finite limit together. This replaces all eight configuration
fields rather than patching them; an absent lifetime means unlimited. The current
finite-owner profile is checked before the desired configuration. Existing
queue-update validation applies: immutable session and duplicate-detection
settings are rejected before other invalid desired configuration, and desired
configuration is validated before a limit below retained usage is rejected.

Changed primary and shadow configurations and/or owner capacity mode, together
with the command clock, commit in one batch. Validation or capacity refusal
commits none of them. Message records, charges, aggregates, counters, session
metadata, and existing deadlines are not rewritten. A changed maximum message
size constrains future message admission, not already retained messages. The
operation does not repair corrupt usage or reconcile the ledger.

Non-finite queues cannot be promoted in place, and finite queues cannot be
demoted. Delete and recreate is explicit, with a new entity generation; there is
no ledger backfill or migration API.

These methods are trusted library APIs, not authorization boundaries. The native
gRPC `FiniteQueueService` separately exposes authenticated create, get and
generation-fenced full-definition replacement, including logical usage; see
[Native Finite Queue Administration](compatibility.md#native-finite-queue-administration).
Legacy `EntityService` capacity fields remain unexposed. The separate opt-in authenticated
HTTPS endpoint exposes finite ordinary queue creation and full-definition
updates through Atom `MaxSizeInMegabytes` and `MaxMessageSizeInKilobytes` fields;
it does not expose usage metrics. Its SAS authorization, limits, defaults and
unsupported definitions are documented in
[Library HTTPS Queue Administration](compatibility.md#library-https-queue-administration).
Both pinned .NET administration gates cover this profile on Memory and Fjall,
including below-retained-usage limit-update refusal with real trusted-owner seeds;
see [Official .NET Queue Administration](compatibility.md#official-net-queue-administration).
The server can explicitly enable this endpoint with dedicated TLS/audience/key
options; see [HTTPS Administration CLI](compatibility.md#https-administration-cli).
Both pinned .NET clients also exercise ordinary message ingress and reservation
recovery over private-CA WSS; see [Official .NET Ingress Gates](#official-net-ingress-gates).
`switchyardctl finite-queue` exposes native create, get and full-definition
replacement with an explicit caller generation; see
[Native Finite Queue CLI](compatibility.md#native-finite-queue-cli).

## Reservation Model

Each retained message has a generation-bound charge with numeric components:

```text
C = P + S + 256 + max(256, D)
```

For legacy byte-body messages, `P` is body bytes plus five bytes and the UTF-8
identifier length. For rich messages, it is the larger of the compatibility-body
size and retained envelope content size, including any authoritative identifier
not already represented there. `S` is zero without an original session ID,
otherwise five bytes plus its UTF-8 size. `D` is zero without dead-letter details,
otherwise 81 bytes plus the UTF-8 reason and description lengths. These constants
are local model version 1, not storage-record or wire-envelope overheads.

The prepaid dead-letter reserve covers automatic lifetime and delivery-count
reasons. Explicit dead-letter details or retained property updates can increase a
charge and are refused if the resulting total exceeds the limit. A valid proposed
reservation that overflows its unsigned total is also a capacity refusal, not
stored-state corruption.

Ready, locked, deferred, scheduled, and dead-letter messages all retain their
reservation. Peek-lock receive, renewal, and lock expiry do not free it. Completion,
receive-delete, schedule cancellation, and expiry without dead-lettering refund
the original charge. Completing with unretained property updates does not charge
those discarded updates. Retained updates can grow or shrink a reservation.
Schedule activation moves the charge to the activated sequence without charging
twice. Dead-letter transfer strips the outer lifetime and session metadata but
preserves the original session-byte reservation and producer envelope.

The normal message mutations, charge sidecars, and aggregate update share one
storage batch. Validation or capacity refusal commits none of them. Ordered
atomic messaging can reuse credit released by an earlier completion in that
same operation; a later refusal rolls back the complete operation. Capacity
refusals map to AMQP `amqp:resource-limit-exceeded` and native resource exhaustion,
without exposing stored records in the error.

## Validation And Work Bounds

Private Mode, Usage, and Charge values use canonical version-11 envelopes of at
most 64 bytes. Their schema, model, generation, arithmetic, and physical owner
must agree. Finite mutations validate each original message observation against
its charge and the aggregate. Distinct observed original charges must fit the
initial aggregate, including a valid residual byte/count pair. This detects
inconsistencies encountered by that operation; it does not scan or reconcile the
entire ledger. A raw queue-config lookup is not an owner-health proof.

One capacity plan bounds 1,024 distinct source or admission message identities,
2,048 events, 2,048 ledger
keys, 4,096 added read/query requests, 4 MiB of added read-key bytes, and 256 KiB
of materialized read values. Ordinary finite owner-health checks include an exact
one-row probe of the owner's subscription-TopicMode prefix, which must be empty.
The native capacity read budget charges that requested prefix once, plus any
returned key/value bytes; it does not charge an implicit start a second time.
These are logical planner bounds, not total command processing, RSS, allocation,
or elapsed-time guarantees. Point reads and the one-row probe materialize their
values before the planner can account for their size. Existing handler validation
and input-preprocessing policies remain separate. Non-finite queues retain their
existing large trusted-vector behavior.

Deletion verifies the owner's identity and capacity mode, then purges Usage and
Charge as opaque bounded runtime data. A previously obtained valid binding can
therefore delete corrupt ledger values without treating deletion as repair or
bypassing the incarnation fence. Empty finite queues do not need lazily allocated
message counters to be deletable. Recreated queues do not adopt old-generation
sidecars.

## Durable And Image Boundaries

Active durable layout 18 protects mandatory queue/topic modes and finite queue
sidecars. Layout 17 and older directories are refused, and an ordinary unversioned directory with nonempty
metadata or message records is refused before a new marker is written. There is
no automatic relabeling, migration, or rollback conversion.

Current Create/Send images use `CreateSendLayout17V1` (role 2) and include exactly
one canonical non-finite mode for each generation-1 primary queue. Current
export, bootstrap, retained catalog validation, replacement, protected-image
checking, and native snapshot metadata use that proof. Historical
`CreateSendV1` (role 1) validation and its pure planning/checking APIs remain
explicitly separate; current restore paths do not strip modes or relabel images.
Source-profile refusals precede target storage access. Finite modes and their
sidecars are outside the current image profile, and finite owners are refused by
legacy committed Create/Send work without checkpoint advancement. Supporting
finite replication needs a new payload contract, not a changed interpretation of
the existing entry or fingerprint.

Tests exercise both memory and Fjall stores, physical Fjall reopen, ordered
atomic credit reuse, ledger corruption, opaque fenced deletion, actual AMQP
socket rejection/recovery, native finite administration, and the separate
library/CLI HTTPS queue path. These
socket checks use in-tree Rust clients. Separate pinned .NET gates cover
administration, capacity updates and the ordinary ingress profile below. Injected
pre-apply backend failures establish no
partial batch in those fixtures; they do not establish the outcome of an
indeterminate physical commit.

## Official .NET Ingress Gates

The independently executed Linux gates passed with Service Bus packages `7.21.0`
and `7.20.2`, each on Memory and Fjall with both named-key and connection-string
constructors. Each pin completes 48 child stages: twelve per constructor per
backend, including repeated quota and completion checks. The ordinary WSS
listener uses isolated private-CA trust and normal hostname verification, without
a certificate bypass or global trust changes. These gates do not enable
experimental transactions or claim all SDK workflows.

Trusted owner operations create the finite queues and set an exact observed
logical reservation limit after an SDK seed. This is not an Atom MiB quota,
physical disk usage or measured Azure accounting. Another SDK send then raises
`QuotaExceeded`, with one isolated proposer clock stamp, no store apply attempt
or committed batch, and an unchanged complete persisted image. Peek-lock receive
and abandon retain the exact original charge and aggregate. Completion must reach
zero native-observed reserved bytes and message count, with no retained message
or charge rows, before the same SDK message can be sent within the unchanged
limit. The retry is completed and checked at zero again.

A separate queue retains an SDK-produced 20-KiB message. A full-definition update
reduces its future configured message maximum to 4 KiB without rewriting that
message, its ledger or deadlines. A cold 20-KiB send is then refused by the broker
as `MessageSizeExceeded`, with one proposer stamp and no store apply attempt or
image change. A 300-KiB message exceeds the independently advertised 256-KiB link
maximum and is refused by the SDK before submission, with zero proposer stamps
and the same unchanged-image checks. Both cases use the same SDK exception
reason; their distinction combines isolated effects with the pinned client and
edge source, not a new wire trace or SDK usage metric. A 512-byte message remains
admissible, and the SDK drains both retained messages with exact body, identifier,
subject, content type and application-property checks.

Every child retains its original bounded process/pipe custody. The fixture waits
for each original connection Wrapper before requesting engine stop, then consumes
the unchanged original Wrapper/Actor/Reader report. A Wrapper deadline is a
failure followed by original stop/finish containment, not successful closure.
Successful primary processing and a clean peer AMQP Close with its actual reply
are required. A cancelled Reader additionally requires its original task identity
and the recorded ActorReaderShutdown request; the request is not proof of the
cancellation's cause.

Actual WSS close failures remain raw errors. The fixture only classifies exact
original IO kinds `UnexpectedEof`, `ConnectionReset` or `BrokenPipe` as a qualified
SDK disposal disposition when the clean peer reply and original reader-shutdown
facts also hold. Missing, opaque, nested, lookalike and other failures are refused.
The two pinned runs observed EOF and reset; the broken-pipe branch has a controlled
original duplex-transport test. None establishes a successful WSS/TLS close
handshake or the cause of the peer transport ending. The pinned caller source
initiates connection closure without awaiting a transport join, as shown in
[7.21.0 AmqpClient](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpClient.cs)
and [7.20.2 AmqpClient](https://github.com/Azure/azure-sdk-for-net/blob/f81988005453a91be7a974bba8c1b69012758e0f/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpClient.cs).
That source observation is not installed-binary equivalence or a runtime cause
certificate.

Facade leases must disappear before a serialized owner read checks the final
stage effects. The fixture separately drops the original broker owner and
requires the original counted store to become unique before dropping it and
reopening the backend. Memory reopens a shared-provider handle, not a process or
disk; Fjall physically reopens its directory. Both complete persisted images and
empty message/charge indexes must match. These boundaries do not prove universal
task termination, bound synchronous BrokerDrop, or turn fallback Drop into a
successful cleanup receipt.

Verification: both opt-in SDK gates closed successfully on the same frozen
fourteen-path source. The focused protocol/server/SDK run passed 880 regular
tests with seventeen ignored SDK cases. The closed full workspace passed 5,806
tests with no failures and seventeen ignored cases, preserving every case
identity, status and ignore reason from the preceding 5,790-test run across the
same 154 source owners and 160 result groups. The sixteen regular additions are
seven connection/close-diagnostic cases and nine SDK harness/effect support cases;
the two added ignored ingress gates were executed separately as described above.
Both strict workspace lint configurations, both all-target builds and formatting
passed serially on CPUs 14,15 with two build jobs and the shared cache. The fifteen
older opt-in SDK gates were not rerun. No dependency, CLI, domain encoding,
message-admission rule or durable layout change is included.
