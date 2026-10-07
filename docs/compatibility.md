# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Queue and topic send, both batch-send APIs, ordinary/session subscription workflows, queue/topic scheduling/cancellation, duplicate detection and message properties; queue session renew/state/scheduling; finite queue quota/credit recovery and message-size refusal over WSS; isolated offline JWT Send/Listen denial over TLS | Finite ordinary queue profile; other administration planned | Experimental gate on 7.21.0 |
| Official .NET SDK, previous stable | Same SAS-gated workflows as current, including finite ingress; isolated offline JWT Send/Listen denial over TLS | Finite ordinary queue profile; other administration planned | Experimental gate on 7.20.2 |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

A capability reaches **State machine** once the deterministic broker core
implements it with tests. That is a prerequisite for compatibility, not a form
of it: nothing below is reachable by a client until the protocol edge exists.

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Protocol edge, Rust client end to end |
| AMQP over WebSockets | Pre-1.0 | Opt-in WS/WSS listener, bounded binary transport, both Rust backends and both pinned .NET clients; see [WebSocket Transport](websocket-transport.md) |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | PLAIN and CBS SAS: protocol edge, Rust client end to end. Offline JWT: opt-in TLS CBS library path; isolated current/previous .NET TokenCredential gates for Linux/Memory/raw TLS; optional TLS/SAS CLI policy-file loading |
| Queue send, receive, and settlement | Pre-1.0 | State machine |
| Atomic message batch send | Pre-1.0 | State machine, AMQP producer mapping, Rust clients on both backends and both pinned .NET batch APIs |
| Message properties and AMQP body preservation | Pre-1.0 | State machine and AMQP mapping; typed properties, application values, annotations, footer and all body kinds. Rust clients on both backends and official .NET property gate |
| Peek without lock acquisition | Pre-1.0 | State machine and AMQP management, including entity-wide session browsing; Rust clients on both backends and both pinned .NET clients |
| Receive-delete | Pre-1.0 | State machine, AMQP mapping |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Message lock renewal | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Time-to-live expiry | Pre-1.0 | State machine and timer; default drop and optional dead-lettering, official .NET deferred-expiry gate |
| Topics and subscriptions | Pre-1.0 | Atomic rule-selected fanout, parent-retained scheduling/cancellation, ordinary/session subscription and dead-letter routing, native create/get/list/update/delete, Rust clients on both backends and both pinned .NET clients; Azure administration not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Persisted Boolean, scalar correlation, and bounded SQL rules through AMQP and native rule CRUD/CLI; bounded REMOVE and String/Boolean/Int64-literal SET actions with independent copies and finite local conversion-error dead letters, not full Azure/CLR actions |
| Scheduling and cancellation | Pre-1.0 | State machine, AMQP management and send-annotation mappings, Rust and current .NET clients end to end |
| Deferral and deferred receive | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Dead-letter | Pre-1.0 | State machine, AMQP mapping |
| Dead-letter receive and resubmit | Pre-1.0 | Receive: state machine, AMQP mapping. Resubmit: not implemented |
| Sessions and session state | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Duplicate detection | Pre-1.0 | State machine, AMQP send/scheduling mappings, Rust and current .NET clients end to end |
| Entity configuration updates | Pre-1.0 | Atomic state-machine patches; native queue, topic, and subscription API; native generation-fenced and HTTPS full-definition replacement for finite ordinary queues |
| Finite queue capacity | Pre-1.0 | Trusted owner API, separate native create/get/full-definition service, and HTTPS Atom fields for ordinary non-session, non-deduplicating queues; primary and DLQ logical reservations, paired storage, Rust AMQP socket tests, both pinned .NET administration/limit-update and WSS ingress/credit-recovery gates, explicit CLI activation; see [Finite Queue Capacity](finite-queue-capacity.md) |
| Same-placement-group transactions | Pre-1.0 | Trusted same-queue foundation and explicit posting/messaging listeners; [same-queue .NET scopes](dotnet-transaction-scopes.md) gate warmed/cold-first immediate send and held PeekLock Complete over experimental TLS on both backends and both pinned clients. General placement-group work is not implemented; default Service Bus listeners still refuse transaction traffic |
| Atom/XML entity and rule administration | Pre-1.0 | Authenticated TLS HTTP/1 finite ordinary queue create/get/full-update/delete/list through library opt-in or dedicated CLI options, gated with both pinned .NET clients on both backends; rules, topics and subscriptions are not implemented |
| Native gRPC administration | Pre-1.0 | Queue/topic/subscription create/get/list/update/delete, separate finite queue create/get/full-definition replacement, and typed rule CRUD with bounded REMOVE/literal SET actions over HTTP/2 and authenticated TLS; offline JWT Manage via library opt-in or the CLI policy-file option; optional development [maintenance clock query](development-maintenance-clock.md), not production readiness; other services not implemented |
| Quorum replication | Pre-1.0 | An isolated [fixed-three-node in-process runtime](experimental-replica-runtime.md) exists for bounded Create/Send, but is not integrated with server listeners or the production proposer; production startup remains refused. Separate committed-queue apply, vote/log storage, and state-machine adapters retain local progress and membership in isolated replica directories. Owned storage-pair preflight validates fingerprints, membership, votes, and cleanup. The runtime exposes no snapshots or production deployment activation. Development Fjall persistence remains local only |
| Partitioned entities | Later | Out of initial scope |
| Cross-placement-group transactions | Later | Out of initial scope |
| Geo-replication | Later | Out of initial scope |
| Premium-tier features | Uncommitted | Out of scope |

## Broker Core

The `domain` crate applies each replicated command as one atomic storage batch
and derives every deadline from the timestamp carried on that command, so a
follower replaying the log reaches the same state as the leader. Delivery
behavior it currently enforces:

- Peek-lock delivery is at-least-once: the lock commits before the message is
  handed out, and completion removes it only after settlement commits.
- Receive-delete is at-most-once: the deletion commits before the transfer.
- Peeking browses stored messages by sequence number without changing delivery
  counts, acquiring locks, or consuming from the queue.
- Deferral removes a locked message from the ready path while keeping its
  sequence number. A deferred receive by sequence can lock it again or consume
  it in receive-delete mode. A deferred message remains discoverable by peek
  after its lifetime ends; expiry is applied when a deferred receive reaches it.
- Scheduling keeps messages visible to peek but out of the ready path until
  their requested time. The timer atomically activates each due message under
  a new sequence number, appending it to the queue, and starts its lifetime at
  that activation. The requested scheduling timestamp remains attached.
  Scheduling a batch is atomic, including validation failures. Cancellation
  removes the scheduled record and deadline entry atomically; it rejects a
  missing or already-active handle. Exact Azure behavior for those rejected
  cancellation cases has not yet been verified.
- The core has a separate atomic ingress-batch command. Every member retains
  its own properties, identifier, session, lifetime, and optional scheduling
  timestamp. The complete batch is validated before staging content, shares
  duplicate history across its members, and commits counters and records once.
  A later validation, allocation, or storage failure leaves no partial batch.
  It accepts at most 1,024 messages, 65,536 retained value items, and 4 MiB of
  retained content, including the compatibility body and normalized identifiers.
  Session queues require every member to name the same session. An empty batch
  validates its target but writes nothing. These are local resource bounds,
  not Azure batch quotas.
- A separate trusted atomic-messaging envelope groups immediate sends and held
  settlements on one non-session primary queue, with shared limits, a bounded
  read-your-writes view, one commit, and committed-only wakeups. This does not
  expose AMQP transactions or retry idempotency; see
  [Atomic Queue Operations](atomic-queue-operations.md). Its separate
  [guarded commit API](atomic-commit-permits.md) adds owner-claimed, pending-only
  cancellation and an explicit indeterminate result for storage uncertainty.
  Optional [owned work reservations](atomic-work-reservations.md) preserve shared
  resource accounting while commands are queued, cancelled, or being applied.
  The [local transaction registry](transaction-registry.md) bounds declarations
  and controller lifetimes but does not enable wire transaction traffic.
  A trusted [paired owner handoff](native-atomic-owner-handoff.md) preserves both
  native receipts and staged logical work through one broker job, without
  establishing their correspondence or authorization. An explicit
  [posting-only listener](atomic-posting-ingress.md) derives actual native message
  work, performs bounded admission and staging, and joins the two lifecycles.
  A separate [atomic messaging listener](atomic-messaging-ingress.md) derives
  Complete from a canonical held delivery and an exact native retirement receipt.
  Its process address is an explicit development-only
  `--experimental-atomic-messaging-listen` option with the existing TLS/SAS policy.
  A separate [pinned .NET scope gate](dotnet-transaction-scopes.md) establishes
  warmed and cold-first same-queue immediate send, plus held Complete over TLS
  on both backends. Cold-first support is Send only; transactional acquisition,
  experimental management, cross-queue work, and recovery remain unsupported.
  Default listeners retain their transaction refusal.
- A queue can enable duplicate detection by message ID, with a 10-minute
  default history window bounded to 20 seconds through 7 days. A duplicate
  send is accepted and dropped, and history survives completion, dead-lettering,
  and schedule cancellation. Ordinary and scheduled sends share history;
  activation does not check it again. History is scoped to the namespace and
  entity, independent of the session. Anonymous messages bypass detection,
  and dropped retries do not extend the original deadline; exact Azure parity
  for these two edge cases remains unverified. A duplicate scheduling request
  receives a fresh sequence handle but stores no message, so that handle has
  nothing to cancel.
- Message identifiers are bounded to 128 UTF-16 code units, matching the
  official .NET client. Overlong identifiers are rejected before enqueue or
  duplicate-history changes, including within an atomic scheduled batch.
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- Abandon, defer and explicit dead-letter operations merge application-property
  updates into the held message atomically. Validation checks the complete
  resulting content before changing the lock or state. Legacy byte-body records
  gain typed content without losing their body or identifier. Reason and
  description on explicit dead-letter operations are bounded to 4,096 UTF-16
  units, matching the official client's argument limit.
- A live message lock can be renewed without changing its token. Renewal moves
  the replicated deadline record and its expiry index in one storage batch.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- A queue's default time to live is also a ceiling for an explicit message
  lifetime, including scheduled messages. Lifetime starts at enqueue/activation.
  A live message lock protects an expired message: completion and renewal
  remain valid. Abandonment or lock expiry applies the elapsed lifetime
  immediately, ahead of the delivery-count limit. Expired messages are dropped
  by default. `dead_lettering_on_message_expiration` instead moves them to the
  dead-letter queue as `TTLExpiredException`, whether reached by a timer,
  receive, abandonment, or lock expiry. The flag does not affect explicit
  dead-lettering or the delivery-count limit. Logical legacy configurations
  retain their former always-dead-letter behavior when decoded.
- The dead-letter queue is a queue: `entity/$deadletterqueue` is drained with
  the same receive and settlement machinery as its parent. Messages arrive
  there stripped of lifetime and session, keep their sequence numbers and the
  reason they were dead-lettered, and never dead-letter again — abandoning in
  a dead-letter queue always returns the message to it. The path is reserved:
  it cannot be created or sent to directly.
- Standalone rejected commands write nothing. The separate
  [committed queue apply API](committed-queue-apply.md) records normal refusals
  as checkpoint-only progress, leaving business records and their clock unchanged.
- On a queue that requires sessions, a message carries a session identifier and
  is only delivered to a receiver holding that session's lock. Ordering is
  guaranteed within a session, which is the only FIFO guarantee made. A session
  lock is exclusive and expires on its own deadline; session state outlives the
  receiver that set it. A receiver holding the session can renew that lock and
  read, replace, or clear the opaque state through the management node.

Ordinary queue ingress retains an optional `SessionId` as metadata for standalone
send, atomic ingress batches and scheduling. The global ready index remains
authoritative: metadata grants no session affinity or ownership and no per-session
FIFO guarantee. Session-filtered browsing, receive with a session hold and session
acquisition remain unsupported there. Microsoft describes the value as
[ignored on session-unaware entities](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messages-payloads);
that does not establish an observed Azure metadata round-trip. This is a local
retention policy, shared with ordinary subscription copies.

Session-required ingress still requires an ID, and its ingress batches still
require one common ID. Trusted atomic messaging still refuses normalized
SessionId-bearing ingress; the frozen `CreateSendV1` committed profile keeps its
strict admission and image agreement. Existing validation limits, DLQ stripping,
record versions and store layout are unchanged by this ingress change.

Ordinary AMQP PeekLock settlement on session-required queues and subscriptions
uses the original session hold captured with that delivery, including deferred
management deliveries. Complete, Abandon, Defer and DeadLetter, with their
property updates, require both that original live hold and the message's lock.
The domain owner checks the original session token and deadline at the command's
single authoritative timestamp, before changing properties or message state.
An expired, released or replaced hold cannot settle the old delivery; a reused
receiver link name does not confer its replacement's authority. A missing hold
is accepted only for ordinary queues, ordinary subscriptions and dead-letter
queues, including messages that retain a SessionId as ordinary metadata.

This uses the appended domain command `SettleHeld` (tag 38), not a new AMQP
field. Trusted legacy `Settle` (tag 24), Complete, Abandon, Defer and DeadLetter
commands remain message-lock-only APIs. Atomic messaging keeps its legacy mapper
and refuses `SettleHeld`; the closed `CreateSendV1` committed profile and queue
log schemas and value format 11 are unchanged by settlement;
an older command decoder is expected to refuse tag 38 as an unknown payload
variant, not as a new value-format header. This is not a mixed-version command
log guarantee.

Management message-lock renewal on session-required queues and subscriptions
presents the delivery's original session hold, including deferred receipts.
An expired, released or replaced hold cannot extend the old message lock.
Both the original session and message deadlines must remain live at the owner's
timestamp; renewing a message does not renew its session. Trusted legacy
`RenewLock` (tag 10) does not present a session hold, but an originally owned
tracking row now requires its stored generation to match a live session lock.
Trusted-unowned and ordinary message locks retain their lock-only renewal path.
Management uses appended
`RenewLockHeld` (tag 39); the closed atomic/`CreateSendV1`/`QueueV1` profiles and
value format 11 are unchanged by renewal. This does not establish
mixed-version command-log compatibility. Both pinned receivers,
[7.21.0](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/Receiver/ServiceBusReceiver.cs)
and [7.20.2](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/Azure.Messaging.ServiceBus_7.20.2/sdk/servicebus/Azure.Messaging.ServiceBus/src/Receiver/ServiceBusReceiver.cs),
reject `RenewMessageLockAsync` on session receivers before transport. Raw AMQP
is the applicable local surface, not a new pinned SDK workflow or observed
Azure error parity.

Next-available AMQP receivers follow bounded session-acceptance pages instead
of treating the first held page as exhaustion. Each owner command inspects at
most 32 distinct ready-index session groups, skipping each held group's entire
backlog. A full held page returns an exclusive, namespace/entity-scoped keyset
cursor. The receiver follows it until a grant, actual exhaustion or a private
10-second budget for admitting another page. Every new page rechecks configured
Listen authorization and the original incoming attach, retaining the original
entity incarnation binding. A cursor is a position, not authority or a frozen snapshot;
insertions behind it may wait for a later walk. This is not a starvation bound.

The page-admission budget does not cancel an already admitted owner command or
bound its completion time. A valid admitted grant may be handed off after that
cutoff. If authorization or the original native attach has become invalid, the
planner awaits release of that exact granted hold through the same fenced
binding before returning its refusal. Final native acceptance failure likewise
awaits release. Cleanup failures retain the primary failure and actual release
result. Pure origin validation does not certify actor-owned pending membership;
final native acceptance remains authoritative. Arbitrary planner cancellation
and backend panic are not covered by this cleanup guarantee.

This uses appended `AcceptNextSessionPage` (tag 40); legacy `AcceptSession`
(tag 12), including named acceptance and its bounded next-session operation,
keeps its wire encoding. The closed atomic/`CreateSendV1`/`QueueV1` profiles and
value format 11 are unchanged by paging. This does not establish
mixed-version command-log compatibility.

Session-required queues and subscriptions now track every PeekLock message
lock independently of the session lock. Held receives retain the exact original
session token; identifier-only trusted deferred commands retain a separate
unowned class, even when that session currently has a holder. Renewing or settling
an unowned row does not convert it into original-generation evidence. Ordinary
session metadata, dead-letter queues and ReceiveAndDelete do not create these rows.

Named, legacy next-session and paged acceptance cannot grant a replacement while
any tracked message lock remains. The new pending-takeover refusal is retryable
`amqp:resource-locked`; management maps it through the existing 503 refusal.
A still-live session retains its earlier lock refusal priority. Next-available
acceptance skips busy sessions and can grant healthy siblings within the existing
32-ready-group page budget. A deadline alone does not prove that a lock exited.

Actual settlement, abandonment, deferral, dead-letter/drop cleanup and message-lock
expiry remove the corresponding ownership rows and decrement their session count
in the same command batch. Renewal changes the deadlines without changing the
original generation, message token or counts. Staged reads see earlier changes in
that same batch, including deletions. Local row/index/summary disagreement refuses
the entire command without applying staged changes or advancing its clock.
An absent summary permits only a one-entry forward-index orphan check at grant;
this is not an arbitrary orphan or forged-count audit.

Session-lock expiry requires every selected elapsed index row to be canonical,
with an empty value and an actual positive session-lock token whose deadline
equals the indexed deadline. A stale row cannot clear a renewed session lock;
a malformed selected row refuses the entire batch without advancing its clock.
The existing 256-row scan and first-future-deadline stop are unchanged. The
deadline index contains no token, so this check does not certify an original
generation against a coherently substituted positive token.

Private ownership indexes require store layout 16. Message and session records,
value format 11, command tags 0-40 and the closed committed/atomic profiles retain
their existing shapes. `RetireSessionGenerationPage` is appended at tag 41; an
older command decoder refuses it. This is not mixed-version command-log support.

Original-generation retirement inspects at most 32 tracked session groups and
retires at most 32 owned message locks per command. A positive owned summary
requires the actual stored session record: a released lock or the same expired
generation is eligible; the same live generation is skipped. Missing or
incoherent session records refuse local corruption. Each selected row must agree
with its canonical forward key, reverse row, staged counts, actual locked
message and present empty general lock index. A last-owned-row tail probe stays
within the selected generation. Cursors are exclusive progress positions, not
ownership, snapshots or certificates about omitted prefixes or reverse orphans.

Retirement leaves session state and its lock/index unchanged. It uses existing
TTL, maximum-delivery and dead-letter paths, or returns the message to Ready
without incrementing delivery count. Trusted-unowned rows are never selected or
converted; their continued allocation or renewal can keep takeover pending.
Replacement acceptance still waits for every tracked message lock to exit.

The timer proposes at most eight retirement pages per visited queue or
subscription after session expiry and before duplicate-history cleanup. Zero
progress with Continue still advances a private cursor; only actual successful
results update it. Its insertion-order cache holds at most 1,024 entity scopes;
End removes a cursor, errors retain the last completed position, and restart or
eviction loses progress rather than authority. No fair or bounded-time takeover
completion, immediate requeue or full session-lock umbrella is guaranteed.

Handler admission caps 1,024 backing read calls, 256 KiB of read-key bytes and
32 MiB of returned read-value bytes; mutations cap 512 entries, 256 KiB of key
bytes and 32 MiB of value bytes, counting repeated overwrites. Requested keys
are charged before calls and returned bytes before decoding; the common owner
clock check precedes this handler budget. Every selected exit checks the exact
possible clock Put, reserved once on preparation success. A late preparation
error or cap refusal discards the whole staged command and clock, not a fitting
prefix. A storage apply error may have an unknown commit decision. These logical
caps are not pre-copy, allocator, RSS or elapsed-time bounds, global repair, or
observed Azure parity.

Microsoft documents a [session-lock umbrella](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sessions#session-features)
and [settlement failure after session expiry](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceiver.completemessageasync?view=azure-dotnet).
This boundary does not implement every umbrella behavior or establish full
Azure error parity. Remaining local session limitations are:

- Releasing or expiring a session does not immediately requeue its locked
  messages. Owned locks may exit through bounded retirement; trusted-unowned
  locks still require actual settlement or message-lock expiry cleanup.
  Replacement acceptance waits for all tracked rows to exit, without a fixed
  completion deadline.
- Expiration is applied to individual messages, not to every message in a session
  when one expires. Azure documents
  [session-wide TTL expiry](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sessions#message-expiration);
  this increment does not add that policy.

Local verification adds 21 regular domain, protocol and raw AMQP checks for
original-hold settlement. Default and all-feature workspace runs each pass
5,244 tests, with the same 11 opt-in SDK checks ignored; those 11 checks pass
separately against the pinned .NET clients. Prior case statuses and ignore
reasons are preserved. Strict lint and builds pass in both configurations,
along with formatting and protobuf validation. The existing SDK gates are
regression evidence, not a new stale-hold SDK workflow or observed Azure error
parity. Initial formatter, test-wiring and controlled-fixture failures were
corrected without changing existing test assertions, waits or ignore status.

Local verification adds 20 regular checks for original-hold message-lock
renewal: 12 domain checks, four protocol checks and four raw AMQP checks.
Default and all-feature workspace runs each pass 5,264 tests, preserving all
prior case statuses and the same 11 opt-in SDK ignore reasons. Those 11 existing
SDK checks pass separately as regression evidence, not a new session-receiver
message-lock renewal workflow. Strict lint and builds pass in both
configurations, along with formatting and protobuf validation. The initial
format check was corrected only by formatting the three new test modules;
existing assertions, waits and ignore status are unchanged.

Local verification adds 27 regular checks for paged next-session acceptance:
14 domain, three native transport, four protocol and six raw AMQP checks. The
default and all-feature workspace runs each pass 5,291 tests, preserving all
prior case statuses and the same 11 opt-in SDK ignore reasons. Those checks pass
separately as regression evidence, not a new SDK paging workflow. Strict lint
and builds pass in both configurations, along with formatting and protobuf
validation. Both complete workspace runs use serial test execution with no skips.
Initial compilation and new wire-fixture failures were corrected without
changing existing assertions, waits or ignore status. An unchanged oversized
storage fixture stalled in the first concurrent workspace run; the same test
binary passed alone, and the complete workspace passed on a serial retry with
no skips. The cause of that stall remains unestablished.

Local verification adds 31 regular checks for session message-lock tracking:
23 domain, three storage-layout, one protocol-condition and four raw AMQP
checks. Default and all-feature workspace runs each pass 5,322 tests, preserving
all prior case statuses and the same 11 opt-in SDK ignore reasons. Those 11 SDK
checks pass separately as regression evidence, not a new SDK stale-lock or
takeover workflow or observed Azure error parity. Strict lint and builds pass
in both configurations, along with formatting and protobuf validation. Both
complete workspace runs use serial test execution with no skips. Existing
authority and paging fixtures now require actual lock exit for same-ID takeover,
or use a different session for replacement-link checks. Initial target-wiring
and lint failures were corrected without test suppression or ignore changes.

Local verification adds 12 regular Memory/Fjall checks for session-lock expiry
integrity. The complete domain run passes 1,528 tests, preserving the prior
1,516 case names and statuses; nine selected server session/timer targets pass
85 tests. Strict workspace lint and builds pass in both configurations, along
with formatting and unchanged protobuf validation. The 11 existing opt-in SDK
checks pass separately as regression evidence. An initial previous-client Fjall
atomic check failed with an Accepted/Declared type mismatch; it passed unchanged
in an isolated diagnostic run and the complete SDK retry. The cause remains
unestablished. Assertions, waits and ignore status were not changed. These are
scoped regressions, not a new full-workspace run, a corrupt-index SDK workflow,
or observed Azure parity.

Local verification adds 49 regular checks for bounded original-generation
retirement: 28 paired public domain checks, six private domain checks and 15
timer, protocol and raw AMQP checks. Default and all-feature workspace runs
each pass 5,383 tests, preserving all prior case statuses and the same 11 opt-in
SDK ignore reasons. Those 11 checks pass separately as regression evidence,
not a new SDK session-retirement workflow. Strict lint and builds pass in both
configurations, along with formatting and unchanged protobuf validation. Both
complete workspace runs use serial test execution with no skips. The declared
trusted-renewal assertion now expects original-session expiry and checks
unchanged state. An initial new-test compile failure called a private validation
method; it was corrected to the existing public validator. Existing waits and
ignore status are unchanged. The earlier SDK Accepted/Declared failure remains
unattributed.

An ordinary receiver can browse all sessions in a session-required queue or
subscription through its management link, without attaching a data receiver or
acquiring a session. This matches the documented
[read-only browse](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-troubleshooting-guide#how-to-browse-session-messages-across-all-sessions).
A supplied session identifier filters the same sequence-ordered scan; it is
still invalid on an entity that does not require sessions. Browsing does not
relax session ownership for ordinary or deferred receive.

Queue settings can be patched atomically with the parent and its dead-letter
shadow in the same batch. An omitted setting is unchanged; an explicit unlimited
TTL clears a finite default. Session and duplicate-detection enablement are
creation-only properties. Updates retain all existing messages, deadlines,
locks, counters, and duplicate-history entries; the new size/TTL/lock/history
limits govern new ingress or newly allocated deadlines. Expiration disposition
and maximum delivery count use the current queue policy. A no-op patch does not
write storage or advance the applied clock. The adjustable per-message size cap
is a Switchyard policy, not a verified Azure queue-update property.

The state machine can create and read distinct topic definitions and bounded,
sorted subscription membership. Queue and topic names cannot occupy the same
namespace path. Subscription creation names its parent topic and atomically
stores the member, its receive-only backing queue, its dead-letter shadow, and
an explicit `$Default` true rule;
invalid settings, path collisions, composed path limits, or storage failure
leave all of them and the applied clock unchanged. Topics have no queue or
dead-letter shadow of their own. Membership is limited to 32 subscriptions per
topic, a local resource policy rather than an Azure quota.
Subscription names use a conservative ASCII subset of the
[Service Bus naming rules](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules):
1 through 50 letters, digits, periods, hyphens, or underscores, starting and
ending with a letter or digit. Their canonical path is `topic/subscriptions/name`.
The control segment is reserved case-insensitively, while topic and member
spelling stays case-sensitive.
Membership reads are bounded and refuse malformed or dangling topology instead
of silently omitting it. The queue timer discovers the backing queues and their
shadows through its existing queue index. Subscriptions cannot be sent to or
updated through queue commands.

Immediate topic send, envelope send, and batch send copy each accepted message
to each currently matching subscription in one storage commit. New subscriptions
initially match all publications through their persisted `$Default` rule.
Duplicate detection runs once at topic ingress;
duplicate publications consume a sequence but create no copies. A topic with
no subscriptions acknowledges publications without retaining messages, and a
later subscription sees only later publications. Each copy preserves its body
and system fields; [REMOVE and literal SET actions](sql-actions.md) transform only
their private application properties. Copies take the shortest requested/topic/subscription TTL and have
independent receive, settlement, lock expiry, deferral, and dead-letter state.
All input and destination validation precedes commit, including duplicate inputs;
one invalid destination or exhausted topic sequence rejects the whole command.

Action-free copies share their publication's topic-assigned sequence; retained
action copies receive additional parent-topic sequences. The shared-sequence policy is
inferred from Microsoft's
[topic-scoped sequencing description](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing),
not a cloud-verified cross-subscription guarantee. A late subscription does not
restart numbering; its sequence gaps reflect earlier publications and duplicate
discard. Subscription lock tokens remain independently allocated. Fanout is
bounded before retained content is cloned: at most 1,024 copies, 4 MiB of
aggregate retained typed content, compatibility bodies and normalized IDs, and
65,536 typed value items. These are local resource policies, not Azure quotas.
Only committed destinations are notified, without a post-commit topology read.

Subscription rules are persisted independently under their exact member and
rule names. True and false filters are supported, including the SDK's exact SQL
aliases `1=1` and `1=0`. Correlation conditions AND together; action-free rules
OR together and emit at most one copy per matching subscription. Removing the
final rule selects nothing, with no implicit default fallback. These Boolean,
default, and combination semantics follow Microsoft's
[topic filter documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/topic-filters).
Bounded SQL predicates are supported alongside those filters. AMQP, native, CLI,
and domain [REMOVE and literal SET actions](sql-actions.md) add independent copies.
Version 1 remains REMOVE-only; new, omitted-version and unversioned AMQP actions
use version 2. SET assigns only String, Boolean or signed-Int64 literals under
exact-key, checked target-family/integer-width rules. Unsupported right-hand
expressions, system mutation, wider Azure/CLR conversions and compound correlation
predicates remain refused rather than treated as successful matches. A finite
local SET conversion failure discards that action's intermediate changes and
retains one original-envelope dead letter plus final RuleName, without replacing
base or healthy sibling outcomes. Its fixed `SwitchyardSqlActionError` description
is TypeMismatch, UnsupportedTargetType or NumericOverflow and contains no source
or producer value. Filter-error precedence and its subscription flag are unchanged;
the independent action-error route precedes missing-session routing.

Correlation rules select the eight retained system string properties and scalar
application properties. `label` maps to subject; message and session identifiers
use the existing authoritative ingress values. Non-string AMQP correlation IDs
do not match a system-string condition. Strings are case-sensitive. Application
property keys and values use exact local typed equality: no case folding or
numeric coercion, float equality follows retained bit patterns, and a missing
property never equals a present Null. An empty correlation filter matches all
messages by vacuous AND. Those exact scalar, key, normalization, and empty-filter
choices are local policies, not cloud-verified guarantees. Original typed message
IDs remain unchanged in retained envelopes. An absent message ID and an explicit
empty message ID follow the existing anonymous ingress ID rather than a new
wire-presence comparison. All destination content-size limits are still checked,
including destinations excluded by a filter; filtering only changes retention.

Rule names retain their spelling and permit `$Default`; the SDK-compatible
length bound counts 50 UTF-16 units rather than UTF-8 bytes. Whitespace-only
names and the SDK's forbidden path characters are rejected. Control characters
are additionally forbidden by the local storage policy. Rules are limited to
32 per subscription, with 32 total conditions per correlation rule, 64 KiB per
versioned stored rule, and 256 KiB across one subscription's complete rule set.
Reads scan at most 33 rule entries, validate the complete topology and rule
metadata, and reject corruption instead of omitting entries. With 32 members,
rule metadata can total 8 MiB independently of the message fanout budget.

Matching has its own deterministic limits: 1,048,576 work units and 32 MiB of
potential comparison bytes per command. All inputs precharge every rule and
condition, including custom-property lookup and potential key/value comparisons.
Possible action planning also charges section-sensitive value-node/body-container
and metadata-entry visits, repeated original-target/final-RuleName measurements,
map/overlay candidates and bounded statement/literal/target scans before action
checks and retained clones. Duplicates, nonmatches and short-circuit success do
not bypass these allowances, which are not all-instruction, allocator, RSS, CPU
or wall-time limits. Immediate and scheduled admission reject an over-budget
command atomically. Activation permits a fitting prefix only for aggregate
ingress/fanout/rule-match limits; an aggregate-unfit head remains pending and
cancelable. A selected per-copy size/value or shape failure, or malformed metadata,
refuses the whole activation without effects, even after earlier candidates fit. Rule
matching precedes missing-session routing, so an excluded publication creates
no dead-letter copy on that subscription.

### SQL Rules

SQL rules retain their original expression and semantic version, not a parser
AST or compiled program. The complete topic rule load shares one compilation
allowance; all correlation and SQL evaluation shares one command allowance.
Only true selects a copy. A finite SQL error overrides a matching rule on the
same subscription, but does not prevent delivery to healthy siblings.

`dead_lettering_on_filter_evaluation_exceptions` defaults to true. Such errors
then create one session-free, lifetime-free shadow copy with the local reason
`SwitchyardSqlFilterError` and a fixed description containing no rule text or
producer values. Setting the flag false drops only that subscription's copy.
Resource exhaustion instead refuses the complete command atomically and never
creates these dead letters. Current rules are evaluated again at scheduled
activation. The supported grammar, local semantic choices, resource accounting,
and wire contract are detailed in [SQL Rules](sql-rules.md).

Topic publications may carry session identifiers, including mixed-session
batches. Session-required subscriptions use session-affine ready indexes and
the existing exclusive ownership, FIFO, state, renewal, release, and deferred
receive machinery. Each subscription owns its sessions independently, even when
sibling subscriptions receive the same identifier. Ordinary subscription copies
preserve that identifier but use their global ready index and ordinary receive,
settlement, and expiry rules, as do ordinary queue messages carrying metadata.
Duplicate detection remains topic-local and based on message ID, independent of
session ID, consistent with Microsoft's
[nonpartitioned duplicate-detection description](https://learn.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection).

A matching publication without a session identifier reaches ordinary subscriptions
normally, while each matching session-required subscription receives its copy directly
in its dead-letter shadow with reason `Session ID is null`. Those copies keep the
topic sequence but have no lifetime or session identifier. This per-copy routing
is a local policy inferred from the documented
[missing-session dead-letter reason](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues),
not a cloud-verified atomic fanout guarantee. Session identifiers and retained
dead-letter reason/description bytes count toward the fanout content budget;
the two projected dead-letter values count toward its value-item budget. All
admission checks precede retained payload clones, including these additions.
Only actual backing or shadow destinations receive committed notifications.

Delivery notifications broadcast to every currently registered waiter on the
destination. The wait captures its signal before submitting receive, so a
commit between an empty result and the first wait poll is not lost. This lets
receivers holding different sessions on the same entity rescan independently
instead of depending on the periodic retry. Ordinary competing receivers may
also rescan; only the serialized receive command can acquire or consume a
message. Repeated notifications coalesce per one-shot wait, and a later
registration does not inherit an earlier broadcast. Cancellation and completion
remove registrations, with no retained payload or session history. Wake work
and memory remain proportional to live waiters, not a fixed process-wide bound.

Future topic publications, including management scheduling and individually
timestamped batch members, retain one scheduled record and deadline index on
the parent topic. They do not create subscription copies, dead letters, or
session ownership before activation. Parent management browsing returns these
pending records without opening a receiving data link, consistent with the
[documented location of scheduled topic messages](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-troubleshooting-guide#how-to-browse-scheduled-or-deferred-messages).
Topic browsing uses the same bounded physical scan and response budgets as queue
browsing, but returns only pending schedules; explicit session filters remain
unsupported on the parent. Subscriptions cannot schedule publications directly.

Activation removes the parent schedule and fans out atomically to current,
validated membership and current rules. Late-created subscriptions and rule
changes before activation participate in this local policy; that behavior has
not been compared with a live Azure namespace. Each
logical publication receives a new shared topic sequence after earlier active
work. Its actual activation timestamp becomes enqueue time and starts the
shortest requested/topic/current-subscription lifetime. These sequence and TTL
rules follow Microsoft's [scheduled sequencing](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing#scheduled-messages)
and [scheduled expiration](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-expiration#scheduled-messages)
descriptions. Session and missing-session dead-letter routing use the same
per-subscription policy as immediate fanout. A timestamp already due at admission
publishes immediately rather than retaining a pending parent record.

Duplicate detection runs once at admission, shared by immediate and scheduled
topic publications. Activation does not recheck or extend history, even if its
window has expired. Canceling a pending schedule removes its parent record and
deadline atomically but retains duplicate history. A multi-handle cancellation
rejects the whole command if any handle is absent or no longer scheduled; a
cancellation handle cannot remove activated subscription copies. Switchyard
serializes activation and cancellation, unlike Azure's documented race between
those operations.

Scheduled batches budget one retained parent copy per accepted future input.
Every future input must also fit an individual fanout against current matching membership
before retention. Activation inspects at most 256 scheduling entries and retains
at most 1,024 copies, 4 MiB content and 65,536 projected values. Only
`IngressBatchLimitExceeded`, `TopicFanoutTooLarge` and `TopicRuleMatchTooLarge`
permit an earlier fitting prefix to commit while the next aggregate-unfit source
remains pending. An aggregate-unfit first source is refused without mutation and
remains cancelable. Per-copy property/header/message/value limits, invalid shape
and malformed metadata instead refuse the whole selected command, including
earlier fitting sources. No prefix is applied for those errors. Source and index
deletions, counters, active/dead-letter copies and clock share one atomic batch.
These bounds and head-of-line policies are local, not Azure quotas or replication.

Plain producer addresses resolve committed metadata and permit queue or topic
send. Ordinary topic receivers and senders to subscriptions or dead-letter
queues are refused. Subscription receivers use
`topic/subscriptions/name`; their dead-letter receivers add `/$deadletterqueue`,
and either target can add `/$management` for request/reply operations. Reserved
control segments are case-insensitive, including the SDK's `/Subscriptions/`
and `/$DeadLetterQueue` forms; user topic and subscription names remain exact.
A literal topic path ending in `Subscriptions` and a member named
`Subscriptions` remain valid. Malformed or nested subscription paths are
refused rather than routed as ordinary queues.

Link planning first reads typed metadata through the serialized owner without
consulting the clock, writing storage, or acquiring a message or session lock.
Missing targets are refused before transfers. Malformed values, dangling
membership or dead-letter projections, and conflicting entity kinds retain
their errors instead of becoming absent targets. Authentication precedes this
read, so an unauthorized connection cannot probe entity existence through the
metadata lookup. A session-required subscription receiver without a session
filter is refused before any command or session hold. Named and next-available
filters then acquire a hold through the existing session command and echo the
granted identifier; their session-free dead-letter queues
remain receivable. Topic management links support scheduled browsing,
scheduling, and cancellation with operation-specific authorization; ordinary
topic data receivers remain refused.
Native administration can create, get, list, partially update, and delete topics
and subscriptions. Subscription
management links support `com.microsoft:add-rule`, `com.microsoft:remove-rule`,
and `com.microsoft:enumerate-rules`. All three use Listen authorization on the
complete endpoint scope, consistent with the SDK's
[ServiceBusRuleManager contract](https://github.com/Azure/azure-sdk-for-net/blob/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/RuleManager/ServiceBusRuleManager.cs),
not the Manage requirement of HTTP administration. Rule enumeration is a pure
serialized metadata read without a command stamp or clock access; mutations use
the normal atomic stamped-command path. Enumeration accepts `top` 1 through 100
and a nonnegative `skip` against the complete bounded, sorted rule set. A requested
page that exceeds the response allowance fails rather than returning a shortened
successful page, which could prematurely stop the SDK's enumeration loop.
SQL filter and [bounded action](sql-actions.md) enumeration returns exact stored
source and AMQP int compatibility level 20. Actions use the full-width SQL-action
descriptor, distinct from the SQL-filter descriptor; no-action rules retain the
empty-action descriptor. Optional action creation is accepted through the same
admitted child binding, with no request-field or associated-link redirection.
Malformed syntax returns status 400; unsupported constructs return 501; compile
and evaluation resource limits return 403. None are simulated successful rules.

Identifier allocation refuses exhaustion instead of saturating and reusing a
stored identity. Sequence numbers are limited to `i64::MAX`, preserving exact
AMQP `long` values for receive, peek, scheduling handles, and deferred receive.
That final value can be allocated once; a command requiring another allocation
is rejected atomically with `amqp:resource-limit-exceeded` (management status
403, non-retryable). Duplicate sends still consume their ordinary sequence
allocation. Lock tokens use `u64::MAX` as an exhausted sentinel, so `u64::MAX - 1`
is the final allocation. Operations that need no fresh identifier, including
receive-delete, cancellation, settlement, renewal, and cleanup, remain usable.
Counters keep their existing stored shape. Fjall preserves exhaustion across
restart; Memory preserves it only while its shared in-process state remains live.
This is a deliberate local bound, not Azure's documented rollover behavior;
the official .NET [sequence-number property](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceivedmessage.sequencenumber)
is signed, while Azure documents rollover in its
[sequencing contract](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing).

The `server` crate's timer worker proposes scheduled activation, lock,
time-to-live, session-lock, and duplicate-history sweeps on an interval, so a
running node activates what is due and releases or prunes what has elapsed.
Queue and topic discovery use independent exclusive keyset pages of at most
1,024 configurations each. Queue pages include subscription backing queues and
dead-letter shadows; topic pages use their distinct metadata index. Retained
cursors visit later pages on later sweeps and wrap at the end. An attempted
entity advances its cursor before its commands, and both entity families are
attempted even if one fails, so a failing queue cannot starve topic history
activation/history cleanup or vice versa. Topics activate schedules before
expiring duplicate history. Queue activation stops after a partial 256-entry
page; topic activation continues while a command makes positive progress because
fanout can fill its budget earlier. Each index gets at most eight bounded command
rounds per sweep. A visited topic can therefore retain up to eight 4 MiB fanout
budgets across its activation commands, not only 4 MiB for the entire sweep.
Discovery reads
do not stamp commands or advance the applied clock.

An AMQP 1.0 client can reach queues, topic producers, and subscription receivers.
The node accepts AMQP over TLS with the
socket secured before the protocol handshake, as Service Bus port 5671
requires. Plain TCP remains available only in development mode. A configured
shared-access policy accepts either SASL PLAIN credentials or SASL ANONYMOUS
or Microsoft's equivalent `MSSBCBS` mechanism followed by a CBS SAS token. CBS
grants are scoped to a namespace or entity and to Send, Listen, or Manage; they
authorize links connection-wide and close an open link when its token expires.
A connection without a valid grant gets 20 seconds to complete CBS
authorization. OIDC and mTLS are not implemented.

The protocol library separately opts into offline JWT CBS authentication with
`SharedAccessAuthentication::with_offline_jwt_policy(JwtPolicy)`. Its default is
disabled; existing SAS and PLAIN behavior is unchanged. The existing legal empty
`SharedAccessPolicy::new([])` can be combined with this policy; that accepts no
PLAIN credential and is not a separate JWT-only constructor. CBS accepts the
exact token type `jwt`: an unknown type returns 400, while disabled or invalid
JWT authorization returns 401. Signed roles and scopes never supply permissions;
the CBS requested entity scope is checked against the local binding, separately
from the JWT's configured resource audience.

Ordinary and experimental atomic serving, including WebSocket mode, refuse a
JWT-configured listener without TLS before accepting sockets. Retained variants
refuse that configuration before claiming their starter. The private transport
fact is supplied only by a successful actual TLS handshake, not by a URL, TLS
configuration or WebSocket flag. A retained setup refusal returns its original
listener/socket carrier before task launch or native engine acceptance; it does
not certify descendant cleanup or general fixture health.

JWT publication reads a checked system epoch before validation and again after
both actual grant/control locks, before any mutation or notification. A failed
pre-Unix epoch conversion refuses authorization rather than substituting zero;
expiry or other validation failure after either wait also leaves grants and
initial-control history unchanged. Cancelling a pending publication does not
publish a grant or notification. Refresh replaces only the same typed principal
and exact scope; SAS and JWT grants cannot erase each other through a shared
subject string. This does not certify a trusted clock. No CLI startup flag or
new official .NET JWT gate is included in this edge; no OIDC discovery, network
key refresh, revocation, Entra/cloud authorization or storage change is implied.

Verification: all sixteen new regular edge checks passed (eight private CBS
checks and eight Memory/Fjall wire checks); the closed default workspace run
passed 5,423 tests with no failures and the same eleven ignored SDK gates. Both
strict Clippy configurations, both workspace builds, formatting and protobuf
compilation passed. Fresh all-features no-run output supplied the same 138 test
executable paths used by that workspace run; it is not a second runtime pass.
Eleven existing official .NET SAS regression cases passed, not a JWT
`TokenCredential` activation gate.

Retained failures identified assumptions in the new fixture: unused queues may
have no persisted counters, and the original client WebSocket must stay owned
through its strict close exchange before the original server roles are joined.
The fixture now distinguishes absent counters, retains that socket through its
existing five-second borrowed cleanup, and uses `SinkExt::close` after a
diagnostic probe recorded `SendAfterClosing`. All eight final wire checks
passed without weakening peer-Close, native Reader identity or server-close
checks. These fixture corrections do not establish a cause or fix for an
earlier SDK failure, a whole-Broker cleanup guarantee or cloud parity.

Official .NET 7.21.0 and 7.20.2 `TokenCredential` gates are scoped to
Linux, MemoryStore and raw TLS. Their shared credential requires the exact SDK
requested scope `https://servicebus.azure.net/.default`; that string is separate
from the pinned JWT resource audience `urn:switchyard:tenant` and the CBS
requested entity scope. The fixture signs a fresh local RS256 `at+jwt` token
with a 3,300-second lifetime and returns an `AccessToken` whose expiry equals
the signed `exp`. The listener combines the existing legal empty SAS policy
with a JWT subject bound only to Send on one queue.

The isolated procedure requires one successful send followed by the specific
`UnauthorizedAccessException` for a Listen attempt, not merely an empty
receive or any failure. Its completion marker is emitted only after the C#
sender, receiver and client disposals. The Rust gate keeps the original client
future and covered holder reports through their joins before propagating the
original error or panic. Its canonical Memory assertions require the original
message to remain Ready with delivery count zero, `next_sequence == 2`,
`next_lock_token == 1` and no message/session lock-index rows. This is not a
whole-Broker cleanup or native-health certificate.

An initial current-JWT run returned null for Listen instead of the required
`UnauthorizedAccessException`. The experimental atomic listener accepted
ordinary Attach before detaching with an error; it now rejects the original
pending Attach with null termini and the authorization error. A wire
regression fails before this change, and the same scoped SDK case passes
afterward. This does not explain unrelated earlier SDK failures.

These gates do not cover WSS, Fjall, CLI-started JWT wire authorization,
token refresh, OIDC or cloud authentication. They are not a general
official-client compatibility claim.

Verification: the closed default workspace passed 5,426 tests with no failures
and twelve ignored SDK gates. The three new regular checks (two finite-label
diagnostic cases and the both-role refusal regression) passed. All twelve
official .NET gates were then executed and passed, including the one new
current-only JWT case and the unchanged eleven SAS case identities. The full
protocol package passed 582 tests. Both strict Clippy configurations, both
workspace builds and formatting passed. Fresh all-features no-run output
supplied the same 138 test executable paths used in that workspace run; it
is not a second runtime pass.

Five failed attempts are retained: one new Rust test compilation error, three
current-JWT runs and the before-fix wire regression. The compilation error
was resolved by an explicit boxed-error coercion in the new test. Fixed-label
diagnostics distinguished a fixture-owned missing-denial assertion without
printing token, key, argument or raw exception text. The final scoped gate
preserves its specific unauthorized exception and canonical state checks.
The value envelope remains version 11 and the durable base layout version 16.

Verification: the previous-SDK extension passed both scoped JWT cases on
7.21.0 and 7.20.2, including the exact unauthorized Listen error and canonical
Memory state assertions. The unchanged shared procedure is extracted from
the original current case, with only the SDK version parameter substituted.
The target also passed all 30 regular cases, with thirteen SDK cases ignored;
only the previous JWT case was added. Focused strict all-feature Clippy and
formatting passed. This test-only increment does not repeat the full workspace
or the other eleven SDK gates; preceding evidence remains historical.

The `switchyard` CLI opts into this policy with
`--offline-jwt-policy-file PATH`; omission leaves existing defaults unchanged.
The option requires the existing TLS certificate/private key and configured
shared-access authentication before opening the policy path. CLI credential
flags retain their existing rule construction; there is no new JWT-only startup
mode or nonempty-policy restriction on the library's legal empty SAS policy.
Normal startup and `--check-config` share this preparation before storage is
opened or listener sockets are bound. Check-config still reads and validates
configured TLS, SAS and policy files, then returns without opening storage.

The opened policy must be a regular file containing at most 64 KiB of UTF-8
configuration accepted by the strict `JwtPolicy::from_json` profile.
Both metadata length and actual captured byte count are checked; the reader
takes at most 65,537 bytes to detect an oversized input. New policy-file errors
are static and omit paths and contents; existing TLS/SAS error behavior remains
unchanged. These input caps are not allocator, RSS or wall-time guarantees.

On Unix, the loader opens with `RDONLY | NONBLOCK | CLOEXEC` and checks the
actual descriptor's metadata before reading; FIFOs and FIFO symlinks are refused.
A symlink to a regular policy file remains supported. The descriptor anchors
that opened file across path replacement, not an immutable snapshot against
in-place writes. Non-Unix uses a weaker path-metadata precheck before ordinary
open, then checks the opened file. Replacement between precheck and open is
not excluded, and no Unix-style nonblocking-open guarantee is made. The FIFO
unit checks an explicitly opened descriptor; separate bounded Linux binary
probes exercise the production opener.

The option supplies the already-loaded policy to configured AMQP listeners and
native `--admin-listen`. Native setup clones the pinned policy borrowed from the
existing authentication configuration, without reopening or reparsing its file.
It reuses the configured TLS identity and resource host, and preserves the
optional development-maintenance readiness setup. CLI preflight probes alone
are not a CLI-started JWT wire gate. OIDC, network key refresh, cloud parity and
existing production/quorum refusals are unchanged, as are value 11 and layout 16.

Verification: the closed CLI increment workspace passed 5,442 tests with no
failures and 12 ignored cases, across 154 printed groups and 148 canonical
owners. Every prior case name, status and ignore reason was preserved; only
13 policy-loading unit cases and three bounded Linux binary probes were added.
The scoped binary suite passed 37 cases and the SDK target passed 30 regular
cases with its 12 SDK gates ignored. All twelve existing SDK gates then passed
in a separate closed run, including the current Memory/raw-TLS JWT case.
Fresh all-feature no-run output contained 480 compiler artifacts, 34 build
scripts, one successful finish and no compiler diagnostics; all 138 complete
test executable paths matched the actual workspace run. This is compiler
provenance, not a second runtime. Both strict lint and build configurations
and final formatting passed, using the shared cache and two-CPU policy.
Full offline metadata enumeration initially refused a missing cached, already
locked fiat-crypto package; the no-deps enumeration completed. Cargo resolution
changed only the server direct rustix edge, with no version changes. That
retained metadata failure was not a runtime-test failure. Value 11, layout 16
and schemas are unchanged; these CLI probes do not certify CLI-started JWT
wire authorization or native JWT administration.

A separate pure `auth::JwtPolicy` library provides a narrow offline JWT
validator; it does not activate the JWT wire path described above.
`JwtPolicy::from_json` pins an exact HTTPS issuer, a distinct resource audience,
public RSA keys and one local scope/permission binding per unique subject.
Validation accepts only RS256 with exact protected-header `typ: at+jwt`, a
configured key identifier, RSA moduli of 2,048--4,096 bits and exponent 65,537.
Signatures use the maintained `jsonwebtoken` RustCrypto backend. Signed role or
scope claims never supply rights; the requested `ResourceScope` must be contained
in the subject's local binding.

Configuration is limited to 64 KiB, eight keys and 64 bindings; compact tokens
to 8 KiB, decoded headers to 2 KiB and claims to 6 KiB. Structured JSON checks
reject duplicate members and excessive depth/node counts, including ignored
claim values. The policy, each key/binding record, the protected header and claims
must be JSON objects; positional-array encodings of those records are rejected.
The protected header admits only `alg`, `kid` and `typ`, not remote
key URLs or algorithm selection. Required `iat`/`exp` and optional `nbf` are
unsigned integral NumericDates. Validation uses the caller's epoch, zero clock
skew and a checked positive lifetime of at most 3,600 seconds; it does not obtain
or certify a trusted clock. Typed issuer identity distinguishes SAS principals
from JWT principals and separates identical subjects issued by different issuers
through `AccessGrant::same_principal`, without encoding provenance into a string.

This is not OIDC discovery, network JWKS refresh, operational revocation, Entra
RBAC, workload identity or full RFC 9068 validation. It adds no TLS proof, CBS
token handling, listener/CLI activation, default authorization change, or storage
format change. No runtime, SDK, cloud, RSS or wall-time validation guarantee is
implied by these input and algorithm limits.

Local verification of this foundation passed all 21 new JWT cases (49 auth tests
total) and 5,407 workspace tests, with the same 11 ignored cases. An all-features
no-run build reused the same 137 fresh test executables as the corrected workspace
run; this was not a second runtime suite. Strict lint and build gates passed in
both feature modes, with formatting and protobuf checks. A pre-fix shape probe
had 18 passes and three failures; explicit object guards corrected those refusals.
The existing pinned .NET regression suite initially passed ten cases and failed
the previous client's batch-identity assertion before settlement. That unchanged
case passed alone, then all eleven cases passed on a full recheck. The first SDK
failure remains unexplained, not resolved; these SAS regressions do not validate
JWT on CBS or resolve earlier transaction-outcome incidents.

Only the experimental atomic messaging listener permits bounded coordinator
declaration and explicit rollback during that fixed
[initial window](initial-transaction-authorization.md). Queue access and every
commit, including an empty commit, still require current authorization. A
successful grant permanently ends the window; expiry never starts it again.
Raw TLS/CBS regressions on both backends verify grant loss, same-connection
reauthorization, and a fresh coordinator without reuse of the old transaction.
AMQP resource scopes normalize only recognized subscription, dead-letter, and
management control segments, never user names. Namespace and parent grants
inherit to their children; exact dead-letter or management grants do not grant
access to the parent or siblings. A management link retains its complete
`/$management` scope for per-request authorization rather than dropping the
endpoint suffix. SAS signatures still cover the original encoded audience;
scope normalization does not rewrite signed bytes. Scoped TLS/CBS socket tests
cover these boundaries separately from the namespace-wide SDK gates.
Native administration retains literal, case-sensitive scopes, including
primary entity names that happen to end in AMQP control words.
The optional WS/WSS listener carries the same engine and authorization through
the exact `/$servicebus/websocket/` endpoint. Its HTTP and buffer limits,
standalone protocol headers, close cleanup, and client gates are described in
[WebSocket Transport](websocket-transport.md).
The listener has local defaults of 128 live connections, including unfinished
security handshakes, and one 10-second deadline covering TLS, HTTP upgrade,
SASL, and AMQP Open.
Excess sockets are refused; handshake progress does not restart that deadline.
Listener builders can configure both limits, with a zero handshake timeout
requesting immediate refusal. The CBS authorization deadline starts after Open.
Graceful connection Close has a two-second default deadline. Timeout or
cancellation of its caller cancels the driver and its socket reader, including
when application dispatch or a socket write is blocked. Explicit shutdown waits
for both tasks to terminate before releasing the listener's admission slot.
These are Switchyard resource policies, not Azure quotas. Each connection enforces a
64 MiB logical retained encoded-content allowance described below; this is not
a comprehensive allocation or RSS limit.
Open advertises a 60-second receive-idle interval by default, with an actual
120-second complete-frame silence deadline, following the
[AMQP idle-timeout recommendation](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idle-timeout).
The peer's independent interval drives outgoing heartbeats at half its value;
ordinary complete writes also satisfy that direction. An omitted or zero peer
interval disables those heartbeats, while zero in the local configuration
disables only the local receive-silence check. Positive intervals below one
second are refused. Complete valid frames, including empty frames on unbound
valid channels, refresh receive activity; partial prefixes or bodies do not.
Each frame's complete write and flush has a separate five-second local limit,
shortened by the peer's remaining interval. Local options can configure both
limits; zero write duration is immediate refusal, not unlimited. Independent
watchdogs enforce silence and Close deadlines even when dispatch or transport
writes are blocked. A partial or interrupted write drops the transport without
appending Close or retrying the partially emitted frame. No heartbeat is sent
before Open or after Close.
AMQP Open frames are limited to the initial 512-byte transport maximum. After
Open, the server advertises and enforces its own 262,144-byte receive maximum,
independent of the peer's receive limit. Oversized frames are rejected from the
four-byte size prefix before allocating or reading their bodies; an open
connection returns the framing-error Close condition. The test client can
configure its own receive maximum between 512 bytes and the codec's 4 MiB
ceiling. SASL reads use the local receive maximum as a resource policy.
Receiving links additionally enforce a 4 MiB encoded-message ceiling across all
fragments and advertise the actual effective `max-message-size` in Attach.
Omission, zero, or a larger requested receive limit selects that local ceiling;
smaller positive limits remain exact. This applies to both the server acceptance
API and the test client's receiver builder, replacing their unlimited receive
default. Exceeding the limit detaches only the offending link with
`amqp:link:message-size-exceeded`, before appending the excess fragment or decoding
the message, even if every Transfer still has `more=true`. Aborting an admitted
partial delivery releases its content and aliases for reuse. The broker's
smaller producer-link limit is unchanged. An absent or zero peer-advertised
outgoing limit still means no peer limit.
Each connection also shares a 64 MiB retained encoded-content allowance across
all sessions and both directions. Incoming partials are charged before append;
completed deliveries keep their original encoded-byte charge while queued in a
receiver's inbox. Detach or End does not refund an unread inbox, and replacing
every session does not reset the connection allowance. Both ordinary `recv` APIs
refund a delivery's charge before returning it to the application, independently
of settlement. Additive [retained receives](retained-ingress.md) keep that charge
with a non-clonable receipt until it is destroyed, without delaying dequeue
credit. The sender-side data listener uses these receipts through ingress parsing,
broker replies, and disposition flush. Settlement does not refund a retained
receipt; dropping an inbox still refunds its queued messages. Incoming
exhaustion detaches only the offending link with `amqp:resource-limit-exceeded`.
Outgoing queued and active payloads keep their full encoded-byte charge through
the final Transfer flush. Failed or cancelled flushes retain it until teardown;
subsequent outcome and acknowledgement metadata does not retain content bytes.
Local send exhaustion is a retryable state error that does not reserve a tag,
advance delivery or credit counters, or write a frame. Aborts, decoding failures,
unavailable inboxes, and actor-owned teardown refund their actual leases.
This allowance counts logical retained content, not exact heap usage. Decoded
object overhead, custom decoder expansion, the bounded reader-frame backlog,
unencoded command messages, transient encoding/decoding and frame copies, and
ordinary application-owned messages and explicit clones are outside it. Outgoing
message encoding now measures
and validates borrowed sections before content admission, applies the peer's
limit with the exact encoded length, reserves the content allowance, and only
then creates one fallibly reserved output buffer. Metadata and bodies are not
cloned into temporary value trees or nested container buffers. The count and
write passes use the same canonical encodings, including shared array
constructors and compact collection widths from the [AMQP type system](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html).
Encoding additionally limits nesting to 68 and cumulative constructed-value
visits to 132,096 across all message sections, including zero-width array values;
violations are detected before output allocation. The standalone
`encode_message_with_max_size` API rejects an exact oversized length with typed
`MessageSizeError` before allocation. `encode_message` keeps no explicit byte
cap, but now uses the same borrowed traversal and structural limits. The native
send path uses the shared connection allowance before allocating its payload,
including when a peer advertises no maximum. Unencoded command contents and
performative metadata conversions still need separate resource policies.
Both connection drivers queue at most 16 complete decoded frame results, rather
than 256. A saturated queue stops further reads until the actor makes room; the
reader can hold one additional complete frame waiting to enqueue, and an actor
handling a frame can hold one more. Those 18 frame slots have an encoded-content
envelope of about 4.5 MiB at the default 256 KiB receive frame limit, or 72 MiB
when the test client selects the 4 MiB ceiling. This is separate from retained
message content accounting and is not a heap bound: decoded metadata expansion,
object overhead, transient codec/conversion copies, and socket buffers are
additional. Existing per-frame decode limits remain in force. Cancellation
aborts and joins the reader and drops the queued results without draining more
wire input.
Frames on channels above the locally advertised limit receive a framing-error
Close without refreshing receive activity. Session channels are independent in
each direction, as specified by the [AMQP session establishment protocol](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
The server advertises its own incoming channel range and reserves a vacant
outgoing channel within the peer's inclusive limit, preferring the incoming
number when it is available. Its Begin response references the actual incoming
channel. The test client correlates a response through Begin's `remote-channel`
and the original pending session generation, not the response frame's number.
Later session traffic resolves only through the peer-channel association; a
numerically equal local channel is not a routing fallback. Duplicate or invalid
associations and traffic on an unknown incoming session channel receive a bounded
framing-error Close. A server unable to reserve an outgoing channel returns a
resource-limit Close without creating a session or application event; a local
client Begin instead returns retryable `InvalidState` without writing a frame.
The test client still does not accept peer-initiated sessions and explicitly
closes with `amqp:not-implemented`.
An ending session discards traffic until End on its mapped incoming channel;
the binding and local reservation survive until that acknowledgement. Session
approval and endpoint generation fences continue to reject stale commands after
channel reuse.
Link handles are also independent in each direction. The server reserves a
vacant local handle within the peer Begin's inclusive `handle-max`, preferring
the peer's number when available; the test client allocates from its own cursor
within that same negotiated output range. Each endpoint advertises its own
incoming handle range independently. Client Attach responses correlate by link
name, mapped session, expected role, and the original link generation before
binding the peer handle. Flow, Transfer, and Detach resolve only through that
published peer binding, never through a numerically equal local handle. Pending,
installed, and closing links reserve their local handles; a matching Detach
acknowledgement releases the closing binding. Exhausting the peer's output range
ends the requesting server session with `amqp:resource-limit-exceeded`, without
creating a link or approval event. A local client Attach instead returns retryable
`InvalidState` without advancing its cursor or writing bytes. Unless a retained
error-link record applies, Flow, Transfer, or Detach on an unpublished peer
handle ends only its session with `amqp:session:unattached-handle`.
A duplicate Attach on a normally bound peer handle, including a pending approval
or a normal close awaiting acknowledgement, instead receives an immediate connection
Close with `amqp:session:handle-in-use`. That refusal precedes link-name matching,
approval publication, and handle-capacity checks. It publishes no lazy Begin or
session End, retires all local session and link owners before the Close write,
and fails pending client operations with a closed-connection error after a
successful flush. Failed or cancelled reply I/O retains the existing teardown
error classifications. The bounded Close handshake controls connection shutdown.
Local error Detach replies mark the exact closing link generation after frame
preflight and before reply I/O. While that peer alias is still reserved, a late
Flow or Transfer ends its session with `amqp:session:errant-link` before changing
session windows, link credit, delivery ownership, or retained message content.
The marker survives failed or cancelled Detach flushes. Normal closing links
continue to tolerate crossing traffic, and a mapped peer Detach acknowledgement
releases the exact alias for reuse. A separate session index retains at most 128
error-detached peer handles through that acknowledgement. An unbound retained
handle still refuses Flow or Transfer with `amqp:session:errant-link`, while a
historical Detach is ignored. A structurally valid, admitted fresh peer binding
supersedes its old numeric record; an exact current binding takes priority over
history. Reusing only the local output handle does not reassign the old peer
handle. These handle records clear when the session ends.
Each connection also retains exact, case-sensitive error-link names separately
for each local link direction, across Detach acknowledgement and session End.
The registry holds at most 256 name/direction keys and 1 MiB of summed UTF-8 name
bytes. Name, peer-handle, and delivery-ID history admission and required reply
frame preflights all precede error-history publication. Exhaustion ends only the
affected session with `amqp:resource-limit-exceeded`, without publishing an
unrecorded error Detach or partially adding a global name record. Committed
history survives failed or cancelled reply I/O; names clear when the connection
is destroyed. Normal closes create no error-name or error-handle records.
An incoming Attach for a known error name and direction with a null unsettled
field receives scoped `amqp:session:errant-link`, including on another session.
A non-null field, including an empty map, instead identifies an unsupported
resume in this context. A server can publish its own Attach followed by an
`amqp:not-implemented` error Detach; an unsolicited client-side request with no
pending local endpoint receives session End with `amqp:not-implemented`.
Fresh unknown empty maps retain their existing behavior. A local client attempt
to freshly attach a known error name in the same direction returns `InvalidState`
before advancing its cursor or publishing bytes; the opposite direction remains
independent.
Normally bound duplicate handles retain immediate Close priority, even if the
request names a historical error link. For a still-reserved error alias, a null
or unrelated-name Attach receives scoped errant-link End; a non-null request for
its known error name receives scoped not-implemented End without replacement
allocation. This is the native conservative policy for the overlapping
[link-error and duplicate-handle rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html),
not an implementation of link resumption.
Live names are also reserved by exact, case-sensitive name and canonical local
direction across a connection's sessions. Current pending and installed links,
including ordinary closes awaiting mapped Detach acknowledgement, hold that
reservation. A normal acknowledgement or session End releases it; error-name
history follows the separate lifetime above. The scan uses existing bounded
aliases rather than another retained-name registry, and ignores ending sessions,
orphaned or foreign-generation aliases, and error-detached reservations.
A fresh peer handle claiming an already-live name in the same direction receives
scoped session End with `amqp:not-implemented`, without replacing the original
endpoint, publishing an approval, or adding error-name history. An original
endpoint on another session remains usable. This is an explicit refusal of
unsupported link stealing, not the occupied-handle Close rule; occupied handles
and known error names retain their earlier classification priority. A local
client duplicate returns `InvalidState` before cursor advancement or output.
Opposite directions and case-distinct names remain independent.
Client pending replies are indexed by exact name and local direction, then
checked against their mapped session generation. Opposite-role same-name pending
links can therefore coexist. An exact directional entry on another session is
ignored without consuming it or falling back to the other role. Only when that
entry is absent can a current-session opposite-role entry receive the existing
invalid-field refusal for a wrong-role reply. Cleanup retires only the owning
session's entries in either direction.
Error Detach also retains the exact owners of known live delivery IDs in separate
incoming and outgoing session indexes. Each direction holds at most 4,096 IDs;
admitting more ends only that session with `amqp:resource-limit-exceeded` rather
than evicting error history. Any peer Disposition touching one of those IDs,
including a mixed or wrapping range, ends the session with
`amqp:session:errant-link` before applying any healthy settlement. This check is
independent of the supplied outcome and settled flag. The indexes retain neither
message bodies nor delivery tags, survive Detach acknowledgement and handle
reuse, and clear when the session ends. The outgoing allocator skips retained
error IDs. A fresh incoming delivery replaces an old incoming error record only
after header and format validation, exact ledger reservation, and successful
credit admission; rejected attempts and continuations do not reassign it.
Normal closes do not create these records. Already released successful or
pre-settled delivery IDs are not tracked. Link and delivery resumption,
cross-connection name ownership and stealing remain unfinished.
Session windows count Transfer frames independently of link delivery counts.
Incoming windows replenish after bounded frame processing; receive links grant
32 message slots and return credit only as the application consumes a delivery
or a partial delivery is aborted. A paused receiver cannot block the connection's
other links. Fragmented sends yield when their session window closes, resume on
session Flow, and consume only one link credit per message. Flow echo, drain,
optional credit, wrapping counts, and early second-mode dispositions have raw
transport regressions. Every outgoing frame is checked against the peer's frame
cap before writing bytes. Additional metadata and command-content resource
policies remain unfinished.
Session startup tracks local Begin publication separately from application
approval. A required response to a pipelined Flow, pending Detach, End, or
session refusal first publishes the server's Begin exactly once; later approval
does not repeat it. This preserves the
[session-state send ordering](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)
without buffering additional control responses. An approval for an absent or
still-ending session returns a local detached error, which the broker listener
skips without disconnecting healthy sibling sessions. Application approval
still controls link installation; pending attaches share a 32-entry bound.
The transport additionally limits one connection to 32 session states, including
unapproved, pending-client, and ending sessions. It permits 128 distinct link
lifecycle handles per session and 256 across the connection. Installed links,
pending approvals, peer aliases, and handles awaiting Detach acknowledgement
share that allowance; overlapping bookkeeping for one local handle counts only
once. Approving an admitted link does not charge it again. A closing handle
remains charged until the peer acknowledges Detach. A session state remains
charged until the peer's End removes it, even though End retires its links.
An excess peer Begin receives an `amqp:resource-limit-exceeded` Close without
creating a session or application event. An excess peer Attach ends only its
requesting session with that condition, without allocating an extra approval or
refusal handle. The test client refuses excess local Begin and Attach calls with
`InvalidState`, before advancing counters or writing bytes, and can retry once
the relevant acknowledgement frees capacity. These cardinality limits do not
change the negotiated numeric channel range and do not bound retained content
bytes or exact heap usage.
Session approvals, installed session endpoints, and client session commands
retain an opaque session generation instead of relying on a reusable channel.
Link approval uses an `IncomingAttach` receipt with that original session,
an exact pending approval token, and immutable peer handle, name, and role. Its
assigned local handle is separate approval authority, not editable raw content.
The application can still edit terminus and response metadata, including the
granted Service Bus session filter. A foreign or altered receipt is refused
locally without consuming the rightful approval or writing bytes. Clones can
approve only once. Raw Attach content cannot be converted back into approval authority.
This is a source-level change for callers of the in-tree transport API.
Additive [native connection identities](native-connection-identity.md) bind
negotiated session and link generations to their exact actor. Retained receipts
expose that origin independently of link settlement and retirement. Observer
clones are inert; the actor retires its identity before publishing termination,
including cancellation and unwinding. Activity is only an observation, not
authorization, a live delivery guarantee, or transaction admission.
Additive [native sender identities](native-sender-provenance.md) identify an
actor-accepted sending-link generation. `PendingSettlement::belongs_to_sender()`
checks exact active link and connection origin, including First-mode outcomes
without an acknowledgment token. This does not prove an original outgoing
delivery generation or settlement usability, and changes no ordinary settlement
or transactional-disposition refusal behavior.
Additive [original outgoing delivery identities](native-outgoing-delivery-provenance.md)
preserve an actor-minted generation through send completion and later
acknowledgment metadata. `PendingSettlement::delivery_identity()` is historical
provenance after outcome resolution, not a pre-outcome handle or proof of an
unsettled delivery. Observer clones retain metadata only; settlement and
transactional-retirement support are unchanged.
The separate [native transactional-work opt-in](native-transactional-retirement.md)
admits Accepted, unsettled retirement dispositions for fully flushed dedicated
Second-mode sends and prepares an exact mixed set with postings. Rollback restores
the same outgoing native delivery without replaying its body or releasing its
alias. This does not change ordinary sender refusals, the posting-only broker
listener, or SDK transaction support; broker correspondence to held receive locks
is supplied only by the separate [atomic messaging listener](atomic-messaging-ingress.md).
Its explicit [attach-default opt-ins](transaction-attach-defaults.md) negotiate
Mixed receiver requests to actual Unsettled/Second and interpret an omitted
fresh coordinator count as zero under an immutable approved exception.
Existing strict native APIs and ordinary/posting-only listeners are unchanged;
the accommodations alone are not an SDK gate. The separate
[pinned .NET scope tests](dotnet-transaction-scopes.md) establish warmed and
cold-first same-queue immediate send, plus held Complete over experimental TLS.
The messaging listener's separate initial control window is socket-tested;
cold-first SDK support is limited to immediate Send, not acquisition or Complete.

Three native regression checks exercise ordinary Accepted versus coordinator
Declared outcomes across ordering, independent sessions and settled delivery-ID
reuse, in both receiver settlement modes. Native package runs pass 810 checks
without `test-client` and 882 with all features, including the same 23 credit and
32 documentation checks; the additional 72 cases are the opt-in client suites.
Two feature-gated test warnings were corrected without changing assertions,
deadlines or ignore status. Strict workspace lint and formatting pass. These
scoped checks add no production routing change, new full-workspace or SDK run,
explanation or fix for the earlier SDK type mismatch, or cloud parity claim.

An End on an unmapped channel is refused without manufacturing a session reply.
Client Begin searches only vacant channels within the peer's inclusive channel
limit. Pending, live, and ending sessions cannot be overwritten. Client End
retires its original endpoints before writing and waits for the matching peer
End acknowledgement before its channel is reusable. Repeated or stale End and
Attach commands are local refusals, not commands against a replacement session.
Locally closing receiver links do not publish further link-credit updates, even
when a queued delivery-consumption notification is processed after Detach.
Session-window replenishment and healthy sibling links remain independent.
First transfers require an explicit delivery ID, binary tag, and message format.
Tags may be empty but cannot exceed 32 bytes. Continuations may omit identity
fields, but repeated ID, tag, and format values must match the first fragment;
an invalid fragment closes only its link. Format zero always uses the standard
message decoder. The transport can opt a receiving link into at most eight
additional exact formats with trusted application decoders; zero cannot be
overridden. A format not registered on that link is refused with
`amqp:not-implemented` before reserving a delivery slot. CBS, management, and
test-client receiving links retain the standard-only default.
Incoming delivery IDs are reserved across the session, and tags across their
receiving link, from the first fragment. A live collision closes only the
offending link with `amqp:invalid-field`; a different original owner's delivery
remains available for settlement. This link-local error scope is a Switchyard
policy. A separate metadata allowance bounds incoming partial, complete, and
second-mode acknowledgement entries to 1,024 per receiving link and 4,096 per
session. Admission beyond either limit closes the offending link with
`amqp:resource-limit-exceeded`, independently of its message-slot credit.
Consumption returns message-slot credit but does not settle a delivery.
Application receipts retain opaque link and delivery generations rather than
relying on reusable numeric aliases. A foreign receiver, retired link, or
aborted receipt cannot emit a settlement. Repeating a terminal settlement on
its original open link is a no-op, even after the numeric ID has been reused.
Local outcomes are checked against the peer's frame cap before state changes;
an oversized outcome leaves the live receipt available for a smaller retry.
Receiver-first outcomes settle immediately. Receiver-second outcomes are sent
unsettled and retain their aliases until the sender acknowledges settlement,
including a state-less or ranged sender disposition. The application call
returns after writing its outcome, not after that acknowledgement. A sender
settling before the application finishes suppresses an unnecessary response;
partial deliveries retain their aliases until completion or abort. Completing
sender-settled deliveries need no outcome or retained alias.
Outgoing tags are reserved per sending link when a send enters its local queue,
and remain reserved through active fragments, unresolved outcomes, and pending
second-mode acknowledgements. This implements the transport's
[live-tag uniqueness rule](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
An identical tag on another link is independent. A duplicate on the same link,
more than 32 tag bytes, a full 32-entry outgoing queue, or admission beyond 1,024
outstanding deliveries on one sending link or 4,096 across a session is a local
`InvalidState` refusal. These checks precede message encoding, credit consumption,
delivery-ID allocation, and wire output, leaving the endpoint usable for retry.
Each delivery counts once even while both active and unsettled. Tags are released
after a successful final pre-settled Transfer flush, completion without a required
acknowledgement, or exact local or remote acknowledgement settlement. A dropped
send waiter does not release a tag; a failed or cancelled final Transfer or
acknowledgement flush retains it until teardown. An old terminal receipt cannot
release a later send's reused tag or numeric ID. These are metadata allowances,
not a connection-wide content or heap limit.
An ordinary native sender can request a unique outgoing reservation with
`reserve_send`; the returned future owns its endpoint identity and does not
borrow the sender. The connection actor admits it against current peer credit
and the existing queue, per-link, and per-session metadata limits. Creating or
claiming a reservation does not advance delivery counts or allocate a delivery
ID. `try_claim` races processed credit withdrawal through shared revocable
state; losing that race returns `SendReservationRevoked`. Dropping an unqueued
reservation or its pending future returns local capacity through shared state
and wakes the actor, including when the command queue is full or its reply was
not consumed. Retiring its exact link also revokes it; numeric handle reuse does
not transfer that authority. Reserved sends retain the ordinary outcome and
second-mode acknowledgement rules.
A claimed reservation is local admission, not an irrevocable wire-credit grant.
The first Transfer still checks the latest peer credit and session window; a
later credit reduction can delay it until a new grant. This follows the
[transport flow-control model](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#section-flow-control).
Reservations do not reserve encoded payload bytes or guarantee successful
encoding, flush, or settlement. Ordinary sends without reservations retain
their existing queued-send API and resource limits.
`send_reserved_with_settlement_owned` consumes a claimed reservation and returns
an owned `Send + 'static` future without borrowing the sender. Creating that
future performs no transport IO; it already owns the message, tag, original
endpoint metadata, and cancellation guard. The borrowed reserved-send method
delegates to the same path when polled. Dropping before the actor consumes the
token cancels the unique slot through shared state, including while command
capacity is unavailable. After native admission, dropping a waiter cannot undo
the send, return spent wire credit, release its retained tag, or settle a broker
lock.
For unsettled sends, the result is the existing `PendingSettlement` after the
peer outcome or applicable default, not a flush-only receipt. Initially
sender-settled sends retain their local Accepted result after final flush. Its
exact delivery provenance and caller-controlled final acknowledgement remain
unchanged. Multiple owned futures may be awaited independently; this native API
does not bound caller-owned message bytes or add broker authorization. The
ordinary listener applies separate held-work admission around it. See
[Native Outgoing Admission](native-outgoing-admission.md) and
[Ordinary Receiving Pipeline](ordinary-receiving-pipeline.md).
Outgoing delivery IDs are allocated independently of incoming IDs, within one
session's sending direction. When the cursor wraps onto a live ID, a bounded
vacancy search skips unresolved deliveries, pending acknowledgements, and active
fragments across every sending link in that session. It does not overwrite an
old alias or stall solely because the next ID is occupied. The selected ID is
captured for every fragment of that delivery; first-frame admission advances the
cursor, while a lack of link credit or session window leaves it unchanged.
Session Transfer-frame counters remain separate and advance once per fragment,
without gaps introduced by skipped delivery IDs.
Outgoing sends and explicit second-mode acknowledgement receipts also retain
their original link generation. A stale sender cannot send or close a
replacement link, and a receipt cannot acknowledge a different delivery that
reuses its numeric ID. Owned terminal acknowledgements are repeatable no-ops
only while that original endpoint remains open. Explicit receipt methods borrow
the receipt, so an oversized rejection leaves it available for a smaller retry;
the exact pending receipt is removed only after writing and flushing its sender
disposition successfully. The test client automatically writes a state-less,
settled sender acknowledgement before reporting a second-mode send outcome,
even if the caller has dropped its waiting future. An early receiver outcome
waits for the final Transfer before that acknowledgement. First-mode, pre-settled,
and initially receiver-settled outcomes do not emit redundant acknowledgements.
Receiver settlement is tracked independently from its first terminal outcome.
A later settled disposition with absent or nonterminal `Received` state preserves
that outcome and cancels any still-pending sender acknowledgement. A fragmented
send still waits for its final Transfer to be written and flushed; the first
terminal outcome received before completion takes precedence over a default.
When the receiver settles without a terminal outcome, the actual selected
Source's explicit default is used. Defaults must be
[terminal outcomes](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-source)
on both encoding and decoding. Invalid application-edited defaults are refused
locally without consuming the original approval or writing a response.
An absent default produces the delivery-level `RemoteSettledWithoutOutcome`
error instead of manufacturing Accepted or closing the link. A nonterminal,
unsettled update does not use the default or complete the send. Remote-settled
manual acknowledgement receipts become owned no-ops without another wire
response; the application remains responsible for its broker settlement.
Local client Detach retires the endpoint before waiting for the peer's reply;
repeated closes are local no-ops and cannot enqueue another Detach.
The negotiated sender mode is enforced at completion, with aborted transfers
implicitly settled. A per-transfer receiver mode defaults to the negotiated
mode on the completing frame, rather than becoming sticky from an earlier
fragment; an illegal receiver-second override on a receiver-first link is
refused unless the delivery is sender-settled or aborted. These policies follow
the [Transfer and Disposition rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer).
Switchyard interprets the receiver-mode exception to include a settled sender
disposition received before completion. Such a disposition never substitutes
for the explicit settled Transfer required by a negotiated settled sender link.
Detach, End, and connection teardown retire the affected generation and remove
its aliases. Link suspension and resumption remain unsupported. An Attach with
retained unsettled entries or incomplete unsettled state receives a stripped,
null-terminus Attach response followed by a closed `amqp:not-implemented` Detach;
it never reaches application approval. A complete empty unsettled map is accepted
as a fresh attach. Pipelined unsupported attaches wait for the local Begin before
their refusal, sharing the 32-entry pending-attach bound. The test client also
refuses retained or incomplete state in an Attach response before exposing an
endpoint or granting receive credit. Caller-mutated recovery state is refused
before enqueueing an approval and leaves the original valid approval available.
A resumed Transfer on a known receiving link is refused before identity, credit,
payload decoding, or abort admission, while still counting its session frame;
cleanup affects only that link's partial content and aliases. This explicit
unsupported-feature policy does not implement the standard's recovery exchange
or claim its unknown-resumed-delivery ignore behavior.
Approved producer links register Service Bus batch format `0x80013700`, whose
[wire constant](https://raw.githubusercontent.com/Azure/azure-amqp/master/src/AmqpConstants.cs)
identifies one encoded standard message per outer Data section. The nonempty
wrapper uses one link delivery/credit regardless of member count, shares its
inner parsing allowance, and submits exactly one atomic ingress command before
the outer Accepted outcome. The entire wrapper must still fit the 256 KiB
producer-link limit. Inner properties, lifetimes, identifiers, and optional
scheduling timestamps remain independent; outer metadata is not inherited or
used as a batch-wide duplicate identifier. A present outer session must agree
with present inner sessions and never fills a missing one. A standard inner Data
body is ordinary content, not another batch. Empty wrappers are refused; an
empty encoded inner message is valid anonymous content. The
[pinned SDK converter](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpMessageConverter.cs)
normally sends singleton batches in format zero, so raw transport tests also
force the nonzero singleton format. Both pinned client gates cover the enumerable
and size-safe batch APIs, session FIFO/refusals, rich content, duplicate detection,
and an oversized `TryAddMessage` refusal.
Message decoding can share one cumulative allocation allowance across embedded
messages: at most 132,096 parsed values and 4 MiB of copied string, symbol, and
binary content, with depth bounded independently for each value. Array members
and repeated named descriptors are charged before allocation. Scheduling
management requests and producer batches share that inner allowance and reject
more than 1,024 members before decoding them. These bounds do not provide a
connection-wide memory limit and are not Azure batch quotas.
If a peer detaches a link while application approval is outstanding, its receipt
is retired immediately. The server first publishes a minimal Attach response
with its assigned local handle, then acknowledges with Detach; any required
Begin precedes both. The pending slot and exact handle binding are released only
after that final Detach flush, without waiting for the application to consume
the cancelled receipt. Pending link-level Flow echoes likewise wait until the
server's Attach is published. If even the minimal Attach cannot fit the peer's
frame limit, a bounded `amqp:frame-size-too-small` End replaces the link reply.
A stale receipt cannot open the replacement link or consume its approval.
Retired queued session and link events are skipped. Cancellation during
asynchronous broker planning releases any newly granted domain session hold.
The listener remains available for later links on the same AMQP session, including CBS and management
approvals. No numeric cancelled-approval tombstones are retained.
The edge resolves a link's address to an entity, turns transfers into send
commands and dispositions into settlements, and answers a rejection with the
condition an SDK keys its behaviour off. A receiving link's settle mode selects
the delivery guarantee: unsettled is peek-lock, pre-settled is receive-delete.
Before each ordinary broker Receive, the listener checks Listen authorization,
obtains and claims one native outgoing reservation, and checks authorization
again before submitting the command. An attached link with no usable peer
credit therefore does not acquire a lock, delete a message, run Receive expiry
cleanup, or stamp a Receive proposal. Replaying an unchanged absolute Flow
grant does not create additional credit. An empty Receive releases its
reservation before waiting for an enqueue notification or the coarse fallback,
so it cannot hold unused credit against drain. Detach and authorization loss
are watched during credit admission, the broker wait, and the empty-queue wait.
Authenticated ordinary Receive additionally uses an owned receive-only ticket.
The owner checks its captured Listen expiry immediately before the proposer;
dropping the armed future cancels Pending work even while queued. The exact
binding and physical target are retained, including subscription and dead-letter
paths. The numeric expiry is not a live grant-revocation lease, and Started
means admission rather than commit. A Started Receive can still commit after
its waiter is cancelled; a committed deletion stays deleted, and an unsettled lock retains
the existing expiry fallback. Teardown releases the exact session hold, not
all message locks. Payload-size admission also remains after Receive.
The ordinary receiving task can hold multiple deliveries on the same link and
finish their independent outcomes out of order. It permits at most 32 work
items, including one admission, pending Receive, or parked response, and
4 MiB of conservatively projected captured-message content. Projection and
content admission precede message conversion and construction of the native
send future. One already acquired and decoded broker response is outside that
content allowance; an individually oversized response is refused, not rolled
back. Temporary aggregate pressure parks that response while existing outcomes
finish. A started Receive remains pinned across other work completions rather
than being cancelled and resubmitted. These are local held-work limits, not an
RSS bound or Azure prefetch quotas. Separate
[SDK receive-batch gates](dotnet-receiving-batches.md) exercise three held action
copies before completion and rolling three-credit queue replenishment on the
same receiver. The experimental transactional receiver is unchanged.
See [Ordinary Receiving Pipeline](ordinary-receiving-pipeline.md).
See [Ordinary Receive Claims](ordinary-receive-claims.md) for expiry, cleanup
ordering, unsupported adapters, and unchanged settlement and development paths.
Peeking is served through the entity's `$management` request/reply links and
returns encoded AMQP messages without touching their broker state.
Each peek inspects at most 256 stored records and retains the existing response
budget. It leaves message states, delivery counts, ready indexes, session holds,
and the persisted clock unchanged, even when skipping expired ready messages.
It still passes through the normal proposal timestamp checks; it is not an
unstamped metadata read. Listen authorization applies to the management link's
entity. An absent `session-id` requests entity-wide browsing, while a present
non-string or invalid identifier is refused before broker submission. An
associated receiver link is not required and cannot redirect the browse.
Receiving transfers encode the number of preceding acquisitions, so the official
.NET client reports 1 on the first receive and 2 after abandonment and redelivery.
Peek retains the stored acquisition count without the client's receive increment.
The core currently counts acquisitions, not only unsuccessful settlements;
counter parity for deferral, locked-message peek, and dead-letter transfers is
not established, nor are generic AMQP Released/Modified counter semantics.
Deferred receive is also served through `$management`, and locks returned that
way are settled through the management `update-disposition` operation.
Session deferred receive requires a live session hold on the associated receiver
link in that same connection. The state machine validates the hold before
touching message records or expiry cleanup; naming a session or replaying an old
hold cannot bypass another receiver's ownership. The old identifier-only core
commands retain their serialized shape for trusted callers, but are not used
by the management edge.
Dead-letter waiters are notified from committed ready-index enqueues, including
lazy expiry during ordinary or deferred receive. Staged writes that are later
rejected, failed storage commits, and drop-on-expiry cleanup do not wake them.
This notification metadata adds no persisted or serialized command fields.
Retrieving only expired deferred messages commits their cleanup before
returning `com.microsoft:message-not-found`; missing messages are distinguished
from a missing queue. Dead-letter management paths normalize the reserved
suffix in the same way as receiving links, including the SDK's mixed-case form.
Deferred receive rejects duplicate sequence numbers before acquiring locks or
removing expired messages, so one message cannot be returned twice by a batch.
Management replies have a local 4 MiB ceiling and honor the reply receiver's
advertised maximum message size. Deferred batches must fit a conservative
content budget, including broker metadata and response fields, before any
locks, deletions, or expired-message cleanup commit. An oversized batch is
refused atomically with `amqp:link:message-size-exceeded`; a smaller retry can
still retrieve its messages. Peek returns a fitting prefix, or a size refusal
if its first eligible message cannot fit. These are local resource policies,
not Azure batch-count limits; the estimate may refuse a compact encoding that
would fit on the wire. It bounds content and staging work, not exact heap usage.
A reply link must be attached before a management
command is submitted. If it cannot be found within two seconds, the request is
rejected without changing broker state. Outgoing oversized messages detach
only their affected link and are never transferred.
Modified outcomes carry application-property updates; SDK dead-letter outcomes
carry the reason, description and updates in their error information. Management
settlement accepts `properties-to-modify` and promotes reserved dead-letter
fields when explicit reason/description fields are absent. Explicit fields win
when both forms are present. Null property values are retained, not interpreted
as deletion; these precedence and null policies have not been compared with a
live Azure namespace. A malformed or refused direct settlement closes its link
with the relevant condition and leaves the lock to expire.
Second-mode settlement is acknowledged only after the broker commits the
change. The Service Bus acknowledgement is Accepted on success or Rejected
with the actual refusal, rather than an echo of the receiver's requested
dead-letter outcome. Receivers that settle in first mode cannot await a broker
acknowledgement; a refusal still closes their link.
Scheduling and cancellation use the management node, while an ordinary send
can also schedule through the `x-opt-scheduled-enqueue-time` timestamp
annotation. Peek returns active, deferred, and scheduled state annotations.
A receiving link's `com.microsoft:session-filter` names a session or, with a
null value, asks for the next available one; the attach response echoes the
granted identifier and the initial session-lock deadline. The receiving task
releases its exact session hold when that link closes, including delivery or
settlement error exits. A first-mode disposition followed immediately by detach
can race native receipt finalization; cleanup neither reverses a committed
settlement nor relaxes the retired-link ownership checks. The detach
acknowledgment is not itself a barrier for committed domain cleanup. Renewing
the session lock and reading or writing its
state use the entity's `$management` request/reply links, as does message-lock
renewal. Scheduling and cancellation require Send authorization; receiving,
peeking, settlement, and lock or session operations require Listen. Management
links accept either permission, and every request rechecks its own permission
when authentication is enabled. A transfer is accepted only after its command
committed. With Fjall, a successful acknowledgement follows a local durable
batch; Memory acknowledges only in-process state. Neither supplies quorum
durability. One node still serves one namespace. A message
drained from a dead-letter queue carries its reason and description in the
`DeadLetterReason` and `DeadLetterErrorDescription` application properties. The
complete protocol coverage uses a Rust AMQP 1.0 client. The current and previous
stable official .NET SDKs also have opt-in gates for ordinary send, receive,
peek, deferral, deferred receive, message-lock renewal, completion, scheduling,
and cancellation, duplicate detection for ordinary and scheduled sends, plus
session state, renewal, receive, completion, and scheduling. Both gates exercise
message properties, application-property CLR types, footer data, a lifetime
longer than the AMQP header can represent, and property-preserving redelivery;
both also exercise immediate topic fanout, independent subscription settlement,
peek, lock renewal, deferral, deferred receive, dead-letter reasons and property
updates, and both topic batch-send APIs with ingress duplicate detection. These
gates cover the local shared-sequence policy, not cloud parity for that policy.
Both pins also cover independent subscription sessions and management-only
session browsing, plus parent topic scheduled browsing, cancellation, annotated
send, and timer activation. Boolean/correlation rule creation, removal,
enumeration, and delivery selection are also exercised. SQL gates cover exact
source enumeration, numeric and case-aware selection, missing/null properties,
correlation overlap, both batch APIs, current-rule scheduled activation, and
filter-error dead-letter/drop policies. Separate [REMOVE action](sql-actions.md)
gates on both pins and backends prove exact action source enumeration, original
filter independence, one OR-combined base plus two independently settled action
copies, exact-key removals, final RuleName collision handling, body/system/footer
preservation, and an awaited unsupported-SET refusal followed by a healthy send.
They restore default rules, verify empty subscriptions and dead-letter queues,
and check committed cleanup again after reopen. Separate
[receive-batch gates](dotnet-receiving-batches.md) prove three action copies held
on one receiver before any completion, independent out-of-order settlement, and
rolling queue replenishment with fixed prefetch three. Both pins run both
backends and check exact empty runtime indexes and counters after reopen.
Azure administration remains
ungated. These checks establish local interoperability, not cloud parity for the
documented SQL semantic choices.

The REMOVE-only action results above record the earlier baseline, not
version-2 literal SET verification.

The SDK gates build into separate temporary directories and run the resulting
assemblies directly. Run them explicitly with
`cargo test -j 2 -p server --test amqp_dotnet_current -- --ignored --test-threads=1`;
each .NET build is limited to two jobs, and serial test execution preserves that
limit across the two releases.
The action and receive-batch gates reuse the bounded transaction process runner:
each build and client process has a 180-second deadline and bounded output capture, with
owned-process cleanup and isolated certificate trust. This does not add process
lifetime guarantees to the older ordinary-message or WebSocket gate runners.

### HTTPS Administration Tokens

`SharedAccessPolicy::authenticate_atom_sas` adds a separate library profile for
HTTPS administration SAS tokens. Its 8 KiB token cap precedes parsing and applies
only to this profile. Audiences require HTTPS with effective default port 443;
an actual listener's request port is not part of the authorization scope. Literal
resource segments are decoded once and validated before URL normalization can
erase dot segments or reinterpret backslashes. Control names and ordinary case
remain literal, without AMQP control-alias conversion.

Signature verification retains the original encoded resource bytes and the
existing UTF-8 key-text, rotation, expiry, and static-error behavior. Authentication
alone does not authorize an operation: callers must check the returned grant's
`Manage` permission against a trusted fixed-host namespace or entity scope using
their supplied epoch. This method supplies neither TLS proof nor a trusted clock.

The native and CBS entry points retain their existing profiles and token limits.
This increment starts no HTTP listener, enables no CLI option, and makes no new
official SDK administration claim. Store formats, dependency versions, and
existing native/AMQP authentication configuration are unchanged.

Verification: the closed full workspace passed 5,663 tests with no failures and
the same thirteen ignored SDK cases, adding twelve authentication cases without
losing any prior case, status, or ignore reason. The focused auth target passed
all 64 cases. Both strict lint configurations, both builds, and formatting passed
against the same frozen source, serially using the shared cache and CPUs 14,15.
No SDK gates were rerun.

### Finite Queue Capacity

The opt-in trusted owner API reserves logical bytes across an ordinary primary
queue and its dead-letter shadow. It excludes required sessions and duplicate
detection. The separate native service below exposes finite definitions and
logical usage without enabling legacy `EntityService` capacity fields. The opt-in
HTTPS endpoint below exposes bounded Atom fields, including explicit CLI startup. The
reservation model, lifecycle refunds, bounded planner, and corruption boundaries
are defined in [Finite Queue Capacity](finite-queue-capacity.md).

Verification: the closed full workspace passed 5,620 tests with no failures and
thirteen unchanged ignored SDK cases, adding 150 cases. Every prior case, status,
and ignore reason was retained; doctest location changes were normalized with
multiplicity. Both strict workspace lint configurations, both build
configurations, and formatting passed. The only source changes after the broad
runtime run were three function-local argument-count lint annotations; removing
them restores the tested file byte-for-byte. All forty capacity lifecycle cases
then passed against the final linted source on Memory and Fjall. Six server
capacity cases passed across both backends, including two actual Rust AMQP socket
cases exercising refusal and retry, not official SDK capacity parity. Scoped
metadata/topology/binding probes retain the old diagnostics before
their final capacity proofs; this is not a global command-priority or whole-ledger
health guarantee. Injected pre-apply storage refusals do not establish the
outcome of an indeterminate physical commit. No SDK gates were rerun.

The durable layout is now 17, with no directory migration. Current paired image
operations use role 2 and require canonical non-finite owner modes; finite image
export, finite committed-entry replication, and ledger migration are unsupported.
Historical role-1 pure codecs remain separate. The value envelope and existing
command, configuration, protobuf, and committed-entry encodings are unchanged.

### Atomic Finite Queue Definitions

The trusted asynchronous and blocking owner APIs can replace all eight queue
configuration fields and the finite limit together using the appended
`SetDefinitionFenced` instruction. The existing primary binding must still be
live before host and stored-clock reads. Current profile validation precedes
desired configuration checks; immutable session and duplicate-detection settings
precede other invalid configuration, which precedes a limit below retained usage.
An unchanged complete definition still stamps and validates time, but commits no
batch and does not advance the stored clock.

Changed primary/shadow configuration, capacity mode, and command clock share one
batch. Existing messages, reservations, counters, session metadata, and deadlines
are preserved; a smaller message-size limit constrains future admission only.
The prepared result is returned without a post-commit read. This is neither
in-place mode conversion nor ledger repair, and no native or Atom/XML fields are
enabled by these library methods.

Verification: the closed full workspace passed 5,651 tests with no failures and
the same thirteen ignored SDK cases, adding 31 cases without losing any prior
case, status, or ignore reason. The focused DTO, domain capacity, and server owner
targets passed 78 cases, including the new Memory/Fjall definition checks. Both
strict lint configurations, both builds, and formatting passed against the same
frozen source. The new server cases exercise owner APIs, not additional AMQP
sockets or official SDK administration. Injected pre-apply refusals do not prove
rollback after an indeterminate physical commit. No SDK gates were rerun. Existing
command/configuration/protobuf encodings and durable layout 17 remain unchanged.

### Finite Queue Deletion Bindings

`StateMachine::bind_finite_queue_for_deletion` reads a live finite queue's
deletion binding without consulting the stored clock or mutating state. It
validates the queue configuration, live incarnation and authoritative primary/DLQ
mode identity, but does not point-read or decode Usage or Charge. This permits
the existing fenced deletion path to purge opaque usage records without first
requiring an ordinary capacity description to succeed.

The absent-target path retains the existing bounded topology and runtime
diagnostics and their failure order. Actual deletion planning, bounded purge,
generation retirement and committed effects are unchanged. A returned binding
is not a successful purge, a whole-ledger health check, repair, or an atomic
owner turn: callers must obtain it and perform the fenced deletion within the
same serialized owner operation. Stale bindings and invalid mode metadata still
refuse, and orphan runtime may make deletion fail after a binding was read.

This library increment adds no HTTP endpoint, CLI operation, storage format or
unknown-commit retry/rollback guarantee.

Verification: all 555 focused domain cases and 102 existing administration/owner
regressions passed, with no failures or ignored cases. The twelve new paired
cases preserve every prior case, status and failure reason in those targets.
Both strict workspace lint configurations, both builds and formatting passed
against the same frozen source, serially on CPUs 14,15 with the shared cache.
The full workspace and official SDK gates were not rerun for this increment.

### Library HTTPS Queue Administration

`AtomAdminListener` serves finite ordinary queues over mandatory TLS and HTTP/1.
Callers supply a broker handle, business namespace, independent
SAS policy and fixed namespace-only audience scope. Request `Host`, port, SNI,
forwarded headers and XML do not choose that scope. The listener changes ALPN
only on its supplied owned TLS configuration; native/CBS authentication and AMQP TLS
configuration are unchanged. The library opt-in and separate CLI activation below
share this profile.
The pinned .NET administration gates below cover this finite queue profile.

Each request authenticates the separate bounded HTTPS SAS profile and requires
`Manage` before polling its body or submitting owner work. The retained grant is
checked again for expiry and permission immediately before starting asynchronous
owner admission. That check does not run again at eventual queue insertion or
execution, and does not revoke owner work already admitted or started. Epoch
conversion failures refuse rather than using zero.

PUT without a condition creates a finite queue (201); exact `If-Match: *` replaces
its full definition (200), not a patch. GET returns 200 or 404, DELETE returns an
empty 200, and the exact `/$Resources/queues` path returns a complete Atom feed.
That literal collection name is reserved; differently cased and percent-literal
ordinary names remain distinct. API versions `2024-05` and `2021-05` are accepted;
`enrich` must be absent or `False`. Listing supports `$top` 1..100 and `$skip`
0..1000, defaulting to 100/0. A page is filled or genuinely exhausted, or fails
without a partial feed. Offsets count validated visible ordinary queues, not
their DLQ shadows. Pages are separate owner turns, not a multi-page snapshot.

Atom defaults are 1024 MiB capacity, 60-second lock, unlimited omitted TTL,
256 KiB maximum message, ten deliveries and one-minute inactive duplicate
history. Supported lock durations are 5..300 seconds, present TTL is at least
one second, and message limits are integral KiB in 1..256. Durations use ordered
day/time components and exact integral milliseconds; extra fractional digits
must be zero, with no calendar months, signs, rounding or truncation. Sessions,
duplicate detection, partitioning, express/ordering, forwarding, nonempty
metadata/rules, auto-delete and runtime metrics are refused rather than emulated.
Active status and enabled batching are the only accepted values. Responses expose
the actual static definition, not invented usage counts or timestamps. SDKs that
omit inactive duplicate history on PUT may reset it to the one-minute default;
this is not a lossless GET/PUT promise for every stored configuration.

The complete definition and limit update share the existing fenced atomic owner
operation. Immutable settings, invalid configuration and below-retained-usage
limits refuse without partial mutation. Unchanged definitions validate/stamp
time but commit no batch. Existing records, reservations and deadlines are not
rewritten; a smaller message limit affects future admission. Deletion binds and
purges in one owner turn while retaining the existing opaque Usage/Charge path.
No retry, rollback or known-commit guarantee follows from losing an HTTP reply.

Bounds include 128 accepted connections (including handshakes), ten-second TLS,
header and body deadlines, a 20-second owner observation deadline and 60-second
total connection lifetime. Requests are single-use, with keepalive disabled.
Target/head/token limits are 4096 bytes, 32 headers/16 KiB logical header bytes
and 8 KiB respectively. Bodies are at most 64 KiB and 1024 frames, including empty
frames and trailers; trailers are refused. XML independently limits depth 16,
2048 events including EOF, 32 attributes per element, 64 active namespace bindings
plus the built-ins, and 128 properties. Replies are capped at 1 MiB and 100 feed
entries. Owner pages additionally bound backend operations and returned logical
key/value bytes; these are work/output bounds, not an allocator or RSS guarantee.

Path segments are strictly decoded once before scope and owner use; malformed
escapes, invalid UTF-8, empty/dot segments, controls and decoded slashes or
backslashes refuse. The adapter sees the pinned HTTP parser's representation:
that parser drops a raw URI fragment and can normalize framing headers. The
endpoint does not claim rejection of every raw-wire spelling erased upstream;
encoded `%23` remains a literal name. Query validation precedes permissive form
decoding. XML uses a pinned parser with explicit namespace/entity/declaration
checks; exactly one optional leading UTF-8 BOM is permitted.

Connection futures remain directly owned by the serve future, without detached
per-connection tasks. Stopping/dropping that future drops its original TLS and
HTTP futures; it is not graceful draining or cancellation of queued owner work.
Public errors are static and redacted. Neither keys, request XML, entity names
nor arbitrary storage diagnostics are included in error bodies.

Verification: the closed full workspace passed 5,757 tests with no failures and
the same thirteen ignored SDK cases. Compared with the previous full run of
5,663 tests, all prior case identities, statuses and ignore reasons were retained;
the 94 additions comprise twelve already-published deletion-binding cases and
82 new XML, owner and HTTPS cases. Doctest line locations alone were normalized,
with multiplicity preserved. All 308 focused server cases passed, including ten
actual private-CA/name-verified TLS cases and paired Memory/Fjall CRUD, refusal,
complete-image oracle and physical reopen checks. Both strict lint configurations,
both builds and formatting passed against the same frozen source, serially on
CPUs 14,15 with two build jobs and the shared cache. No official SDK gates were
rerun, and CLI activation was absent in that library increment. The only newly resolved package is the
pinned XML parser; existing package versions/checksums and durable layout 17 are
unchanged.

### Official .NET Queue Administration

The independently executed administration gates passed with Service Bus packages
`7.21.0` and `7.20.2`, each on Memory and Fjall, using both named-key and
connection-string constructors. They retain each package's default API version
(`2024-05` and `2021-05`) and original request serialization/authentication. The
fixture's owned per-message transport enforces HTTP/1.1 with a dedicated private
CA, normal hostname checks, no validation callback, no proxy/redirect fallback
and no global trust changes.

Each backend establishes a successful same-listener healthy control before
interpreting wrong-CA and wrong-name refusals. Configured SEND-only authorization
is refused without owner effects. Successful queue operations require exact
create/update/delete statuses, ordinal names and all supported static fields;
no SDK equality shortcut masks case or inactive-history differences. Full PUT
omission resets TTL to unlimited and disabled duplicate history to one minute.
Duplicate creation and unsupported definitions refuse without partial mutation.

Trusted owner seeds provide five real 256-KiB messages for below-retained-usage
limit refusals and real retained messages for lowering the future message limit
and clearing TTL. Their records, reservations and original deadlines survive.
These are SDK administration/limit-update gates. Separate
[ordinary ingress gates](finite-queue-capacity.md#official-net-ingress-gates)
cover SDK quota refusal, reservation recovery and message-size admission.
The complete committed image matches an independent native Memory oracle, and
Fjall is reopened only after original listener/broker/store ownership is released.
A separate initially empty namespace exercises SDK-parsed pages for 101 queues
and preserves the unrelated CRUD namespace. Losing a reply or an indeterminate
physical commit still provides no new retry or rollback guarantee.

Actual owned restore/dependency manifests resolve Azure.Core `1.62.0` with
Service Bus `7.21.0`, and Azure.Core `1.60.0` with Service Bus `7.20.2`. The two
loaded assembly Location-file SHA256 observations match the corresponding owned
output DLLs; assembly versions alone are not NuGet package identities, and file
hashes are not executable-memory-image proofs. Build and child execution reuse
the bounded original-process-group/pipe cleanup harness. Fixture failure, cleanup
failure and original panic remain failures, not success markers.

Verification: both new opt-in SDK gates closed successfully, each covering both
backends and constructors. The frozen SDK Rust target passed 38 regular cases
with no failures and fifteen ignored cases. All original thirty regular cases
and thirteen SDK case identities, statuses and ignore reasons were preserved;
the additions are eight regular support cases and two opt-in administration
gates, executed separately above. Both strict workspace lint configurations,
both all-target builds and formatting passed on the same twelve-path test-only
source, serially on CPUs 14,15 with two build jobs and the shared cache. The full
workspace and thirteen older SDK gates were not rerun for this increment.
No Rust dependency, production protocol, storage layout or CLI change is included.

### HTTPS Administration CLI

The server enables this endpoint only when `--atom-admin-listen` is supplied
together with `--atom-admin-audience-host`, `--atom-admin-key-name` and
`--atom-admin-key-file`. Configured TLS credentials are mandatory. There is no
default HTTPS listener, inline key, credential reread, legacy SAS/JWT fallback,
rotation or multiple-rule configuration. The dedicated policy contains exactly
one namespace-wide Manage rule. All other listener defaults and production
startup refusals remain unchanged.

The audience host is explicit and independent of the business namespace, bound
address, request Host and TLS SNI. HTTPS SAS uses effective port 443 even when
the listener uses another port. A dotless `--namespace` still has the existing
legacy AMQP/native SAS hostname mapping; enabling Atom does not change it.

For example, with an existing certificate valid for `localhost`:

```sh
switchyard --namespace development --storage fjall --data-dir ./data \
  --tls-certificate ./localhost.pem --tls-private-key ./localhost-key.pem \
  --atom-admin-listen 127.0.0.1:8443 --atom-admin-audience-host localhost \
  --atom-admin-key-name AtomManage --atom-admin-key-file ./atom-key.txt
```

This example enables only Atom authentication; configure the existing
shared-access options separately to protect AMQP/native listeners. The Atom key
file contains the literal UTF-8 HMAC key, not decoded Base64. Its raw contents,
including trailing newlines, are bounded to 8 KiB; only terminal CR/LF characters
are removed. Spaces are significant. Unix opens nonblocking, validates the
original descriptor as a regular file, then reads at most the cap plus one byte.
Ordinary regular-file symlinks are accepted; FIFOs, devices and directories are
refused. Non-Unix prechecking is not a universal special-file race guarantee.
New Atom configuration errors are static and omit paths, keys and arbitrary
I/O details; existing TLS/legacy credential diagnostics are unchanged.

Existing startup validation runs first, followed by Atom option completeness,
TLS, audience/name and key-file validation. `--check-config` reads configured
credentials but opens no storage or listeners. Disabled Atom performs no Atom
key-file I/O. Runtime uses the prepared policy and a cloned existing TLS
configuration, preserving AMQP ALPN. All configured sockets bind before accepting
connections; the Atom serve future stays in the existing top-level selection.
This adds no signal handler or graceful shutdown guarantee.

Four Linux actual-binary cases check disabled/valid preflight, occupied listener
addresses without binding, nonexistent storage parents without creation,
bounded FIFO/symlink refusal and one durable healthy server child. That child
establishes a successful strict private-CA/name-verified HTTPS control before
wrong-CA/name tests, creates a queue, refuses invalid or unconfigured credentials
without mutation, and preserves the definition across physical Fjall reopen.
The whole reopened logical image matches an independently constructed native
Memory oracle at the observed persisted command time. These CLI 401 probes use
unknown rules, not a configured SEND-only rule; that permission proof belongs to
the separate library/SDK administration fixtures.

The binary harness retains original child/process-group/pipe handles, bounds
normal probes and cleanup, and verifies successful kill, reap and output EOF.
Fallback Drop and panic cleanup do not independently prove all those outcomes;
the post-abort reader join has no separate deadline. A long explicit timer
interval prevents an initial sweep in this short fixture because the unchanged
timer waits before its first sweep. This does not certify production readiness,
corrupt-store health, Azure quota accounting or indeterminate commit rollback.

Verification: the closed full workspace passed 5,790 tests with no failures and
fifteen ignored SDK cases. All case identities, statuses and ignore reasons from
the preceding 5,757-test full run were preserved across the same 154 source
owners and 160 result groups. The 33 regular additions comprise eight already
published SDK support cases and 25 CLI cases: twenty configuration cases, four
actual-binary cases and one inherited diagnostic-redaction identity. The two
additional ignored administration gates were published and separately executed
in the preceding SDK increment. Doctest line locations alone were normalized,
with multiplicity preserved.

All 375 focused library, binary and SDK-harness regular cases passed, with the
same fifteen ignored cases. Both strict workspace lint configurations, both
all-target builds and formatting passed against the same nine-path CLI source,
serially on CPUs 14,15 with two build jobs and the shared cache. No official SDK
gate was rerun for this CLI increment. Cargo dependencies, the .NET project,
messaging protocol and durable layout 17 are unchanged.

### Message Content

Stored content has a protocol-neutral typed representation. Message and
correlation identifiers keep their AMQP types, including the distinction
between a missing identifier and an explicitly empty string or binary value.
Standard properties, application properties, message annotations and footer
entries survive receive, redelivery, deferral, scheduling and dead-lettering.
Data and sequence bodies retain every section, including empty sections; value
bodies retain nested containers, symbolic descriptors and scalar types. Empty
AMQP arrays are explicitly refused because the current value model cannot
retain their element constructor; heterogeneous arrays and duplicate map keys
are refused before storage. Application properties accept scalar values and
scalar described extensions, not compound containers. Float
and decimal values retain their raw bits. Delivery annotations are hop-local
and are consumed rather than forwarded.

Untrusted value parsing is bounded to 68 nesting levels, 132,096 value
nodes, and 4 MiB of copied binary/string/symbol data across a message. Shared
array descriptor names count once per expanded element, before allocation.
Stored message content is bounded separately to 64 levels and 65,536
nodes; the parser allowance covers envelope sections and metadata map keys.
Malformed sizes/counts, duplicate map keys and invalid symbols are refused.

Broker-owned sequence, enqueue time, state, lock and scheduling annotations
override producer values. Delivery count is broker-owned, and `first-acquirer`
is emitted as false rather than repeating an unverified producer assertion.
Dead-letter reason and description override those
application properties when draining the dead-letter queue, while the rest of
the application bag remains intact. Reserved dead-letter properties are cleared
from ordinary deliveries; exact Azure parity for that case is unverified.
The broker strips lifetime and session
from dead-letter deliveries. For active messages with finite lifetimes, the
creation/absolute-expiry timestamp pair is emitted as broker enqueue/deadline:
the official clients use that pair both to reconstruct long lifetimes and to
expose expiration. Producer creation time is otherwise retained, including in
dead letters. Pending schedules retain their effective lifetime before it
starts at activation.

These are semantic preservation guarantees, not byte-identical forwarding for
signed AMQP envelopes: map order and optional empty sections may normalize.
Duplicate detection still compares the text-normalized identifier, so distinct
AMQP identifier types with the same normalized text share history; exact Azure
behavior for that case is unverified. Rich messages enforce the configured
size limit using a conservative content tally that includes metadata and body
sections, rather than only flattened body bytes. Properties have a local 32 KiB
limit, and the header has a local 64 KiB limit, including standard properties,
application properties and message annotations. Accounting uses UTF-8 key bytes
plus conservative type/length/value overhead. New sends reserve 512 header bytes
for broker fields and ordinary dead-letter reasons; projected dead letters use
256 bytes for remaining broker fields. Footer content counts toward the message
limit, not the header limit. Settlement checks the merged property bag and
projected canonical dead-letter fields. These bounds implement the documented
quota sizes conservatively; exact Azure byte accounting and AMQP wire-size
parity remain unverified.

## Native Administration

The optional `--admin-listen` endpoint serves native queue/topic/subscription
create/get/list/update/delete through the broker owner. It is a
separate HTTP/2 listener and reuses the AMQP
TLS identity and configured SAS/offline JWT policies. Authenticated requests
require TLS and either a SAS token or, when an offline JWT policy is configured,
a Bearer token in `authorization` metadata. The credential must grant Manage
permission for the requested entity. Queue and topic listings need namespace Manage;
subscription listing needs Manage on its parent topic, so an exact-child grant
cannot enumerate siblings. The configured namespace is the only namespace
accessible through that endpoint. Dead-letter shadows cannot be administered.
Subscription definitions use their typed configuration rather than exposing
their backing queue through queue commands. Capacity and usage fields remain
absent from the legacy `EntityService` API. A separate `FiniteQueueService`
exposes the ordinary finite queue profile described below; unsupported usage
measurements are not reported as zero-byte measurements.

Library callers opt into offline JWT Manage authorization through
`NativeAdminService::with_offline_jwt_policy(policy, audience_host)`. It accepts
an `auth::JwtPolicy`, alone or alongside optional SAS. Default SAS-only and
unauthenticated development behavior is unchanged. Normal CLI startup calls this
builder for native `--admin-listen` when `--offline-jwt-policy-file` is configured,
using the already-loaded policy and the existing authentication resource host.
The CLI still requires configured TLS and SAS before opening the policy file;
it does not add a JWT-only startup mode.

In JWT-configured mode, each request must carry exactly one `authorization`
value, bounded to 8,199 encoded bytes. Bearer credentials require the exact
`Bearer ` prefix and a nonempty token of at most 8,192 bytes without ASCII
whitespace. Raw `SharedAccessSignature ` credentials select the original SAS
branch, which still needs a configured SAS policy; they never authorize a
JWT-only service. The single-header and encoded-value bounds also apply to that
SAS fallback. Default SAS behavior without the JWT opt-in is unchanged.

Bearer authorization requires Tonic's `TlsConnectInfo<TcpConnectInfo>` from the
completed TLS transport, not a TLS setting, URL or forwarded metadata.
In-process extension cloning is outside this transport trust boundary; the
extension is not an attestation API. A JWT-configured listener refuses startup
without TLS before accepting sockets. Each request reads a checked system epoch,
validates the token against its locally pinned policy, and requires local Manage
rights for the exact requested namespace or entity scope. Native validation keeps
literal binding paths rather than AMQP control-name aliases. Subscription bindings
use the native endpoint's canonical `/subscriptions/` spelling. The configured resource
host is separate from the JWT claim audience; signed roles and scopes do not
supply permissions. Authorization precedes substantive configuration, filter,
action and cursor validation and owner operations, not every path parse: existing
resource canonicalization and JWT resource-scope construction occur earlier.
Credential failures use static responses without echoing token bytes. This
policy opt-in adds no discovery, OIDC/cloud authorization, refresh or revocation.

Verification: the native JWT suite passed twelve regular cases on Memory and
Fjall, covering actual trusted TLS, namespace and entity Manage rights,
credential denials before owner access, SAS coexistence and plaintext/spoofed
transport refusals. A pre-fix literal-control-name regression failed on both
backends by wrongly creating the lowercase queue; both cases pass after native
validation preserves literal binding paths. Existing AMQP alias handling is
retained. The auth package passed 52 cases, including three new scope/profile
checks, and the AMQP JWT wire suite passed all eight cases.

The closed full workspace passed 5,457 tests with no failures and thirteen
ignored SDK cases, across 155 printed groups, 149 canonical owners and 139
executables. Every prior case name, status and ignore reason was preserved;
the additions are twelve native cases, three auth cases and the preceding
previous-SDK JWT gate. The separate SDK run passed all thirteen gates across
the two pinned versions. Both strict lint and build configurations and final formatting passed
with the shared cache and two-CPU policy. The earlier workspace attempt was
intentionally interrupted to include the reproduced scope fix; it is not a
completed gate. No CLI-native JWT wire claim is made by this library increment.
Dependency versions, schemas, value format 11 and durable layout 16 are unchanged.

Verification: the CLI activation increment passed two regular binary/helper
cases, three native setup cases and one borrowed-policy getter case. The actual
server binary ran twice over trusted TLS against the same Fjall directory.
Manage created the queue; after restart and removal of the loaded policy file,
Manage Get/List and the original SAS branch still worked. Send-only mutations
were denied, and physical reopen retained the exact independently constructed
business snapshot, including the command clock. Setup checks retained default
and SAS-only behavior, readiness opt-in and credential-before-owner ordering.
The native library and AMQP JWT wire suites again passed twelve and eight cases.

The closed full workspace passed 5,463 tests with no failures and thirteen
ignored SDK cases, across 156 printed groups, 150 canonical owners and 140
executables. Every preceding case name, status and ignore reason was retained;
six regular cases were added. Both strict lint and build configurations and
final formatting passed with the shared cache and two-CPU policy. The SDK gates
were not rerun for this CLI increment. The test-only process feature uses the
already-locked dependency; package versions, schemas, value format 11 and durable
layout 16 are unchanged.

The existing `EntityService` RPCs and field numbers are retained. Creation has
separate presence-aware queue, topic, and subscription configurations; wrong-kind
or mixed legacy settings are refused instead of ignored. Get returns the
committed entity kind and only its matching configuration. A subscription path
is canonicalized only at its structural `/subscriptions/` separator, preserving
literal parent and member spelling. Native SAS audiences use canonical entity
paths and remain case-sensitive; AMQP scope conversion is not applied here.
Read-only topology queries run on the owner without consulting the clock or
stamping commands, validate complete metadata before returning a result, and
refuse corrupt or dangling topology without partial responses.
The presence-aware subscription setting
`dead_lettering_on_filter_evaluation_exceptions` preserves an explicit false;
creation omission uses the default true, while update omission preserves the
committed value. Get/list and `switchyardctl` JSON report the
committed value. It is not a backing-queue setting.

List defaults to queues only, preserving existing queue clients and their `v1.`
tokens. Explicit topic and subscription kinds use separate tokens bound to the
namespace, entity kind, and subscription parent. Pages are ordered and exclusive,
with a default size of 100 and a maximum of 1,024. Topic discovery scans one
bounded index page; subscription listing validates all at most 32 members before
slicing a page. Queue listing hides subscription backings and all shadows. Each
request discovers at most 4,096 physical queue-index rows, counting backend
lookahead, over at most 16 owner discovery turns. These bounds apply to queue
discovery, not the subsequent metadata reads for returned entities. A request
that reaches either budget before exhaustion may return a partial or empty page
with an opaque `queue.scan.v1.` progress token. That token resumes exclusively
after the last consumed raw row, never after an unseen lookahead row; a deleted
marker remains a valid keyset position. Clients must continue while the token is
nonempty, regardless of the number of entities in the page. Ordinary visible
lookahead still emits the existing `v1.` token, whose decoding is unchanged.
All token families have a 512-byte ceiling and reject cross-context tokens before
owner reads. Topic, subscription, and queue-progress tokens also require
canonical base64 and protobuf encodings. A hidden path in a queue-progress token
is only a cursor position; it never authorizes or returns that entity through
queue listing.

Local defaults admit 128 sockets and 128 concurrent requests across service clones,
with at most 32 HTTP/2 streams per connection, a 10-second TLS handshake deadline,
30-second request deadlines, 64 KiB decoded requests, and 1 MiB encoded replies.
HTTP/2 connections send keepalive pings after 30 seconds, allow 10 seconds for an
acknowledgment, and retire after five minutes with a 30-second graceful deadline.
These are local resource policies. The cluster, namespace,
backup, and audit services return unimplemented
rather than simulated success.
This endpoint is not Azure Atom/XML administration compatibility.
An explicit development-only [maintenance clock query](development-maintenance-clock.md)
adds `MaintenanceService/GetClockReadiness` to this route only when enabled.
It observes one existing command-stamping check; its result may already be stale
and is not timer progress, storage health, whole-node or production readiness.
The additive `RuleService` serves create/get/list/delete on a canonical
subscription path with Manage authorization before filter/action parsing or store
access. It preserves exact scalar constructors and SQL source/version, uses an
in-flight subscription-incarnation fence, and returns complete sorted lists
under the existing 32-rule limit. Bounded REMOVE and literal SET actions use a separate
`CreateRuleWithAction` method; the original `CreateRule` remains action-free.
Get/List default to refusing action metadata unless `include_actions` is true,
so older clients do not silently receive incomplete definitions. Mutations are
synchronous, with no upserts, retry deduplication, or post-commit reread. It shares the entity service's
admission and transport bounds; see [Native Rule Administration](native-rules.md).
Native rule binding preserves literal parent/control-name bytes without AMQP
address parsing. SAS-authenticated paths retain the existing SAS grammar,
which excludes leading empty segments; no native-name or SAS normalization is
added by this API.
`switchyardctl queue create|get|list|update|delete` exposes these operations with JSON
responses and nonzero errors. Authorization headers are supplied only through
`--token-file`, from regular files bounded to 16 KiB. The CLI trims surrounding
whitespace and accepts the existing `SharedAccessSignature ` scheme or the exact
`Bearer ` prefix followed by a nonempty opaque ASCII token of at most 8,192 bytes
without internal whitespace. It does not decode JWTs or verify their signatures;
the server's configured policy remains the authority. Authorization metadata is
marked sensitive, including request clones, and token-file errors are static.
Token-bearing connections require HTTPS, checked before credential-file I/O;
TLS verifies against explicitly supplied CA certificates. Plaintext is opt-in,
loopback-only, and cannot carry a token. Command-line settings preserve omitted,
false, zero, and unlimited TTL.
`switchyardctl rule create|get|list|delete` addresses a topic/subscription member
and uses a bounded typed filter file plus an optional `--action-file` for creation.
An action selects only the new RPC, with no action-free retry on older servers.
Its JSON preserves scalar
constructors, exact floating-point bits and octets, and 64-bit values as strings.
It requests complete action metadata and validates whole replies before emitting
them; absent actions keep their original JSON shape. It does not add updates or
retries; see [Native Rule Administration](native-rules.md).

Entity deletion commits one bounded atomic purge, cascades topic-owned
subscriptions and shadows, and retains counter tombstones across recreation.
Missing targets return NotFound; oversized plans return ResourceExhausted with
all state unchanged. The successful native `Operation` is synchronous, not a
pollable job. Receiver wakeups, exact ownership validation, cleanup limits, and
live-link incarnation fencing are defined in
[Entity Deletion](entity-deletion.md). No Azure administration endpoint or
immediate global link-retirement parity is claimed.

Verification: the client Bearer increment passed four regular authorization
cases, three Linux binary/cleanup cases, and all 105 client-package tests. The
built `switchyardctl` exercised Manage-authorized queue CRUD, Send-only denials,
an invalid opaque credential, and SAS coexistence against the library native
listener over trusted TLS. Denials performed no owner reads or writes and left
the complete Memory business snapshot, including its clock, unchanged. This
fixture does not install the client, start the server CLI, or reopen Fjall;
the preceding server-CLI verification covers that separate path. Child cleanup
signals the original unreaped process group, waits for the original child, and
observes both output EOFs; it is not an all-descendant cleanup guarantee.
The closed full workspace passed 5,470 tests with no failures and thirteen
ignored cases, preserving every prior test name, status, and ignore reason.
Both strict workspace lint configurations, both build configurations, and
formatting passed. No SDK gates were rerun for this client-only increment.
The lockfile adds only the client's test edge to the already-locked
`futures-util`; package versions, durable layout, and value formats are unchanged.

### Native Finite Queue Administration

The existing native listener also registers `switchyard.admin.v1.FiniteQueueService`.
It exposes three separate unary methods: `CreateFiniteQueue`, `GetFiniteQueue`
and `SetFiniteQueueDefinition`. They share the existing Manage authorization,
configured namespace, SAS/offline JWT policy and 128-request admission pool
with legacy services. Authentication precedes definition conversion and owner
work. No separate listener, credential policy or CLI command is added here.
Plaintext remains an unauthenticated development option only under the existing
native listener policy; authenticated use requires TLS.

Create and update require a positive unsigned 64-bit `reservation_limit_bytes`
and a complete `QueueConfiguration`: all eight settings must be present,
including explicit false values and the TTL oneof. There are no creation
defaults or patch semantics in this service. Numeric settings are passed to the
owner unchanged after checked platform-width conversion. Required-session and
duplicate-detection definitions remain unsupported for finite queues.

Get does not stamp or consult the proposer command clock and returns the complete
definition, owner `generation`, reservation limit, `reserved_logical_bytes` and
`retained_message_count`. Authentication still checks credential expiry.
The aggregate covers the primary and its dead-letter shadow, not just ready
messages. It is Switchyard's logical reservation accounting, not disk usage,
Azure quota parity or a whole-ledger health certificate. A non-finite queue
returns `FailedPrecondition`, not invented zero-byte capacity measurements.

Update additionally requires the positive `expected_generation` returned by
the service. The owner fences that identity before consulting the host clock;
delete/recreate makes the old generation stale. This is an incarnation fence,
not a definition revision or compare-and-swap between concurrent updates.
Each mutation submits one existing owner operation and returns its prepared
view without a postcommit storage read. Full definition and limit changes
commit atomically; retained records, charges, usage and existing deadlines are
not rewritten. Equal definitions still validate the clock but commit nothing.
Injected pre-apply failures do not prove rollback after an indeterminate commit.

The legacy `EntityService` schema, defaults and partial-update behavior are
unchanged. A server registering only the current legacy Entity/Rule services
refuses these new method paths as `Unimplemented`, rather than silently ignoring
a limit on an older create method. This controlled registration is not proof
about every historical binary. The new service does not add list, delete,
in-place finite promotion/demotion, topics, subscriptions, migration or repair.

Verification: all 38 new regular cases passed: five handler units, one literal
protobuf wire case and 32 paired owner/actual HTTP2/TLS cases on Memory and Fjall.
They cover complete presence, prepared responses without postcommit reads,
exact config/limit/no-op batches, stale identities before the command clock,
retained records and usage, shared admission, private-CA/name refusals, SAS/JWT
denials and controlled legacy-only registration. Original counted-backend
discharge precedes physical Fjall reopen; Memory reopens a handle to its shared
keyspace, not a physical directory. Listener abort/join observations do not prove
termination of every Tonic connection task, and synchronous broker cleanup has
no public deadline or exposed join result.

The closed full workspace passed 5,844 tests with no failures and seventeen
unchanged ignored SDK gates. Every prior test name, status and ignore reason was
retained. The focused targets passed all 312 cases. Both strict workspace lint
configurations, both builds and formatting passed against the same frozen
source, using the shared cache and CPUs 14,15. The official SDK gates were not
rerun for this native-only increment. The schema extension is additive;
dependencies, CLI commands, value formats and durable layout 17 are unchanged.

### Native Finite Queue CLI

`switchyardctl finite-queue` exposes `create`, `get` and `set-definition` through
the separate native service. Create and set require every queue configuration
setting, an explicit TTL and positive `--reservation-limit-bytes`. Set also
requires positive `--expected-generation` from a prior finite response. Missing
shape or identity fields are refused before credential-file reads or connection;
explicit numeric zero and false configuration values are forwarded for owner
validation. These are full definitions, not legacy `queue update` patches.

Each prepared operation invokes one finite client RPC. There is no implicit Get, generation
refresh, retry, default merging or fallback to legacy methods. A successful
response must match the requested namespace/path, include complete configuration
and positive generation/limit, and retain the caller generation for set. JSON
reports `namespace`, `path`, `generation`, `config`, `reservation_limit_bytes`,
`reserved_logical_bytes` and `retained_message_count`; unsigned values retain
their full 64-bit range. Unlimited TTL uses the existing null output convention
only after required TTL presence is checked. These aggregates remain owner
observations, not whole-ledger health or physical/Azure capacity certificates.

The commands reuse existing CA/name-checked TLS, sensitive token-file metadata,
request/response limits, timeouts and static status-only errors. Development
plaintext still requires explicit loopback opt-in and cannot carry credentials.
Legacy queue commands and output are unchanged. The `compatibility` JSON adds
only `finite_queue_operations`; all prior operation arrays retain their values.
No finite list/delete, in-place promotion/demotion or limit-only command is added.

Verification: all 119 client-package tests passed, retaining all 105 preceding
case names, outcomes and details. The fourteen additions are six units, six new
Linux binary scenarios and two unchanged process-helper cases under the new
target. The built client exercised private-CA TLS, JWT Manage/SAS success,
credential and name/root denials, full replacement, unsigned JSON, exact no-ops,
retained usage and below-usage refusal, stale incarnation before a backward
command clock, and controlled legacy-only `Unimplemented` without fallback.
The separately replayed seed uses the same domain implementation, not an
independent charge oracle. This client fixture uses Memory only, with no Fjall
reopen, server-CLI startup, historical-binary or official SDK claim.

Original-child group/wait/pipe observations and listener abort/join observations
do not prove every descendant or HTTP2 task terminated. The reused reader
post-abort join and synchronous broker cleanup have no complete public deadline.
Both strict workspace lint configurations, both builds and formatting passed
against the final source with the shared cache and two-CPU policy. The full
workspace and SDK gates were not rerun for this client-only increment; the
preceding 5,844-test native run is separate evidence. Server/domain sources,
dependencies, protobuf, value formats and durable layout 17 are unchanged.

### Configuration Updates

Updates use independent presence-aware patch fields: queue remains protobuf
tag 3, with topic and subscription appended at tags 4 and 5. Exactly one family
is required, and it must match the target kind. Authorization precedes substantive
patch validation and owner access. Omitted settings stay unchanged; explicit false and
unlimited lifetime are distinct from omission. Empty or equal patches stage
nothing and do not advance the applied clock, but still validate topology and
pass the ordinary proposer clock check.

Topics allow changes to default lifetime, message size, and duplicate-history
window; subscriptions allow lock duration, delivery count, default lifetime,
message size, and both dead-letter policies. Topic duplicate-detection enablement
and subscription session enablement are creation-only, consistent with the
documented [duplicate-detection](https://learn.microsoft.com/en-us/azure/service-bus-messaging/enable-duplicate-detection)
and [session](https://learn.microsoft.com/en-us/azure/service-bus-messaging/enable-message-sessions)
constraints. Restating the same value is allowed; a change returns native
`FailedPrecondition` without committing any part of the patch.

A topic update validates complete bounded membership and changes one metadata
key. A subscription update validates its target and parent, then commits
membership, backing queue, and shadow configurations together. Neither repairs
corruption, compiles rules, or scans and rewrites messages, indexes, counters,
sessions, locks, or duplicate history. Existing deadlines remain captured.
New receive/renew operations use the current lock default; later expiry,
delivery-count decisions, and SQL error routing use current policy. Existing
dead letters are not moved or changed. Changing a history window affects newly
accepted IDs, not existing history deadlines.

A pending topic publication retains its admission-clamped topic lifetime, so
raising that default cannot extend it. Activation applies any shorter current
topic ceiling and the current subscription ceiling. A raised subscription
ceiling can therefore change a future copy's lifetime at activation, but not
an existing copy. New size ceilings apply at activation too; a failed activation
leaves the original schedule pending and cancelable. Already-attached AMQP
producer links keep their advertised maximum size until reopened. These are
local update policies, not cloud-verified timing guarantees. The value format
and store layout remain unchanged by configuration updates.

`switchyardctl topic update` and `subscription update` expose the same partial
patches, scoped credentials, typed JSON responses, and nonzero refusal behavior
as `queue update`.

## Durable Format

The current value envelope remains version 11; the active durable base layout
is version 17. Isolated replicas derive `0x80000011`, catalog replicas derive
`0xc0000011`, and protected publication derives `0xd0000011` from that same
`ACTIVE_STORE_FORMAT`. Their exact profile-v1 tags are unchanged. Existing v16
and older directories in every derived namespace are refused; older builds
likewise refuse new v17 directories. A nonempty unversioned ordinary directory
is refused before a marker is written. Mandatory capacity-mode metadata and
finite reservation sidecars are protected by this layout; see
[Finite Queue Capacity](finite-queue-capacity.md).
No automatic relabeling, repair, migration or rollback conversion is provided.

Replica profiles have an initialized flag updated with each privileged batch;
ordinary open refuses those profiles, and replica open does not adopt standalone
directories. The bounded committed progress record has its own version-1 envelope,
not the ordinary message-value codec. Separate [catalog](snapshot-catalog-storage.md)
and [protected](durable-protected-publication.md) APIs remain explicit selections.
The private paired-storage fixture also derives its State/Log numbers from the
active base; that is not a production creation or migration API. There is no
standalone-to-replica migration or production runtime replication; see
[Committed Queue Apply](committed-queue-apply.md).

Value format 11 appends optional source-only [SQL actions](sql-actions.md) with
stored semantic version 1 or 2; legacy rule definitions decode with no action.
New and unversioned actions default to 2, while explicit and stored version 1
retain REMOVE-only interpretation. Compilation dispatches on that exact stored
version, so a version-1 SET source never becomes executable after reopen. Maximum-sized
old rules are validated and charged by their actual stored envelopes, not a
larger rewritten shape. Relabeling the new rule shape as an older format is
refused. Value format 10 introduced the subscription filter-error policy and a
source-only SQL filter variant with semantic version 1. Legacy subscription
configurations decode with the default true; relabeling the new shape as an older
value format is refused. SQL rules likewise cannot be relabeled as pre-version-10 records.
Message and queue shapes are unchanged, and existing reason tags stay intact;
the missing-session reason retains its version-9 rollback guard.
The layout also requires retained [entity incarnations](entity-incarnations.md),
which prevent old admitted endpoints from addressing recreated names. The
layout protects SQL filter/action interpretation and the subscription policy,
in addition to explicit rules, session-bearing ordinary subscription indexes,
parent-retained topic schedules, and session-message-lock ownership indexes.
An older build could otherwise decode the wrong configuration shape, fail to
interpret SQL, or route publications under an older contract.
Earlier message and queue-configuration shapes have tested decoders, but an
earlier store directory is refused at open because its broker contract differs.
There is no directory migration tooling yet; development directories
from older builds must be recreated. A rollback likewise refuses a newer layout.

The same semantics suite runs against both backends. Fjall fsyncs a command's
batch before reporting it applied and retains local messages, locks, delivery
counts and sequence numbers across process restarts. Memory retains them only
while its shared in-process state remains live; reopening a test handle is not
disk or process recovery. Preserving acknowledged state across the loss of a
node still requires production replication, which is not integrated.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
message validation, including a default 256 KiB content-size limit, while
allowing operators to configure larger limits. Full wire quota parity is not
yet claimed.
