# Compatibility

Switchyard targets Azure Service Bus Standard. This matrix describes single-node
`main`, not reference-branch capabilities. Core tests establish behavior;
protocol/client gates establish the surface. Neither is certification.

## Client Gates

| Client | Available gate | Qualification |
| --- | --- | --- |
| Official .NET, declared-current 7.21.0 / Core 1.62.0 | Opt-in TCP and WSS data-plane workflows | Exact locked graph and approved runtime bytes; experimental Memory coverage |
| Official .NET, previous 7.20.2 / Core 1.60.0 | Same workflows, isolated package/build roots | Exact fixed selector; not durable or administration certification |
| Pinned Sift | Planned | No gate implemented |
| Rust AMQP client | Protocol end-to-end suites | Broader protocol checks; not a substitute for official SDK gates |

The .NET workflows cover queue/batch send, prefetch and independent
settlement, receive-delete, envelope fidelity, renew/abandon/defer/peek, DLQ,
queue sessions/state, duplicate detection, queue/topic scheduling and
cancellation, filtered topic/subscription delivery, rule management, and
case-insensitive addressing. Selected gates require Linux, .NET 10 and NuGet
restore; missing prerequisites fail. Ordinary workspace tests ignore them.
Both selectors use exact ranges and checked-in lockfiles; declared-current is
a fixed choice, not a latest-release claim. Selected ServiceBus/Core package,
output and nonce-bound loaded-file hashes must match checked-in approvals.

TCP uses a permissive certificate callback for a generated test identity. WSS
uses the platform trust path with a generated root scoped to the child process
through `SSL_CERT_FILE`, not the TCP callback or a machine-wide trust change.
Nonce-bound loaded-assembly records match launched file hashes; completion
follows async disposal. Neither fixture certifies production trust, durable
SDK or whole-test task-tree shutdown. See [gate commands and limits](sdk-gates.md),
[TCP gate](../crates/server/tests/amqp_dotnet_current.rs),
[WSS gate](../crates/server/tests/amqp_dotnet_websockets.rs), and
[declared project](../crates/conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj).

## Implemented Surface

| Area | Coverage and limit |
| --- | --- |
| Transport | AMQP 1.0 TCP/TLS and WSS binary tunnel at `/$servicebus/websocket` (`AMQPWSB10` or `amqp`); plaintext only in development |
| Native lifecycle | Accepted connection owns its original driver/reader tasks; sticky stop bypasses command capacity; joined shutdown retains both results |
| Authentication | TLS before AMQP; SASL PLAIN or ANONYMOUS/`MSSBCBS` then CBS SAS; namespace/entity Send, Listen, Manage grants |
| Queue delivery | Singular/atomic batch send, peek-lock, receive-delete, independent out-of-order settlement, bounded prefetch/credit drain |
| Envelope | Durable encoded AMQP body/forms, identifiers, properties, annotations, application properties, footer; broker-authoritative delivery overlays |
| Message management | Renew, abandon, defer/retrieve, ordered peek, schedule/cancel, custom dead-letter, DLQ receive/complete |
| Queue sessions | Named/next-available attach filter, exclusive locks, renewal, opaque state, release on link close |
| Queue duplicates | Exact non-empty message ID suppression across immediate, batch, and scheduled sends; bounded history cleanup |
| Topics/subscriptions | Non-session immediate/scheduled singular/batch fanout, topic placeholder peek/cancel, independent subscription queue lifecycle |
| Rules | Durable `$Default`, actionless true/false/correlation, typed equality; create/delete/paginated AMQP rule management |
| Time | Bounded runtime activation plus lock, TTL, session-lock, and duplicate-history expiry |
| Identity | ASCII-folded namespace/entity/subscription addressing and SAS scope; session/placement IDs retain case |
| Offline identity policy | Pure local RS256 JWT verification with pinned public keys, injected time and local rights; no JWT transport activation |
| Persistence | Paired memory/Fjall semantics; Fjall journal fsync before applied outcome and single-directory ownership |

Manage includes Send and Listen. Scheduling/cancellation require Send;
receive-side management requires Listen. CBS grants authorize links
connection-wide and close them on token expiry; a connection has 20 seconds to
finish initial CBS authorization. Receiving settle mode selects peek-lock
(unsettled) or receive-delete (pre-settled). Session attach echoes the granted
identifier/deadline; renewal and state use `$management`, not automatic link
renewal. Protocol rejections carry compatible conditions/retry hints rather
than silently falling back to unsupported behavior.
Outbound receive reserves remote credit before broker mutation; an empty result
releases it. Drain completes after reservations are consumed or released.

The [offline JWT API](offline-jwt.md) produces issuer-qualified grants; SAS/PLAIN
principals are qualified by their verified namespace host. Protocol refresh and
time consumers still await [#71](https://github.com/DeandreT/switchyard/issues/71);
JWT CBS activation remains [#17](https://github.com/DeandreT/switchyard/issues/17).
Existing SAS/PLAIN authentication and resource-scope behavior are unchanged.

The [SQL predicate kernel](sql-predicates.md) is a bounded, ephemeral typed domain
API only. Its local grammar/error profile does not enable persisted SQL rules,
subscription fanout or actions; #96/#97 extend it before #20 integration.

## Known Differences And Bounds

| Behavior | Current contract |
| --- | --- |
| Session ID on ordinary queue | Refused; Azure accepts and ignores it. Only session queues promise FIFO. |
| Session message settlement | The live message lock token/deadline is authoritative even after session-lock loss; Azure also requires session ownership. |
| Next available session | Examines at most 32 candidates; all held returns none available and the receiver retries, not a full walk. |
| TTL | Always dead-letters as `TTLExpiredException`; Azure's configurable expiration dead-letter policy defaults off. Live locks remain settleable until lock expiry. |
| Duplicate sequence gaps | Suppressed requests consume acknowledgement sequence slots but store no message. History starts at command time, duplicate hits do not extend it, and settlement/cancellation/expiry/DLQ do not erase it. Missing raw-AMQP IDs bypass detection. |
| Scheduled topic snapshot | Child/rule snapshot occurs at activation, not send; newly created subscriptions can receive an earlier scheduled publication. All-scheduled sends do not prove child topology early. |
| Browse and deferred limits | Peek accepts positive signed counts but caps replies at 250; scans to that cap or true end. Deferred retrieval accepts at most 32 sequence numbers atomically in caller order. |
| Service limits | 2,000 subscriptions/topic and separate topic/subscription name bounds; default Standard wire-message limit 256 KiB. Commercial namespace/operations quotas are not reproduced. |

### Atomicity And Lifecycle

Each command derives deadlines from its carried timestamp and commits one
storage batch. Rejection changes no messages, counters, duplicate history, or
Clock. Batch validation precedes allocation/history mutation; session batches
require one session. Duplicate detection is send-side suppression, not
exactly-once receive: peek-lock still permits redelivery.

Peek-lock commits ownership before transfer and deletion only on settlement;
receive-delete commits deletion first. A live lock can renew without token
change. Abandon/lock expiry redelivers until maximum delivery count, then sends
the message to DLQ as `MaxDeliveryCountExceeded`. Deferral hides a message from
ordinary receive; abandon/expiry of a deferred delivery restores that deferred
state. Management and delivery-link property updates persist in the envelope.
Peek never locks or increments delivery count; a non-session receiver can browse
across sessions, while a held-session receiver sees only its session.

Scheduling stores a browseable, non-receivable placeholder that does not make a
session available. Activation retires it, allocates an active sequence/enqueue
time, and starts TTL. For topics, evaluation, every matching subscription copy,
and the topic counter are atomic. Rules OR together; populated correlation
fields AND together with exact types; several matches yield one copy. No matches
still succeeds. Each copy settles, expires, defers, browses, and dead-letters
independently. Full payload copies, not shared payload references, are stored.

`entity/$deadletterqueue` is a reserved real queue: no direct create/send and no
shadow-of-a-shadow. DLQ messages retain sequence and reason, lose lifetime/session,
and never dead-letter again. Delivered reasons use `DeadLetterReason` and
`DeadLetterErrorDescription`; direct DLQ drain has no `DeadLetterSource` (Azure
uses it for forwarded dead letters). DLQ resubmission/auto-forwarding is absent.

### Topic Integrity

Routing and public subscription pages validate the complete listed graph before
taking a prefix or committing copies. Keys and stored values must name the exact
canonical child; each backing queue must match its valid parent-derived receive
profile. Its real DLQ must match that profile with `u32::MAX` delivery count,
no TTL/session/duplicate detection, and neither may have a conflicting topic
record. The bounded scan uses 2,001 entries to enforce the 2,000 cap. Corruption
refuses atomically as a broker fault; small pages cannot hide a later bad member.

Topic creation refuses orphan membership; subscription creation refuses an
occupied DLQ rather than overwriting it. The proof excludes unindexed backing
queues, orphan rules, and retained runtime; it does not newly fence ordinary
retained-message/rule management or establish live incarnations/capacity. It
changes no storage/value/wire format. See
[routing implementation](../crates/domain/src/machine/topic.rs).

### Storage And Runtime Limits

Fjall fsyncs before returning applied state and preserves messages, locks,
delivery counts, sessions, and sequence numbers across restart. It does not
preserve them after loss of the only node; replication is absent. Memory is
volatile. Production durable startup refuses with a static replication-unavailable
error before opening storage or listening. Development remains single-node;
production memory keeps its existing refusal. CLI TLS/auth/storage-argument and
cluster-validation error precedence is unchanged.

A missing format marker is stamped only when known `meta` and `records`
keyspaces are empty. Existing rows, even empty-valued ones, refuse opening
without a marker write. Active store format 2 requires private live owner heads;
format 1 directories refuse, even when empty. Value envelopes remain V1. There
is no migration, foreign-keyspace proof, or filesystem-byte invariance claim.
See [opener](../crates/storage/src/durable.rs).

Present configurations and listed topic profiles require canonical live owner
heads; DLQs share their owner's head. Bound broker calls capture immutable
namespace/target/owner/kind/generation through the owner queue. They recheck the
live head, forbidden shadow head, generation and existing target profile before
host or stored Clock, including Complete and session-state/release commands.
Malformed heads are corruption; same-kind generation drift or a vanished target
is stale. Wrapper scope mismatch precedes these reads. This is not a whole-store
or parent-membership proof, live deletion, or a global mixed-corruption priority.
Wire links, legacy name calls, timers and raw catalog/diagnostic reads remain
outside retained-authority protection; #57-#60 will adopt it at the edge.

Native `stop` interrupts driver IO/channel work; `shutdown` joins the original
driver and reader, retaining results for cancellation-safe retry. Drop requests
stop only. Join success does not acknowledge AMQP Close: `close` and
`close_with_error` still require the peer reply. Sender capacity waits observe
detach and command closure, even when callers retain all 256 confirmation permits.
Queued credit-grant replies own cleanup before observation; dropping an accepted
reply queues cleanup for its exact reservation, not a replacement link's credit.

Receiving-pump teardown joins its original settlement workers (at most 32)
before residual route cleanup and session release. Retirement stops unanswered
remote/confirmation waits, not started broker submissions; ready outcomes still
apply. Second-mode success follows durable settlement. Cancelled finish observers
retain original handles/results. Drop only requests retirement and detaches.

The pump also retains one original Receive and its reserved credit through
natural teardown. Never-polled work is discarded; a polled attempt is drained
and cached before residual route/session cleanup. Late results cause no Transfer
or implicit settlement: PeekLock waits for expiry; ReceiveAndDelete can be lost.
Cancelled borrowed observers retain custody, not an aborted parent task.

Once native transfer starts, teardown retains its original future, Delivery and
reservation. Intake, transfer and workers retire before drains; a late Pending
joins the same retired worker owner before its first finish. Ready outcomes use
existing authorization/settlement rules before route/session cleanup. Auth
retirement does not roll back an already-started Transfer or guarantee progress
behind blocked native I/O.

Entity attaches retain original native acceptance and any session grant through
borrowed observers. Observed End discards only never-polled phases; polled
originals are observed and cached. An unused hold gets one release attempt using
its captured entity/full hold; refusal leaves ordinary expiry. Receiving entity
attaches claim registry ownership before their first helper await. Installation
checks the latest claim and observed original End/Detach after write-lock
admission; stale cleanup matches the captured owner, entity and full hold.
Failed newest claims preserve installed rows without reviving older pending work.
This is not atomic link/hold liveness or ancestor-task shielding (#75).

Inbound links retain one original Send/Batch result through borrowed observers
and natural Detach/auth retirement. Never-polled retired work submits nothing;
begun work drains without a replacement or new late acknowledgement. A committed
send is not rolled back, and cancellation of a queued wire acknowledgement is
not proof it was unsent. Whole-task abortion remains #75.
Receive and session-grant callers defer broker method invocation to the retained
original's first poll, including eager adapters. This is not an enqueue receipt.

Management retirement discards preparation before broker invocation, but drains
begun commands and their post-result registry work. Original native acknowledgements,
replies and begun confirmations remain owned; no new late reply/confirmation starts.
Every returned reply exit closes its captured channel and unregisters by identity.
Original native errors precede cleanup errors; blocked writers still need joined
native stop. CBS (#86), task trees (#75) and process shutdown remain
[roadmap work](roadmap.md#next-main-increments); stalled broker/I/O can delay cleanup.

Sustained inbound traffic beyond initial credit is not certified. Session
transfer-window accounting and receiving-credit refill remain
[#68](https://github.com/DeandreT/switchyard/issues/68)/[#69](https://github.com/DeandreT/switchyard/issues/69).
Native Flow-echo replies remain unimplemented ([#77](https://github.com/DeandreT/switchyard/issues/77)).

Timer commands, not local wall-clock mutation, drive activation/expiry. Small
host-clock regressions hold command time still; large ones refuse and retry,
without a readiness signal yet. One server serves one namespace. Namespace
capacity quotas, fair multi-tenant scheduling, and production durability are
not implemented.

## Not Implemented

| Scope | Missing work |
| --- | --- |
| Service Bus semantics | General SQL filters/actions, session subscriptions/session-ID predicates, topic duplicates, configurable TTL policy, DLQ resubmit/forwarding, same-group transactions |
| Administration | Atom/XML entity/rule administration and working native gRPC/CLI transport (contract only) |
| Identity/security | JWT grant-consumer integration and wire activation, network OIDC discovery, mTLS, policy administration, full RBAC, per-namespace encryption/KMS, tamper-evident audit |
| Production runtime | Raft/placement, hard quotas/fairness, encrypted backup/restore, readiness/observability, release/performance evidence |

Partitioned entities and cross-placement-group transactions are later scope;
geo-replication and Premium features are not initial commitments. Track focused
work and dependencies in the [roadmap](roadmap.md), with the target design in
[ARCHITECTURE.md](../ARCHITECTURE.md).
