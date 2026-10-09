# Compatibility

Switchyard targets Azure Service Bus Standard. This matrix describes single-node
`main`, not reference branches. Core tests establish behavior and protocol/client
gates establish the surface; neither certifies compatibility.

## Client Gates

| Client | Available gate | Qualification |
| --- | --- | --- |
| Official .NET, declared-current 7.21.0 / Core 1.62.0 | Opt-in TCP and WSS data-plane workflows | Exact locked graph and approved runtime bytes; experimental Memory coverage |
| Official .NET, previous 7.20.2 / Core 1.60.0 | Same workflows, isolated package/build roots | Exact fixed selector; not durable or administration certification |
| Pinned Sift | Planned | No gate implemented |
| Rust AMQP client | Protocol end-to-end suites | Broader protocol checks; not a substitute for official SDK gates |

Four experimental Memory gates cover two pins over TCP/WSS: queue/batch send,
prefetch/settlement, receive-delete, envelopes, renew/abandon/defer/peek, DLQ,
sessions/state, duplicates, scheduling/cancellation, filtered subscriptions, rules
and case-insensitive addresses. Linux, .NET 10 and NuGet restore are required;
missing prerequisites fail. Workspace tests ignore these workflows; durable
coverage remains pending.

Exact selectors/lockfiles and approved ServiceBus/Core, output and nonce-bound
loaded-file hashes are required; completion follows async disposal. TCP uses a
permissive generated-identity callback; WSS uses platform trust with a child-scoped
generated root via `SSL_CERT_FILE`, not that callback or machine-wide changes.
Neither certifies production trust, administration or whole-test task-tree shutdown.
See [gate commands and limits](sdk-gates.md),
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
| Queue profiles | Domain-only partial ordinary-queue updates; unchanged patches write nothing; existing records/deadlines and owner identity stay intact |
| Topics/subscriptions | Non-session immediate/scheduled singular/batch fanout, topic placeholder peek/cancel, independent subscription queue lifecycle |
| Rules | Durable `$Default`, actionless true/false/correlation, typed equality; create/delete/paginated AMQP rule management |
| Time | Bounded runtime activation plus lock, TTL, session-lock, and duplicate-history expiry |
| Identity | ASCII-folded namespace/entity/subscription addressing and SAS scope; session/placement IDs retain case |
| Offline identity policy | Pure local RS256 JWT verification with pinned public keys, injected time and local rights; no JWT transport activation |
| Persistence | Paired memory/Fjall semantics; Fjall journal fsync before applied outcome and single-directory ownership |

Manage includes Send/Listen. Scheduling/cancellation require Send; receive-side
management requires Listen. CBS authorizes links connection-wide, closes them on
expiry and allows 20 seconds for initial authorization. Unsettled receiving means
peek-lock; pre-settled means receive-delete. Session attach echoes identifier/deadline;
renewal/state use `$management`, not automatic renewal. Unsupported requests reject
with compatible conditions/retry hints. Outbound receive reserves credit before
broker mutation, releases empty results, and completes drain after consumption/release.

Queue profile updates validate the parent and exact DLQ before atomically changing
only their profiles and Clock. Session/duplicate enablement is immutable; lock,
TTL, message-size and history changes govern future work, not existing deadlines.
Topic/subscription propagation, administration setters and capacity accounting
remain pending.

The [offline JWT API](offline-jwt.md) produces issuer-qualified grants;
SAS/PLAIN principals retain verified namespace-host qualification. Protocol
refresh/time consumers [#71](https://github.com/DeandreT/switchyard/issues/71) and
JWT CBS activation [#17](https://github.com/DeandreT/switchyard/issues/17) remain pending.
The merged [SQL predicate kernel](sql-predicates.md), including #96/#97 IN/LIKE
and scalar arithmetic, is a bounded ephemeral domain API with an explicitly local
grammar/error profile. Typed content #16 and rule integration #20/#21 remain
pending; it enables no persisted SQL rules, fanout or actions.

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

Each command uses its carried timestamp and commits one storage batch. Rejection
changes no messages, counters, duplicate history or Clock. Validation precedes
batch allocation/history mutation; session batches require one session. Duplicate
detection suppresses sends, not peek-lock redelivery.

Peek-lock commits ownership before transfer and deletes only on settlement;
receive-delete deletes first. Renewal preserves a live token. Abandon/lock expiry
redelivers until maximum delivery count, then DLQs as `MaxDeliveryCountExceeded`. Deferral hides
ordinary receive; deferred-delivery abandon/expiry restores deferral. Management
and delivery-link property updates persist in the envelope. Peek never locks or
increments delivery count; ordinary receivers browse across sessions, held-session
receivers only within theirs.

Scheduled placeholders are browseable, non-receivable and do not make sessions
available. Activation retires them, allocates active sequence/enqueue time and
starts TTL. Topic evaluation, matching copies and counter updates are atomic:
rules OR, populated correlation fields AND with exact types, several matches yield
one copy, no matches succeeds. Full-payload copies settle, expire, defer, browse
and dead-letter independently.

`entity/$deadletterqueue` is a reserved real queue: no direct create/send or
shadow-of-shadow.
DLQ messages retain sequence/reason, lose lifetime/session and never dead-letter
again. Reasons use `DeadLetterReason`/`DeadLetterErrorDescription`; direct drain
omits `DeadLetterSource` (Azure uses it for forwarded dead letters). Resubmission
and auto-forwarding are absent.

### Topic Integrity

Routing/subscription pages validate the full listed graph before prefixes/copies:
keys/values name the exact canonical child; backing queues match valid parent-derived
receive profiles; real DLQs use that profile with `u32::MAX` delivery count and no
TTL/session/duplicate detection. Neither may conflict with a topic record. The
2,001-entry scan enforces the 2,000 cap. Corruption refuses atomically as a broker fault,
including bad members beyond small pages.

Topic creation refuses orphan membership; subscription creation refuses occupied
DLQs. Proof excludes unindexed backing queues, orphan rules and retained runtime;
it establishes no new retained-message/rule fence, live incarnations or capacity.
Formats are unchanged. See [routing implementation](../crates/domain/src/machine/topic.rs).

### Storage And Runtime Limits

Fjall fsyncs before applied outcomes and preserves messages, locks, delivery counts,
sessions and sequences across restart, not loss of the only node. Memory is volatile;
replication is absent. Production durable startup returns static replication-unavailable
before storage/listeners; production memory still refuses. Development is single-node.
CLI TLS/auth/storage/cluster-validation error precedence is unchanged.

A missing marker is stamped only with empty known `meta`/`records` keyspaces.
Any row, including empty-valued, refuses opening without a marker write. Store
format 2 requires private live owner heads; format 1 refuses even empty. Value
envelopes remain V1; no migration, foreign-keyspace proof or byte-invariance claim.
See [opener](../crates/storage/src/durable.rs).

Configurations/listed topic profiles require canonical live heads; DLQs share their
owner's head. Bound calls capture immutable namespace/target/owner/kind/generation through the
owner queue and recheck live/forbidden shadow heads, generation and target profile
before host/stored Clock, including Complete and session-state/release. Malformed
heads are corruption; same-kind generation drift/missing targets are stale. Wrapper
scope mismatch comes first. No whole-store/parent-membership proof, live deletion or
global mixed-corruption priority is claimed. Wire links, legacy names, timers and
raw catalog/diagnostic reads await #57-#60 retained-authority adoption.

Current lifecycle contracts are scoped to these owners, not whole task trees:

| Owner | Implemented Contract |
| --- | --- |
| Native connection | `stop` interrupts driver IO/channel work without command capacity; `shutdown` joins the original driver/reader and caches results for cancelled/repeated observers. Outer-pump panic retains original acceptance/Close packets and shutdown results; retirement discards unstarted work and starts no fresh Close. Drop requests Stop only; joins do not acknowledge Close, whose waiters still require the peer reply. |
| Native sender/credit | Capacity waits observe Detach/command closure even with all 256 confirmation permits retained. Queued credit-grant replies own cleanup before observation; dropping an accepted reply cleans its exact reservation, not replacement credit. |
| Receiving pump | Natural teardown/outer-pump panic retain one Receive/credit, begun Transfer/Delivery/reservation and at most 32 original settlement workers. Retire intake/native/workers before drains; late Pending joins the same retired worker. Ready outcomes use existing auth/settlement rules; second-mode success follows durable settlement. Unanswered remote/confirmation waits retire; begun broker submissions drain. Cleanup drains originals, conditionally removes registrations, then observes one lazy original session release. Drop retires/detaches only. |
| Attachment/session registry | Natural retirement/outer-pump panic retain original grants, native acceptance and ready packets through receiving-entry preparation and move-only adoption. Cancelled cleanup keeps exact unregister and one lazy captured-entity/full-hold release; refusal leaves expiry. Claim before the first helper await; installation rechecks latest claim and original End/Detach after row-lock admission. Failed newest claims preserve installed rows without reviving older work. No atomic link/hold liveness or successor task-family custody. |
| Inbound Send/Batch | Natural retirement/per-delivery pump panic retain one original command, raw result and native Accept/Reject/Unauthorized Close until cleanup. Selected Detach/auth exits keep late native results benign and suppress reporting-only panics; genuinely panicked drain originals remain faults. No replacement, rollback or new late acknowledgement; cancelling a queued acknowledgement does not prove it was unsent. |
| Management | Natural retirement/outer-pump panic discard pre-invocation preparation but retain begun commands/post-result registry work and original native acknowledgements/replies/confirmations. Cancelled cleanup keeps both reply/Close outcomes and captured-route identity. No new late reply/confirmation. |
| CBS | Natural retirement/outer-pump panic retain original token validation/store, native work and completed packets through captured-route cleanup. No installed-grant rollback, selected-route retry, new acknowledgement or second confirmation. Bootstrap needs no existing grant. |

Admitted-work rules: discard never-polled retired phases; drain/cache begun originals
without resubmission. Cancelled borrowed finish retains phases, handles and raw
results. Begun connection acceptance may still hand off a Session after retirement;
#133 owns session-family custody. Receive/session-grant broker invocation starts inside the retained
original's first poll, including eager adapters; this is not an enqueue receipt.
Management/CBS returned reply exits close captured channels and identity-unregister;
original native errors precede cleanup errors. Connection, management, CBS and attachment primary faults/native errors
also precede secondary diagnostic failures; native attachment Detach remains benign.
Late Receive results cause no Transfer or implicit settlement: PeekLock waits for
expiry; ReceiveAndDelete can be lost. Auth retirement does not roll back a begun
Transfer. Merged connection/receiving/Send/management/CBS/attachment panic custody terminal-marks a panicked original
without repoll, retry or fabricated success; accepted work is not recovered.

Captured delivery identities fence worker/residual cleanup and management
renewal/disposition writes under the row lock; equal-value/cross-entity replacements
survive. Cancelled residual cleanup retains unfinished handles until removal
completes. Lookup preference, TTL purge and delayed-install ordering are unchanged.
Session cleanup matches captured owner/entity/full hold. Original End observes
End/Stop/driver panic during attachment auth/registry preparation, even with its
row held; readiness proves neither answering End nor native joins. Receiving auth
preparation observes captured Detach before Receive.

Caught primary faults in all six spawned data/CBS/management leaves request the
captured connection's latched retirement and native Stop before original drains.
Receiving includes already-retained worker failures; its original protocol cause
survives best-effort error Close. Benign retirement and broker refusals do not
signal a fault. Attachment primary faults #161 and cleanup-first faults #162,
task families/ancestor shielding #75 and process shutdown #7 remain
[roadmap work](roadmap.md#next-main-increments). No aborted-ancestor protection,
graceful Close acknowledgement or finite broker/cleanup latency is implied.

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
