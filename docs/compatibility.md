# Compatibility

Switchyard targets Azure Service Bus Standard. This describes single-node `main`,
not reference branches; core/protocol/client gates establish tested behavior,
not compatibility certification.

## Client Gates

| Client | Evidence |
| --- | --- |
| Official .NET | Four opt-in experimental Memory TCP/WSS gates: declared-current ServiceBus 7.21.0 / Core 1.62.0 and previous 7.20.2 / 1.60.0, not latest. Isolated locked graphs/roots; approved ServiceBus/Core, output and nonce-bound loaded-file bytes; completion after async disposal. |
| Rust AMQP | Broader protocol end-to-end suites, not official-SDK substitutes. |
| Pinned Sift | Planned; no gate. |

.NET gates cover queue/batch send, prefetch/settlement, receive-delete, envelopes,
renew/abandon/defer/peek, DLQ, sessions/state, duplicates, scheduling/cancellation,
filtered subscriptions, rules and case-insensitive addresses. Linux/.NET 10/NuGet
restore are required; missing prerequisites fail. Workspace runs ignore these
workflows; durable coverage is pending. TCP uses a permissive generated-identity
callback; WSS instead uses platform trust and child-scoped `SSL_CERT_FILE` root,
without machine-wide changes. Neither certifies production trust, administration
or whole-test task-tree shutdown. See [selectors/limits](sdk-gates.md),
[TCP](../crates/server/tests/amqp_dotnet_current.rs),
[WSS](../crates/server/tests/amqp_dotnet_websockets.rs) and
[declared project](../crates/conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj).

## Implemented Surface

| Area | Contract |
| --- | --- |
| Transport/auth | AMQP 1.0 TCP/TLS; binary WSS `/$servicebus/websocket` (`AMQPWSB10`/`amqp`); development-only plaintext. TLS precedes AMQP; SASL PLAIN or ANONYMOUS/`MSSBCBS`, then CBS SAS. |
| Rights | Namespace/entity Send, Listen, Manage (includes both). Send for scheduling/cancellation; Listen for receive management. CBS grants are connection-wide, expire links, and allow 20s initial authorization. |
| Delivery | Singular/atomic batch send; peek-lock (unsettled) or receive-delete (pre-settled); independent out-of-order settlement; bounded prefetch/drain. Credit is reserved before mutation, empty results release it, and consumption/release completes drain. |
| Envelope | Durable AMQP body/forms, identifiers, properties, annotations, application properties/footer; broker-authoritative delivery overlays. |
| Management | Renew, abandon, defer/retrieve, ordered peek, schedule/cancel, custom dead-letter and DLQ receive/complete. Unsupported requests reject with compatible conditions/retry hints. |
| Sessions | Named/next-available attach echoes identifier/deadline; exclusive locks, opaque state, release on close. Renewal/state use `$management`, not automatic renewal. |
| Duplicates | Exact non-empty message IDs across immediate/batch/scheduled sends; bounded history expiry. |
| Profiles | Domain-only partial ordinary-queue updates atomically validate/change parent and exact DLQ profiles plus Clock. No-op patches write nothing; owner, records/deadlines remain. Session/duplicate modes are immutable; lock/TTL/message-size/history changes govern future work. |
| Topics/rules | Non-session immediate/scheduled singular/batch fanout; placeholder peek/cancel; independent subscription queues. Durable `$Default`, actionless true/false/correlation, typed equality; create/delete/paginated AMQP rules. |
| Time/identity | Bounded activation/lock/TTL/session-lock/history expiry. ASCII-folded namespace/entity/subscription addresses and SAS scope; session/placement IDs retain case. |
| Local APIs | [Offline RS256 JWT](offline-jwt.md): pinned keys, injected time/local rights, issuer-qualified grants; SAS/PLAIN retain verified namespace-host qualification. [SQL kernel](sql-predicates.md): bounded ephemeral local grammar/errors, IN/LIKE/scalar arithmetic. No JWT wire activation or persisted SQL rules/fanout/actions. |
| Storage | Paired Memory/Fjall semantics; Fjall journal fsync before applied outcomes; exclusive directory ownership. |

Selected operational queue-profile reads validate after decode/owner proof,
including backing queues/DLQs; invalid limits are corruption. Unread rows/raw
catalogues are not certified; profile-free legacy operations remain unchanged. Topic/subscription
profile propagation, administration setters/capacity, JWT consumers [#71](https://github.com/DeandreT/switchyard/issues/71)
and activation [#17](https://github.com/DeandreT/switchyard/issues/17), typed content
#16 and rule integration #20/#21 remain pending.

## Known Differences And Bounds

| Behavior | Current contract |
| --- | --- |
| Session ID on ordinary queue | Refused (Azure ignores it); only session queues promise FIFO. |
| Session settlement | Live message token/deadline suffice after session-lock loss; Azure also requires session ownership. |
| Next session | At most 32 candidates; all held returns unavailable/retry, not a full walk. |
| TTL | Always DLQs as `TTLExpiredException`; Azure's configurable expiration policy defaults off. Live locks remain settleable until expiry. |
| Duplicate gaps/history | Suppression consumes acknowledgement sequence slots, stores no message. History starts at command time; hits do not extend it, nor settlement/cancellation/expiry/DLQ erase it. Missing raw-AMQP IDs bypass it. |
| Scheduled topics | Children/rules snapshot at activation: later subscriptions may receive older publications. All-scheduled sends do not prove topology early. |
| Browse/deferred | Positive signed peek count, reply cap 250, scan to cap/end; deferred retrieval atomically accepts at most 32 sequences in caller order. |
| Service limits | 2,000 subscriptions/topic; separate topic/subscription name bounds; Standard default 256 KiB wire-message limit. Commercial namespace/operation quotas absent. |

### Atomicity And Lifecycle

Commands commit one batch; domain refusals preserve messages, counters, history
and Clock. Indexed apply, journal and replay have separate checkpoint/retire/reopen
rules, not panic recovery or quorum guarantees. Snapshot format is structural,
not complete state/backend-provenance/authenticity proof or capture/install.
See [atomicity and lifecycle](lifecycle.md#atomicity-and-lifecycle) for settlement,
deferral, scheduling, DLQ and replay contracts.

### Topic Integrity

Selected routing/page and rule readers validate listed graphs and canonical rows
before returning prefixes or copies; corruption refuses atomically. This is not
global catalogue health, live-incarnation or retained-runtime proof.
See [topic integrity](lifecycle.md#topic-integrity) for exact bounds and exclusions.

### Storage And Runtime Limits

Memory is volatile; neither backend is replicated. Production durable startup
returns static replication-unavailable before storage/listeners; production Memory
refuses too. Fjall restart is not node-loss recovery; development is single-node.

Custody is owner-scoped, not whole-task-tree shielding. Borrowed cancellation/retry
requires a surviving owner; retirement requests prove neither joins nor peer answers.
Raw priorities and cleanup notices differ by owner. Begun broker/cleanup drains
remain unbounded; owner-drop and aborted-ancestor survival are excluded. TCP custody
is included; WSS #225, admission #135, deadlines #136 and process shutdown #7 remain
separate. See [storage limits](lifecycle.md#storage-and-runtime-limits) and the
[eleven owner contracts](lifecycle.md#owner-custody).

Sustained traffic beyond initial credit is uncertified: [#68](https://github.com/DeandreT/switchyard/issues/68)
windows/[#69](https://github.com/DeandreT/switchyard/issues/69) refill and
[#77](https://github.com/DeandreT/switchyard/issues/77) Flow echo remain absent.
Timer commands drive expiry/activation, not wall-clock mutation. Small clock
regressions hold time; large ones refuse/retry without readiness signal. One
namespace/server; no quotas, fair multi-tenancy or production durability.

## Not Implemented

| Scope | Missing |
| --- | --- |
| Semantics | General SQL/actions, session subscriptions/session-ID predicates, topic duplicates, configurable TTL, DLQ forwarding/resubmit, same-group transactions. |
| Administration | Atom/XML entities/rules; native gRPC/CLI transport (contract only). |
| Security | JWT consumers/wire, network OIDC, mTLS, policy/full RBAC, namespace KMS/encryption, tamper-evident audit. |
| Production | Raft/placement, quotas/fairness, encrypted backup/restore, readiness/observability, release/performance evidence. |

Partitioning/cross-placement transactions are later scope; geo-replication/Premium
are not initial commitments. See [roadmap](roadmap.md) and [target design](../ARCHITECTURE.md).
