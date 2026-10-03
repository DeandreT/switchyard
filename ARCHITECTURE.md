# Switchyard Architecture

## Status

This document is the implementation contract for Switchyard. The repository is
currently pre-alpha. A deterministic state machine covers queue send and
receive, peek, both settlement modes, lock renewal and expiry, time-to-live
expiry, scheduling and cancellation, deferral, duplicate detection,
dead-lettering and dead-letter receive, and the session ownership and session state described under
[Message Semantics](#message-semantics). It runs over
either the Fjall backend or the memory backend, so a single node survives a
restart.

The `switchyard` binary accepts AMQP connections over development plaintext or
TLS and an optional WS/WSS listener, authenticates a configured shared-access
policy through SASL PLAIN or CBS SAS, carries messages and sessions across that
edge, and sweeps scheduled
activation, lock, time-to-live, session-lock, and duplicate-history expiry.
JWT/OIDC, mTLS, policy administration,
Raft, and compliance implementations remain to be built. Production startup
is refused before storage is opened because no replicated command proposer
exists. Development with Fjall provides local persistence only.

Separate trusted prerequisites now provide [atomic committed queue apply](docs/committed-queue-apply.md)
and [experimental vote/log storage](docs/experimental-log-storage.md). They do
not implement the replicated proposer, state-machine adapter, snapshots,
network, leader barriers, or quorum acknowledgements required below.

Within the semantics below, topics have persisted definitions, bounded
subscription topology, and atomic rule-selected fanout, parent-retained topic
scheduling, AMQP subscription and dead-letter routing,
and subscription management operations. Both pinned .NET gates cover immediate
topic publications and independent subscription workflows. Session-required
subscriptions reuse entity-local session ownership and state, while ordinary
subscriptions retain session identifiers without session-affine delivery.
Read-only management browsing can inspect all sessions without acquiring a hold.
Committed delivery notifications wake all registered entity waiters, including
independent session receivers; registration precedes each receive attempt.
Boolean, scalar correlation, and bounded SQL rules are persisted and managed
through AMQP and the native gRPC [rule API](docs/native-rules.md).
SQL error routing has an explicit subscription policy. AMQP, native, CLI, and domain
[REMOVE actions](docs/sql-actions.md) create independently transformed copies;
Azure administration remains unimplemented. Native
administration creates, reads, lists, partially updates, and atomically deletes
queues, topics, and subscriptions. Deletion purges owned state under explicit
work limits while retaining counters and entity identities as recreation fences; see
[Entity Deletion](docs/entity-deletion.md) and
[Entity Incarnations](docs/entity-incarnations.md). The timer worker covers
scheduled activation and the four expiry indexes that exist,
and the storage keyspace layout under [Storage](#storage) is still a single
record keyspace rather than the split listed there.

Compatibility means observable protocol and SDK behavior backed by automated
tests. It does not mean byte-for-byte implementation similarity, Microsoft
certification, or support for undocumented Azure internals.

## Goals

- Provide the Azure Service Bus Standard queue and publish/subscribe model.
- Run production workloads on a minimum three-node Linux cluster.
- Preserve acknowledged messages through any single-node failure.
- Scale to multi-terabyte retained datasets and 100,000 aggregate durable
  1 KiB messages per second on the published reference hardware.
- Isolate mutually untrusted namespaces through authentication, authorization,
  quotas, fair scheduling, encryption keys, and audit scopes.
- Supply controls and evidence hooks for SOC 2 and HIPAA-oriented deployments.
- Keep the server runtime in Rust without RocksDB, OpenSSL, or system TLS
  dependencies.

## Initial Non-Goals

- Azure Service Bus Premium-specific behavior
- Partitioning one queue or topic across multiple Raft groups
- Transactions spanning Raft groups
- Active-active or standby cross-region replication
- A browser administration dashboard
- A Kubernetes operator
- Windows or macOS production servers
- Regulatory certification supplied by the software itself

## Node Architecture

Every production node runs the same `switchyard` binary:

```text
 AMQP/TLS 5671       WSS + HTTPS 443       Admin gRPC 9443
        |                    |                     |
        +---------------- protocol edge ----------+
                             |
                  authentication + admission
                             |
                       request router
                             |
              +--------------+--------------+
              |                             |
       metadata Raft group          data Raft groups
              |                             |
              +------- replicated log ------+
                             |
                 deterministic state machine
                             |
                      Fjall keyspaces
                             |
              backup, audit, metrics, tracing
```

Client connections terminate on any node. That edge node authenticates the
connection, enforces connection-level limits, resolves the entity placement,
and forwards typed broker commands to the current Raft leader. AMQP connection
and link state remain local, while message ownership, locks, sessions,
transactions, and settlements are replicated.

## Protocol Edge

The repository-owned AMQP engine handles framing, sessions, links, flow
control, settlement, and SASL. The protocol listener establishes TLS before
that engine starts. The optional WebSocket listener uses the same engine behind
a bounded binary adapter, with standalone protocol headers and explicit close
cleanup; see [WebSocket Transport](docs/websocket-transport.md).
Switchyard implements the Service Bus-specific layer:

- SASL PLAIN for shared access policies
- SASL ANONYMOUS followed by CBS `$cbs` SAS or JWT authorization
- Queue, topic, subscription, and dead-letter entity paths
- `$management` request/reply operations
- Peek-lock and receive-delete settlement mappings
- Scheduled, deferred, dead-letter, session, and sequence annotations
- AMQP transaction coordinator links and transactional dispositions
- Compatible errors, status codes, link detach conditions, and retry hints

An HTTPS compatibility endpoint for the Atom/XML entity and rule operations
required by Sift and `ServiceBusAdministrationClient` is planned, not implemented.
The implemented native control plane is gRPC only.

Production listeners require TLS. Plaintext AMQP and HTTP are available only
when the explicit development profile is active.

## Control Plane

One metadata Raft group owns:

- Cluster membership and node availability-zone labels
- Namespace definitions, quotas, RBAC bindings, and KMS references
- Entity definitions, immutable placement groups, and replica assignments
- Storage, snapshot, protocol, and Raft command format versions
- Backup manifests, feature gates, and cluster-wide audit configuration
- Namespace storage quota leases allocated to data groups

Metadata operations are low volume and never carry message bodies. Placement
changes add a learner, install a snapshot, catch it up, promote it through
joint consensus, and only then remove the previous replica.

## Data Plane And Sharding

A placement group maps to one Raft group with three voters. A queue receives a
new placement group by default. A topic and every subscription and rule below
it always share one group so filter evaluation and fanout commit atomically.

An entity can be created with an explicit `placement_group_id`. This permits
same-group transactions between queues and topics. The setting is immutable
after creation; moving an entity requires a controlled drain and recreation.

The first production release does not split a hot entity. Its 100,000
messages-per-second target is aggregate across independently placed entities.
Session ordering is local to the owning entity group.

## Durable Write Path

1. The edge validates size, authorization, namespace quota, and protocol
   fields.
2. It resolves and forwards the typed command to the data-group leader.
3. The leader encrypts protected fields, allocates deterministic identifiers,
   and proposes the command.
4. Every voter writes the Raft entry and hard state to its Fjall journal and
   performs `SyncAll` before acknowledging replication.
5. After quorum commit, each replica applies the command through one atomic
   Fjall batch.
6. The leader returns the protocol outcome only after its local apply
   completes.

Acknowledgement therefore means that a quorum holds a durable replayable
record. Applied indexes and snapshots are persisted before older log segments
can be compacted.

If quorum is unavailable, sends, receives that acquire locks, settlements,
transactions, and consistent management reads fail with retryable availability
errors. Switchyard does not acknowledge through a minority partition.

## Storage

The production backend is Fjall. One database per node contains isolated
keyspaces for:

- Raft hard state, entries, membership, applied indexes, and snapshots
- Namespace and entity metadata cached from the metadata group
- Encrypted message records and topic payload reference counts
- Ready, scheduled, expiry, deferred, and dead-letter indexes
- Peek locks, delivery counts, session ownership, and session state
- Duplicate-detection windows and keyed identifier indexes
- Staged transactions and durable forwarding outboxes
- Logical quota accounting and audit-chain records
- Backup checkpoints and exporter cursors

The state machine uses explicit big-endian keys and versioned value envelopes.
Schema upgrades are online and resumable. A new format is activated only after
all voters advertise support, allowing one-minor-version rolling upgrades.

What exists today is one `records` keyspace holding every state-machine key, and
a `meta` keyspace holding the on-disk layout version. Splitting `records` into
the keyspaces listed above is a layout change, which is what the layout version
exists to gate: an open refuses any version other than the one the build reads
and writes, in both directions, so a rollback fails rather than misreading a
newer store. A command's batch is journalled and fsynced before the store
reports it applied, and a store directory has a single owner — a second open of a
live directory is refused rather than shared.

Isolated replica directories have a disjoint durable layout and a unique
privileged writer with read-only views. The committed queue machine and the
experimental log adapter retain distinct inner profiles in separate directories;
neither adopts the other's records. Standalone format 14 is unchanged. These
prerequisites do not implement production keyspace placement or online upgrades.

The memory backend implements the same atomic batch and snapshot contract, and
one conformance suite runs against both backends so they cannot drift. It is
reserved for unit tests, deterministic simulations, Sift demos, and local
development and never satisfies production readiness.

## Message Semantics

Peek-lock provides at-least-once delivery. Lock acquisition is committed before
delivery, and completion removes the message only after settlement commits. A
failed transfer or receiver leaves the durable lock to expire and permits
redelivery.

Receive-delete provides at-most-once delivery. Deletion commits before the
transfer, so a client failure can lose that delivery by design.

Ordinary AMQP receivers obtain an actor-owned, exact-link outgoing credit
reservation before submitting each Receive. They check authorization before
admission and again before submission. Empty reads release the reservation
before waiting for deliverability, permitting the transport to drain unused
credit. Reservations share existing outgoing metadata limits and do not
advance delivery counters until a Transfer begins.

Authenticated ordinary receiving adds a unique [owner claim](docs/ordinary-receive-claims.md):
dropping its owned future cancels only Pending work, and the owner checks the
captured Listen expiry immediately before starting the proposer. Started is
irrevocable admission, not a commit or live authorization lease. Its
[bounded pipeline](docs/ordinary-receiving-pipeline.md) supports independent
held deliveries while committing settlement before the final acknowledgement.
Credit withdrawn after claim can delay a transfer; disconnect after a committed
Receive retains the deletion or lock-expiry semantics above. Unsecured legacy
submission and experimental transactional receiving keep their separate paths.

FIFO is guaranteed only within a session. Session ownership, its lock deadline,
and opaque session state are replicated. A new owner cannot acquire a session
until the previous lock expires or is released.

Leader-only timer workers scan scheduled, lock-expiry, TTL, duplicate, and
auto-delete indexes. They propose explicit state-machine commands; local wall
clock never mutates state directly. An injected hybrid logical clock prevents
time from moving backward. Clock jumps beyond the configured safety threshold
pause timers and fail readiness until an operator resolves the condition.

The worker that exists today sweeps scheduled activation, lock-expiry, TTL,
session-lock, and duplicate-history indexes. Activation gives a scheduled
message a new active sequence, records the actual enqueue time, and starts its TTL at that time.
Each sweep visits at most 1,024 queue configurations, including subscription
backing queues and dead-letter shadows, and independently at most 1,024 topic
configurations in exclusive key order. The worker retains separate cursors
between sweeps and wraps after each final page, so later entities are not starved
by earlier ones. Topics receive scheduled activation before duplicate-history cleanup.
One sweep command processes a bounded number of entries, and the worker
re-proposes at most eight times per index before moving on. It advances past a
entity before attempting its commands; a failed entity is revisited after the
cursor wraps rather than preventing every later entity from being swept. Both
queue and topic phases are attempted even if the first fails; the first error
is reported after both phases.
Time reaches the state machine only through the proposer, which stamps each
command: a host clock that steps back a little holds the applied timestamp still
rather than regressing it, and one that steps back further has the command
refused. Refusal is not yet wired to a readiness signal — the sweep is logged and
retried on the next tick.

Duplicate detection is an opt-in queue or topic setting. Its history records the original
submission deadline by message ID and expires independently of settlement or
schedule cancellation. Queue send and schedule check the same history, including
earlier entries in an atomic batch; topic publishing checks topic-owned history
once at immediate or scheduled admission. Activation makes previously accepted
work ready without rechecking or extending duplicate history. History cleanup is bounded, and overdue cleanup never
extends the detection window because submissions check deadlines directly.

In the planned replicated design, topic sends evaluate the current subscription
rule revision before proposing fanout. The command records the matched subscriptions and encrypted property
overlays, making follower application deterministic. One encrypted payload can
be referenced by multiple subscriptions and is removed after the final
reference disappears.

The implementation currently evaluates Boolean, scalar correlation, and SQL rules
inside the deterministic state machine, using its validated bounded membership,
complete rule sets, and one atomic batch. Subscription creation persists an
explicit `$Default` true rule; removing the final rule selects nothing. Conditions
AND within a correlation rule and action-free rules OR without extra copies.
Topic ingress owns sequence allocation and duplicate
history; action-free subscription copies share that sequence but have independent receive
and settlement state. It stores separate payload records rather than shared
encrypted payloads. Future publications retain one scheduled record on the topic,
not copies in its subscriptions. Activation uses current validated membership and rules,
assigns a new shared action-free sequence, and starts each copy's TTL. Late-member
participation and rule timing are local policies, not cloud-verified guarantees. Session-required
subscriptions own sessions independently; missing identifiers route copies to
their respective dead-letter shadows only when their rules select the publication.
SQL rules retain original source and a semantic version, never a parser AST.
Ephemeral compilation shares one allowance across the complete topic load;
correlation and SQL evaluation shares one command allowance. Finite SQL errors
override matches within one subscription. By default they route one session-free,
lifetime-free copy to its shadow with fixed local error fields; disabling the
subscription option drops only that copy. Resource limits instead refuse the
entire command atomically, including limits found after a finite error.
The domain action command adds one independently annotated copy per matching
REMOVE action, beyond the single OR-combined action-free copy. Those copies use
additional parent counter sequences after all original input acknowledgements;
the exact-key removal and final RuleName collision policy are local. AMQP
enumeration preserves complete actions; native reads expose actions only with
explicit opt-in, which the CLI requests automatically. A separate native action
creation method prevents silent downgrade on older servers. Detailed policies
are in [SQL Rules](docs/sql-rules.md) and [SQL Actions](docs/sql-actions.md).
Fanout admission bounds retained copies, content, and typed value items before
cloning; committed application effects name only actual ready destinations.
Topic activation commits a fitting due prefix of at most 256 inspected sources,
1,024 copies, 4 MiB retained content, and 65,536 projected values per command.
The timer continues positive prefixes for at most eight rounds per visited
topic, so the sweep bound is eight command budgets, not one. An unfit first
publication remains pending and cancelable rather than partially fanning out.
Rule metadata has separate per-rule, per-subscription, and count bounds, while
every input precharges all possible rule work and comparison bytes before
payload cloning. Duplicate and nonmatching inputs do not bypass those limits.
Detailed scalar semantics and local limits are recorded in
[compatibility.md](docs/compatibility.md).

Configuration patches preserve omitted values and reject changes to
creation-only session and duplicate-detection enablement. A topic update writes
one metadata record; a subscription update commits its membership, backing
queue, and dead-letter projection together. Equal patches stage nothing and do
not advance the applied clock. Updates validate existing topology without
repairing it, compiling rules, or rewriting retained messages, indexes, locks,
sessions, or duplicate history. Captured deadlines remain intact; later receives,
renewals, expiry, rule evaluation, and scheduled activation consult the applicable
current settings. Native administration, not Azure Atom/XML administration,
exposes these operations.

## Transactions And Forwarding

The current core has a trusted atomic-messaging foundation for immediate sends
and held settlements on one primary non-session queue. A bounded point-read
overlay prepares an ordered group and commits one normalized batch, publishing
only committed ready-index effects; see [Atomic Queue Operations](docs/atomic-queue-operations.md).
Explicit trusted native listeners provide coordinator and staged posting
receipts, with a separate [atomic messaging listener](docs/atomic-messaging-ingress.md)
joining held PeekLock completion to the same-queue owner. These are local,
bounded lifecycles rather than replicated transaction records or SDK scopes.
Default Service Bus listeners still refuse transaction traffic.
The following lifecycle remains the production design.

AMQP transactions are represented by replicated begin, stage, commit, and
abort commands. Staged operations remain invisible until one atomic commit
batch applies. Connection loss causes an explicit or lease-expiry abort.

Transactions can include sends, settlements, and forwarding only when every
entity belongs to the same placement group. Cross-group attempts fail before
performing any member operation.

Non-transactional forwarding across groups uses a durable source-side outbox,
an idempotent destination command, and a replicated completion marker. This
provides at-least-once forwarding without pretending to provide distributed
atomicity.

## Quotas And Isolation

Namespaces are security and resource-isolation boundaries. Each namespace has
limits for stored logical bytes, entities, subscriptions, connections,
in-flight requests, message rate, bandwidth, and audit backlog.

The metadata group grants bounded storage leases to data groups. A group cannot
accept bytes beyond its lease, and the sum of leases cannot exceed the
namespace quota. Rate limits use bounded per-node token allocations. The
request scheduler applies namespace-weighted fairness so one tenant cannot
consume every worker or connection slot.

Reaching a storage quota rejects new sends. Switchyard never deletes live
messages to make room.

## Identity, Encryption, And Audit

Authorization supports Azure-style SAS rights (`Send`, `Listen`, `Manage`),
OIDC issuer/audience validation with claim-to-role bindings, and mTLS
certificate or SPIFFE mappings. Native roles are scoped to cluster, namespace,
or entity.

Bodies, user properties, session state, credentials, and sensitive identifiers
are encrypted with per-namespace AES-256-GCM data keys. Required sequence,
timestamp, delivery, and routing fields remain plaintext. Equality lookup
indexes use keyed digests instead of raw message or session identifiers.

Data keys are wrapped by AWS KMS, Azure Key Vault, Google Cloud KMS, or Vault
Transit. A local provider exists only for development. New writes use the
active key version; old wrapped versions remain available for reads, backups,
and background rotation.

Every authentication decision, administration change, send, receive,
settlement, backup, restore, and export emits a replicated audit record. Audit
records contain actor and operation metadata but no message body or sensitive
property value. Records form a per-group hash chain and are exported as signed
batches to S3-compatible object storage with object lock and live OTLP.

Compliance mode requires TLS, external KMS, complete audit scope, declared WORM
retention, and encrypted backups. If its durable audit backlog reaches the
reserved limit, audited operations fail closed instead of silently dropping
evidence.

## Backup And Recovery

Each Raft group periodically emits an encrypted logical snapshot and
continuously archives immutable committed-log chunks. A signed cluster backup
manifest records the metadata revision, group membership, applied index,
checksums, and required namespace key versions for every included group.

Restore operates only into an empty cluster. It verifies manifests, signatures,
checksums, and KMS key availability before installing snapshots and replaying
logs to their pinned indexes. Continuous cross-region replication is deferred;
v1 disaster recovery is encrypted snapshot and point-in-time log restore.

## Observability

Nodes expose Prometheus metrics, OpenTelemetry traces and logs, structured
correlation identifiers, and separate liveness and readiness endpoints.
Production alerts cover quorum, leader churn, replication lag, fsync latency,
disk pressure, quota exhaustion, clock skew, KMS failures, audit backlog, and
backup freshness.

No telemetry leaves a cluster unless an operator configures an exporter.

## Distribution

Supported production targets are Linux x86_64 and arm64. Releases contain
signed standalone binaries, OCI images, SBOMs, checksums, license reports,
systemd examples, and a Helm chart with StatefulSets and replica anti-affinity.

A production deployment requires at least three odd-numbered voters on durable
NVMe storage. The reference performance environment uses three nodes connected
by 10 GbE. A single node is supported only as an explicit development mode.

## Verification And Release Gates

Switchyard will publish a compatibility matrix rather than make a blanket
compatibility claim. The initial gate runs the current and previous stable
official .NET SDKs and a pinned Sift revision against Switchyard. Differential
tests compare supported behavior with Azure Service Bus; external emulators are
test oracles only and are not runtime dependencies.

The test program includes:

- State-machine model and property tests
- AMQP, XML, filter, and configuration golden vectors
- Parser and protocol fuzzing
- Crash injection around each fsync and snapshot boundary
- Network partitions, leader changes, and deterministic Raft simulation
- KMS outage, key rotation, authorization, and audit-chain tests
- Backup corruption, empty-cluster restore, and rolling-upgrade tests
- Namespace fairness and hard-quota tests
- A 5 TiB retained-data compaction, failover, backup, and restore soak
- A 100,000 messages/second benchmark using persistent 1 KiB messages,
  replication factor three, batching, encryption, full auditing, NVMe, and
  10 GbE, with send acknowledgement below 20 ms p99

Version `1.0` is reserved until the compatibility, durability, security,
recovery, and performance gates pass.
