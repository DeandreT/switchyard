# Switchyard Architecture

## Status

This document is the implementation contract for Switchyard. The repository is
currently pre-alpha. A deterministic state machine covers queue send and
receive, both settlement modes, lock expiry, time-to-live expiry,
dead-lettering and dead-letter receive, and the session ownership and session
state described under [Message Semantics](#message-semantics). It runs over
either the Fjall backend or the memory backend, so a single node survives a
restart.

The `switchyard` binary accepts AMQP connections over development plaintext or
TLS, authenticates a configured shared-access policy through SASL PLAIN or CBS
SAS, carries messages and sessions across that edge, and sweeps lock,
time-to-live, and session-lock expiry. JWT/OIDC, mTLS, policy administration,
Raft, and compliance implementations remain to be built. Within the semantics
below, scheduling, deferral, duplicate detection, and topics are not
implemented, the timer worker covers only the three expiry indexes that exist,
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

The protocol crate will use `amqp-runtime` acceptors for AMQP framing, sessions,
links, flow control, SASL, TLS, and WebSocket transport. Switchyard implements
the Service Bus-specific layer:

- SASL PLAIN for shared access policies
- SASL ANONYMOUS followed by CBS `$cbs` SAS or JWT authorization
- Queue, topic, subscription, and dead-letter entity paths
- `$management` request/reply operations
- Peek-lock and receive-delete settlement mappings
- Scheduled, deferred, dead-letter, session, and sequence annotations
- AMQP transaction coordinator links and transactional dispositions
- Compatible errors, status codes, link detach conditions, and retry hints

The HTTPS compatibility endpoint implements the Atom/XML entity and rule
operations required by Sift and `ServiceBusAdministrationClient`. The native
control plane is gRPC only.

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

FIFO is guaranteed only within a session. Session ownership, its lock deadline,
and opaque session state are replicated. A new owner cannot acquire a session
until the previous lock expires or is released.

Leader-only timer workers scan scheduled, lock-expiry, TTL, duplicate, and
auto-delete indexes. They propose explicit state-machine commands; local wall
clock never mutates state directly. An injected hybrid logical clock prevents
time from moving backward. Clock jumps beyond the configured safety threshold
pause timers and fail readiness until an operator resolves the condition.

The worker that exists today sweeps the lock-expiry, TTL, and session-lock
indexes, which are the ones the state machine has. One sweep command processes a
bounded number of entries, so the worker re-proposes until an index reports less
than a full batch, and a backlog on one queue cannot starve the rest of the tick.
Time reaches the state machine only through the proposer, which stamps each
command: a host clock that steps back a little holds the applied timestamp still
rather than regressing it, and one that steps back further has the command
refused. Refusal is not yet wired to a readiness signal — the sweep is logged and
retried on the next tick.

Topic sends evaluate the current subscription rule revision before proposing
fanout. The command records the matched subscriptions and encrypted property
overlays, making follower application deterministic. One encrypted payload can
be referenced by multiple subscriptions and is removed after the final
reference disappears.

## Transactions And Forwarding

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
