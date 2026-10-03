# Switchyard

Switchyard is an message broker written in Rust. Its compatibility
target is the Azure Service Bus Standard messaging model and its official
client SDKs, without requiring an Azure subscription.

The production design is a quorum-durable, horizontally scalable broker for
queues, topics, subscriptions, sessions, scheduling, dead-lettering, filters,
and transactions. A separate single-node memory mode is intended for local
development, conformance tests, and applications such as the Sift demo.

> [!IMPORTANT]
> Switchyard is pre-alpha. A single node accepts AMQP connections over
> development plaintext or authenticated TLS and persists queue state, but
> replication, administration, and the official client gates are incomplete.
> Do not use it for production workloads.

## Design Targets

- Azure Service Bus-compatible AMQP 1.0 over TLS and WebSockets
- Official .NET SDK data-plane and administration compatibility
- Sift data-plane and Atom/XML management compatibility
- Rust-native storage and TLS stacks without RocksDB, OpenSSL, or system TLS
  dependencies
- Hard namespace isolation, quotas, RBAC, OIDC, SAS, and mTLS
- Per-namespace envelope encryption and external KMS integration
- Tamper-evident audit records and encrypted incremental backups

The precise component boundaries, consistency rules, and release gates are in
[ARCHITECTURE.md](ARCHITECTURE.md). Current and planned protocol coverage is
tracked in [docs/compatibility.md](docs/compatibility.md).

## Workspace

| Crate | Responsibility |
| --- | --- |
| `domain` | Broker identifiers, commands, state-machine rules, and errors |
| `storage` | Atomic storage contract with a Fjall backend and a memory backend |
| `cluster` | Cluster invariants, placement, Raft integration, and routing |
| `protocol-amqp` | AMQP 1.0 and Azure Service Bus protocol adaptation |
| `auth` | SAS, OIDC, mTLS, RBAC, encryption, and audit policy |
| `admin-api` | Versioned native gRPC administration contract |
| `server` | Broker process: backend selection, command proposal, and timers |
| `switchyardctl` | Native administration CLI |
| `testkit` | Deterministic fixtures and cluster test support |
| `conformance` | SDK and behavioral compatibility suites |

Workspace crates are unprefixed and match their directory under `crates/`. The
domain crate is named `domain` rather than `core` because a package named
`core` shadows the Rust sysroot crate of that name in every dependent.

The native API contract begins in
[`proto/switchyard/admin/v1/admin.proto`](proto/switchyard/admin/v1/admin.proto).

## Development

Switchyard pins its Rust toolchain. Build and test the complete workspace with:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

Inspect the current development configuration:

```sh
cargo run -p server -- \
  --mode development \
  --storage memory \
  --voters 1
```

Inspect the compatibility status exposed by the CLI:

```sh
cargo run -p switchyardctl -- compatibility
```

Development defaults to plaintext AMQP on `127.0.0.1:5672`. Supplying
`--tls-certificate` and `--tls-private-key` secures the listener and changes its
default port to 5671. A shared-access policy is configured with
`--shared-access-key-name` and `--shared-access-key-file`; it enables SASL PLAIN
and CBS SAS authorization. Production mode refuses to start without both TLS
and a shared-access policy. Even with those prerequisites, durable production
startup is refused until quorum replication and a replicated command proposer
are implemented. Development with Fjall remains locally durable, not replicated.

A separate trusted [committed queue apply API](docs/committed-queue-apply.md)
atomically records bounded queue work and replay progress in isolated replica
stores. It is a durability prerequisite, not a running consensus service.

AMQP over WebSockets is opt-in with `--websocket-listen 127.0.0.1:8080`. It uses
the exact `/$servicebus/websocket/` endpoint and `amqp` subprotocol. The existing
TLS identity and shared-access policy also apply to this separate listener.
See [WebSocket Transport](docs/websocket-transport.md) for limits and client gates.

Native entity administration is opt-in with `--admin-listen`. For an isolated
development node, add `--admin-listen 127.0.0.1:9080`, then create and inspect a
queue:

```sh
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  queue create orders
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  queue get orders
```

The CLI also supports `queue list` and `queue update`, and emits JSON. Topics
and subscriptions have separate create/get/list/update commands:

```sh
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  topic create events --requires-duplicate-detection
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  subscription create events audit --dead-letter-on-expiration
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  subscription list events
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  subscription update events audit --dead-letter-on-filter-exceptions false
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  rule create events audit Red --filter-file examples/rules/red.json
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  rule create events audit RedWithoutAudit --filter-file examples/rules/red.json \
  --action-file examples/rules/remove-audit.json
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  rule list events audit
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  subscription delete events audit
```

Listing is exhausted only when `next_page_token` is empty. Queue discovery has a
per-request work budget, so an empty page may still carry a token; pass it to the
next list request with `--page-token` to continue.

HTTPS requires `--ca-certificate`; use `--tls-server-name` when connecting through a
local address that differs from the certificate name. Supply a Manage SAS token
with `--token-file`, never on the command line. Authenticated administration
requires TLS and uses the node's existing shared-access policy. Queue, topic, and
subscription deletion is synchronous and destructive, with atomic cleanup limits
and retained recreation fences described in [Entity Deletion](docs/entity-deletion.md).
The same endpoint also exposes typed subscription rule create/get/list/delete
through [Native Rule Administration](docs/native-rules.md), including bounded
REMOVE actions but not upserts. The rule CLI uses bounded, typed JSON filter
and optional action files and preserves
scalar widths and bits. Subscription creation keeps an explicit `$Default`
true rule; delete that rule when selection should depend only on custom filters.
Other native services and Azure administration compatibility remain unfinished.
AMQP rule management, native administration, the CLI, and the domain provide bounded
[REMOVE rule actions](docs/sql-actions.md) with independent selected copies.
Native action creation uses a separate RPC, with no action-free fallback on
older servers. Native reads require action opt-in; the CLI requests it automatically.

The trusted Rust broker API also provides bounded, same-queue atomic sends and
settlements. These primitives underpin the separate opt-in native listeners;
scope and retry boundaries are in [Atomic Queue Operations](docs/atomic-queue-operations.md).
The optional [guarded commit API](docs/atomic-commit-permits.md) adds pending-only
cancellation and a runtime commit decision.
The [owned work API](docs/atomic-work-reservations.md) carries shared resource
reservations through queueing, cancellation, and owner completion.
The [local registry](docs/transaction-registry.md) adds bounded declarations,
controller lifetimes, and one-time discharge without enabling wire transactions.
The [native transaction types](docs/amqp-transaction-types.md) are represented
explicitly. An opt-in [native posting lifecycle](docs/native-transactional-ingress.md)
adds coordinator receipts and an ordered owner handoff. The trusted
[paired owner API](docs/native-atomic-owner-handoff.md) carries native receipts
and logical work through one broker job. An explicit
[posting-only listener](docs/atomic-posting-ingress.md) now derives and stages
actual native receipts through that owner. Default Service Bus listeners still
refuse transactions. A separate
[atomic messaging listener](docs/atomic-messaging-ingress.md) adds actual held
PeekLock completion to the same owner, without changing the posting-only endpoint.
The process exposes it only through the separate, development-only
`--experimental-atomic-messaging-listen` address; ordinary AMQP and WebSocket
listeners retain their previous policies.
Both pinned .NET clients have an opt-in [transaction-scope gate](docs/dotnet-transaction-scopes.md)
for warmed and cold-first same-queue immediate send, plus held Complete over TLS
on that address, with memory and Fjall coverage. Its fixed
[initial control window](docs/initial-transaction-authorization.md) admits bounded
declaration and rollback before the first grant, never queue access or commit.
Cold-first support is Send only; transactional acquisition, experimental
management, cross-queue work, and transaction recovery remain unsupported.
Separate ordinary [receiving-batch gates](docs/dotnet-receiving-batches.md) hold
three action copies on one receiver before completion and exercise out-of-order
queue settlement with rolling count-prefetch replenishment on both SDK pins and
both storage backends.
Authenticated ordinary receiving uses [owned pending-only broker claims](docs/ordinary-receive-claims.md)
to refuse cancelled or expired queued work before the proposer starts. Already
Started work retains its existing commit and lock-expiry boundaries.
The sender-side listener uses [retained ingress receipts](docs/retained-ingress.md)
to preserve native content accounting through broker replies and acknowledgment flush.
[Native connection identities](docs/native-connection-identity.md) preserve exact
receipt origin and expose actor retirement without granting transaction authority.
[Native receiver identities](docs/native-receiver-provenance.md) distinguish
replaced receiving-link generations; decoder-aware acceptance remains opt-in
per link and does not stage logical batch work.
[Native sender identities](docs/native-sender-provenance.md) check active
sending-link origin without granting delivery settlement or retirement authority.
[Original outgoing delivery identities](docs/native-outgoing-delivery-provenance.md)
preserve exact historical generations after outcome resolution, without reporting
live delivery state or granting settlement authority.
A separate opt-in [native transactional retirement](docs/native-transactional-retirement.md)
path captures exact peer dispositions for fully flushed outgoing deliveries and
combines their prepared receipts with postings. It does not enable transactional
receiving by default or SDK transaction scopes.

## Production Contract

This is a design contract, not an available deployment profile. Production
startup currently fails before opening storage or binding listeners; a valid
voter count and a durable directory cannot make the local writer a quorum.

A production cluster has at least three odd-numbered voters and stores three
replicas of every metadata or entity placement group. A mutation succeeds only
after its Raft record is fsynced by a quorum and applied by the leader. A
minority partition rejects mutations rather than risking acknowledged message
loss or split-brain behavior.

One queue is one placement group by default. A topic and all of its
subscriptions share a placement group. Entities that must participate in one
transaction can be assigned to the same immutable placement group. Partitioned
entities and cross-placement-group transactions are not part of the first
production release.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development policy.

## License

Licensed under the [MIT License](LICENSE).

Azure and Azure Service Bus are trademarks of Microsoft Corporation.
Switchyard is an independent project and is not affiliated with or endorsed by
Microsoft.
