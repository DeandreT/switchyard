# Switchyard

Switchyard is an message broker written in Rust. Its compatibility
target is the Azure Service Bus Standard messaging model and its official
client SDKs, without requiring an Azure subscription.

The production design is a quorum-durable, horizontally scalable broker for
queues, topics, subscriptions, sessions, scheduling, dead-lettering, filters,
and transactions. A separate single-node memory mode is intended for local
development, conformance tests, and applications such as the Sift demo.

> [!IMPORTANT]
> Switchyard is currently a pre-alpha architecture and workspace scaffold. It
> does not yet accept AMQP connections or persist messages. Do not use it for
> production workloads until the published conformance and durability gates
> are complete.

## Design Targets

- Azure Service Bus-compatible AMQP 1.0 over TLS and WebSockets
- Official .NET SDK data-plane and administration compatibility
- Sift data-plane and Atom/XML management compatibility
- Pure-Rust storage and TLS dependencies
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

Inspect the compatibility status exposed by the CLI scaffold:

```sh
cargo run -p switchyardctl -- compatibility
```

The development command currently validates and prints configuration; network
listeners will be added with the first AMQP vertical slice.

## Production Contract

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
