# Switchyard

Switchyard is a Rust message broker targeting the Azure Service Bus Standard
messaging model and official clients, without an Azure subscription.

> [!IMPORTANT]
> Pre-alpha. The implemented broker is single-node, with memory or Fjall storage
> and an AMQP TCP/TLS or WSS edge, plus opt-in TLS native entity administration.
> Raft replication and production security and recovery are not implemented.
> Do not use it for production workloads; the `production` startup profile is not a readiness claim.

## Current Progress

- [x] Atomic queue send/batch, receive, settlement, expiry, DLQ, deferral, and peek
- [x] Queue sessions, scheduling/cancellation, and duplicate detection
- [x] Immediate/scheduled topics, subscriptions, and actionless correlation rules
- [x] SASL PLAIN/CBS SAS and two fixed .NET TCP/WSS loaded-file custody gates
- [x] Fsynced single-node storage and bounded topic routing integrity checks
- [ ] Remaining administration, Service Bus semantics, and broader client gates
- [ ] Quorum durability, tenant isolation, encryption/audit, and recovery

See the [roadmap](docs/roadmap.md) for issue ownership, dependencies, and merge
order. [Compatibility](docs/compatibility.md) records implemented behavior and
known differences. [Architecture](ARCHITECTURE.md) separates the current runtime
from the production design.

## Development

Use the pinned Rust toolchain. Keep builds bounded and reuse a compatible Cargo
target directory; check disk headroom before a full workspace build.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
cargo test --locked --workspace -j2
cargo build --locked --workspace -j2
cargo run --locked -p server -j2 -- --mode development --storage memory --voters 1
```

That starts an empty development node at `127.0.0.1:5672`, without a native
administration listener. For local persistence, select
`--storage fjall --data-dir <directory>`. [Native administration](crates/server/README.md)
can explicitly enable TLS/h2 Create/Get/List with offline namespace-Manage JWTs.

The [server arguments](crates/server/src/main.rs) expose TLS certificate/key
files, shared-access policy files, an explicit offline `--jwt-policy-file`, namespace,
and transport selection. The AMQP JWT policy requires TLS and is independent of
native administration; it can be used alone or alongside SAS. TLS
defaults to port 5671; `--transport amqp-websockets` requires TLS and defaults to
443. `--max-connections <count>` bounds live listener sockets, handshakes
included; omitted, admission is unbounded. `--admission-timeout-secs <seconds>`
(default 60) is one budget per accepted socket for TLS, any WebSocket upgrade,
SASL and AMQP Open together; a socket still handshaking at the deadline is
closed. Plaintext is development-only. These checks do not provide replication.

SIGTERM or SIGINT stops the node in order: both configured listeners retire
admission (including new RPCs on existing HTTP/2 connections), open connections
are retired and closed and their original protocol and accepted-RPC tasks
joined, then the timer and the broker stop and the store closes. `--shutdown-timeout-secs <seconds>` (default 30) bounds the
whole sequence; a repeated signal joins it. Within budget the process exits 0;
past it, the process reports the unfinished stage and exits with status 3
without acknowledging anything that was not committed.

## Workspace

| Crates | Responsibility |
| --- | --- |
| `domain`, `storage` | Deterministic commands and atomic memory/Fjall state |
| `amqp` | Repository-owned framing, sessions, links, SASL, and AMQP types |
| `protocol-amqp`, `server` | Service Bus adaptation, listener/runtime, local proposer, timers |
| `auth` | SAS and offline JWT policies; verified grants |
| `cluster` | Configuration; opt-in [journal](crates/cluster/src/journal.rs), [replay](crates/cluster/src/replay.rs), and [raw snapshot format](crates/cluster/src/snapshot/format.rs); replication remains planned |
| `admin-api`, `switchyardctl` | [Native entity bindings/handlers](crates/admin-api/README.md) and CLI scaffold |
| `testkit`, `conformance` | Fixtures and client gates |

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Use one issue and one focused feature
PR per independently mergeable change; do not merge reference mega branches.

## License

[MIT](LICENSE). Azure and Azure Service Bus are Microsoft trademarks. Switchyard
is independent and is not affiliated with or endorsed by Microsoft.
