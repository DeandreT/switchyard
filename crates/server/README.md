# Native entity administration

The server can explicitly enable the existing tonic 0.14 EntityService over
rustls TLS and HTTP/2. It uses the real single-owner broker and the same
memory/Fjall state as AMQP. Queue, topic and named-subscription Create/Get/List
are supported; Delete returns Unimplemented, and other native services are not registered.
This does not enable the CLI, Atom, AMQP JWT, OAuth discovery or production use.

## Configuration

The listener is disabled unless **all four** arguments are supplied:

```sh
switchyard --mode development --namespace tenant \
  --storage fjall --data-dir /path/to/state \
  --admin-listen 127.0.0.1:8443 \
  --admin-tls-certificate /path/to/certificate-chain.pem \
  --admin-tls-private-key /path/to/private-key.pem \
  --admin-jwt-policy-file /path/to/offline-policy.json
```

These are file paths, not secret command-line values. The admin TLS identity is
explicit and independent of the AMQP TLS arguments. There is no plaintext admin
mode, even in development; ALPN must negotiate `h2`. TLS/key matching, the offline
policy and namespace configuration are validated before opening durable state.
Both sockets bind before either transport starts serving. A bind failure closes
any earlier socket and shuts down the timer and broker under the shutdown budget.
Existing production TLS, SAS, storage and unavailable-replication guards remain.

The [offline JWT policy](../../docs/offline-jwt.md) supplies pinned public RSA keys,
issuer, deployment audience and local subject/scope/rights bindings. No signing
or discovery service is provided. Every accepted RPC independently verifies exactly one
`authorization: Bearer <JWT>` value against the current epoch and requires Manage
at the configured namespace root. Entity-scoped Manage is insufficient.

For `--namespace tenant`, the resource root is
`amqps://tenant.servicebus.windows.net`. A supplied namespace hostname is used
as the root host; its first DNS label binds the broker namespace. This authority
is independent of the HTTPS endpoint hostname and JWT deployment audience.
Request namespaces cannot change it. Neither a TLS connection nor a prior
successful call caches authorization.

## Requests and readiness

Use the generated `admin-api::wire::entity_service_client::EntityServiceClient`
with a trusted CA and matching TLS hostname. A successful authenticated
Get/List RPC proves readiness; a bound socket, TLS handshake or log line alone
does not. Keep private keys and tokens out of logs and process arguments.

Create/Get/List retain the [native entity contract](../admin-api/README.md):
explicit subscription topic/name components, optional setting presence,
namespace/kind-bound opaque continuations, and empty continuing pages. Follow
continuations until absent, even when a page contains no entities. Pages are
bounded owner turns, not cross-page snapshots. Responses do not invent runtime
counters, capacity or placement.

Missing/malformed/expired credentials are Unauthenticated; verified insufficient
rights/scope or a foreign request namespace is PermissionDenied. Invalid entity
requests/configuration are InvalidArgument, duplicates AlreadyExists, missing
entities NotFound, and unsupported settings/Delete Unimplemented. A stopped
broker is Unavailable; clock-order refusal is FailedPrecondition. Unexpected
backend failures remain Internal without exposing backend text.

Retained RPC originals are bounded to 1024 across the entire listener,
including operations waiting for the broker's bounded channel. This is not a
per-connection allowance: cancelling streams or replacing TLS/h2 channels does
not reset it. Admission acquires capacity before spawning retained work; excess
calls return ResourceExhausted without spawning a backend task or waiting for
a slot. The retained task record holds its permit through backend completion and
the response handoff attempt until the original is actually joined, even on panic
or client disconnect. Completed-but-unjoined originals still consume capacity.
RPC, stream and connection families are reaped with rotating priority, so a
continuously ready family cannot starve the others. Retirement is checked atomically
with capacity admission and takes precedence over saturation. This limit bounds
accepted RPC work, not all transport sockets or unfinished request bodies.

## Lifecycle

SIGTERM/SIGINT or either accept-pump failure retires both listener admissions
before either protocol drain. This includes new RPCs on existing HTTP/2 peers.
An incomplete admin TLS handshake is retired; otherwise its admission timeout is
the existing `--admission-timeout-secs` (default 60).

The admin owner retains and joins its original connection, HTTP/2 stream and RPC
tasks. Once an RPC is accepted, client cancellation does not cancel its backend
work. A connected caller can receive the committed response during graceful
shutdown; a disconnected caller has no delivery guarantee and must not infer
failure means the operation was rolled back. No abort/drop is reported as
successful cleanup. The broker stays alive through both protocol drains, then
the timer stops and finally the broker joins and closes durable state.

One `--shutdown-timeout-secs` deadline (default 30) covers every stage. Repeated
signals join that deadline rather than resetting it. An overrun exits 3 without
claiming the unfinished tasks completed; an accept failure remains an error even
after cleanup succeeds. AMQP-only startup retains the same shutdown ordering.
