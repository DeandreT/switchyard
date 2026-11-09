# Server security and native entity administration

The server can explicitly enable the existing tonic 0.14 EntityService over
rustls TLS and HTTP/2. It uses the real single-owner broker and the same
memory/Fjall state as AMQP. Queue, topic and named-subscription Create/Get/List
are supported; Delete returns Unimplemented, and other native services are not registered.
This does not enable Atom, OAuth discovery or production use. The native CLI is
configured separately.

## Offline AMQP JWT

The AMQP listener separately accepts `--jwt-policy-file /path/to/amqp-policy.json`
with `--tls-certificate` and `--tls-private-key`. It supports trusted TLS TCP and
`--transport amqp-websockets`. This policy is independent of
`--admin-jwt-policy-file`; enabling either does not enable the other. Native
administration can seed entities but is not an AMQP authorization mechanism.

The file contains the [offline RS256 profile](../../docs/offline-jwt.md), not a
generic Azure/OIDC access token configuration. The server reads at most 64 KiB
plus one byte and rejects invalid, oversized, unreadable or non-UTF-8 policy files
with a fixed error that excludes file contents and paths, before opening durable
state. Plaintext JWT activation refuses before policy I/O. Policy changes require
a restart; there is no network discovery, signing or key-provisioning service.

JWT-only configuration needs no SAS rule. Supplying both existing shared-access
arguments composes SAS and JWT on the same listener; supplying only one still
refuses. Without `--jwt-policy-file`, the existing SAS-only behavior is unchanged.
Neither profile bypasses production storage or unavailable-quorum guards.

Clients must validate the actual endpoint certificate chain and hostname. CBS
uses exact token type `jwt`, with `name` identifying an AMQPS namespace/entity
resource. That resource, the policy's deployment audience, and an SDK credential's
token-acquisition scope are distinct. Locally bound Send/Listen/Manage rights
govern data and management operations, including retained links and refresh.

The opt-in `declared_current_offline_jwt_credential_wss_on_spawned_server` test in
`admin_tls_process` selects only the declared-current official SDK's custom
`TokenCredential` workflow. It requires Linux, .NET 10 and a locked NuGet restore;
missing prerequisites fail when selected. It uses a token file, child-scoped
`SSL_CERT_FILE` trust and normal platform hostname verification, retaining SDK
loaded-file/start/completion custody through the existing SDK harness.

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
All configured sockets bind before any transport starts serving. A bind failure closes
any earlier socket and shuts down the timer and broker under the shutdown budget.
Existing production TLS, authentication, storage and unavailable-replication guards remain.

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

SIGTERM/SIGINT or any accept-pump failure retires all configured listener admissions
before any protocol drain. This includes new RPCs on existing HTTP/2 peers.
An incomplete admin TLS handshake is retired; otherwise its admission timeout is
the existing `--admission-timeout-secs` (default 60).

The admin owner retains and joins its original connection, HTTP/2 stream and RPC
tasks. Once an RPC is accepted, client cancellation does not cancel its backend
work. A connected caller can receive the committed response during graceful
shutdown; a disconnected caller has no delivery guarantee and must not infer
failure means the operation was rolled back. No abort/drop is reported as
successful cleanup. The broker stays alive through all protocol drains, then
the timer stops and finally the broker joins and closes durable state.

One `--shutdown-timeout-secs` deadline (default 30) covers every stage. Repeated
signals join that deadline rather than resetting it. An overrun exits 3 without
claiming the unfinished tasks completed; an accept failure remains an error even
after cleanup succeeds. AMQP-only startup retains the same shutdown ordering.

## Atom HTTPS

Atom transport is a separate opt-in HTTP/1 TLS listener. Supply all four options;
partial configuration refuses before opening durable state:

```sh
switchyard --mode development --namespace tenant \
  --atom-listen 127.0.0.1:8444 \
  --atom-tls-certificate /path/to/certificate-chain.pem \
  --atom-tls-private-key /path/to/private-key.pem \
  --atom-jwt-policy-file /path/to/offline-policy.json
```

Certificate-chain, key and offline JWT policy reads are bounded to 64 KiB,
16 KiB and 64 KiB respectively. TLS identity and policy validation precede store
open; no URL, key or bearer value is a command-line argument. These options do
not enable AMQP JWT or native h2, nor bypass the existing production guards.
All configured sockets bind before any serving family starts.

The server installs only closed routes, with no broker handle or XML parser.
After authorization, a definition-singleton GET with explicit `enrich=False`
returns 501 (adapter unavailable); ambiguous collections/runtime profiles and
unsupported writes return 400. Unknown/duplicate query options and unsupported
versions refuse. These responses do not establish administration readiness,
CRUD, rules, runtime metadata or SDK administration compatibility.

Each request independently requires exactly one Bearer credential, validated
against the [offline policy](../../docs/offline-jwt.md) at the current time.
Missing/invalid credentials return 401; insufficient scope/rights return 403.
Namespace collections require namespace Manage; entity/subscription addresses
require Manage covering that canonical resource. Authorization precedes profile
selection, trusted handler callbacks and body polling. HTTP Host never selects
the namespace authority; resource scope uses the configured AMQPS namespace host,
independently of the endpoint TLS hostname and JWT deployment audience. Clients
must verify the endpoint's real certificate chain and hostname.

The fixed startup limits are 128 TLS connections and 128 accepted request
originals listener-wide, 16 KiB/64 headers, and 256 KiB body/reply payloads.
Handshake/header/body deadlines are 10 seconds, the request deadline is 30 seconds,
and the whole connection is capped at 60 seconds from accept. Each TLS connection
serves one HTTP/1.1 request; keep-alive and upgrades are disabled. The reusable
Rust API validates configurable limits, but no CLI limit surface is exposed.
Bodies, when a trusted adapter opts in, must be bounded UTF-8 Atom content;
compressed content, unsupported framing/types and trailers refuse.

Excess accepted requests return 503 without waiting or spawning a callback;
connection admission pauses at its bound. Body timeout/oversize return 408/413,
oversized headers 431, and oversized handler replies a fixed 500. A wire deadline
can return 504 with an unknown-outcome warning; it never cancels begun work or
returns its slot. Completed and panicked originals remain charged until actual
join. Request and connection reaping alternate priority.

Retirement immediately closes new TCP/request admission, including on existing
TLS peers. Cleanup then joins original connections and accepted callback work
alongside AMQP/native h2 under the one process shutdown budget. Cancellation of
a cleanup borrower preserves custody for another wait. Deadline expiry is not
a successful drain or proof that a mutation rolled back. The broker and timer
outlive protocol cleanup; neither disconnect nor wire expiry permits premature
store reopen.
