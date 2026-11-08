# Native entity RPC library

`wire` contains tonic/prost 0.14 client/server bindings generated from the
authoritative `proto/switchyard/admin/v1/admin.proto`. System `protoc` is required
at build time. Output stays in `OUT_DIR`; protobuf field numbers are unchanged.
The existing typed `EntityService<B>` API remains separate and transport-free.

`rpc::EntityRpc` adapts Create/Get/List to any `EntityBackend`. Construct it with
the typed service, an offline `auth::JwtPolicy`, a namespace-root `ResourceScope`
and a backend error classifier. `into_service()` returns only the generated
entity server. This crate starts no listener, registers no other services, and
does not configure TLS, shutdown or the CLI. Delete is unimplemented.

Every unary call, including Get/List and the reserved Delete, requires exactly
one `authorization: Bearer <JWT>` metadata value. The offline policy verifies the
token at the current epoch on each call and requires Manage at the configured
namespace root. Use `amqps://namespace-host` for this resource scope even when
the eventual transport is HTTPS. The JWT deployment audience is a separate
policy value; neither the HTTP host nor a request path selects authority.
Entity-scoped grants cannot authorize this service. A valid credential cannot
dispatch requests for a namespace other than the typed service's bound namespace.
There is no discovery, signing, SAS mode or connection authorization cache.

Missing, malformed, duplicate, expired or otherwise invalid credentials return
Unauthenticated. Verified insufficient rights/scope or foreign namespaces return
PermissionDenied. Request validation returns InvalidArgument, unsupported options
and Delete return Unimplemented, and missing entities return NotFound. Backend
classifiers return a finite `BackendErrorKind`, not a status or arbitrary text;
messages and details never include backend errors. `InternalBackendErrors` is
the safe default for unclassified failures. Owner-specific domain validation,
duplicate and clock ordering remain the backend's responsibility.

Wire adapters retain optional-field presence, including explicit zero/false,
check size conversions and refuse unknown enums and mixed/incomplete addresses.
Subscriptions use explicit topic/name components, not a composite input path.
Legacy duplicate-history zero selects the queue default and is omitted for
topic/subscription requests. Placement, capacity and usage remain unavailable;
responses do not fabricate values. Nonempty deprecated textual page tokens
refuse. Opaque versioned local continuations round-trip unchanged, including on
empty continuing pages. They are not durable cursors or cross-page snapshots.

Runtime codec is `tonic-prost`; build generation is `tonic-prost-build`. Tonic's
explicit transport/router/codegen and ring TLS features use the workspace's
rustls 0.23 / tokio-rustls 0.26 family, without enabling an AWS-LC provider. Direct
tonic crates are pinned to 0.14.9 (MIT), prost to 0.14.4 (Apache-2.0), and the
resolved graph is pinned in `Cargo.lock`.
