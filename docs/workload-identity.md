# Local workload identity foundation

The `auth` crate has two opt-in features:

- `workload`: bounded local policy and certificate inspection, without a TLS runtime.
- `workload-tls`: the policy plus a ring/rustls endpoint that authenticates the
  actual client stream and returns an opaque, endpoint-bound peer capability.

Default features remain empty. Existing SAS and offline JWT APIs are unchanged.
This foundation does **not** activate AMQP, WSS, native administration or Atom,
install certificates, provision identities, fetch issuers or enable production.
An integrating transport must enforce credential selection, operation admission,
socket retirement and original-task custody itself.

## Policy format

`WorkloadPolicy::from_json(json, namespace_root)` loads one immutable version-1
snapshot. The caller selects a namespace-root `ResourceScope`, independently of
endpoint DNS, HTTP Host, AMQP hostname, JWT audience and the workload trust domain.
The following public-root placeholder is intentionally not usable:

```json
{
  "version": 1,
  "trust_domain": "example.test",
  "roots_pem": ["<INVALID_PUBLIC_CA_CERTIFICATE_PEM_PLACEHOLDER>"],
  "bindings": [{
    "principal": "spiffe://example.test/ns/orders/sa/producer",
    "scope": "amqps://tenant.example/orders",
    "permissions": ["send"]
  }],
  "revoked_leaf_sha256": [],
  "client_auth": "required",
  "max_connection_seconds": 3600
}
```

There is one exact trust domain per profile. Every configured public CA pin is
authoritative only for that domain. There is no system root store or federation.
Bindings are unique full principals, each with one scope and unique
`send`/`listen`/`manage` rights. An empty binding list intentionally denies every
certificate. Rights come only from these local bindings. Manage includes Send
and Listen, never Audit or Cluster. Existing `ResourceScope` ASCII case folding
and path-segment hierarchy apply: an entity includes its descendants, not a
similarly prefixed sibling. Foreign namespace hosts refuse during policy load.

The certificate must contain **exactly one SAN GeneralName**, a URI of the form
`spiffe://<trust-domain>/<nonempty-path>`. Missing/duplicate SAN extensions,
duplicate URIs (even identical), multiple domains and mixed URI/DNS/IP SANs refuse.
The domain uses lowercase ASCII DNS-shaped labels without trailing dot, wildcard,
IDNA, port or userinfo. Path segments use ASCII letters/digits and `_-.`, preserve
case and cannot be empty, `.` or `..`. Percent escapes, backslashes, query,
fragment, whitespace and normalization aliases refuse. Full canonical original
URI bytes must equal the binding. CN and DNS names never supply workload identity.
This is a deliberately narrow SPIFFE-style local profile, not full SPIFFE/SVID
interoperability or compliance certification.

Unknown fields, duplicate decoded JSON member names, repeated principals, roots,
fingerprints or rights, unsupported versions and malformed/non-certificate PEM
refuse. Each root entry contains exactly one public CA certificate; private keys,
URLs and additional policy file lookups are not accepted. The policy loader checks
certificate structure/date intervals; TLS preparation checks actual current time.

| Boundary | Limit |
| --- | --- |
| Policy | 256 KiB; depth 8; 8192 JSON nodes |
| Roots / bindings / revoked leaf fingerprints | 1–8 / 0–64 / 0–256 |
| Principal / scope | 512 bytes / 2 KiB |
| Certificate DER / peer or server chain | 16 KiB each / 8 certificates and 64 KiB total |
| Private-key PEM | 32 KiB |
| ALPN | 0–8 nonempty values, each at most 255 bytes |
| Admission timeout | Nonzero, at most 60 seconds |
| Maximum connection lease | 1–3600 seconds; default 3600 |

`client_auth` defaults to `required`. Explicit `optional` still requests and
verifies presented certificates; absence yields **no peer proof**. A consumer
using optional mode must require an independently configured alternative
credential. A presented invalid, unbound or revoked certificate always fails
admission; it cannot fall back to a token or an identity header.

## Actual TLS proof

`PreparedWorkloadTls::new(policy, server_chain_pem, server_key_pem, alpn, timeout)`
constructs its own immutable rustls configuration and restrictive client verifier.
It accepts no caller-selected verifier or assertion of prior authentication.
Its read-only `namespace()` and `requires_client_certificate()` metadata let
consumers check listener binding and reject optional mode without another
configured credential source before serving.
TLS 1.2/1.3 chain, signature, path and client-usage validation use rustls/webpki;
CertificateVerify must prove possession of the client private key. The maintained
`x509-parser` supplies full-consumption DER/unique-extension/SAN/date/KU/EKU
inspection, not an alternative signature verifier. The non-CA leaf must have
digitalSignature KU and explicit clientAuth EKU, without any-EKU. Supplied
intermediates and pinned roots must be CA/keyCertSign certificates. Duplicate
extensions, malformed DER and trailing bytes refuse.

`accept_before(owned_stream, original_deadline)` runs the real TLS handshake
against the earlier of the original deadline and the endpoint timeout. The future
owns the stream and spawns no task. Cancellation drops that stream; this is not
a joined consumer shutdown receipt. `AdmittedWorkloadStream` keeps the resulting
stream and optional proof together, implements asynchronous IO and exposes
negotiated ALPN, but not the underlying rustls connection or raw certificates.

Only completed verified handshakes mint `VerifiedWorkloadPeer`. Its fields and
constructor are private; it is not deserializable or constructible from DER,
headers, a parser result, a boolean or an arbitrary grant. A cloned endpoint
shares instance identity; a separately prepared endpoint, even with identical
policy bytes, rejects the old peer as `WrongEndpoint`. Consumers must preserve
this socket/proof association when adapting streams or installing trusted request
extensions. The capability proves a completed handshake, not that its socket is
still open, and must never be rebound to a different peer's stream.

`endpoint.authorize(peer, resource, permission)` checks instance provenance,
current validity, local scope and rights. It does not create an `AccessGrant`,
interpret token claims or alter JWT/SAS refresh behavior. Errors are fixed safe
categories; debug output excludes certificates, policy contents and principals.

Server session storage, tickets (including ticket requests), early data and
half-RTT data are disabled. Configuration is not exported for callers to replace
these settings. Cached sessions cannot mint renewed authority; every admission
requires a fresh handshake. Clients must still verify the server certificate
chain and hostname normally, using their independently configured server trust.

The endpoint sets rustls's outgoing application-write buffer limit to 64 KiB.
This setting does not limit internally generated handshake messages and is
**not** a total handshake-memory limit. The locked rustls 0.23.42
deframer separately limits joined handshake messages/buffering to 65535 bytes;
the profile's chain/count/DER limits are applied in the verification callback,
after rustls has decoded the message. No new handshake-allocation limit is
claimed. There is no global connection admission bound in this library: callers
must bound concurrent accept futures and retain/join their own connection tasks.

## Expiry and consumer obligations

Dates use checked nonnegative epoch values and zero skew: `not_before <= now <
not_after`. The effective certificate interval intersects the leaf, **all supplied
intermediates**, and **all configured roots**. The conservative earliest-root
bundle rule also applies when a different root could verify this peer. An expired
overlap pin must be removed before admitting new clients with that profile.

After handshake completion, the endpoint rechecks validity and fixes a monotonic
lease to the earlier of the remaining certificate interval and the configured
connection cap. `lease_deadline()` is this effective deadline;
`expires_at_epoch_seconds()` is the certificate/chain/bundle wall limit and can
be later. Cloning a peer shares the same lease and sticky invalidation.
`ensure_live()` reads actual wall and monotonic time; callers cannot supply a
fake time or freshness assertion. Expiry, a regression before not-before, or clock
unavailability invalidates the peer; later clock changes cannot resurrect it.
A rollback within the valid wall interval cannot extend the fixed monotonic cap.

`wait_until_expired()` is a task-free future for the existing connection owner's
select loop. It wakes at the monotonic deadline and rechecks wall time at most
every second, including idle connections. It does not itself close sockets,
retire AMQP links/HTTP streams, cancel accepted work or own a shutdown budget.
IO forwarding alone does not enforce application authorization.

Consumers must check live authority immediately before admitting an operation,
cap later upgrade/protocol stages by the peer deadline, retire affected admission
and I/O on expiry, and preserve already begun originals through their normal
drain. A token refresh or successful RPC cannot renew this TLS lease; renewal
requires another verified connection. A capability is not permission to bypass
future operation checks or discard a begun commit. Those transport integrations
are separate from this foundation.

## Restart-only trust rotation and local revocation

Preparing or editing policy JSON does not mutate any old endpoint, new handshake
on that endpoint, or retained peer. There is no reload service, watcher, signal,
OCSP, CRL processing, issuer lookup or online revocation guarantee.

Activate changes by retiring and joining the old transport owners/process, then
preparing replacement listeners with the new snapshot. Root overlap can progress
from old, to old+new, to new only. After activation the removed root cannot admit
clients, and old peers cannot authorize against replacement instances. The
embedding server must ensure old connections do not continue serving; merely
constructing a replacement endpoint does not retire them. No certificates or
grants are transferred into the replacement. A failed candidate serves nothing.

`revoked_leaf_sha256` contains unique lowercase 64-character hexadecimal SHA-256
digests of exact leaf DER. Denial activates with the replacement snapshot. A
reissued leaf has a different digest; remove the principal binding to deny all
its certificates. This is not serial-only or public-key revocation. Before
activation the old policy remains effective, subject to its existing leases.
Do not describe a file edit or library object replacement alone as completed
revocation, zero-downtime rotation or consumer drain. Policy changes do not undo
already committed operations.

## Dependency profile

The optional parser is pinned to `x509-parser` 0.18.1 (MIT OR Apache-2.0,
declared MSRV 1.67.1), with signature-verification features disabled. The runtime
uses the existing ring/rustls family, not OpenSSL/system TLS or AWS-LC. The locked
parser graph includes `time` 0.3.54 (MSRV 1.88), below the workspace's pinned
Rust 1.97.1. Disabled workload features add no workload-specific parser or PEM
dependencies. Rustls and Tokio remain required by the separate OIDC client;
enabling workload TLS reuses them. Synthetic fixture signing is test-only and provides no runtime
signing or certificate-provisioning interface.
