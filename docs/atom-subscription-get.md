# Ordinary subscription definition Get

The opt-in Atom HTTPS listener installs one read-only adapter. A request must be
HTTP/1.1 GET to `/topic/Subscriptions/name`, with `api-version=2021-05` or
`api-version=2024-05` and explicit `enrich=False`. Each request requires offline
JWT Manage covering the canonical subscription; namespace and parent-topic grants
also cover it. HTTP Host cannot change the configured namespace resource host.
TLS endpoint identity, AMQPS resource scope and JWT deployment audience are distinct.

Authorization precedes profile selection, trusted callbacks, body polling and
all backend reads. The adapter privately uses `EntityService<B>::get`, not the
native namespace-root-only RPC authorization wrapper. The real backend runs the
complete subscription Get in one broker-owner turn without Clock, writes,
snapshot acquisition or an independent reader. It validates membership, parent,
live owner, inherited backing configuration and the real DLQ. That is selected
read integrity, not a certification of unrelated entities, rules or messages.

## Input and closed routes

The installed handler selects `BodyMode::Empty`. Absent Content-Length or zero
means empty HTTP input; no XML parser exists in this adapter. Positive length,
every Transfer-Encoding (including chunked-empty/trailers), and Content-Encoding
refuse before body polling and the owner read. Oversized declared length retains
413 priority; smaller nonempty/encoded bodies return 415. DTD/entity, non-UTF-8
or compressed payload bytes cannot introduce a parsing/resolver path.

Hyper rejects differing Content-Length values. Its existing CL+TE normalization
selects chunked framing; empty-body mode still rejects Transfer-Encoding rather
than pretending to detect raw headers removed by Hyper. One request per TLS
connection prevents a pipelined second request from dispatching. An empty request's
Content-Type is not a document parser or another profile selector.

All other requests delegate to the closed contract: queue/topic definition Get
remains 501, collections and missing/True enrich runtime selectors remain 400,
and writes remain 400. Rules/namespace routes stay unsupported with their existing
scope checks. Unknown/duplicate query keys, unsupported versions and invalid
canonical paths do not fall back to another profile. Keyword entity/subscription
names `rules` and `subscriptions` retain their domain/structural meanings.

## Output

Success is 200 with `application/atom+xml; charset=utf-8`, exactly one Atom entry,
not a feed. Entry/title/content use `http://www.w3.org/2005/Atom`; title has
`type="text"` and the canonical subscription name, and content has
`type="application/xml"`. SubscriptionDescription and all six ordered children
use `http://schemas.microsoft.com/netservices/2010/10/servicebus/connect`:

| Child | Value and selected representability |
| --- | --- |
| LockDuration | Actual lock milliseconds as an exact XSD duration, 5,000..300,000 ms. |
| RequiresSession | False, justified by the ordinary subscription/backing invariant, not coercion of session-aware state. |
| DefaultMessageTimeToLive | Actual finite milliseconds, 1,000..922337203685477 ms; None uses explicit `P10675199DT2H48M5.4775807S`, the pinned client's unlimited TimeSpan.MaxValue sentinel. |
| DeadLetteringOnMessageExpiration | Actual stored true/false, not a default substituted for false. |
| DeadLetteringOnFilterEvaluationExceptions | Actual membership/profile true/false. |
| MaxDeliveryCount | Actual decimal value, 1..2147483647. |

Durations use integer day/hour/minute/second/millisecond arithmetic without
rounding. The finite TTL maximum is floor(i64::MAX ticks / 10,000). Valid local
profiles outside the selected client range return unsupported 501, not clamped
values, corruption or absence. Domain-invalid selected state is corruption.

There are no counts, usage, capacity, placement, private generations, ETags,
timestamps, id/link URLs, forwarding, status, user metadata or rule descriptions.
Unexpected supplied top-level availability fields refuse instead of being
serialized or silently treated as unknown extensions. A returned wrong kind,
namespace or path is internal failure, not a queue/topic fallback.

The MIT `quick-xml` writer is pinned to 0.42.0 without optional features. Static
element/attribute names and escaped validated scalars are written into a 4 KiB
sink that checks remaining space before extending. Intermediate scalar text is
bounded, and the sink allocates only its fixed allowance. The complete result
still passes the transport's reply bound before any wire output; failure never
returns a truncated successful entry. No handwritten XML markup or raw backend
fragment is interpolated. The library's `HttpsListener::new` stays closed; only
the server's existing opt-in installation chooses this handler.

## Errors and lifetime

Adapter-produced errors use `application/xml; charset=utf-8` and a fresh,
no-namespace Error with Code matching HTTP status and one fixed Detail:

- 404: healthy selected absence, including data that exists only in another namespace.
- 500: selected corruption/storage failure or inconsistent backend result.
- 503: stopped backend.
- 403: foreign/misbound namespace.
- 400: invalid typed request.
- 501: unsupported/unrepresentable profile.

There are no backend Display/Debug messages, keys, credentials, bodies, fabricated
Azure SubCodes or tracking identifiers. Existing transport/auth/deadline and
closed-route errors retain their fixed plain-text status/header behavior.

The existing task families own the entire callback through actual join, including
panic and failed response delivery. Disconnect or wire deadline does not cancel
a begun Get or refund its slot. Reaping, all-socket startup order, admission
retirement and the one shared ShutdownBudget are unchanged. Protocol cleanup
joins before timer/broker stop and durable reopen; expiry is not a drain.

## Fixture and client boundaries

`crates/conformance/atom-fixtures/profiles/subscription-get.rng` is the selected
six-field, entry-only shape; it does not broaden the original five-field #10
definition grammar or activate collections. Structural grammar alone does not
enforce transport resource limits or client scalar ranges. The test-only
roxmltree 0.21.1 validator uses a 4 KiB input cap, DTD disabled, 64-node bound,
four-element depth bound and expanded-name/attribute/order checks.

The real Memory/Fjall HTTPS tests check deterministic response bytes against the
new XML fixtures and can emit bounded status/media/body JSON with
`SWITCHYARD_ATOM31_CAPTURE_ROOT`. The standalone .NET `subscription-get` mode
requires those fresh captures; it never falls back to a static fixture. It keeps
the existing declared-current/previous pins, loaded-file/nonces/start-completion
custody and hashes every capture it reads. Those in-memory request/response
controls are not SDK network execution, Sift evidence or administration
compatibility certification. Mutable CRUD, collections/runtime, Topic endpoints,
rule actions and the wider administration client gate remain separate work.
