# Compatibility

Switchyard targets Azure Service Bus Standard. This describes single-node `main`,
not reference branches; core/protocol/client gates establish tested behavior,
not compatibility certification.

## Client Gates

| Client | Evidence |
| --- | --- |
| Official .NET | Four opt-in experimental Memory TCP/WSS gates: declared-current ServiceBus 7.21.0 / Core 1.62.0 and previous 7.20.2 / 1.60.0, not latest. Isolated locked graphs/roots; approved ServiceBus/Core, output and nonce-bound loaded-file bytes; completion after async disposal. |
| Rust AMQP | Broader protocol end-to-end suites, not official-SDK substitutes. |
| Pinned Sift | Source-only Atom fixture pin; no execution gate. |

.NET gates cover queue/batch send, prefetch/settlement, receive-delete, envelopes,
renew/abandon/defer/peek, DLQ, sessions/state, duplicates, scheduling/cancellation,
filtered subscriptions, rules and case-insensitive addresses. Linux/.NET 10/NuGet
restore are required; missing prerequisites fail. Workspace runs ignore these
workflows; durable coverage is pending. TCP uses a permissive generated-identity
callback; WSS instead uses platform trust and child-scoped `SSL_CERT_FILE` root,
without machine-wide changes. Neither certifies production trust, administration
or whole-test task-tree shutdown. See [selectors/limits](sdk-gates.md),
[TCP](../crates/server/tests/amqp_dotnet_current.rs),
[WSS](../crates/server/tests/amqp_dotnet_websockets.rs) and
[declared project](../crates/conformance/dotnet-current/Switchyard.Conformance.DotNetCurrent.csproj).

Source-derived [Atom profiles](../crates/conformance/atom-fixtures/README.md)
record indistinguishable definition/runtime collection requests and closed XML
shapes. The in-memory .NET controls exercise request construction and synthetic
response parsing, not a running endpoint. Atom remains unimplemented; ambiguous
collections require refusal until truthful combined runtime data is available.

## Implemented Surface

| Area | Contract |
| --- | --- |
| Transport/auth | AMQP 1.0 TCP/TLS; binary WSS `/$servicebus/websocket` (`AMQPWSB10`/`amqp`); development-only plaintext. TLS precedes AMQP; SASL PLAIN or ANONYMOUS/`MSSBCBS`, then CBS SAS. |
| Rights | Namespace/entity Send, Listen, Manage (includes both). Send for scheduling/cancellation; Listen for receive management. CBS grants are connection-wide, expire links, and allow 20s initial authorization. Refresh replaces rights/validity only for the same verified principal and scope; checks and expiry waits honor the full validity interval. |
| Delivery | Singular/atomic batch send; peek-lock (unsettled) or receive-delete (pre-settled); independent out-of-order settlement; bounded prefetch/drain. Credit is reserved before mutation, empty results release it, and consumption/release completes drain. |
| Envelope | Durable AMQP body/forms, identifiers, properties, annotations, application properties/footer; copy-local property patches; broker-authoritative delivery overlays. |
| Management | Renew, abandon, defer/retrieve, ordered peek, schedule/cancel, custom dead-letter and DLQ receive/complete. Unsupported requests reject with compatible conditions/retry hints. |
| Sessions | Named/next-available attach echoes identifier/deadline; exclusive locks, opaque state, release on close. Renewal/state use `$management`, not automatic renewal. |
| Duplicates | Exact non-empty message IDs across immediate/batch/scheduled offers on enabled ordinary queues and topics; bounded history expiry. Topic enablement/window are creation-only (default off/10 minutes, inclusive 20 seconds–7 days). |
| Profiles | Domain-only partial ordinary-queue updates atomically validate/change parent and exact DLQ profiles plus Clock. No-op patches write nothing; owner, records/deadlines remain. Session/duplicate modes are immutable; lock/TTL/message-size/history/expiry-policy changes govern future work. |
| Topic profiles | Domain-only parent-topic size/TTL and named-subscription lock/delivery-count/TTL/expiry-policy/filter-error-policy patches prove all listed children before changing profiles. Size propagates to backing queues/DLQs; topic TTL does not overwrite child TTL. No-op direct calls preserve Clock; no administration or protocol activation. |
| Topics/rules | Non-session immediate/scheduled singular/batch fanout; placeholder peek/cancel; independent subscription queues. Durable `$Default`, actionless true/false/correlation, typed equality; create/delete/paginated AMQP rules. Bounded [SQL](sql-predicates.md) filters over the stored typed projection: `add-rule` stores any compilable `sql-filter` expression verbatim and refuses others with 400 and the compile error; `enumerate-rules` returns them as `sql-filter` with the exact text and compatibility level 20. The SDK expressions `1=1`/`1=0` stay true/false filters. Non-empty `sql-rule-action` refuses. |
| Time/identity | Bounded activation/lock/TTL/session-lock/history expiry. ASCII-folded namespace/entity/subscription addresses and SAS scope; session/placement IDs retain case. |
| Local APIs | [Offline RS256 JWT](offline-jwt.md): pinned keys, injected time/local rights, issuer-qualified grants; SAS/PLAIN retain verified namespace-host qualification. [SQL kernel](sql-predicates.md): bounded local grammar/errors, IN/LIKE/scalar arithmetic. No JWT wire activation or SQL actions. |
| Storage | Paired Memory/Fjall semantics; Fjall journal fsync before applied outcomes; exclusive directory ownership. |

Library-only administration Get/page reads select one namespace and validate
canonical public queue/topic/subscription rows, live owners and required profiles.
Internal queues are excluded; selected corruption, including lookahead, rejects
the whole page. Pages return at most 100 entries and consume at most 256 raw rows
plus one lookahead, in queue/topic/subscription key order. Namespace/kind-bound
opaque cursors can continue an empty page through internal rows. Callers must
exclude all writers during each complete read; there is no cross-page snapshot,
runtime-statistics estimate or global health proof.

The typed `admin-api::EntityService` binds a canonical namespace and the server's
`AdminService` adapter uses the existing bounded broker owner. Create/Get/List
support queues, topics and explicit subscriptions; subscription addresses carry
parent and name separately. Foreign namespaces, reserved/malformed addresses and
wrong-scope cursors refuse before dispatch, even after the owner stops. Each Get
or complete bounded page occupies one owner turn without host or persisted Clock
access, writes or a separate store writer. Raw store users must still exclude
writes while the owner is active. Continuations do not provide cross-page snapshots.

Create uses existing owner-stamped commands, without existence preflights or early
domain configuration validation, preserving clock/duplicate/domain error priority.
Success requires the exact created outcome, including the subscription path.
Supported omitted settings select domain defaults (unlimited TTL, 60 s locks,
10 deliveries, 256 KiB queue/topic message size); explicit zero limits refuse.
Duplicate-history zero retains its 600000 ms default. Returned profiles are
effective domain settings, not usage estimates. Placement, usage and the top-level
entity-size field are absent, and requests for them refuse. A queue profile carries
its domain capacity mode, so a domain-created finite queue returns
`capacity: finite` in its profile while the top-level size stays absent until
[#28](https://github.com/DeandreT/switchyard/issues/28). Message size is not entity capacity;
subscriptions inherit it from their topic. Topic sessions/duplicates and independent
subscription message limits remain unsupported. Errors preserve typed catalog,
domain, storage, clock, unexpected-outcome and stopped-owner information.

The channel remains bounded to 1024 requests with asynchronous admission
backpressure. Cancelling an unadmitted request does not enqueue it; dropping a
reply after admission does not undo Create. Stop remains a queued request: work
ahead completes, work behind may observe a stopped owner. No stronger drain or
shutdown guarantee is implied. The additive protobuf profile fields preserve
existing numbers and legacy duplicate-history zero semantics; presence on scalar
profile/availability fields distinguishes omission from explicit values. Its
contract does not activate a network administration API: HTTP/OpenAPI, generated
gRPC handlers/listeners, startup wiring, authentication, CLI, Update/Delete, rules,
placement/capacity mutation and runtime counters remain outside this surface.

Selected operational queue-profile reads validate after decode/owner proof,
including backing queues/DLQs; invalid limits are corruption. Unread rows/raw
catalogues are not certified; profile-free legacy operations remain unchanged.
Typed content (#16) and actionless SQL filters (#20) are complete. Administration
setters/capacity, JWT activation [#17](https://github.com/DeandreT/switchyard/issues/17),
and bounded SQL actions (#21) remain pending. Grant consumers preserve verified
principal identity and both validity bounds without activating JWT on the wire.

### Absolute link addresses

Data and entity-management links accept relative entity paths or absolute
`amqps://<configured-host>/<entity>` addresses. Absolute addresses are normalized
against the listener's configured shared-access audience, not a peer-supplied
Open or SASL hostname. Queue, subscription, dead-letter, and management paths then
use the same canonical entity identities and authorization checks as relative
links. Without a configured audience, only relative addresses are accepted.

Foreign authorities, user information, explicit ports, queries, fragments,
control characters, dot-segment rewriting, and escaped path separators are
refused. Valid URI escapes are decoded once, consistently with SAS scopes. The
low-level `parse_attachment` API accepts relative paths; callers with absolute
input must first use `normalize_link_address` with a trusted authority.

Session receiver filters accept the legacy raw string/null form and recognized
described forms of `com.microsoft:session-filter`. Symbolic descriptors, the
historical Service Bus numeric descriptor, and the vendor/descriptor numeric
encoding are recognized; other descriptors and invalid enclosed values are
refused. The granted session ID preserves a recognized described wrapper in
the attach response.

## Known Differences And Bounds

| Behavior | Current contract |
| --- | --- |
| Session ID on ordinary queue | Refused (Azure ignores it); only session queues promise FIFO. |
| Session settlement | Live message token/deadline suffice after session-lock loss; Azure also requires session ownership. |
| Next session | At most 32 candidates; all held returns unavailable/retry, not a full walk. |
| Lock tokens | Message and session holds share the selected queue's token counter. MAX-1 is the last fresh token; MAX refuses new holds with nonretryable `LockTokenExhausted` (`amqp:resource-limit-exceeded`, management 403). Empty/no-selection and receive-delete operations need no token; named empty session acquisition does. Existing holds and expiry remain usable. |
| TTL | Queues and subscriptions carry `dead_lettering_on_message_expiration`, defaulting on (Azure defaults off). On, expiry dead-letters as `TTLExpiredException`; off, it deletes the message and its indexes with no DLQ copy. Either applies on sweep, receive, abandon, lock expiry, deferred retrieval and activated schedules; delivery-count and application dead-lettering are unaffected. The setting is domain-only: administration and AMQP still create entities with the default. Expiry outcomes count deletions separately from DLQ copies, and the timer treats both as work for its paging and idle checks. Live locks remain settleable until expiry. |
| Duplicate gaps/history | Suppression consumes acknowledgement sequence slots, stores no message. History starts at command time; hits do not extend it, nor settlement/cancellation/expiry/DLQ erase it. Missing raw-AMQP IDs bypass it. |
| Scheduled topics | Children/rules snapshot at activation: later subscriptions may receive older publications. All-scheduled sends do not prove topology early. |
| SQL filter errors | Azure dead-letters a message whose subscription filter throws by default; Switchyard's specific behavior is its own choice, not verified against Azure. When no rule of a subscription matches and a SQL rule fails to evaluate (type mismatch, unsupported or ambiguous property, overflow, budget), the subscription gets one dead-lettered copy with reason `FilterException` and description `rule <name>: <fixed error text>` for the first failing rule in name order. Any matching rule still delivers despite a failing sibling. Ordinary, batch and scheduled fanout behave alike. |
| Filter-error discard | Subscriptions carry `dead_lettering_on_filter_evaluation_exceptions` (Azure `DeadLetteringOnFilterEvaluationExceptions`), defaulting on as in Azure. Off, the copy a filter error would dead-letter is discarded: nothing is written for that subscription and its DLQ is not reported as populated. It is set at creation and changed by subscription profile updates, independently of `dead_lettering_on_message_expiration`; a change governs later fanout and activations only. Domain-only: administration and AMQP still create subscriptions with the default. |
| Filter identifiers | New AMQP sends expose message/correlation IDs to SQL and correlation filters as unchanged strings, invariant decimal ulong, lowercase hyphenated UUID or uppercase hexadecimal binary. Retained V1/V2 projections keep legacy UUID debug text and lowercase binary text; duplicate-detection identities remain legacy regardless of projection version. These forms follow Azure SDK source, not live Azure filter-service verification; see the record-format contract below. |
| SQL rule quota | At most 2,000 SQL rules across all subscriptions of a topic (Azure's quota); the next SQL rule refuses with `SqlRuleLimitExceeded`. True/false/correlation rules keep only the per-subscription limit. |
| Topic sequences | Each offered publication and selected due placeholder consumes one topic-owned slot, even with no matching subscriptions; fanout copies consume none. MAX-1 is the last fresh slot and MAX refuses with nonretryable `SequenceNumberExhausted`. Crossing batches or bounded activation sets retain every original record, counter and Clock without partial fanout. Idle activation and cancellation need no slot. |
| Browse/deferred | Positive signed peek count, reply cap 250, scan to cap/end; deferred retrieval atomically accepts at most 32 sequences in caller order. |
| Outbound message size | A receiving peer's nonzero link `max-message-size` bounds each rendered transfer; absent, zero or the maximum value is unlimited. An oversized delivery is never sent: the link detaches with `amqp:link:message-size-exceeded` and the message lock is left to expire, not dead-lettered at once. Each refused lock counts a delivery, so a message whose lock expires at the queue's max delivery count dead-letters with `MaxDeliveryCountExceeded`, including staged receive-and-delete and receive-by-sequence locks. Receive-and-delete on a bounded link locks, measures, then deletes, so a refusal loses nothing; it costs an extra command per message. Peek returns the longest prefix that fits the reply link; receive-by-sequence is all or nothing. Either answers 403 `amqp:link:message-size-exceeded` when nothing fits, leaving any locks to expire. A reply link attached after its request was processed gets that error instead of an oversized reply. Receive-and-delete by sequence waits up to the reply-routing timeout for its reply link and otherwise answers 400 without deleting anything; peek-lock and peek need no attached reply link. |
| Entity capacity | Every queue profile carries a mandatory `capacity`: `non_finite` (every existing queue, DLQ shadow and subscription backing) or `finite { max_size_bytes }`, a whole MiB from 1 MiB to 5,120 MiB (Azure Standard accepts only 1–5 GB). Finite is accepted only on ordinary queues without sessions or duplicate detection; other finite combinations refuse. The mode is creation-only; a finite size may change. Topics are non-finite only. Finite sizes are domain-only recorded state: no admission charges or refuses retained bytes. Administration refuses capacity requests and creates only non-finite queues; get/list return the recorded mode inside the queue profile, but the top-level `max_size_bytes` field stays absent. |
| Service limits | 2,000 subscriptions/topic; separate topic/subscription name bounds; Standard default 256 KiB wire-message limit. Commercial namespace/operation quotas absent. |
| AMQP value shapes | Received frames and messages refuse as malformed when a map has an odd element count, an array uses a compound or described element constructor, a compound's size does not exactly match its counted contents, or nesting exceeds 64 levels; an empty array's element constructor is accepted only as the last encoded bytes of a performative or message section. Stored messages keep decoding without the exact-size, constructor-position and non-map compound-array rules. |

### Atomicity And Lifecycle

AMQP data senders capture an opaque entity binding after address validation and
Send authorization, before attach acceptance. Ordinary, batch and annotated
scheduled transfers reuse that binding. A same-kind owner generation change
refuses well-formed transfers with `amqp:not-found`, without clock access,
state/sequence changes or deliverability wakeups; a fresh healthy link can send.
Authorization expiry and malformed-message refusals retain their priority, and
successful sends are confirmed only after commit. DLQ/subscription sender
addresses remain invalid before any owner lookup.

Management request links also capture scheduling authority before acceptance,
after their Send-or-Listen attach authorization. Only `schedule-message` consumes
it, after request parsing and Send authorization; a capture failure is retained
for scheduling rather than rejecting other operations on that link. Scheduling
does not require an associated data link. Cancellation, receiving, sessions and
other management operations are not incarnation-fenced by this contract. No
live deletion/recreation API or new stored/wire format is introduced.

Second-mode delivery confirmations retain their original link incarnation. If
native Detach or End retires that owner before confirmation, the native API returns
`RemoteDetached` without writing a disposition, even after channel/handle reuse.
The receiving owner treats this as clean link retirement rather than faulting the
shared session. Live wrong-role commands and transport write failures remain
errors. Broker settlement still commits before confirmation; retirement does not
undo or fabricate its outcome. Stored formats and live-link wire encodings are
unchanged, with no storage migration required.

Commands commit one batch; domain refusals preserve messages, counters, history
and Clock. Indexed apply, journal and replay have separate checkpoint/retire/reopen
rules, not panic recovery or quorum guarantees. Snapshot format is structural;
opt-in offline capture validates and encodes one exact domain/F0/F1 image without
source writes. Post-read caps precede semantic decoding, but do not bound backend
snapshot allocation. Callers must exclude every writer and raw/keyspace handle; the
API does not enforce exclusion or enable online capture. Fjall provenance stays
typed beside the result, never in the unchanged SWSS frame. Neither capture nor
decoding proves authenticity, historical authority or installability.
Capture and frames use layout 3, which also requires every retained envelope and
filter/rule AMQP value to satisfy the fixed content profile. Ordinary writes do
not enforce it until #16's typed-content admission, so capture, and install's
complete old-target validation, refuse a format 3 store holding later
out-of-profile content. Layout 2 frames and
native format 2 sources refuse decoding, capture and installation, including with
cross-backend opt-in; conversion is an explicit store upgrade and recapture, never
relabeling. Anchors keep their recorded source layout, so layout 2 anchors stay readable.
Offline typed-capture installation bounds and validates one complete old target
image, then atomically deletes old records and puts the exact captured records
without replay or metadata changes. Default provenance classes must match;
cross-backend acceptance is explicit and keeps source/target evidence separate.
Corrupt or populated unindexed targets refuse, and native Fjall inspection has no
logical fallback. Caller exclusion includes all writers, raw/topology handles
and previous domain/journal/replay owners. Apply errors or escaping original
panics retire the non-Clone owner until reopen; receipts do not certify recovery,
authentication or anti-rollback authority. Limits are post-snapshot logical
acceptance bounds, not physical allocation/disk guarantees.

Separate offline `SnapshotAnchorOwner` attachment accepts only the exact installed
capture, including every opaque tail record, and requires a nonzero applied index.
It atomically puts an anchor, journal-container V2 and indexed-owner V2 without
deleting entries or changing domain rows, Clock, frontier or checkpoint bytes.
The anchor retains the original applied proposal envelope, source manifest and
pre-attachment frame identity. A later recapture has its own different frame hash.
Legacy unanchored stores remain V1 on open and write. Entry/frontier/checkpoint V1
bytes and indexed proposal hashes do not change. Old journal and indexed-owner
readers respectively refuse the V2 fences; this is not a global barrier for old
unindexed APIs or Fjall. Rollback requires a coherent compatible image, not marker
stripping. Attachment neither prunes nor reconciles suffixes.
Separate offline `SnapshotCompactionOwner` compaction requires a valid anchor with
B<A and atomically deletes entries B+1..=A while rewriting only the anchor's B to
A; every other record stays byte-exact. Unanchored, already compacted, divergent
or invalid images refuse without writes. Anchor V1 and container/owner V2 are
unchanged, so compacted images open under the same readers. Read-only
`select_recovery` classifies a validated image as legacy, anchored or compacted.
Attachment, compaction and selection validate images as capture does, including
format 3 provenance and the content profile. Until #16's admission work enforces
the profile on writes, out-of-profile content retained after upgrade blocks
capture, installation, attachment, compaction and recovery selection, without
writes, until it is consumed.
Token-exhausted receive, deferred receive and session acceptance discard every
staged effect, including earlier TTL routing in the same command. Indexed refusals
may initialize their private owner and advance only the `0xF1` checkpoint; domain
rows and Clock stay unchanged. These checks do not repair previously reused tokens
or certify naturally reached exhaustion.
See [atomicity and lifecycle](lifecycle.md#atomicity-and-lifecycle) for settlement,
deferral, scheduling, DLQ and replay contracts.

### Topic Integrity

Selected routing/page and rule readers validate listed graphs and canonical rows
before returning prefixes or copies; corruption refuses atomically. This is not
global catalogue health, live-incarnation or retained-runtime proof.
See [topic integrity](lifecycle.md#topic-integrity) for exact bounds and exclusions.

Durable proposals preserve legacy-representable commands' private V1 bytes and
checkpoint identities. Topic/subscription profile updates use unchanged private V2
DTOs. Only nonlegacy topic creation uses private V3, including invalid new settings
that must survive for deterministic refusal. Legacy content sends use private V4, carrying
each message's original AMQP bytes once, or raw bytes with explicit normalized fields;
decoding never parses or polices that content. Abandon, defer and dead-letter with a
sparse property patch also use V4; their legacy replacement-envelope forms keep exact
V1 bytes. Queue/subscription creation that disables expiry dead-lettering, and
queue/subscription updates naming that setting, use private V5. SQL rule creation uses
private V6, retaining the expression text uncompiled so an invalid predicate survives for
deterministic refusal; true/false/correlation rule creation keeps V1. Finite queue
creation and queue updates naming `capacity` use private V7; non-finite creation and
updates that omit capacity keep their V1/V5 bytes. Subscription creation that discards
filter-error copies, and subscription updates naming that setting, use private V8;
subscription intent without it keeps its V1/V2/V5 bytes. All other forms keep their V1/V2
bytes. Azure-text-ID content sends use private V9, composing an exact canonical
V4 send envelope with one 0/1 mode byte per child; at least one child must select new
AMQP projection semantics. Nested envelopes must be V4, never recursively V9. Decoding
does not parse AMQP. New readers accept exactly V1 through V9; older
readers refuse unsupported proposal versions. AMQP ingress sends every message as V9:
a transfer as `SendContent`, and a
Service Bus batch or scheduled-message request as `SendContentBatch` with each child's
original bytes. Nothing new emits V1 sends; historical V1 sends still decode and replay
unchanged. Link and management dispositions that change properties emit the V4 patch
forms; dispositions without property changes keep their legacy V1 forms with no
replacement envelope.

Store format 3 adds no record encoding: journal-entry/frontier/indexed-checkpoint
formats remain unchanged, and snapshot anchors use the separate owner/container
fences described above. Canonical
topic values alone use V2 when duplicate detection is enabled or its window differs
from 600,000ms; disabled/default-window topics retain exact legacy V1 bytes. The
topic codec rejects unknown, truncated, trailing, noncanonical and redundant V2
values. Old readers reject these V2 topic records, but can still open the store and
operate unrelated endpoints: this is not a whole-store downgrade barrier. Rollback
requires readers compatible with every retained topic/proposal, or a coherent
pre-feature state/journal/checkpoint backup. Never strip markers, rewrite profiles
or delete history as a downgrade procedure.
Queue profiles, including subscription backings, likewise use a queue-specific V2 only
when expiry dead-lettering is disabled; enabled profiles and every DLQ shadow keep exact
legacy V1 bytes, and the same refusals and rollback requirements apply. Finite queue
profiles alone use queue V3, whatever their expiry policy; V1 and V2 always mean
non-finite. Readers before V3 refuse those records, and the same rollback rules apply.
Topic membership records hold the filter-error setting: a subscription that discards
filter-error copies stores a membership V2 (child path and setting), while every
dead-lettering subscription keeps the exact legacy V1 envelope of its child path.
Readers before V2 refuse those memberships as topology corruption; noncanonical,
redundant and foreign-child records are refused, and the same rollback rules apply.
Rule records follow the same pattern: SQL
rules alone use a V2 rule value (display name, exact expression text, creation time)
that old readers refuse as an unsupported version, while true/false/correlation rules
keep exact V1 bytes. Every reader recompiles stored SQL text under the current limits;
text that no longer compiles is corruption, refused by rule reads, fanout and snapshot
validation. The SQL grammar, value semantics and limits are therefore frozen for V2 rule
records: a change that could accept, refuse or evaluate stored text differently needs a
new rule-record version. Fanout compiles each distinct expression once per command, in
memory only. Filter-error copies use the existing application dead-letter reason
encoding, so their message records are unchanged.
See [profile lifecycle](lifecycle.md#profile-updates).

Message records use a dedicated frozen codec. Legacy sends write V1, byte for byte
the former generic encoding; legacy content sends write V2: raw bytes, or original AMQP bytes
with a filter projection and a sparse property overlay (unique names in first-insertion
order; each value one encoded AMQP value, where null is a value) and no duplicated body.
`AmqpTextIds` content writes V3, using the frozen V2 content layout with explicitly
different system-ID projection semantics. Old message-record readers refuse V3 at the
version byte instead of silently treating it as V2. Raw content still writes V2.
Rewrites, topic copies, scheduled activation and dead-letter moves keep the version a
record was read with; only a property patch converts V1 to V2. Durable proposals
refuse envelopes carrying overlays. Runtime V1 reads keep tolerating trailing bytes;
V2/V3 framing is exact, and snapshot validation requires canonical rows of all versions.
Store-upgrade preflight, snapshot capture/restore/compaction and committed replay use
the same versioned record/proposal readers. No open, preflight or upgrade rewrites old
record bytes. This is a per-record/per-proposal refusal fence, not a whole-store
downgrade barrier: an older binary may open the outer format-3 store before encountering
an unsupported row. Rollback needs compatible readers or a coherent pre-feature
state/journal/checkpoint backup; stripping version markers is not a downgrade.

For new `AmqpTextIds` sends, SQL `sys.MessageId`/`sys.CorrelationId` and correlation
filters see strings unchanged, ulong as invariant unsigned decimal, UUID as lowercase
hyphenated `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` in AMQP network byte order, and binary
as **uppercase hexadecimal without separators** (two digits per byte; empty binary is
the empty string). These forms follow Azure SDK source:
[MessageId.ToString](https://github.com/Azure/azure-amqp/blob/b690d6d6e15c3ee8844b570d65e7e1af657a0f55/src/Framing/MessageId.cs),
[binary GetString](https://github.com/Azure/azure-amqp/blob/b690d6d6e15c3ee8844b570d65e7e1af657a0f55/src/AmqpExtensions.cs),
[UUID decoding](https://github.com/Azure/azure-amqp/blob/b690d6d6e15c3ee8844b570d65e7e1af657a0f55/src/Encoding/AmqpBitConverter.cs),
[Guid.ToString](https://learn.microsoft.com/en-us/dotnet/api/system.guid.tostring),
and the Service Bus SDK's
[annotated-message converter](https://github.com/Azure/azure-sdk-for-net/blob/6a909a735c23fa8e0f73e6bf3e1adf8cd7478611/sdk/core/Azure.Core.Amqp/src/Shared/AmqpAnnotatedMessageConverter.cs).
This is source-based compatibility, not a claim of live Azure filter-service testing.
The retained canonical AMQP bytes and typed outbound IDs never change.

V1/V2 records and replayed V4 content keep their historical projection: UUID debug
text `Uuid([..])`, lowercase hexadecimal binary, decimal ulong and unchanged strings.
No stored string is reinterpreted as a UUID, including a literal string exactly equal
to that debug text or to a hyphenated UUID. Property patches re-derive system IDs
from the original typed bytes, using legacy semantics for V1/V2 and Azure-text
semantics for V3; historical V4 replay therefore keeps its original behavior. A
legacy whole-envelope replacement on
a V3 copy derives that copy's new projection from the replacement's typed bytes.
Duplicate detection deliberately keeps its existing identity: UUID debug text and
lowercase hexadecimal binary, regardless of filter-projection version. Existing
cross-type aliases therefore remain (a UUID and its literal debug-text string collide;
binary and its lowercase-hex string collide), while an equivalent-looking hyphenated
UUID string is distinct. This does not implement Azure's typed duplicate identity.

A content send parses AMQP content when it applies, under the fixed content-safety
profile (16 MiB, depth 128, 4,194,304 nodes, 268,435,456 work units), and derives the
duplicate identity, session, TTL, scheduled enqueue time and version-selected filter
projection. Ingress still applies the received-message
decode to each message or batch child before sending. Invalid content refuses
deterministically, and one invalid child refuses its whole batch: transfers reject with
`amqp:invalid-field`. Scheduling requests check each message, including its message-id
and session cross-checks, with the same derivation and the batch's shared projection
budget, so one that fails it returns 400 before submission. The entity's message-size limit, the parser profile and the one-MiB
proposal envelope are separate budgets.

The derived projection must satisfy the same bounded-content contract that snapshot
capture, compaction and store upgrade enforce, so a content send refuses at derive time
in several cases. Two bound size: a single filterable property value whose canonical
encoding exceeds the correlation-value ceiling, and a projection larger than its budget.
That budget is per command, not per message: the combined original length of all of a
command's children plus one correlation-value ceiling. A batch therefore shares one
allowance rather than multiplying it by its child count, which bounds how much a single
`SendContentBatch` can expand (for example many small children that are each a
wide-re-encoding zero-width array) into one write batch, copied once per subscription. A
single `SendContent` uses the same formula over its one child.

Some array shapes are refused because they are not sendable. Arrays whose elements are
compound or described values are refused, which legacy AMQP ingress also refuses. Two
further shapes are content-send refusals, which historical V1 sends stored as their old bytes:
an array of empty lists (`list0` elements), which the canonical encoder has no spec
rendering for, and an array of strings or symbols containing non-ASCII text, whose stored
length would disagree with the text and not be decodable AMQP. Because ingress now sends
content intent, every AMQP send path refuses these shapes for new writes; projections that
historical V1 sends already stored are unchanged. Property-modifying dispositions send
patches whose values are checked under the same profile. Each derived value is
additionally re-decoded under the profile as a safety net and refused if it does not
round-trip.

A V4 disposition patch maps property names to one encoded AMQP value each, checked
under the content-safety profile when it applies. It rewrites only the locked copy
it names: original bytes never change, names coalesce into their first position, an
explicit null is a value rather than a deletion, and the filter projection is
re-derived from the effective properties with the send derivation and its budget
formula over the effective length. Patching a V1 record converts it to V2, with its
envelope as the original and its body dropped; a V1 envelope that does not parse, a
copy without AMQP content, or a value that is not bounded AMQP refuses. The original
plus every overlay name and value must fit the entity's message-size limit plus a
fixed 64 KiB headroom, the Service Bus header cap, so a message admitted at the limit
can still be patched and dead-lettered. The allowance never exceeds the 16 MiB
content-safety ceiling, so every patched copy still renders; a patch past either bound
refuses as `MessageTooLarge` with the effective maximum. An empty patch changes no content. A legacy
replacement envelope on a copy that already has an overlay refuses rather than drop
the patch. Refusals leave the copy, its lock and delivery count unchanged.

AMQP builds each patch from the properties a disposition changes: the Modified
outcome's message annotations (abandon, or defer with `undeliverable-here`), the
`com.microsoft:dead-letter` rejection's info map, or management `properties-to-modify`
on `abandoned`, `defered` or `suspended`. Each value is encoded on its own and must
pass the received-message checks, and the reserved `DeadLetterReason` and
`DeadLetterErrorDescription` names are dropped on every path, so a disposition that
changes only those sends the legacy command. Management updates patch locks received
on a link as well as by sequence number. A value the checks refuse is
`amqp:invalid-field`; a patched copy over its size allowance is
`amqp:link:message-size-exceeded`, and a domain content refusal is
`amqp:invalid-field`.

Receive, peek and receive-by-sequence render the stored original, then the overlay in
stored order: a name already present keeps its position and takes the overlay value, a
new name is appended, and an explicit null renders as an AMQP null. Broker-owned fields
follow and always win: header delivery count and TTL, absolute expiry, sequence,
enqueue, scheduling, lock and state annotations, and the dead-letter source. The
dead-letter reason and description are removed wherever the sender or a patch put them
and, on a dead-lettered copy, appended from broker state. A patch never touches an
annotation section, and a sender's `x-opt-lock-token` delivery annotation is replaced by
the broker's on a locked receive and removed otherwise. A copy at the ceiling renders;
one whose original plus overlay names and values exceeds it, which the broker never
writes, or whose stored overlay value does not decode, refuses to render.

### Storage And Runtime Limits

Memory is volatile; neither backend is replicated. Production durable startup
returns static replication-unavailable before storage/listeners; production Memory
refuses too. Fjall restart is not node-loss recovery; development is single-node.

Fjall stores use format 3. Ordinary opening never upgrades format 2; development
startup refuses it before listening and names the offline
`switchyardctl store upgrade --data-dir <path>` command. The upgrade preflights
retained AMQP content with fixed per-content parser bounds (16 MiB, depth 128,
4,194,304 nodes, 268,435,456 work units), which are not queue limits or quotas,
and validates journal/indexed owners only where present. It then changes only the
marker, so older builds refuse the upgraded store; rollback requires a pre-upgrade
copy, never marker stripping. See [store upgrade](lifecycle.md#store-format-upgrade).

Custody is owner-scoped, not whole-task-tree shielding. Borrowed cancellation/retry
requires a surviving owner; retirement requests prove neither joins nor peer answers.
Raw priorities and cleanup notices differ by owner. Begun broker/cleanup drains
remain unbounded; owner-drop and aborted-ancestor survival are excluded. TCP custody
is included; WSS #225 remains separate. SIGTERM/SIGINT shut the process down in
order (listener admission, protocol cleanup, timer, broker) within
`--shutdown-timeout-secs` (default 30); an overrun exits with status 3 and
acknowledges nothing uncommitted.
The optional listener socket limit (`--max-connections`, unbounded by default)
counts TLS/upgrade/AMQP handshakes and open connections until their original task
joins; at the limit the listener stops accepting and new peers wait in the OS
backlog. Each accepted socket has one admission budget
(`--admission-timeout-secs`, default 60) from accept through TLS, WebSocket
upgrade, SASL and AMQP Open, not reset per stage. A peer still handshaking at the
deadline has its socket closed without an AMQP Close, and the listener records a
`TimedOut` connection failure; its slot frees when the original task is reaped.
The budget ends at Open; the CBS authorization deadline is separate. See
[storage limits](lifecycle.md#storage-and-runtime-limits) and the
[eleven owner contracts](lifecycle.md#owner-custody).

Native outgoing sends enforce the peer's session receive window per Transfer
frame, independently of link credit and delivery IDs. Fragmented sends retain
their original payload, completion and capacity permit across zero-window waits;
session-only Flow or Flow on another link can resume them. Repeated Flow cannot
restore consumed capacity, invalid acknowledgements refuse, and frame progress
wraps modulo 2^32. Detach, End, EOF and local stop release queued/partial work;
native shutdown still joins the original connection tasks. Flow/drain reports
actual outgoing frame progress rather than delivery counts. The feature-gated
test client replenishes received session frames; it is not production incoming
buffer accounting.

Sustained bidirectional traffic remains uncertified: incoming session conservation
in [#332](https://github.com/DeandreT/switchyard/issues/332) (part of
[#68](https://github.com/DeandreT/switchyard/issues/68)), receiving-link refill in
[#69](https://github.com/DeandreT/switchyard/issues/69), and
[#77](https://github.com/DeandreT/switchyard/issues/77) Flow echo remain absent.
Timer commands drive expiry/activation, not wall-clock mutation. Small clock
regressions hold time; large ones refuse/retry without readiness signal. One
namespace/server; no quotas, fair multi-tenancy or production durability.

## Not Implemented

| Scope | Missing |
| --- | --- |
| Semantics | SQL actions/parameters/functions beyond the kernel, session subscriptions/session-ID predicates, administrable TTL expiry policy, DLQ forwarding/resubmit, same-group transactions. |
| Administration | Atom/XML entities/rules; native gRPC/CLI transport (contract only). |
| Security | JWT wire activation, network OIDC, mTLS, policy/full RBAC, namespace KMS/encryption, tamper-evident audit. |
| Production | Raft/placement, quotas/fairness, encrypted backup/restore, readiness/observability, release/performance evidence. |

Partitioning/cross-placement transactions are later scope; geo-replication/Premium
are not initial commitments. See [roadmap](roadmap.md) and [target design](../ARCHITECTURE.md).
