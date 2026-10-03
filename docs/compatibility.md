# Compatibility

Switchyard targets the Azure Service Bus Standard messaging model. A capability
is marked supported only after it has protocol-level tests and end-to-end
coverage with the relevant client.

## Client Gates

| Client | Data plane | Administration | Status |
| --- | --- | --- | --- |
| Official .NET SDK, current stable | Queue and topic send, both batch-send APIs, ordinary/session subscription workflows, queue/topic scheduling/cancellation, duplicate detection and message properties; queue session renew/state/scheduling | Planned | Experimental gate on 7.21.0 |
| Official .NET SDK, previous stable | Same gated workflows as current | Planned | Experimental gate on 7.20.2 |
| Sift pinned revision | Planned | Planned | Not implemented |

## Capability Matrix

A capability reaches **State machine** once the deterministic broker core
implements it with tests. That is a prerequisite for compatibility, not a form
of it: nothing below is reachable by a client until the protocol edge exists.

| Capability | Target release | Status |
| --- | --- | --- |
| AMQP 1.0 over TLS | Pre-1.0 | Protocol edge, Rust client end to end |
| AMQP over WebSockets | Pre-1.0 | Opt-in WS/WSS listener, bounded binary transport, both Rust backends and both pinned .NET clients; see [WebSocket Transport](websocket-transport.md) |
| SASL PLAIN and CBS SAS/JWT | Pre-1.0 | PLAIN and CBS SAS: protocol edge, Rust client end to end. JWT: not implemented |
| Queue send, receive, and settlement | Pre-1.0 | State machine |
| Atomic message batch send | Pre-1.0 | State machine, AMQP producer mapping, Rust clients on both backends and both pinned .NET batch APIs |
| Message properties and AMQP body preservation | Pre-1.0 | State machine and AMQP mapping; typed properties, application values, annotations, footer and all body kinds. Rust clients on both backends and official .NET property gate |
| Peek without lock acquisition | Pre-1.0 | State machine and AMQP management, including entity-wide session browsing; Rust clients on both backends and both pinned .NET clients |
| Receive-delete | Pre-1.0 | State machine, AMQP mapping |
| Lock expiry and redelivery | Pre-1.0 | State machine |
| Message lock renewal | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Time-to-live expiry | Pre-1.0 | State machine and timer; default drop and optional dead-lettering, official .NET deferred-expiry gate |
| Topics and subscriptions | Pre-1.0 | Atomic rule-selected fanout, parent-retained scheduling/cancellation, ordinary/session subscription and dead-letter routing, native create/get/list/update/delete, Rust clients on both backends and both pinned .NET clients; Azure administration not implemented |
| Correlation and SQL filters/actions | Pre-1.0 | Persisted Boolean, scalar correlation, and bounded SQL rules through AMQP and native rule CRUD/CLI; bounded REMOVE actions with independent copies across these surfaces, not SET/full Azure actions |
| Scheduling and cancellation | Pre-1.0 | State machine, AMQP management and send-annotation mappings, Rust and current .NET clients end to end |
| Deferral and deferred receive | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Dead-letter | Pre-1.0 | State machine, AMQP mapping |
| Dead-letter receive and resubmit | Pre-1.0 | Receive: state machine, AMQP mapping. Resubmit: not implemented |
| Sessions and session state | Pre-1.0 | State machine, AMQP management mapping, Rust and current .NET clients end to end |
| Duplicate detection | Pre-1.0 | State machine, AMQP send/scheduling mappings, Rust and current .NET clients end to end |
| Entity configuration updates | Pre-1.0 | Atomic state-machine patches; native queue, topic, and subscription API |
| Same-placement-group transactions | Pre-1.0 | Trusted same-queue foundation and explicit posting/messaging listeners; [same-queue .NET scopes](dotnet-transaction-scopes.md) gate warmed/cold-first immediate send and held PeekLock Complete over experimental TLS on both backends and both pinned clients. General placement-group work is not implemented; default Service Bus listeners still refuse transaction traffic |
| Atom/XML entity and rule administration | Pre-1.0 | Not implemented |
| Native gRPC administration | Pre-1.0 | Queue/topic/subscription create/get/list/update/delete and typed rule CRUD with bounded REMOVE actions over HTTP/2 and authenticated TLS; other services not implemented |
| Quorum replication | Pre-1.0 | Not implemented; durable production startup is refused before opening storage or binding listeners. Development Fjall persistence is local only |
| Partitioned entities | Later | Out of initial scope |
| Cross-placement-group transactions | Later | Out of initial scope |
| Geo-replication | Later | Out of initial scope |
| Premium-tier features | Uncommitted | Out of scope |

## Broker Core

The `domain` crate applies each replicated command as one atomic storage batch
and derives every deadline from the timestamp carried on that command, so a
follower replaying the log reaches the same state as the leader. Delivery
behavior it currently enforces:

- Peek-lock delivery is at-least-once: the lock commits before the message is
  handed out, and completion removes it only after settlement commits.
- Receive-delete is at-most-once: the deletion commits before the transfer.
- Peeking browses stored messages by sequence number without changing delivery
  counts, acquiring locks, or consuming from the queue.
- Deferral removes a locked message from the ready path while keeping its
  sequence number. A deferred receive by sequence can lock it again or consume
  it in receive-delete mode. A deferred message remains discoverable by peek
  after its lifetime ends; expiry is applied when a deferred receive reaches it.
- Scheduling keeps messages visible to peek but out of the ready path until
  their requested time. The timer atomically activates each due message under
  a new sequence number, appending it to the queue, and starts its lifetime at
  that activation. The requested scheduling timestamp remains attached.
  Scheduling a batch is atomic, including validation failures. Cancellation
  removes the scheduled record and deadline entry atomically; it rejects a
  missing or already-active handle. Exact Azure behavior for those rejected
  cancellation cases has not yet been verified.
- The core has a separate atomic ingress-batch command. Every member retains
  its own properties, identifier, session, lifetime, and optional scheduling
  timestamp. The complete batch is validated before staging content, shares
  duplicate history across its members, and commits counters and records once.
  A later validation, allocation, or storage failure leaves no partial batch.
  It accepts at most 1,024 messages, 65,536 retained value items, and 4 MiB of
  retained content, including the compatibility body and normalized identifiers.
  Session queues require every member to name the same session. An empty batch
  validates its target but writes nothing. These are local resource bounds,
  not Azure batch quotas.
- A separate trusted atomic-messaging envelope groups immediate sends and held
  settlements on one non-session primary queue, with shared limits, a bounded
  read-your-writes view, one commit, and committed-only wakeups. This does not
  expose AMQP transactions or retry idempotency; see
  [Atomic Queue Operations](atomic-queue-operations.md). Its separate
  [guarded commit API](atomic-commit-permits.md) adds owner-claimed, pending-only
  cancellation and an explicit indeterminate result for storage uncertainty.
  Optional [owned work reservations](atomic-work-reservations.md) preserve shared
  resource accounting while commands are queued, cancelled, or being applied.
  The [local transaction registry](transaction-registry.md) bounds declarations
  and controller lifetimes but does not enable wire transaction traffic.
  A trusted [paired owner handoff](native-atomic-owner-handoff.md) preserves both
  native receipts and staged logical work through one broker job, without
  establishing their correspondence or authorization. An explicit
  [posting-only listener](atomic-posting-ingress.md) derives actual native message
  work, performs bounded admission and staging, and joins the two lifecycles.
  A separate [atomic messaging listener](atomic-messaging-ingress.md) derives
  Complete from a canonical held delivery and an exact native retirement receipt.
  Its process address is an explicit development-only
  `--experimental-atomic-messaging-listen` option with the existing TLS/SAS policy.
  A separate [pinned .NET scope gate](dotnet-transaction-scopes.md) establishes
  warmed and cold-first same-queue immediate send, plus held Complete over TLS
  on both backends. Cold-first support is Send only; transactional acquisition,
  experimental management, cross-queue work, and recovery remain unsupported.
  Default listeners retain their transaction refusal.
- A queue can enable duplicate detection by message ID, with a 10-minute
  default history window bounded to 20 seconds through 7 days. A duplicate
  send is accepted and dropped, and history survives completion, dead-lettering,
  and schedule cancellation. Ordinary and scheduled sends share history;
  activation does not check it again. History is scoped to the namespace and
  entity, independent of the session. Anonymous messages bypass detection,
  and dropped retries do not extend the original deadline; exact Azure parity
  for these two edge cases remains unverified. A duplicate scheduling request
  receives a fresh sequence handle but stores no message, so that handle has
  nothing to cancel.
- Message identifiers are bounded to 128 UTF-16 code units, matching the
  official .NET client. Overlong identifiers are rejected before enqueue or
  duplicate-history changes, including within an atomic scheduled batch.
- A settlement is rejected unless it presents the live lock token, and rejected
  again once the lock deadline has passed.
- Abandon, defer and explicit dead-letter operations merge application-property
  updates into the held message atomically. Validation checks the complete
  resulting content before changing the lock or state. Legacy byte-body records
  gain typed content without losing their body or identifier. Reason and
  description on explicit dead-letter operations are bounded to 4,096 UTF-16
  units, matching the official client's argument limit.
- A live message lock can be renewed without changing its token. Renewal moves
  the replicated deadline record and its expiry index in one storage batch.
- Abandoning a message, or letting its lock elapse, returns it to the queue
  until it reaches the queue's maximum delivery count, after which it is
  dead-lettered as `MaxDeliveryCountExceeded`.
- A queue's default time to live is also a ceiling for an explicit message
  lifetime, including scheduled messages. Lifetime starts at enqueue/activation.
  A live message lock protects an expired message: completion and renewal
  remain valid. Abandonment or lock expiry applies the elapsed lifetime
  immediately, ahead of the delivery-count limit. Expired messages are dropped
  by default. `dead_lettering_on_message_expiration` instead moves them to the
  dead-letter queue as `TTLExpiredException`, whether reached by a timer,
  receive, abandonment, or lock expiry. The flag does not affect explicit
  dead-lettering or the delivery-count limit. Logical legacy configurations
  retain their former always-dead-letter behavior when decoded.
- The dead-letter queue is a queue: `entity/$deadletterqueue` is drained with
  the same receive and settlement machinery as its parent. Messages arrive
  there stripped of lifetime and session, keep their sequence numbers and the
  reason they were dead-lettered, and never dead-letter again — abandoning in
  a dead-letter queue always returns the message to it. The path is reserved:
  it cannot be created or sent to directly.
- Rejected commands write nothing, so every replica rejects at the same point.
- On a queue that requires sessions, a message carries a session identifier and
  is only delivered to a receiver holding that session's lock. Ordering is
  guaranteed within a session, which is the only FIFO guarantee made. A session
  lock is exclusive and expires on its own deadline; session state outlives the
  receiver that set it. A receiver holding the session can renew that lock and
  read, replace, or clear the opaque state through the management node.

Four session behaviors differ from Azure Service Bus under the current local
policies:

- A session identifier on a queue that does not require sessions is refused
  rather than carried, because it would promise an ordering that queue cannot
  keep. Azure accepts and ignores it.
- Settling a message inside a session needs the message's own lock token, not a
  live session lock. Azure fails settlement once the session lock is lost. The
  message lock is treated as the authority over that message, so a receiver that
  did the work can still settle it.
- Accepting the next available session examines a bounded number of sessions and
  reports none available if they are all held, rather than walking the entity.
  The receiver retries.
- Expiration is applied to individual messages, not to every message in a session
  when one expires. Azure documents
  [session-wide TTL expiry](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sessions#message-expiration);
  this increment does not add that policy.

An ordinary receiver can browse all sessions in a session-required queue or
subscription through its management link, without attaching a data receiver or
acquiring a session. This matches the documented
[read-only browse](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-troubleshooting-guide#how-to-browse-session-messages-across-all-sessions).
A supplied session identifier filters the same sequence-ordered scan; it is
still invalid on an entity that does not require sessions. Browsing does not
relax session ownership for ordinary or deferred receive.

Queue settings can be patched atomically with the parent and its dead-letter
shadow in the same batch. An omitted setting is unchanged; an explicit unlimited
TTL clears a finite default. Session and duplicate-detection enablement are
creation-only properties. Updates retain all existing messages, deadlines,
locks, counters, and duplicate-history entries; the new size/TTL/lock/history
limits govern new ingress or newly allocated deadlines. Expiration disposition
and maximum delivery count use the current queue policy. A no-op patch does not
write storage or advance the applied clock. The adjustable per-message size cap
is a Switchyard policy, not a verified Azure queue-update property.

The state machine can create and read distinct topic definitions and bounded,
sorted subscription membership. Queue and topic names cannot occupy the same
namespace path. Subscription creation names its parent topic and atomically
stores the member, its receive-only backing queue, its dead-letter shadow, and
an explicit `$Default` true rule;
invalid settings, path collisions, composed path limits, or storage failure
leave all of them and the applied clock unchanged. Topics have no queue or
dead-letter shadow of their own. Membership is limited to 32 subscriptions per
topic, a local resource policy rather than an Azure quota.
Subscription names use a conservative ASCII subset of the
[Service Bus naming rules](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules):
1 through 50 letters, digits, periods, hyphens, or underscores, starting and
ending with a letter or digit. Their canonical path is `topic/subscriptions/name`.
The control segment is reserved case-insensitively, while topic and member
spelling stays case-sensitive.
Membership reads are bounded and refuse malformed or dangling topology instead
of silently omitting it. The queue timer discovers the backing queues and their
shadows through its existing queue index. Subscriptions cannot be sent to or
updated through queue commands.

Immediate topic send, envelope send, and batch send copy each accepted message
to each currently matching subscription in one storage commit. New subscriptions
initially match all publications through their persisted `$Default` rule.
Duplicate detection runs once at topic ingress;
duplicate publications consume a sequence but create no copies. A topic with
no subscriptions acknowledges publications without retaining messages, and a
later subscription sees only later publications. Each copy preserves its body
and system fields; [REMOVE actions](sql-actions.md) transform only their private
application properties. Copies take the shortest requested/topic/subscription TTL and have
independent receive, settlement, lock expiry, deferral, and dead-letter state.
All input and destination validation precedes commit, including duplicate inputs;
one invalid destination or exhausted topic sequence rejects the whole command.

Action-free copies share their publication's topic-assigned sequence; retained
action copies receive additional parent-topic sequences. The shared-sequence policy is
inferred from Microsoft's
[topic-scoped sequencing description](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing),
not a cloud-verified cross-subscription guarantee. A late subscription does not
restart numbering; its sequence gaps reflect earlier publications and duplicate
discard. Subscription lock tokens remain independently allocated. Fanout is
bounded before retained content is cloned: at most 1,024 copies, 4 MiB of
aggregate retained typed content, compatibility bodies and normalized IDs, and
65,536 typed value items. These are local resource policies, not Azure quotas.
Only committed destinations are notified, without a post-commit topology read.

Subscription rules are persisted independently under their exact member and
rule names. True and false filters are supported, including the SDK's exact SQL
aliases `1=1` and `1=0`. Correlation conditions AND together; action-free rules
OR together and emit at most one copy per matching subscription. Removing the
final rule selects nothing, with no implicit default fallback. These Boolean,
default, and combination semantics follow Microsoft's
[topic filter documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/topic-filters).
Bounded SQL predicates are supported alongside those filters. AMQP, native, CLI,
and domain [REMOVE actions](sql-actions.md) add independent copies; `SET` actions
and compound correlation predicates remain explicitly unsupported rather than
treated as successful matches.

Correlation rules select the eight retained system string properties and scalar
application properties. `label` maps to subject; message and session identifiers
use the existing authoritative ingress values. Non-string AMQP correlation IDs
do not match a system-string condition. Strings are case-sensitive. Application
property keys and values use exact local typed equality: no case folding or
numeric coercion, float equality follows retained bit patterns, and a missing
property never equals a present Null. An empty correlation filter matches all
messages by vacuous AND. Those exact scalar, key, normalization, and empty-filter
choices are local policies, not cloud-verified guarantees. Original typed message
IDs remain unchanged in retained envelopes. An absent message ID and an explicit
empty message ID follow the existing anonymous ingress ID rather than a new
wire-presence comparison. All destination content-size limits are still checked,
including destinations excluded by a filter; filtering only changes retention.

Rule names retain their spelling and permit `$Default`; the SDK-compatible
length bound counts 50 UTF-16 units rather than UTF-8 bytes. Whitespace-only
names and the SDK's forbidden path characters are rejected. Control characters
are additionally forbidden by the local storage policy. Rules are limited to
32 per subscription, with 32 total conditions per correlation rule, 64 KiB per
versioned stored rule, and 256 KiB across one subscription's complete rule set.
Reads scan at most 33 rule entries, validate the complete topology and rule
metadata, and reject corruption instead of omitting entries. With 32 members,
rule metadata can total 8 MiB independently of the message fanout budget.

Matching has its own deterministic limits: 1,048,576 work units and 32 MiB of
potential comparison bytes per command. All inputs precharge every rule and
condition, including custom-property lookup work and potential key/value
comparisons, before retained content is cloned. Duplicates, nonmatches, and
short-circuit success do not bypass these charges. Immediate and scheduled
admission reject an over-budget command atomically. Activation selects a fitting
due prefix, or leaves the first unfit publication pending and cancelable. Rule
matching precedes missing-session routing, so an excluded publication creates
no dead-letter copy on that subscription.

### SQL Rules

SQL rules retain their original expression and semantic version, not a parser
AST or compiled program. The complete topic rule load shares one compilation
allowance; all correlation and SQL evaluation shares one command allowance.
Only true selects a copy. A finite SQL error overrides a matching rule on the
same subscription, but does not prevent delivery to healthy siblings.

`dead_lettering_on_filter_evaluation_exceptions` defaults to true. Such errors
then create one session-free, lifetime-free shadow copy with the local reason
`SwitchyardSqlFilterError` and a fixed description containing no rule text or
producer values. Setting the flag false drops only that subscription's copy.
Resource exhaustion instead refuses the complete command atomically and never
creates these dead letters. Current rules are evaluated again at scheduled
activation. The supported grammar, local semantic choices, resource accounting,
and wire contract are detailed in [SQL Rules](sql-rules.md).

Topic publications may carry session identifiers, including mixed-session
batches. Session-required subscriptions use session-affine ready indexes and
the existing exclusive ownership, FIFO, state, renewal, release, and deferred
receive machinery. Each subscription owns its sessions independently, even when
sibling subscriptions receive the same identifier. Ordinary subscription copies
preserve that identifier but use their global ready index and ordinary receive,
settlement, and expiry rules; this does not relax ordinary queue ingress.
Duplicate detection remains topic-local and based on message ID, independent of
session ID, consistent with Microsoft's
[nonpartitioned duplicate-detection description](https://learn.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection).

A matching publication without a session identifier reaches ordinary subscriptions
normally, while each matching session-required subscription receives its copy directly
in its dead-letter shadow with reason `Session ID is null`. Those copies keep the
topic sequence but have no lifetime or session identifier. This per-copy routing
is a local policy inferred from the documented
[missing-session dead-letter reason](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues),
not a cloud-verified atomic fanout guarantee. Session identifiers and retained
dead-letter reason/description bytes count toward the fanout content budget;
the two projected dead-letter values count toward its value-item budget. All
admission checks precede retained payload clones, including these additions.
Only actual backing or shadow destinations receive committed notifications.

Delivery notifications broadcast to every currently registered waiter on the
destination. The wait captures its signal before submitting receive, so a
commit between an empty result and the first wait poll is not lost. This lets
receivers holding different sessions on the same entity rescan independently
instead of depending on the periodic retry. Ordinary competing receivers may
also rescan; only the serialized receive command can acquire or consume a
message. Repeated notifications coalesce per one-shot wait, and a later
registration does not inherit an earlier broadcast. Cancellation and completion
remove registrations, with no retained payload or session history. Wake work
and memory remain proportional to live waiters, not a fixed process-wide bound.

Future topic publications, including management scheduling and individually
timestamped batch members, retain one scheduled record and deadline index on
the parent topic. They do not create subscription copies, dead letters, or
session ownership before activation. Parent management browsing returns these
pending records without opening a receiving data link, consistent with the
[documented location of scheduled topic messages](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-troubleshooting-guide#how-to-browse-scheduled-or-deferred-messages).
Topic browsing uses the same bounded physical scan and response budgets as queue
browsing, but returns only pending schedules; explicit session filters remain
unsupported on the parent. Subscriptions cannot schedule publications directly.

Activation removes the parent schedule and fans out atomically to current,
validated membership and current rules. Late-created subscriptions and rule
changes before activation participate in this local policy; that behavior has
not been compared with a live Azure namespace. Each
logical publication receives a new shared topic sequence after earlier active
work. Its actual activation timestamp becomes enqueue time and starts the
shortest requested/topic/current-subscription lifetime. These sequence and TTL
rules follow Microsoft's [scheduled sequencing](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing#scheduled-messages)
and [scheduled expiration](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-expiration#scheduled-messages)
descriptions. Session and missing-session dead-letter routing use the same
per-subscription policy as immediate fanout. A timestamp already due at admission
publishes immediately rather than retaining a pending parent record.

Duplicate detection runs once at admission, shared by immediate and scheduled
topic publications. Activation does not recheck or extend history, even if its
window has expired. Canceling a pending schedule removes its parent record and
deadline atomically but retains duplicate history. A multi-handle cancellation
rejects the whole command if any handle is absent or no longer scheduled; a
cancellation handle cannot remove activated subscription copies. Switchyard
serializes activation and cancellation, unlike Azure's documented race between
those operations.

Scheduled batches budget one retained parent copy per accepted future input.
Every future input must also fit an individual fanout against current matching membership
before retention. Activation processes a fitting due prefix within the existing
ingress budgets and at most 256 inspected scheduling entries, 1,024 copies,
4 MiB retained content, and 65,536 projected values. A later input that would
overflow remains pending for another command. If later membership makes the
first input unfit, activation rejects without mutation and the schedule remains
cancelable; it is not silently skipped or partially delivered. These bounds and
head-of-line behavior are local resource policies, not Azure quotas.

Plain producer addresses resolve committed metadata and permit queue or topic
send. Ordinary topic receivers and senders to subscriptions or dead-letter
queues are refused. Subscription receivers use
`topic/subscriptions/name`; their dead-letter receivers add `/$deadletterqueue`,
and either target can add `/$management` for request/reply operations. Reserved
control segments are case-insensitive, including the SDK's `/Subscriptions/`
and `/$DeadLetterQueue` forms; user topic and subscription names remain exact.
A literal topic path ending in `Subscriptions` and a member named
`Subscriptions` remain valid. Malformed or nested subscription paths are
refused rather than routed as ordinary queues.

Link planning first reads typed metadata through the serialized owner without
consulting the clock, writing storage, or acquiring a message or session lock.
Missing targets are refused before transfers. Malformed values, dangling
membership or dead-letter projections, and conflicting entity kinds retain
their errors instead of becoming absent targets. Authentication precedes this
read, so an unauthorized connection cannot probe entity existence through the
metadata lookup. A session-required subscription receiver without a session
filter is refused before any command or session hold. Named and next-available
filters then acquire a hold through the existing session command and echo the
granted identifier; their session-free dead-letter queues
remain receivable. Topic management links support scheduled browsing,
scheduling, and cancellation with operation-specific authorization; ordinary
topic data receivers remain refused.
Native administration can create, get, list, partially update, and delete topics
and subscriptions. Subscription
management links support `com.microsoft:add-rule`, `com.microsoft:remove-rule`,
and `com.microsoft:enumerate-rules`. All three use Listen authorization on the
complete endpoint scope, consistent with the SDK's
[ServiceBusRuleManager contract](https://github.com/Azure/azure-sdk-for-net/blob/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/RuleManager/ServiceBusRuleManager.cs),
not the Manage requirement of HTTP administration. Rule enumeration is a pure
serialized metadata read without a command stamp or clock access; mutations use
the normal atomic stamped-command path. Enumeration accepts `top` 1 through 100
and a nonnegative `skip` against the complete bounded, sorted rule set. A requested
page that exceeds the response allowance fails rather than returning a shortened
successful page, which could prematurely stop the SDK's enumeration loop.
SQL filter and [REMOVE action](sql-actions.md) enumeration returns exact stored
source and AMQP int compatibility level 20. Actions use the full-width SQL-action
descriptor, distinct from the SQL-filter descriptor; no-action rules retain the
empty-action descriptor. Optional action creation is accepted through the same
admitted child binding, with no request-field or associated-link redirection.
Malformed syntax returns status 400; unsupported constructs return 501; compile
and evaluation resource limits return 403. None are simulated successful rules.

Identifier allocation refuses exhaustion instead of saturating and reusing a
stored identity. Sequence numbers are limited to `i64::MAX`, preserving exact
AMQP `long` values for receive, peek, scheduling handles, and deferred receive.
That final value can be allocated once; a command requiring another allocation
is rejected atomically with `amqp:resource-limit-exceeded` (management status
403, non-retryable). Duplicate sends still consume their ordinary sequence
allocation. Lock tokens use `u64::MAX` as an exhausted sentinel, so `u64::MAX - 1`
is the final allocation. Operations that need no fresh identifier, including
receive-delete, cancellation, settlement, renewal, and cleanup, remain usable.
Counters keep their existing stored shape and exhaustion survives restart.
This is a deliberate local bound, not Azure's documented rollover behavior;
the official .NET [sequence-number property](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceivedmessage.sequencenumber)
is signed, while Azure documents rollover in its
[sequencing contract](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-sequencing).

The `server` crate's timer worker proposes scheduled activation, lock,
time-to-live, session-lock, and duplicate-history sweeps on an interval, so a
running node activates what is due and releases or prunes what has elapsed.
Queue and topic discovery use independent exclusive keyset pages of at most
1,024 configurations each. Queue pages include subscription backing queues and
dead-letter shadows; topic pages use their distinct metadata index. Retained
cursors visit later pages on later sweeps and wrap at the end. An attempted
entity advances its cursor before its commands, and both entity families are
attempted even if one fails, so a failing queue cannot starve topic history
activation/history cleanup or vice versa. Topics activate schedules before
expiring duplicate history. Queue activation stops after a partial 256-entry
page; topic activation continues while a command makes positive progress because
fanout can fill its budget earlier. Each index gets at most eight bounded command
rounds per sweep. A visited topic can therefore retain up to eight 4 MiB fanout
budgets across its activation commands, not only 4 MiB for the entire sweep.
Discovery reads
do not stamp commands or advance the applied clock.

An AMQP 1.0 client can reach queues, topic producers, and subscription receivers.
The node accepts AMQP over TLS with the
socket secured before the protocol handshake, as Service Bus port 5671
requires. Plain TCP remains available only in development mode. A configured
shared-access policy accepts either SASL PLAIN credentials or SASL ANONYMOUS
or Microsoft's equivalent `MSSBCBS` mechanism followed by a CBS SAS token. CBS
grants are scoped to a namespace or entity and to Send, Listen, or Manage; they
authorize links connection-wide and close an open link when its token expires.
A connection without a valid grant gets 20 seconds to complete CBS
authorization. JWT, OIDC, and mTLS are not implemented.
Only the experimental atomic messaging listener permits bounded coordinator
declaration and explicit rollback during that fixed
[initial window](initial-transaction-authorization.md). Queue access and every
commit, including an empty commit, still require current authorization. A
successful grant permanently ends the window; expiry never starts it again.
Raw TLS/CBS regressions on both backends verify grant loss, same-connection
reauthorization, and a fresh coordinator without reuse of the old transaction.
AMQP resource scopes normalize only recognized subscription, dead-letter, and
management control segments, never user names. Namespace and parent grants
inherit to their children; exact dead-letter or management grants do not grant
access to the parent or siblings. A management link retains its complete
`/$management` scope for per-request authorization rather than dropping the
endpoint suffix. SAS signatures still cover the original encoded audience;
scope normalization does not rewrite signed bytes. Scoped TLS/CBS socket tests
cover these boundaries separately from the namespace-wide SDK gates.
Native administration retains literal, case-sensitive scopes, including
primary entity names that happen to end in AMQP control words.
The optional WS/WSS listener carries the same engine and authorization through
the exact `/$servicebus/websocket/` endpoint. Its HTTP and buffer limits,
standalone protocol headers, close cleanup, and client gates are described in
[WebSocket Transport](websocket-transport.md).
The listener has local defaults of 128 live connections, including unfinished
security handshakes, and one 10-second deadline covering TLS, HTTP upgrade,
SASL, and AMQP Open.
Excess sockets are refused; handshake progress does not restart that deadline.
Listener builders can configure both limits, with a zero handshake timeout
requesting immediate refusal. The CBS authorization deadline starts after Open.
Graceful connection Close has a two-second default deadline. Timeout or
cancellation of its caller cancels the driver and its socket reader, including
when application dispatch or a socket write is blocked. Explicit shutdown waits
for both tasks to terminate before releasing the listener's admission slot.
These are Switchyard resource policies, not Azure quotas. Connection-wide
message-allocation budgets are not yet enforced.
Open advertises a 60-second receive-idle interval by default, with an actual
120-second complete-frame silence deadline, following the
[AMQP idle-timeout recommendation](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idle-timeout).
The peer's independent interval drives outgoing heartbeats at half its value;
ordinary complete writes also satisfy that direction. An omitted or zero peer
interval disables those heartbeats, while zero in the local configuration
disables only the local receive-silence check. Positive intervals below one
second are refused. Complete valid frames, including empty frames on unbound
valid channels, refresh receive activity; partial prefixes or bodies do not.
Each frame's complete write and flush has a separate five-second local limit,
shortened by the peer's remaining interval. Local options can configure both
limits; zero write duration is immediate refusal, not unlimited. Independent
watchdogs enforce silence and Close deadlines even when dispatch or transport
writes are blocked. A partial or interrupted write drops the transport without
appending Close or retrying the partially emitted frame. No heartbeat is sent
before Open or after Close.
AMQP Open frames are limited to the initial 512-byte transport maximum. After
Open, the server advertises and enforces its own 262,144-byte receive maximum,
independent of the peer's receive limit. Oversized frames are rejected from the
four-byte size prefix before allocating or reading their bodies; an open
connection returns the framing-error Close condition. The test client can
configure its own receive maximum between 512 bytes and the codec's 4 MiB
ceiling. SASL reads use the local receive maximum as a resource policy.
Receiving links additionally enforce a 4 MiB encoded-message ceiling across all
fragments and advertise the actual effective `max-message-size` in Attach.
Omission, zero, or a larger requested receive limit selects that local ceiling;
smaller positive limits remain exact. This applies to both the server acceptance
API and the test client's receiver builder, replacing their unlimited receive
default. Exceeding the limit detaches only the offending link with
`amqp:link:message-size-exceeded`, before appending the excess fragment or decoding
the message, even if every Transfer still has `more=true`. Aborting an admitted
partial delivery releases its content and aliases for reuse. The broker's
smaller producer-link limit is unchanged. An absent or zero peer-advertised
outgoing limit still means no peer limit.
Each connection also shares a 64 MiB retained encoded-content allowance across
all sessions and both directions. Incoming partials are charged before append;
completed deliveries keep their original encoded-byte charge while queued in a
receiver's inbox. Detach or End does not refund an unread inbox, and replacing
every session does not reset the connection allowance. Both ordinary `recv` APIs
refund a delivery's charge before returning it to the application, independently
of settlement. Additive [retained receives](retained-ingress.md) keep that charge
with a non-clonable receipt until it is destroyed, without delaying dequeue
credit. The sender-side data listener uses these receipts through ingress parsing,
broker replies, and disposition flush. Settlement does not refund a retained
receipt; dropping an inbox still refunds its queued messages. Incoming
exhaustion detaches only the offending link with `amqp:resource-limit-exceeded`.
Outgoing queued and active payloads keep their full encoded-byte charge through
the final Transfer flush. Failed or cancelled flushes retain it until teardown;
subsequent outcome and acknowledgement metadata does not retain content bytes.
Local send exhaustion is a retryable state error that does not reserve a tag,
advance delivery or credit counters, or write a frame. Aborts, decoding failures,
unavailable inboxes, and actor-owned teardown refund their actual leases.
This allowance counts logical retained content, not exact heap usage. Decoded
object overhead, custom decoder expansion, the bounded reader-frame backlog,
unencoded command messages, transient encoding/decoding and frame copies, and
ordinary application-owned messages and explicit clones are outside it. Outgoing
message encoding now measures
and validates borrowed sections before content admission, applies the peer's
limit with the exact encoded length, reserves the content allowance, and only
then creates one fallibly reserved output buffer. Metadata and bodies are not
cloned into temporary value trees or nested container buffers. The count and
write passes use the same canonical encodings, including shared array
constructors and compact collection widths from the [AMQP type system](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html).
Encoding additionally limits nesting to 68 and cumulative constructed-value
visits to 132,096 across all message sections, including zero-width array values;
violations are detected before output allocation. The standalone
`encode_message_with_max_size` API rejects an exact oversized length with typed
`MessageSizeError` before allocation. `encode_message` keeps no explicit byte
cap, but now uses the same borrowed traversal and structural limits. The native
send path uses the shared connection allowance before allocating its payload,
including when a peer advertises no maximum. Unencoded command contents and
performative metadata conversions still need separate resource policies.
Both connection drivers queue at most 16 complete decoded frame results, rather
than 256. A saturated queue stops further reads until the actor makes room; the
reader can hold one additional complete frame waiting to enqueue, and an actor
handling a frame can hold one more. Those 18 frame slots have an encoded-content
envelope of about 4.5 MiB at the default 256 KiB receive frame limit, or 72 MiB
when the test client selects the 4 MiB ceiling. This is separate from retained
message content accounting and is not a heap bound: decoded metadata expansion,
object overhead, transient codec/conversion copies, and socket buffers are
additional. Existing per-frame decode limits remain in force. Cancellation
aborts and joins the reader and drops the queued results without draining more
wire input.
Frames on channels above the locally advertised limit receive a framing-error
Close without refreshing receive activity. Session channels are independent in
each direction, as specified by the [AMQP session establishment protocol](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
The server advertises its own incoming channel range and reserves a vacant
outgoing channel within the peer's inclusive limit, preferring the incoming
number when it is available. Its Begin response references the actual incoming
channel. The test client correlates a response through Begin's `remote-channel`
and the original pending session generation, not the response frame's number.
Later session traffic resolves only through the peer-channel association; a
numerically equal local channel is not a routing fallback. Duplicate or invalid
associations and traffic on an unknown incoming session channel receive a bounded
framing-error Close. A server unable to reserve an outgoing channel returns a
resource-limit Close without creating a session or application event; a local
client Begin instead returns retryable `InvalidState` without writing a frame.
The test client still does not accept peer-initiated sessions and explicitly
closes with `amqp:not-implemented`.
An ending session discards traffic until End on its mapped incoming channel;
the binding and local reservation survive until that acknowledgement. Session
approval and endpoint generation fences continue to reject stale commands after
channel reuse.
Link handles are also independent in each direction. The server reserves a
vacant local handle within the peer Begin's inclusive `handle-max`, preferring
the peer's number when available; the test client allocates from its own cursor
within that same negotiated output range. Each endpoint advertises its own
incoming handle range independently. Client Attach responses correlate by link
name, mapped session, expected role, and the original link generation before
binding the peer handle. Flow, Transfer, and Detach resolve only through that
published peer binding, never through a numerically equal local handle. Pending,
installed, and closing links reserve their local handles; a matching Detach
acknowledgement releases the closing binding. Exhausting the peer's output range
ends the requesting server session with `amqp:resource-limit-exceeded`, without
creating a link or approval event. A local client Attach instead returns retryable
`InvalidState` without advancing its cursor or writing bytes. Unless a retained
error-link record applies, Flow, Transfer, or Detach on an unpublished peer
handle ends only its session with `amqp:session:unattached-handle`.
A duplicate Attach on a normally bound peer handle, including a pending approval
or a normal close awaiting acknowledgement, instead receives an immediate connection
Close with `amqp:session:handle-in-use`. That refusal precedes link-name matching,
approval publication, and handle-capacity checks. It publishes no lazy Begin or
session End, retires all local session and link owners before the Close write,
and fails pending client operations with a closed-connection error after a
successful flush. Failed or cancelled reply I/O retains the existing teardown
error classifications. The bounded Close handshake controls connection shutdown.
Local error Detach replies mark the exact closing link generation after frame
preflight and before reply I/O. While that peer alias is still reserved, a late
Flow or Transfer ends its session with `amqp:session:errant-link` before changing
session windows, link credit, delivery ownership, or retained message content.
The marker survives failed or cancelled Detach flushes. Normal closing links
continue to tolerate crossing traffic, and a mapped peer Detach acknowledgement
releases the exact alias for reuse. A separate session index retains at most 128
error-detached peer handles through that acknowledgement. An unbound retained
handle still refuses Flow or Transfer with `amqp:session:errant-link`, while a
historical Detach is ignored. A structurally valid, admitted fresh peer binding
supersedes its old numeric record; an exact current binding takes priority over
history. Reusing only the local output handle does not reassign the old peer
handle. These handle records clear when the session ends.
Each connection also retains exact, case-sensitive error-link names separately
for each local link direction, across Detach acknowledgement and session End.
The registry holds at most 256 name/direction keys and 1 MiB of summed UTF-8 name
bytes. Name, peer-handle, and delivery-ID history admission and required reply
frame preflights all precede error-history publication. Exhaustion ends only the
affected session with `amqp:resource-limit-exceeded`, without publishing an
unrecorded error Detach or partially adding a global name record. Committed
history survives failed or cancelled reply I/O; names clear when the connection
is destroyed. Normal closes create no error-name or error-handle records.
An incoming Attach for a known error name and direction with a null unsettled
field receives scoped `amqp:session:errant-link`, including on another session.
A non-null field, including an empty map, instead identifies an unsupported
resume in this context. A server can publish its own Attach followed by an
`amqp:not-implemented` error Detach; an unsolicited client-side request with no
pending local endpoint receives session End with `amqp:not-implemented`.
Fresh unknown empty maps retain their existing behavior. A local client attempt
to freshly attach a known error name in the same direction returns `InvalidState`
before advancing its cursor or publishing bytes; the opposite direction remains
independent.
Normally bound duplicate handles retain immediate Close priority, even if the
request names a historical error link. For a still-reserved error alias, a null
or unrelated-name Attach receives scoped errant-link End; a non-null request for
its known error name receives scoped not-implemented End without replacement
allocation. This is the native conservative policy for the overlapping
[link-error and duplicate-handle rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html),
not an implementation of link resumption.
Live names are also reserved by exact, case-sensitive name and canonical local
direction across a connection's sessions. Current pending and installed links,
including ordinary closes awaiting mapped Detach acknowledgement, hold that
reservation. A normal acknowledgement or session End releases it; error-name
history follows the separate lifetime above. The scan uses existing bounded
aliases rather than another retained-name registry, and ignores ending sessions,
orphaned or foreign-generation aliases, and error-detached reservations.
A fresh peer handle claiming an already-live name in the same direction receives
scoped session End with `amqp:not-implemented`, without replacing the original
endpoint, publishing an approval, or adding error-name history. An original
endpoint on another session remains usable. This is an explicit refusal of
unsupported link stealing, not the occupied-handle Close rule; occupied handles
and known error names retain their earlier classification priority. A local
client duplicate returns `InvalidState` before cursor advancement or output.
Opposite directions and case-distinct names remain independent.
Client pending replies are indexed by exact name and local direction, then
checked against their mapped session generation. Opposite-role same-name pending
links can therefore coexist. An exact directional entry on another session is
ignored without consuming it or falling back to the other role. Only when that
entry is absent can a current-session opposite-role entry receive the existing
invalid-field refusal for a wrong-role reply. Cleanup retires only the owning
session's entries in either direction.
Error Detach also retains the exact owners of known live delivery IDs in separate
incoming and outgoing session indexes. Each direction holds at most 4,096 IDs;
admitting more ends only that session with `amqp:resource-limit-exceeded` rather
than evicting error history. Any peer Disposition touching one of those IDs,
including a mixed or wrapping range, ends the session with
`amqp:session:errant-link` before applying any healthy settlement. This check is
independent of the supplied outcome and settled flag. The indexes retain neither
message bodies nor delivery tags, survive Detach acknowledgement and handle
reuse, and clear when the session ends. The outgoing allocator skips retained
error IDs. A fresh incoming delivery replaces an old incoming error record only
after header and format validation, exact ledger reservation, and successful
credit admission; rejected attempts and continuations do not reassign it.
Normal closes do not create these records. Already released successful or
pre-settled delivery IDs are not tracked. Link and delivery resumption,
cross-connection name ownership and stealing remain unfinished.
Session windows count Transfer frames independently of link delivery counts.
Incoming windows replenish after bounded frame processing; receive links grant
32 message slots and return credit only as the application consumes a delivery
or a partial delivery is aborted. A paused receiver cannot block the connection's
other links. Fragmented sends yield when their session window closes, resume on
session Flow, and consume only one link credit per message. Flow echo, drain,
optional credit, wrapping counts, and early second-mode dispositions have raw
transport regressions. Every outgoing frame is checked against the peer's frame
cap before writing bytes. Additional metadata and command-content resource
policies remain unfinished.
Session startup tracks local Begin publication separately from application
approval. A required response to a pipelined Flow, pending Detach, End, or
session refusal first publishes the server's Begin exactly once; later approval
does not repeat it. This preserves the
[session-state send ordering](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)
without buffering additional control responses. An approval for an absent or
still-ending session returns a local detached error, which the broker listener
skips without disconnecting healthy sibling sessions. Application approval
still controls link installation; pending attaches share a 32-entry bound.
The transport additionally limits one connection to 32 session states, including
unapproved, pending-client, and ending sessions. It permits 128 distinct link
lifecycle handles per session and 256 across the connection. Installed links,
pending approvals, peer aliases, and handles awaiting Detach acknowledgement
share that allowance; overlapping bookkeeping for one local handle counts only
once. Approving an admitted link does not charge it again. A closing handle
remains charged until the peer acknowledges Detach. A session state remains
charged until the peer's End removes it, even though End retires its links.
An excess peer Begin receives an `amqp:resource-limit-exceeded` Close without
creating a session or application event. An excess peer Attach ends only its
requesting session with that condition, without allocating an extra approval or
refusal handle. The test client refuses excess local Begin and Attach calls with
`InvalidState`, before advancing counters or writing bytes, and can retry once
the relevant acknowledgement frees capacity. These cardinality limits do not
change the negotiated numeric channel range and do not bound retained content
bytes or exact heap usage.
Session approvals, installed session endpoints, and client session commands
retain an opaque session generation instead of relying on a reusable channel.
Link approval uses an `IncomingAttach` receipt with that original session,
an exact pending approval token, and immutable peer handle, name, and role. Its
assigned local handle is separate approval authority, not editable raw content.
The application can still edit terminus and response metadata, including the
granted Service Bus session filter. A foreign or altered receipt is refused
locally without consuming the rightful approval or writing bytes. Clones can
approve only once. Raw Attach content cannot be converted back into approval authority.
This is a source-level change for callers of the in-tree transport API.
Additive [native connection identities](native-connection-identity.md) bind
negotiated session and link generations to their exact actor. Retained receipts
expose that origin independently of link settlement and retirement. Observer
clones are inert; the actor retires its identity before publishing termination,
including cancellation and unwinding. Activity is only an observation, not
authorization, a live delivery guarantee, or transaction admission.
Additive [native sender identities](native-sender-provenance.md) identify an
actor-accepted sending-link generation. `PendingSettlement::belongs_to_sender()`
checks exact active link and connection origin, including First-mode outcomes
without an acknowledgment token. This does not prove an original outgoing
delivery generation or settlement usability, and changes no ordinary settlement
or transactional-disposition refusal behavior.
Additive [original outgoing delivery identities](native-outgoing-delivery-provenance.md)
preserve an actor-minted generation through send completion and later
acknowledgment metadata. `PendingSettlement::delivery_identity()` is historical
provenance after outcome resolution, not a pre-outcome handle or proof of an
unsettled delivery. Observer clones retain metadata only; settlement and
transactional-retirement support are unchanged.
The separate [native transactional-work opt-in](native-transactional-retirement.md)
admits Accepted, unsettled retirement dispositions for fully flushed dedicated
Second-mode sends and prepares an exact mixed set with postings. Rollback restores
the same outgoing native delivery without replaying its body or releasing its
alias. This does not change ordinary sender refusals, the posting-only broker
listener, or SDK transaction support; broker correspondence to held receive locks
is supplied only by the separate [atomic messaging listener](atomic-messaging-ingress.md).
Its explicit [attach-default opt-ins](transaction-attach-defaults.md) negotiate
Mixed receiver requests to actual Unsettled/Second and interpret an omitted
fresh coordinator count as zero under an immutable approved exception.
Existing strict native APIs and ordinary/posting-only listeners are unchanged;
the accommodations alone are not an SDK gate. The separate
[pinned .NET scope tests](dotnet-transaction-scopes.md) establish warmed and
cold-first same-queue immediate send, plus held Complete over experimental TLS.
The messaging listener's separate initial control window is socket-tested;
cold-first SDK support is limited to immediate Send, not acquisition or Complete.
An End on an unmapped channel is refused without manufacturing a session reply.
Client Begin searches only vacant channels within the peer's inclusive channel
limit. Pending, live, and ending sessions cannot be overwritten. Client End
retires its original endpoints before writing and waits for the matching peer
End acknowledgement before its channel is reusable. Repeated or stale End and
Attach commands are local refusals, not commands against a replacement session.
Locally closing receiver links do not publish further link-credit updates, even
when a queued delivery-consumption notification is processed after Detach.
Session-window replenishment and healthy sibling links remain independent.
First transfers require an explicit delivery ID, binary tag, and message format.
Tags may be empty but cannot exceed 32 bytes. Continuations may omit identity
fields, but repeated ID, tag, and format values must match the first fragment;
an invalid fragment closes only its link. Format zero always uses the standard
message decoder. The transport can opt a receiving link into at most eight
additional exact formats with trusted application decoders; zero cannot be
overridden. A format not registered on that link is refused with
`amqp:not-implemented` before reserving a delivery slot. CBS, management, and
test-client receiving links retain the standard-only default.
Incoming delivery IDs are reserved across the session, and tags across their
receiving link, from the first fragment. A live collision closes only the
offending link with `amqp:invalid-field`; a different original owner's delivery
remains available for settlement. This link-local error scope is a Switchyard
policy. A separate metadata allowance bounds incoming partial, complete, and
second-mode acknowledgement entries to 1,024 per receiving link and 4,096 per
session. Admission beyond either limit closes the offending link with
`amqp:resource-limit-exceeded`, independently of its message-slot credit.
Consumption returns message-slot credit but does not settle a delivery.
Application receipts retain opaque link and delivery generations rather than
relying on reusable numeric aliases. A foreign receiver, retired link, or
aborted receipt cannot emit a settlement. Repeating a terminal settlement on
its original open link is a no-op, even after the numeric ID has been reused.
Local outcomes are checked against the peer's frame cap before state changes;
an oversized outcome leaves the live receipt available for a smaller retry.
Receiver-first outcomes settle immediately. Receiver-second outcomes are sent
unsettled and retain their aliases until the sender acknowledges settlement,
including a state-less or ranged sender disposition. The application call
returns after writing its outcome, not after that acknowledgement. A sender
settling before the application finishes suppresses an unnecessary response;
partial deliveries retain their aliases until completion or abort. Completing
sender-settled deliveries need no outcome or retained alias.
Outgoing tags are reserved per sending link when a send enters its local queue,
and remain reserved through active fragments, unresolved outcomes, and pending
second-mode acknowledgements. This implements the transport's
[live-tag uniqueness rule](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html).
An identical tag on another link is independent. A duplicate on the same link,
more than 32 tag bytes, a full 32-entry outgoing queue, or admission beyond 1,024
outstanding deliveries on one sending link or 4,096 across a session is a local
`InvalidState` refusal. These checks precede message encoding, credit consumption,
delivery-ID allocation, and wire output, leaving the endpoint usable for retry.
Each delivery counts once even while both active and unsettled. Tags are released
after a successful final pre-settled Transfer flush, completion without a required
acknowledgement, or exact local or remote acknowledgement settlement. A dropped
send waiter does not release a tag; a failed or cancelled final Transfer or
acknowledgement flush retains it until teardown. An old terminal receipt cannot
release a later send's reused tag or numeric ID. These are metadata allowances,
not a connection-wide content or heap limit.
An ordinary native sender can request a unique outgoing reservation with
`reserve_send`; the returned future owns its endpoint identity and does not
borrow the sender. The connection actor admits it against current peer credit
and the existing queue, per-link, and per-session metadata limits. Creating or
claiming a reservation does not advance delivery counts or allocate a delivery
ID. `try_claim` races processed credit withdrawal through shared revocable
state; losing that race returns `SendReservationRevoked`. Dropping an unqueued
reservation or its pending future returns local capacity through shared state
and wakes the actor, including when the command queue is full or its reply was
not consumed. Retiring its exact link also revokes it; numeric handle reuse does
not transfer that authority. Reserved sends retain the ordinary outcome and
second-mode acknowledgement rules.
A claimed reservation is local admission, not an irrevocable wire-credit grant.
The first Transfer still checks the latest peer credit and session window; a
later credit reduction can delay it until a new grant. This follows the
[transport flow-control model](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#section-flow-control).
Reservations do not reserve encoded payload bytes or guarantee successful
encoding, flush, or settlement. Ordinary sends without reservations retain
their existing queued-send API and resource limits.
`send_reserved_with_settlement_owned` consumes a claimed reservation and returns
an owned `Send + 'static` future without borrowing the sender. Creating that
future performs no transport IO; it already owns the message, tag, original
endpoint metadata, and cancellation guard. The borrowed reserved-send method
delegates to the same path when polled. Dropping before the actor consumes the
token cancels the unique slot through shared state, including while command
capacity is unavailable. After native admission, dropping a waiter cannot undo
the send, return spent wire credit, release its retained tag, or settle a broker
lock.
For unsettled sends, the result is the existing `PendingSettlement` after the
peer outcome or applicable default, not a flush-only receipt. Initially
sender-settled sends retain their local Accepted result after final flush. Its
exact delivery provenance and caller-controlled final acknowledgement remain
unchanged. Multiple owned futures may be awaited independently; this native API
does not bound caller-owned message bytes or add broker authorization. The
ordinary listener applies separate held-work admission around it. See
[Native Outgoing Admission](native-outgoing-admission.md) and
[Ordinary Receiving Pipeline](ordinary-receiving-pipeline.md).
Outgoing delivery IDs are allocated independently of incoming IDs, within one
session's sending direction. When the cursor wraps onto a live ID, a bounded
vacancy search skips unresolved deliveries, pending acknowledgements, and active
fragments across every sending link in that session. It does not overwrite an
old alias or stall solely because the next ID is occupied. The selected ID is
captured for every fragment of that delivery; first-frame admission advances the
cursor, while a lack of link credit or session window leaves it unchanged.
Session Transfer-frame counters remain separate and advance once per fragment,
without gaps introduced by skipped delivery IDs.
Outgoing sends and explicit second-mode acknowledgement receipts also retain
their original link generation. A stale sender cannot send or close a
replacement link, and a receipt cannot acknowledge a different delivery that
reuses its numeric ID. Owned terminal acknowledgements are repeatable no-ops
only while that original endpoint remains open. Explicit receipt methods borrow
the receipt, so an oversized rejection leaves it available for a smaller retry;
the exact pending receipt is removed only after writing and flushing its sender
disposition successfully. The test client automatically writes a state-less,
settled sender acknowledgement before reporting a second-mode send outcome,
even if the caller has dropped its waiting future. An early receiver outcome
waits for the final Transfer before that acknowledgement. First-mode, pre-settled,
and initially receiver-settled outcomes do not emit redundant acknowledgements.
Receiver settlement is tracked independently from its first terminal outcome.
A later settled disposition with absent or nonterminal `Received` state preserves
that outcome and cancels any still-pending sender acknowledgement. A fragmented
send still waits for its final Transfer to be written and flushed; the first
terminal outcome received before completion takes precedence over a default.
When the receiver settles without a terminal outcome, the actual selected
Source's explicit default is used. Defaults must be
[terminal outcomes](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-source)
on both encoding and decoding. Invalid application-edited defaults are refused
locally without consuming the original approval or writing a response.
An absent default produces the delivery-level `RemoteSettledWithoutOutcome`
error instead of manufacturing Accepted or closing the link. A nonterminal,
unsettled update does not use the default or complete the send. Remote-settled
manual acknowledgement receipts become owned no-ops without another wire
response; the application remains responsible for its broker settlement.
Local client Detach retires the endpoint before waiting for the peer's reply;
repeated closes are local no-ops and cannot enqueue another Detach.
The negotiated sender mode is enforced at completion, with aborted transfers
implicitly settled. A per-transfer receiver mode defaults to the negotiated
mode on the completing frame, rather than becoming sticky from an earlier
fragment; an illegal receiver-second override on a receiver-first link is
refused unless the delivery is sender-settled or aborted. These policies follow
the [Transfer and Disposition rules](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer).
Switchyard interprets the receiver-mode exception to include a settled sender
disposition received before completion. Such a disposition never substitutes
for the explicit settled Transfer required by a negotiated settled sender link.
Detach, End, and connection teardown retire the affected generation and remove
its aliases. Link suspension and resumption remain unsupported. An Attach with
retained unsettled entries or incomplete unsettled state receives a stripped,
null-terminus Attach response followed by a closed `amqp:not-implemented` Detach;
it never reaches application approval. A complete empty unsettled map is accepted
as a fresh attach. Pipelined unsupported attaches wait for the local Begin before
their refusal, sharing the 32-entry pending-attach bound. The test client also
refuses retained or incomplete state in an Attach response before exposing an
endpoint or granting receive credit. Caller-mutated recovery state is refused
before enqueueing an approval and leaves the original valid approval available.
A resumed Transfer on a known receiving link is refused before identity, credit,
payload decoding, or abort admission, while still counting its session frame;
cleanup affects only that link's partial content and aliases. This explicit
unsupported-feature policy does not implement the standard's recovery exchange
or claim its unknown-resumed-delivery ignore behavior.
Approved producer links register Service Bus batch format `0x80013700`, whose
[wire constant](https://raw.githubusercontent.com/Azure/azure-amqp/master/src/AmqpConstants.cs)
identifies one encoded standard message per outer Data section. The nonempty
wrapper uses one link delivery/credit regardless of member count, shares its
inner parsing allowance, and submits exactly one atomic ingress command before
the outer Accepted outcome. The entire wrapper must still fit the 256 KiB
producer-link limit. Inner properties, lifetimes, identifiers, and optional
scheduling timestamps remain independent; outer metadata is not inherited or
used as a batch-wide duplicate identifier. A present outer session must agree
with present inner sessions and never fills a missing one. A standard inner Data
body is ordinary content, not another batch. Empty wrappers are refused; an
empty encoded inner message is valid anonymous content. The
[pinned SDK converter](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/Azure.Messaging.ServiceBus_7.21.0/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpMessageConverter.cs)
normally sends singleton batches in format zero, so raw transport tests also
force the nonzero singleton format. Both pinned client gates cover the enumerable
and size-safe batch APIs, session FIFO/refusals, rich content, duplicate detection,
and an oversized `TryAddMessage` refusal.
Message decoding can share one cumulative allocation allowance across embedded
messages: at most 132,096 parsed values and 4 MiB of copied string, symbol, and
binary content, with depth bounded independently for each value. Array members
and repeated named descriptors are charged before allocation. Scheduling
management requests and producer batches share that inner allowance and reject
more than 1,024 members before decoding them. These bounds do not provide a
connection-wide memory limit and are not Azure batch quotas.
If a peer detaches a link while application approval is outstanding, its receipt
is retired immediately. The server first publishes a minimal Attach response
with its assigned local handle, then acknowledges with Detach; any required
Begin precedes both. The pending slot and exact handle binding are released only
after that final Detach flush, without waiting for the application to consume
the cancelled receipt. Pending link-level Flow echoes likewise wait until the
server's Attach is published. If even the minimal Attach cannot fit the peer's
frame limit, a bounded `amqp:frame-size-too-small` End replaces the link reply.
A stale receipt cannot open the replacement link or consume its approval.
Retired queued session and link events are skipped. Cancellation during
asynchronous broker planning releases any newly granted domain session hold.
The listener remains available for later links on the same AMQP session, including CBS and management
approvals. No numeric cancelled-approval tombstones are retained.
The edge resolves a link's address to an entity, turns transfers into send
commands and dispositions into settlements, and answers a rejection with the
condition an SDK keys its behaviour off. A receiving link's settle mode selects
the delivery guarantee: unsettled is peek-lock, pre-settled is receive-delete.
Before each ordinary broker Receive, the listener checks Listen authorization,
obtains and claims one native outgoing reservation, and checks authorization
again before submitting the command. An attached link with no usable peer
credit therefore does not acquire a lock, delete a message, run Receive expiry
cleanup, or stamp a Receive proposal. Replaying an unchanged absolute Flow
grant does not create additional credit. An empty Receive releases its
reservation before waiting for an enqueue notification or the coarse fallback,
so it cannot hold unused credit against drain. Detach and authorization loss
are watched during credit admission, the broker wait, and the empty-queue wait.
This is an edge-before-enqueue check, not an authorization or cancellation
guard at final owner claim. A started Receive can still commit after its waiter
is cancelled; a committed deletion stays deleted, and an unsettled lock retains
the existing expiry fallback. Teardown releases the exact session hold, not
all message locks. Payload-size admission also remains after Receive.
The ordinary receiving task can hold multiple deliveries on the same link and
finish their independent outcomes out of order. It permits at most 32 work
items, including one admission, pending Receive, or parked response, and
4 MiB of conservatively projected captured-message content. Projection and
content admission precede message conversion and construction of the native
send future. One already acquired and decoded broker response is outside that
content allowance; an individually oversized response is refused, not rolled
back. Temporary aggregate pressure parks that response while existing outcomes
finish. A started Receive remains pinned across other work completions rather
than being cancelled and resubmitted. These are local held-work limits, not an
RSS bound or Azure prefetch quotas. Separate
[SDK receive-batch gates](dotnet-receiving-batches.md) exercise three held action
copies before completion and rolling three-credit queue replenishment on the
same receiver. The experimental transactional receiver is unchanged.
See [Ordinary Receiving Pipeline](ordinary-receiving-pipeline.md).
Peeking is served through the entity's `$management` request/reply links and
returns encoded AMQP messages without touching their broker state.
Each peek inspects at most 256 stored records and retains the existing response
budget. It leaves message states, delivery counts, ready indexes, session holds,
and the persisted clock unchanged, even when skipping expired ready messages.
It still passes through the normal proposal timestamp checks; it is not an
unstamped metadata read. Listen authorization applies to the management link's
entity. An absent `session-id` requests entity-wide browsing, while a present
non-string or invalid identifier is refused before broker submission. An
associated receiver link is not required and cannot redirect the browse.
Receiving transfers encode the number of preceding acquisitions, so the official
.NET client reports 1 on the first receive and 2 after abandonment and redelivery.
Peek retains the stored acquisition count without the client's receive increment.
The core currently counts acquisitions, not only unsuccessful settlements;
counter parity for deferral, locked-message peek, and dead-letter transfers is
not established, nor are generic AMQP Released/Modified counter semantics.
Deferred receive is also served through `$management`, and locks returned that
way are settled through the management `update-disposition` operation.
Session deferred receive requires a live session hold on the associated receiver
link in that same connection. The state machine validates the hold before
touching message records or expiry cleanup; naming a session or replaying an old
hold cannot bypass another receiver's ownership. The old identifier-only core
commands retain their serialized shape for trusted callers, but are not used
by the management edge.
Dead-letter waiters are notified from committed ready-index enqueues, including
lazy expiry during ordinary or deferred receive. Staged writes that are later
rejected, failed storage commits, and drop-on-expiry cleanup do not wake them.
This notification metadata adds no persisted or serialized command fields.
Retrieving only expired deferred messages commits their cleanup before
returning `com.microsoft:message-not-found`; missing messages are distinguished
from a missing queue. Dead-letter management paths normalize the reserved
suffix in the same way as receiving links, including the SDK's mixed-case form.
Deferred receive rejects duplicate sequence numbers before acquiring locks or
removing expired messages, so one message cannot be returned twice by a batch.
Management replies have a local 4 MiB ceiling and honor the reply receiver's
advertised maximum message size. Deferred batches must fit a conservative
content budget, including broker metadata and response fields, before any
locks, deletions, or expired-message cleanup commit. An oversized batch is
refused atomically with `amqp:link:message-size-exceeded`; a smaller retry can
still retrieve its messages. Peek returns a fitting prefix, or a size refusal
if its first eligible message cannot fit. These are local resource policies,
not Azure batch-count limits; the estimate may refuse a compact encoding that
would fit on the wire. It bounds content and staging work, not exact heap usage.
A reply link must be attached before a management
command is submitted. If it cannot be found within two seconds, the request is
rejected without changing broker state. Outgoing oversized messages detach
only their affected link and are never transferred.
Modified outcomes carry application-property updates; SDK dead-letter outcomes
carry the reason, description and updates in their error information. Management
settlement accepts `properties-to-modify` and promotes reserved dead-letter
fields when explicit reason/description fields are absent. Explicit fields win
when both forms are present. Null property values are retained, not interpreted
as deletion; these precedence and null policies have not been compared with a
live Azure namespace. A malformed or refused direct settlement closes its link
with the relevant condition and leaves the lock to expire.
Second-mode settlement is acknowledged only after the broker commits the
change. The Service Bus acknowledgement is Accepted on success or Rejected
with the actual refusal, rather than an echo of the receiver's requested
dead-letter outcome. Receivers that settle in first mode cannot await a broker
acknowledgement; a refusal still closes their link.
Scheduling and cancellation use the management node, while an ordinary send
can also schedule through the `x-opt-scheduled-enqueue-time` timestamp
annotation. Peek returns active, deferred, and scheduled state annotations.
A receiving link's `com.microsoft:session-filter` names a session or, with a
null value, asks for the next available one; the attach response echoes the
granted identifier and the initial session-lock deadline. The receiving task
releases its exact session hold when that link closes, including delivery or
settlement error exits. A first-mode disposition followed immediately by detach
can race native receipt finalization; cleanup neither reverses a committed
settlement nor relaxes the retired-link ownership checks. The detach
acknowledgment is not itself a barrier for committed domain cleanup. Renewing
the session lock and reading or writing its
state use the entity's `$management` request/reply links, as does message-lock
renewal. Scheduling and cancellation require Send authorization; receiving,
peeking, settlement, and lock or session operations require Listen. Management
links accept either permission, and every request rechecks its own permission
when authentication is enabled. A transfer is accepted only after its command
committed, so the
acknowledgement means durable. One node still serves one namespace. A message
drained from a dead-letter queue carries its reason and description in the
`DeadLetterReason` and `DeadLetterErrorDescription` application properties. The
complete protocol coverage uses a Rust AMQP 1.0 client. The current and previous
stable official .NET SDKs also have opt-in gates for ordinary send, receive,
peek, deferral, deferred receive, message-lock renewal, completion, scheduling,
and cancellation, duplicate detection for ordinary and scheduled sends, plus
session state, renewal, receive, completion, and scheduling. Both gates exercise
message properties, application-property CLR types, footer data, a lifetime
longer than the AMQP header can represent, and property-preserving redelivery;
both also exercise immediate topic fanout, independent subscription settlement,
peek, lock renewal, deferral, deferred receive, dead-letter reasons and property
updates, and both topic batch-send APIs with ingress duplicate detection. These
gates cover the local shared-sequence policy, not cloud parity for that policy.
Both pins also cover independent subscription sessions and management-only
session browsing, plus parent topic scheduled browsing, cancellation, annotated
send, and timer activation. Boolean/correlation rule creation, removal,
enumeration, and delivery selection are also exercised. SQL gates cover exact
source enumeration, numeric and case-aware selection, missing/null properties,
correlation overlap, both batch APIs, current-rule scheduled activation, and
filter-error dead-letter/drop policies. Separate [REMOVE action](sql-actions.md)
gates on both pins and backends prove exact action source enumeration, original
filter independence, one OR-combined base plus two independently settled action
copies, exact-key removals, final RuleName collision handling, body/system/footer
preservation, and an awaited unsupported-SET refusal followed by a healthy send.
They restore default rules, verify empty subscriptions and dead-letter queues,
and check committed cleanup again after reopen. Separate
[receive-batch gates](dotnet-receiving-batches.md) prove three action copies held
on one receiver before any completion, independent out-of-order settlement, and
rolling queue replenishment with fixed prefetch three. Both pins run both
backends and check exact empty runtime indexes and counters after reopen.
Azure administration remains
ungated. These checks establish local interoperability, not cloud parity for the
documented SQL semantic choices.

The SDK gates build into separate temporary directories and run the resulting
assemblies directly. Run them explicitly with
`cargo test -j 2 -p server --test amqp_dotnet_current -- --ignored --test-threads=1`;
each .NET build is limited to two jobs, and serial test execution preserves that
limit across the two releases.
The action and receive-batch gates reuse the bounded transaction process runner:
each build and client process has a 180-second deadline and bounded output capture, with
owned-process cleanup and isolated certificate trust. This does not add process
lifetime guarantees to the older ordinary-message or WebSocket gate runners.

### Message Content

Stored content has a protocol-neutral typed representation. Message and
correlation identifiers keep their AMQP types, including the distinction
between a missing identifier and an explicitly empty string or binary value.
Standard properties, application properties, message annotations and footer
entries survive receive, redelivery, deferral, scheduling and dead-lettering.
Data and sequence bodies retain every section, including empty sections; value
bodies retain nested containers, symbolic descriptors and scalar types. Empty
AMQP arrays are explicitly refused because the current value model cannot
retain their element constructor; heterogeneous arrays and duplicate map keys
are refused before storage. Application properties accept scalar values and
scalar described extensions, not compound containers. Float
and decimal values retain their raw bits. Delivery annotations are hop-local
and are consumed rather than forwarded.

Untrusted value parsing is bounded to 68 nesting levels, 132,096 value
nodes, and 4 MiB of copied binary/string/symbol data across a message. Shared
array descriptor names count once per expanded element, before allocation.
Stored message content is bounded separately to 64 levels and 65,536
nodes; the parser allowance covers envelope sections and metadata map keys.
Malformed sizes/counts, duplicate map keys and invalid symbols are refused.

Broker-owned sequence, enqueue time, state, lock and scheduling annotations
override producer values. Delivery count is broker-owned, and `first-acquirer`
is emitted as false rather than repeating an unverified producer assertion.
Dead-letter reason and description override those
application properties when draining the dead-letter queue, while the rest of
the application bag remains intact. Reserved dead-letter properties are cleared
from ordinary deliveries; exact Azure parity for that case is unverified.
The broker strips lifetime and session
from dead-letter deliveries. For active messages with finite lifetimes, the
creation/absolute-expiry timestamp pair is emitted as broker enqueue/deadline:
the official clients use that pair both to reconstruct long lifetimes and to
expose expiration. Producer creation time is otherwise retained, including in
dead letters. Pending schedules retain their effective lifetime before it
starts at activation.

These are semantic preservation guarantees, not byte-identical forwarding for
signed AMQP envelopes: map order and optional empty sections may normalize.
Duplicate detection still compares the text-normalized identifier, so distinct
AMQP identifier types with the same normalized text share history; exact Azure
behavior for that case is unverified. Rich messages enforce the configured
size limit using a conservative content tally that includes metadata and body
sections, rather than only flattened body bytes. Properties have a local 32 KiB
limit, and the header has a local 64 KiB limit, including standard properties,
application properties and message annotations. Accounting uses UTF-8 key bytes
plus conservative type/length/value overhead. New sends reserve 512 header bytes
for broker fields and ordinary dead-letter reasons; projected dead letters use
256 bytes for remaining broker fields. Footer content counts toward the message
limit, not the header limit. Settlement checks the merged property bag and
projected canonical dead-letter fields. These bounds implement the documented
quota sizes conservatively; exact Azure byte accounting and AMQP wire-size
parity remain unverified.

## Native Administration

The optional `--admin-listen` endpoint serves native queue/topic/subscription
create/get/list/update/delete through the broker owner. It is a
separate HTTP/2 listener and reuses the AMQP
TLS identity and shared-access policy when configured. Authenticated requests
require TLS and a SAS token in `authorization` metadata with Manage permission
for the requested entity. Queue and topic listings need namespace Manage;
subscription listing needs Manage on its parent topic, so an exact-child grant
cannot enumerate siblings. The configured namespace is the only namespace
accessible through that endpoint. Dead-letter shadows cannot be administered.
Subscription definitions use their typed configuration rather than exposing
their backing queue through queue commands. Entity capacity and usage fields
are absent because quota accounting
is not implemented; they are not reported as zero-byte measurements.

The existing `EntityService` RPCs and field numbers are retained. Creation has
separate presence-aware queue, topic, and subscription configurations; wrong-kind
or mixed legacy settings are refused instead of ignored. Get returns the
committed entity kind and only its matching configuration. A subscription path
is canonicalized only at its structural `/subscriptions/` separator, preserving
literal parent and member spelling. Native SAS audiences use canonical entity
paths and remain case-sensitive; AMQP scope conversion is not applied here.
Read-only topology queries run on the owner without consulting the clock or
stamping commands, validate complete metadata before returning a result, and
refuse corrupt or dangling topology without partial responses.
The presence-aware subscription setting
`dead_lettering_on_filter_evaluation_exceptions` preserves an explicit false;
creation omission uses the default true, while update omission preserves the
committed value. Get/list and `switchyardctl` JSON report the
committed value. It is not a backing-queue setting.

List defaults to queues only, preserving existing queue clients and their `v1.`
tokens. Explicit topic and subscription kinds use separate tokens bound to the
namespace, entity kind, and subscription parent. Pages are ordered and exclusive,
with a default size of 100 and a maximum of 1,024. Topic discovery scans one
bounded index page; subscription listing validates all at most 32 members before
slicing a page. Queue listing hides subscription backings and all shadows. Each
request discovers at most 4,096 physical queue-index rows, counting backend
lookahead, over at most 16 owner discovery turns. These bounds apply to queue
discovery, not the subsequent metadata reads for returned entities. A request
that reaches either budget before exhaustion may return a partial or empty page
with an opaque `queue.scan.v1.` progress token. That token resumes exclusively
after the last consumed raw row, never after an unseen lookahead row; a deleted
marker remains a valid keyset position. Clients must continue while the token is
nonempty, regardless of the number of entities in the page. Ordinary visible
lookahead still emits the existing `v1.` token, whose decoding is unchanged.
All token families have a 512-byte ceiling and reject cross-context tokens before
owner reads. Topic, subscription, and queue-progress tokens also require
canonical base64 and protobuf encodings. A hidden path in a queue-progress token
is only a cursor position; it never authorizes or returns that entity through
queue listing.

Local defaults admit 128 sockets and 128 concurrent requests across service clones,
with at most 32 HTTP/2 streams per connection, a 10-second TLS handshake deadline,
30-second request deadlines, 64 KiB decoded requests, and 1 MiB encoded replies.
HTTP/2 connections send keepalive pings after 30 seconds, allow 10 seconds for an
acknowledgment, and retire after five minutes with a 30-second graceful deadline.
These are local resource policies. The cluster, namespace,
backup, and audit services return unimplemented
rather than simulated success.
This endpoint is not Azure Atom/XML administration compatibility.
The additive `RuleService` serves create/get/list/delete on a canonical
subscription path with Manage authorization before filter/action parsing or store
access. It preserves exact scalar constructors and SQL source/version, uses an
in-flight subscription-incarnation fence, and returns complete sorted lists
under the existing 32-rule limit. Bounded REMOVE actions use a separate
`CreateRuleWithAction` method; the original `CreateRule` remains action-free.
Get/List default to refusing action metadata unless `include_actions` is true,
so older clients do not silently receive incomplete definitions. Mutations are
synchronous, with no upserts, retry deduplication, or post-commit reread. It shares the entity service's
admission and transport bounds; see [Native Rule Administration](native-rules.md).
Native rule binding preserves literal parent/control-name bytes without AMQP
address parsing. Authenticated paths still require the existing SAS grammar,
which excludes leading empty segments; no native-name or SAS normalization is
added by this API.
`switchyardctl queue create|get|list|update|delete` exposes these operations with JSON
responses and nonzero errors. It reads SAS tokens only from bounded regular
files, marks their metadata sensitive, and verifies TLS against explicitly
supplied CA certificates. Plaintext is opt-in, loopback-only, and cannot carry a
token. Command-line settings preserve omitted, false, zero, and unlimited TTL.
`switchyardctl rule create|get|list|delete` addresses a topic/subscription member
and uses a bounded typed filter file plus an optional `--action-file` for creation.
An action selects only the new RPC, with no action-free retry on older servers.
Its JSON preserves scalar
constructors, exact floating-point bits and octets, and 64-bit values as strings.
It requests complete action metadata and validates whole replies before emitting
them; absent actions keep their original JSON shape. It does not add updates or
retries; see [Native Rule Administration](native-rules.md).

Entity deletion commits one bounded atomic purge, cascades topic-owned
subscriptions and shadows, and retains counter tombstones across recreation.
Missing targets return NotFound; oversized plans return ResourceExhausted with
all state unchanged. The successful native `Operation` is synchronous, not a
pollable job. Receiver wakeups, exact ownership validation, cleanup limits, and
live-link incarnation fencing are defined in
[Entity Deletion](entity-deletion.md). No Azure administration endpoint or
immediate global link-retirement parity is claimed.

### Configuration Updates

Updates use independent presence-aware patch fields: queue remains protobuf
tag 3, with topic and subscription appended at tags 4 and 5. Exactly one family
is required, and it must match the target kind. Authorization precedes path,
patch, and owner access. Omitted settings stay unchanged; explicit false and
unlimited lifetime are distinct from omission. Empty or equal patches stage
nothing and do not advance the applied clock, but still validate topology and
pass the ordinary proposer clock check.

Topics allow changes to default lifetime, message size, and duplicate-history
window; subscriptions allow lock duration, delivery count, default lifetime,
message size, and both dead-letter policies. Topic duplicate-detection enablement
and subscription session enablement are creation-only, consistent with the
documented [duplicate-detection](https://learn.microsoft.com/en-us/azure/service-bus-messaging/enable-duplicate-detection)
and [session](https://learn.microsoft.com/en-us/azure/service-bus-messaging/enable-message-sessions)
constraints. Restating the same value is allowed; a change returns native
`FailedPrecondition` without committing any part of the patch.

A topic update validates complete bounded membership and changes one metadata
key. A subscription update validates its target and parent, then commits
membership, backing queue, and shadow configurations together. Neither repairs
corruption, compiles rules, or scans and rewrites messages, indexes, counters,
sessions, locks, or duplicate history. Existing deadlines remain captured.
New receive/renew operations use the current lock default; later expiry,
delivery-count decisions, and SQL error routing use current policy. Existing
dead letters are not moved or changed. Changing a history window affects newly
accepted IDs, not existing history deadlines.

A pending topic publication retains its admission-clamped topic lifetime, so
raising that default cannot extend it. Activation applies any shorter current
topic ceiling and the current subscription ceiling. A raised subscription
ceiling can therefore change a future copy's lifetime at activation, but not
an existing copy. New size ceilings apply at activation too; a failed activation
leaves the original schedule pending and cancelable. Already-attached AMQP
producer links keep their advertised maximum size until reopened. These are
local update policies, not cloud-verified timing guarantees. The value format
and store layout remain unchanged by configuration updates.

`switchyardctl topic update` and `subscription update` expose the same partial
patches, scoped credentials, typed JSON responses, and nonzero refusal behavior
as `queue update`.

## Durable Format

The current value format is version 11 and durable store layout is version 14.
Value format 11 appends optional source-only [SQL actions](sql-actions.md) with
semantic version 1; legacy rule definitions decode with no action. Maximum-sized
old rules are validated and charged by their actual stored envelopes, not a
larger rewritten shape. Relabeling the new rule shape as an older format is
refused. Value format 10 introduced the subscription filter-error policy and a
source-only SQL filter variant with semantic version 1. Legacy subscription
configurations decode with the default true; relabeling the new shape as an older
value format is refused. SQL rules likewise cannot be relabeled as pre-version-10 records.
Message and queue shapes are unchanged, and existing reason tags stay intact;
the missing-session reason retains its version-9 rollback guard.
The layout also requires retained [entity incarnations](entity-incarnations.md),
which prevent old admitted endpoints from addressing recreated names. The
layout protects SQL filter/action interpretation and the subscription policy,
in addition to explicit rules, session-bearing ordinary subscription indexes,
and parent-retained topic schedules. An older build could otherwise decode the
wrong configuration shape, fail to interpret SQL, or route publications under
an older contract.
Earlier message and queue-configuration shapes have tested decoders, but an
earlier store directory is refused at open because its broker contract differs.
There is no directory migration tooling yet; development directories
from older builds must be recreated. A rollback likewise refuses a newer layout.

All of it now runs on either backend. The Fjall backend fsyncs a command's batch
before reporting it applied, and the same semantics suite runs against both
backends, so a single node keeps its messages, locks, delivery counts, and
sequence numbers across a restart. Preserving them across the loss of a node
still needs replication.

Switchyard intentionally does not reproduce Azure subscription, namespace
capacity, or operations-per-second commercial quotas. It defaults to compatible
message validation, including a default 256 KiB content-size limit, while
allowing operators to configure larger limits. Full wire quota parity is not
yet claimed.
