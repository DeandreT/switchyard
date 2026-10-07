# Atom Subscription Administration

The opt-in HTTPS administration listener provides a closed create/get/delete profile for ordinary, non-session subscriptions under topics already created through native administration. It shares the existing mandatory TLS, fixed audience and independent HTTPS SAS Manage policy; it adds no listener, credentials, CLI flags or domain/store format. General Azure administration remains incomplete.

## Supported Requests

| Operation | Request | Response |
| --- | --- | --- |
| Create | PUT /{topic}/Subscriptions/{name}, no If-Match | 201 with the prepared description |
| Get, including existence checks | GET /{topic}/Subscriptions/{name} | 200 with static configuration, or 404 |
| Delete | DELETE /{topic}/Subscriptions/{name}, empty body | Empty 200 after the existing deletion commits |

Every request requires `api-version=2024-05` or `2021-05`. Get accepts absent `enrich` or `enrich=False` only. Subscription replacement with `If-Match: *`, list/paging, runtime properties, rule administration and topic creation are unsupported. A primary queue literally named `topic/Subscriptions` or `topic/subscriptions` keeps its existing ordinary queue lookup contract; omission of subscription listing does not reserve that path or guarantee a particular collection-route status.

## Definition Profile

The description is Atom entry/content with a Service Bus `SubscriptionDescription`. Response Atom title is the subscription leaf name only, not the topic or canonical owner path. The static fields are:

| Field | Default | Accepted values |
| --- | --- | --- |
| LockDuration | 60 seconds | Exactly representable milliseconds, 5..300 seconds |
| RequiresSession | false | false only |
| DefaultMessageTimeToLive | Unlimited | Omitted for Unlimited; present duration at least one second |
| DeadLetteringOnMessageExpiration | false | Boolean |
| DeadLetteringOnFilterEvaluationExceptions | true | Boolean |
| MaxDeliveryCount | 10 | Positive signed 32-bit range |
| EnableBatchedOperations | true | true only |
| Status | Active | Active only |

The 5-second lock minimum is this server's deliberate Atom profile restriction; the core and .NET setters permit some smaller positive values. Durations use the existing ordered day/time grammar and exact integral milliseconds, without calendar months, signs, rounding or truncation. The finite TTL maximum is the existing checked duration limit.

Forwarding, idle deletion, user metadata, unknown properties and queue/topic-specific flags are refused. This model carries no subscription quota, MiB capacity, maximum-message-size or runtime-count field. The native per-copy `max_message_bytes` must equal the default 262,144 for this profile; a native session subscription or other incompatible scalar setting is refused by Get and Delete rather than projected lossily. Parent topic configuration and data-plane limits remain native settings, not values reported by this description.

On create, `DefaultRuleDescription` may be absent or describe only the supported default: Name `$Default`, Filter `xsi:type="TrueFilter"`, SqlExpression `1=1`, an optional empty Parameters element, and no Action. The pinned SDK default serializers include that nested rule and an empty Parameters element. Custom rule names, filters, actions or parameters are refused before mutation; the server never creates then changes a rule to approximate the request.

Domain creation atomically installs its existing single `$Default` true/no-action rule. Get does not expose the rule set and does not fabricate `DefaultRuleDescription`. Compatible native custom rules can coexist with configuration Get; their contents are neither observed nor certified by the HTTP response. Rule management remains on the existing native/AMQP interfaces.

The two pinned SDK source contracts are described by [SubscriptionPropertiesExtensions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/SubscriptionPropertiesExtensions.cs), [CreateRuleOptions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/Rules/CreateRuleOptions.cs), and [RuleDescriptionExtensions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/Rules/RuleDescriptionExtensions.cs). These source observations are not new restored/loaded-binary or SDK conformance evidence.

## Literal Authorization

Both pinned SDKs emit the reserved marker `Subscriptions`. The adapter accepts that spelling or lowercase `subscriptions` for member routes and maps only that marker to core lowercase BEFORE both ResourceScope creation and owner selection. Topic/name case and literal bytes are preserved; there is no general path lowercasing or AMQP control-alias conversion. A credential scoped only to an alias spelling cannot authorize the canonical owner by accident.

Thus `/orders/Subscriptions/audit` selects the literal scope/owner `orders/subscriptions/audit`. Scoped HTTPS SAS audiences must select that canonical literal path. Request path segments and signed audience URI segments each decode once through their respective existing validators. For a literal topic segment `Orders%2Fbranch`, use URI segment `Orders%252Fbranch`; token `sr` form encoding is a separate layer, not permission for recursive decoding.

Configured host and namespace, never Host/forwarded headers, socket port, SNI or XML, choose authority. HTTPS SAS uses effective default audience port 443; the actual ephemeral/listening socket port is not its resource scope. Manage is checked before XML/body polling and owner work, then the original grant is rechecked immediately before starting the asynchronous owner operation. This does not promise another check at eventual enqueue or execution, or revocation/cancellation of work already admitted.

## Owner and Deletion

Each new request family runs in one serialized owner turn. Create preflights the desired profile and live parent, then submits the existing ordinary atomic subscription creation: membership, backing configuration, DLQ, child incarnation and default rule share one batch. Its matching committed outcome returns the admitted configuration without a postcommit store read. No parent binding is misused as a prospective-child fence.

Get proves complete existing child topology and live child identity, then the live parent Topic identity in that same turn, before the closed response projection. It neither stamps a command nor applies a batch. Existing stored corruption keeps its original priority; a missing or retired parent identity is not manufactured as an absent child. This is not a whole-ledger or rule-health proof.

Live Delete validates the supported current scalar profile, captures the current child binding and applies the existing child-fenced deletion in that same turn. It retains original bounded runtime/rule purge, retirement and committed-only child/DLQ wakeups. It does not call XML Get as a prerequisite, repair corrupted metadata or treat Usage/Charge rows as subscription quotas.

When the child is absent, Delete deliberately runs the ORIGINAL ordinary DeleteSubscription planner in that same owner turn, retaining its orphan runtime/rule diagnostics. That fallback honestly stamps one command, so absent Delete is NOT clock-free and preserves the original clock error priority. A lost HTTP reply, timeout or cancellation provides no known-commit retry or rollback guarantee.

## Bounds and Errors

The existing listener bounds remain: 128 connections including handshakes; 10-second TLS/header/body deadlines; 20-second owner observation and 60-second connection lifetime. Single-use HTTP requests have 4,096-byte targets, 32 headers/16 KiB logical header bytes, 8 KiB tokens, and 64 KiB bodies with at most 1,024 frames. XML limits depth 16, 2,048 events including EOF, 32 attributes per element, 64 active namespace bindings plus parser built-ins, and 128 properties; replies are at most 1 MiB. These are checked work/output limits, not allocator/RSS or graceful-drain guarantees.

Missing topic/subscription maps 404, duplicate subscription 409, unsupported definition/replacement 400, known 32-subscription topology limit 503 and stored corruption 500. The topology bound is a local work/admission limit, not an Azure subscription quota. All public errors are static and omit keys, XML, entity names and arbitrary backend diagnostics. Already admitted owner work may finish after the HTTP observation deadline.

## Verification

The full Rust workspace passed 5,898 tests, with 17 existing opt-in cases ignored, across 162 result groups. All 5,844 cases in the preceding full run were retained, including statuses and ignore reasons. The 54 additions comprise 14 already-published finite-queue CLI cases and 40 subscription library cases. All 119 current CLI cases were retained.

The complete focused library/transport run passed 353 cases across three targets: all 313 prior cases plus 16 codec, 16 paired owner and eight paired TLS additions. The TLS cases use actual private-CA and hostname validation with healthy controls; the owner cases run on Memory and Fjall. Both strict workspace lint/build configurations and formatting passed on the same 17-source revision. Builds and tests used two CPU cores and two build jobs. Domain/store layouts remain 17/11, with no new dependencies or CLI flags.

Injected storage refusals occur before apply; they do not establish rollback after an ambiguous physical commit or lost reply. Same-domain replay comparisons are not an independent implementation oracle. Reopen checks cover the named fixture-owned stores, not universal process shutdown or cleanup.

New official SDK subscription gates have NOT run. Existing finite-queue SDK receipts remain separate and are not subscription compatibility claims.
