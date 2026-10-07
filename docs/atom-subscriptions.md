# Atom Subscription Administration

The opt-in HTTPS administration listener provides a closed create/get/full-update/delete profile for ordinary, non-session subscriptions under topics already created through native administration. It shares the existing mandatory TLS, fixed audience and independent HTTPS SAS Manage policy; it adds no listener, credentials, CLI flags or domain/store format. General Azure administration remains incomplete.

## Supported Requests

| Operation | Request | Response |
| --- | --- | --- |
| Create | PUT /{topic}/Subscriptions/{name}, no If-Match | 201 with the prepared description |
| Full update | PUT /{topic}/Subscriptions/{name}, singleton If-Match: * | 200 with the prepared replacement description |
| Get, including existence checks | GET /{topic}/Subscriptions/{name} | 200 with static configuration, or 404 |
| Delete | DELETE /{topic}/Subscriptions/{name}, empty body | Empty 200 after the existing deletion commits |

Every request requires `api-version=2024-05` or `2021-05`. Get accepts absent `enrich` or `enrich=False` only. Only a singleton `If-Match: *` selects full update; other or duplicate conditions are refused. Subscription list/paging, runtime properties and topic creation are unsupported. The separate [Atom Rule Administration](atom-rules.md) profile supports True/False and bounded native SQL no-action/no-parameter rule create/get/list/delete, not rule updates, correlation filters or actions. A primary queue literally named `topic/Subscriptions` or `topic/subscriptions` keeps its existing ordinary queue lookup contract; omission of subscription listing does not reserve that path or guarantee a particular collection-route status.

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

Forwarding, idle deletion, user metadata, unknown properties and queue/topic-specific flags are refused. This model carries no subscription quota, MiB capacity, maximum-message-size or runtime-count field. The native per-copy `max_message_bytes` must equal the default 262,144 for this profile; a native session subscription or other incompatible scalar setting is refused by Get, Update and Delete rather than projected lossily. Parent topic configuration and data-plane limits remain native settings, not values reported by this description.

On create, `DefaultRuleDescription` may be absent or describe only the supported default: Name `$Default`, Filter `xsi:type="TrueFilter"`, SqlExpression `1=1`, an optional empty Parameters element, and no Action. The accepted bare type must resolve to the Service Bus namespace at its Filter element, not merely match the literal TrueFilter spelling. The pinned SDK default serializers include that nested rule and an empty Parameters element. Custom rule names, filters, actions or parameters are refused before mutation; the server never creates then changes a rule to approximate the request.

Update refuses every `DefaultRuleDescription`, including an empty or otherwise supported default rule. Each update is a complete definition, not a patch: omitted fields reset to the table defaults, including Unlimited TTL. The hidden message limit is proved but never changed. Existing custom rules remain opaque and untouched.

Domain creation atomically installs its existing single `$Default` true/no-action rule. Get does not expose the rule set and does not fabricate `DefaultRuleDescription`. Compatible native custom rules can coexist with configuration Get; their contents are neither observed nor certified by the HTTP response. The separate closed Atom rule profile exposes supported rules without changing the native/AMQP rule interfaces.

The two pinned SDK source contracts are described by [SubscriptionPropertiesExtensions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/SubscriptionPropertiesExtensions.cs), [CreateRuleOptions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/Rules/CreateRuleOptions.cs), and [RuleDescriptionExtensions](https://raw.githubusercontent.com/Azure/azure-sdk-for-net/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/Rules/RuleDescriptionExtensions.cs). These source observations are separate from the actual loaded-binary and SDK gate evidence below.

## Literal Authorization

Both pinned SDKs emit the reserved marker `Subscriptions`. The adapter accepts that spelling or lowercase `subscriptions` for member routes and maps only that marker to core lowercase BEFORE both ResourceScope creation and owner selection. Topic/name case and literal bytes are preserved; there is no general path lowercasing or AMQP control-alias conversion. A credential scoped only to an alias spelling cannot authorize the canonical owner by accident.

Thus `/orders/Subscriptions/audit` selects the literal scope/owner `orders/subscriptions/audit`. Scoped HTTPS SAS audiences must select that canonical literal path. Request path segments and signed audience URI segments each decode once through their respective existing validators. For a literal topic segment `Orders%2Fbranch`, use URI segment `Orders%252Fbranch`; token `sr` form encoding is a separate layer, not permission for recursive decoding.

Configured host and namespace, never Host/forwarded headers, socket port, SNI or XML, choose authority. HTTPS SAS uses effective default audience port 443; the actual ephemeral/listening socket port is not its resource scope. Manage is checked before XML/body polling and owner work, then the original grant is rechecked immediately before starting the asynchronous owner operation. This does not promise another check at eventual enqueue or execution, or revocation/cancellation of work already admitted.

## Owner and Deletion

Each new request family runs in one serialized owner turn. Create preflights the desired profile and live parent, then submits the existing ordinary atomic subscription creation: membership, backing configuration, DLQ, child incarnation and default rule share one batch. Its matching committed outcome returns the admitted configuration without a postcommit store read. No parent binding is misused as a prospective-child fence.

Get proves complete existing child topology and live child identity, then the live parent Topic identity in that same turn, before the closed response projection. It neither stamps a command nor applies a batch. Existing stored corruption keeps its original priority; a missing or retired parent identity is not manufactured as an absent child. This is not a whole-ledger or rule-health proof.

Within the owner turn, after bounded HTTP XML decoding, Update proves the current complete child topology and live parent/child identities before validating the current and desired closed profiles. It uses the current child fence and the original atomic subscription-update planner in that same owner turn. A changed update writes only the membership, backing and DLQ configuration projections plus the durable Clock, and returns the prepared desired configuration without a postcommit read. It does not rewrite existing messages, expiry/lock deadlines, rules, identities or counters, and sends no deliverability wakeups. Identity fencing protects against delete/recreate, not concurrent configuration edits.

An exact no-op still stamps a command and checks the host/stored Clock, but performs no store apply and does not advance the durable Clock even if the host clock advanced. An absent Update runs the original ordinary UpdateSubscription refusal planner with one stamp and its existing clock/topology priority; unlike absent Delete, it does not prove orphan runtime or rule health.

Live Delete validates the supported current scalar profile, captures the current child binding and applies the existing child-fenced deletion in that same turn. It retains original bounded runtime/rule purge, retirement and committed-only child/DLQ wakeups. It does not call XML Get as a prerequisite, repair corrupted metadata or treat Usage/Charge rows as subscription quotas.

When the child is absent, Delete deliberately runs the ORIGINAL ordinary DeleteSubscription planner in that same owner turn, retaining its orphan runtime/rule diagnostics. That fallback honestly stamps one command, so absent Delete is NOT clock-free and preserves the original clock error priority. A lost HTTP reply, timeout or cancellation provides no known-commit retry or rollback guarantee.

## Bounds and Errors

The existing listener bounds remain: 128 connections including handshakes; 10-second TLS/header/body deadlines; 20-second owner observation and 60-second connection lifetime. Single-use HTTP requests have 4,096-byte targets, 32 headers/16 KiB logical header bytes, 8 KiB tokens, and 64 KiB bodies with at most 1,024 frames. XML limits depth 16, 2,048 events including EOF, 32 attributes per element, 64 active namespace bindings plus parser built-ins, and 128 properties; replies are at most 1 MiB. These are checked work/output limits, not allocator/RSS or graceful-drain guarantees.

Missing topic/subscription maps 404, duplicate subscription 409, unsupported definition or update condition 400, known 32-subscription topology limit 503 and stored corruption 500. The topology bound is a local work/admission limit, not an Azure subscription quota. All public errors are static and omit keys, XML, entity names and arbitrary backend diagnostics. Already admitted owner work may finish after the HTTP observation deadline.

## Library Verification

The preceding create/get/delete library publication's full Rust workspace passed 5,898 tests, with 17 existing opt-in cases ignored, across 162 result groups. All 5,844 cases in the preceding full run were retained, including statuses and ignore reasons. The 54 additions comprise 14 already-published finite-queue CLI cases and 40 subscription library cases. All 119 current CLI cases were retained.

The complete focused library/transport run passed 353 cases across three targets: all 313 prior cases plus 16 codec, 16 paired owner and eight paired TLS additions. The TLS cases use actual private-CA and hostname validation with healthy controls; the owner cases run on Memory and Fjall. Both strict workspace lint/build configurations and formatting passed on the same 17-source revision. Builds and tests used two CPU cores and two build jobs. Domain/store layouts remain 17/11, with no new dependencies or CLI flags.

Injected storage refusals occur before apply; they do not establish rollback after an ambiguous physical commit or lost reply. Same-domain replay comparisons are not an independent implementation oracle. Reopen checks cover the named fixture-owned stores, not universal process shutdown or cleanup.

## Official Client Verification

For the preceding create/get/delete publication, the pinned .NET clients 7.21.0 and 7.20.2 each passed a separate opt-in subscription gate on Memory and Fjall, using both named-key and connection-string administration clients. Each pin ran 11 awaited child processes per backend with exact completed markers. Wrong CA, wrong hostname and Send-only credentials were refused before owner work, bracketed by healthy controls. The requested SDK versions were built separately; each child's loaded Service Bus and Azure.Core file records were compared with its owned build output by the Rust verifier. Those output directories were temporary; this records runtime checks, not ongoing custody or an independent NuGet cache inventory.

The successful scenarios cover complete default/custom scalar creation, Get/Exists, duplicate and unsupported-profile/rule refusals, deletion and generation-two recreation. Trusted native fixtures add ready/expiring messages, dead letters and non-default rules before configuration Get and Delete. Raw-state checks verify unchanged Get results, bounded owned-state purge, restored default rules, retained counter fences, and unaffected parent/native-sibling state. Reopen checks cover the fixture-owned Memory/Fjall stores. Topics and subscriptions naturally have no CapacityMode/Usage/Charge rows; their absence is checked, not presented as nonvacuous quota coverage.

| SDK pin | Assembly | Loaded version | SHA-256 |
| --- | --- | --- | --- |
| 7.21.0 | Azure.Messaging.ServiceBus | 7.21.0.0 | `8B43506EEA82C852639E81754D1553DCD29816E8EE9A1F208E0CAD1F82F0A8B7` |
| 7.21.0 | Azure.Core | 1.62.0.0 | `176236AFE4BB4D07773806D3473654E57B3A42C3D9A6D639EF03290669AB7AAE` |
| 7.20.2 | Azure.Messaging.ServiceBus | 7.20.2.0 | `FAB2ACB2D56FC9AFB8CA7ADFE373ABD5E4E645355A03E7461D4384298D1BA891` |
| 7.20.2 | Azure.Core | 1.60.0.0 | `D7DFB9CC346B225A661C71F93D2996667B8E2F56B6E4FAB8F825AB6B205C0939` |

The complete Rust SDK-target run passed 53 regular cases, with 19 opt-in cases ignored: all preceding 52 regular and 17 ignored cases retained, plus one regular replay-isolation test and the two new opt-in gates. The two gates passed separately, rather than being counted as regular workspace cases. Existing finite-queue and other SDK gates were not rerun.

The preceding SDK publication's full Rust workspace run passed 5,899 tests, with 19 opt-in cases ignored, across 162 result groups. All 5,898 previously passing cases and all 17 prior ignored cases were retained with their statuses and ignore reasons; only the one regular test and two opt-in gates were added. All 119 current CLI cases were retained. Formatting, both strict workspace lint/build configurations, the complete focused target, both SDK gates and the full workspace run passed on the same eight test-source images and unchanged 17 library-source images. Builds/tests used the shared cache, two CPU cores and two build jobs; the SDK gates ran serially.

The HTTP adapter remains a closed library profile, not general Azure administration. Those preceding gates did not certify subscription updates. Those preceding SDK gates also did not certify rule lifecycle. The separate closed rule library profile is described in [Atom Rule Administration](atom-rules.md). Subscription list/runtime, topic HTTP administration, production readiness and a CLI-launched broker remain outside that evidence. Same-domain replay is not an independent implementation oracle. The inherited TLS fixture still lacks a universal shutdown deadline; synchronous broker drop can block, and a primary panic can mask secondary cleanup diagnostics. Successful stage/cleanup records do not strengthen those lifecycle guarantees.

## Full-Update Verification

The focused update run passed 428 cases across four distinct server targets, with 19 existing opt-in cases ignored. All 406 preceding passing and 19 ignored cases were retained with their statuses and ignore reasons. The 22 additions are four codec cases, twelve paired owner cases and six paired TLS cases. The SDK support target remains 53 regular passing cases and 19 ignored opt-ins; the two existing opt-ins were extended rather than adding or renaming test identities.

Both pinned .NET clients separately passed the expanded subscription gate on Memory and Fjall. Each backend ran twelve awaited child processes, adding full updates after retained-state inspection and before deletion. Both administration constructors change the Default subscription and reset Definition to complete defaults/Unlimited, with an exact no-op repeat of each. Each backend observes exactly four changed config-triple-plus-Clock batches and eight update stamps. Full raw checks preserve pre-existing Unlimited messages after a finite default is installed, and the original 45-second messages/expiry index after configuration resets to Unlimited. Rules, identities, counters, parent and native sibling rows remain exact.

All four stage/cleanup finishes succeeded; the Rust verifier checked 96 loaded assembly observations across both pins against their owned build outputs. The loaded versions and hashes match the preceding table. This is runtime evidence, not ongoing DLL custody or an independent package-cache inventory. Preapply refusal, same-domain replay and named-store reopen limits remain as stated above; no other SDK opt-ins were rerun.

The full workspace passed 5,921 cases, with 19 opt-ins ignored, across 162 result groups. All 5,899 preceding passing cases and all 19 ignored cases were retained, including statuses, ignore reasons and multiplicities; the same 22 update cases are the only additions. All 119 current CLI cases were retained. Formatting, both strict workspace lint/build configurations, the complete focused run, both expanded SDK opt-ins and the full workspace run passed on the same frozen 18-source revision. Commands used the shared cache, two CPU cores and two build jobs; SDK runs were serial. Domain/store layouts remain 17/11, with no new dependencies or CLI flags.
