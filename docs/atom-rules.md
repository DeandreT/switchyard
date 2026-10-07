# Atom Rule Administration

The opt-in HTTPS administration listener provides a closed create/get/list/delete rule profile under existing ordinary subscriptions. It shares mandatory TLS, the fixed audience and HTTPS SAS Manage policy with [subscription administration](atom-subscriptions.md). It does not add credentials, listener options, dependencies or a domain/store format.

## Requests

| Operation | Request | Response |
| --- | --- | --- |
| Create | PUT /{topic}/Subscriptions/{subscription}/Rules/{rule}, no If-Match | 201 with the prepared static description |
| Get | GET /{topic}/Subscriptions/{subscription}/Rules/{rule} | 200 with a static description, or 404 |
| List | GET /{topic}/Subscriptions/{subscription}/Rules | 200 with an Atom feed |
| Delete | DELETE /{topic}/Subscriptions/{subscription}/Rules/{rule}, empty body | Empty 200 after the original deletion commits |

All requests require `api-version=2024-05` or `2021-05`. Get/List accept absent `enrich` or `enrich=False` only. List accepts decimal `$skip` in 0..1000 and `$top` in 1..100, defaulting to 0/100. Every If-Match condition, including `*`, is refused; rule update is not implemented. Read/delete/list bodies must be empty.

## Closed Definition

Create accepts Atom entry/content containing a Service Bus `RuleDescription` with explicit Name and Filter. Name must exactly match the decoded route leaf. The only filters are `xsi:type="TrueFilter"` with SqlExpression `1=1`, or `FalseFilter` with `1=0`. The accepted bare type must resolve to the Service Bus namespace at that element. Parameters may be absent or empty. SQL/correlation filters, nonempty parameters, every Action (including empty), CreatedAt and unknown properties are refused rather than approximated. Missing names/filters are never defaulted.

Names preserve case, surrounding spaces, Unicode and literal percent. They must be nonblank, XML-legal, at most 50 UTF-16 units, and contain no controls or `/`, `\`, `@`, `?`, `#`, `*`; dot/dotdot leaves are refused. The subscription must meet the existing ordinary Atom scalar profile, including `requires_session=false` and the hidden 262,144-byte per-copy message limit. Parent topic settings remain native, not a fabricated Azure topic quota profile.

Responses include matching leaf title and explicit Name/typed Filter, with no Action, creation date or runtime counts. Get projects only the selected compatible rule after complete stored-set health checks. List first validates the complete set's representability, then selects a page: an unsupported rule outside the requested page still refuses the whole list. Other healthy native SQL/action rules can coexist with compatible single-rule Get and ordinary Create/Delete, but are not wire-supported or silently stripped.

The native maximum is 32 rules per subscription (including $Default), 64 KiB per stored rule and 256 KiB per complete set. Small raw HTTP pages are supported; normal pinned SDK top=100 requests fit one page. Empty feeds use paired start/end tags, not a self-closing element. No SDK multipage or rule-date claim follows.

## Authorization

Only structural `Subscriptions`/`subscriptions` and `Rules`/`rules` markers are canonicalized to lowercase, before BOTH literal ResourceScope creation and owner selection. Topic, subscription and rule spelling remains unchanged. Thus `/Orders/Subscriptions/Worker/Rules/Keep` selects `Orders/subscriptions/Worker/rules/Keep`; alias-only scoped SAS credentials do not gain canonical access. URI path and audience segments decode once, with token form encoding as a separate layer.

A subscription literally named Rules remains a subscription member; terminal primary `topic/Subscriptions` paths retain ordinary queue behavior. Authority comes from the configured host/namespace, never Host, forwarding headers, socket port, SNI or XML. Manage precedes operation/XML/body polling and owner work. The original grant is rechecked immediately before STARTING the asynchronous owner operation, not at eventual enqueue/execution; timeout or cancellation does not revoke already-admitted work.

## Owner Effects

Each operation runs in one serialized owner turn. It proves complete current child metadata/capacity exclusions and live child identity, then minimal live parent Topic identity and the closed subscription profile. Get/List use the complete bounded fenced rule getter before selection, validating every stored rule's key/name/topology/size and native SQL/action semantics. They stamp no command, apply no batch and retain stored timestamps/bytes.

Create/Delete use the original child-fenced planners. Success stamps once and atomically writes only the targeted rule Put/Delete plus Clock. No configuration, message, deadline, DLQ, identity, counter or other rule row is rewritten; no deliverability wakeup is published. Create returns its checked prepared definition without a postcommit read. Duplicate creation remains an error, not overwrite/no-op. Deleting `$Default` when it is the only rule leaves a genuinely empty set; nothing recreates it automatically. An empty set or sole False rule routes no subscription copy, while explicit True recreation restores matching delivery. Already retained copies stay untouched. Explicit recreation stores the new command's domain timestamp, which is not projected onto the wire.

Missing-child admission is bind-first, pre-stamp 404; it is NOT an orphan-rule/whole-ledger health proof or repair. On a live child, duplicate Create and missing-rule Delete stamp once, apply nothing and do not advance durable Clock. Stored admission/profile corruption and pure-read failures are pre-stamp. Malformed stored RULE values in mutations retain original AFTER-stamp planner priority, not a new pre-read shortcut. Stale child fences after delete/recreate fail before stamping; identity fencing is not a configuration compare-and-swap.

## Bounds and Errors

The existing listener, header, target, token, body-frame and XML budgets remain unchanged: 64 KiB input, depth 16, 2048 events, 32 attributes, 64 active namespace bindings, 128 properties, at most 100 encoded entries and 1 MiB replies. These are checked work/output limits, not allocator/RSS or graceful-drain guarantees.

Missing subscription/rule maps 404, duplicate rule 409, unsupported shape/condition/page 400, native rule-count/stored-byte and XML-work limits 503, and stored corruption 500. Existing header admission 431, HTTP body/target admission 400 and reply-output limit 500 remain unchanged. Diagnostics are static and omit names, bodies, credentials and backend details. Observation timeouts and lost replies provide no known-commit retry, rollback or cancellation guarantee.

## Verification

The complete focused run passed 493 cases across four server targets, with 19 existing opt-ins ignored. All preceding 428 passing and 19 ignored cases were retained with their statuses and ignore reasons. The 65 additions are nineteen rule codec cases, nine request cases, twenty-six paired owner cases, ten paired actual-TLS cases and one related subscription QName regression. Memory/Fjall owner tests check exact rule-plus-Clock batches, unchanged retained state, no postcommit reads or delivery wakeups, refusal priority, and actual native no-copy/copy behavior after default-rule deletion and False/True recreation.

The full Rust workspace passed 5,986 cases, with zero failures and 19 opt-ins ignored, across 162 result groups. All 5,921 preceding passing cases and all 19 ignored cases were retained, including statuses, reasons and multiplicities; only the same 65 regular cases were added. All 119 CLI cases across eight original owners were retained. Formatting, both strict workspace lint configurations, both all-target builds, the complete focused run, full workspace and the two separately executed subscription SDK regressions passed on the same frozen 24-source revision. Those nine gates ran serially using the shared cache, two CPU cores and two build jobs. Domain/store layouts remain 17/11, with no new dependencies or CLI flags.

The existing subscription opt-ins passed separately with Service Bus .NET SDK versions 7.21.0 and 7.20.2 on Memory and Fjall, using both named-key and connection-string constructors. Twelve awaited children per backend total forty-eight completed children and ninety-six loaded assembly observations, with successful stage/cleanup outcomes. The loaded versions and all four file fingerprints match the preceding [subscription SDK table](atom-subscriptions.md#official-client-verification); the source-bound verifier compared each observation with its owned build output during execution, not an ongoing DLL or package-cache inventory. The narrow regression reads $Default and refuses UpdateRule without mutation. It is not an official Rule Create/Get/List/Delete lifecycle or SDK message-selection gate; that verification remains pending. The other seventeen SDK opt-ins were not rerun, and all nineteen remain ignored in the regular workspace total.

The retained initial focused compiler failure identified two test-helper references: an unqualified SubmitError and a string QName copy using a byte-vector method. Only those two test expressions were corrected; production was unchanged. The first post-fix passing output was capped and is excluded from the admitted case ledger, not reconstructed. The complete focused and full-workspace receipts above are the actual verification authority.

Same-domain comparisons are not independent implementation oracles; preapply failures do not prove rollback after an ambiguous physical commit. Named Memory/Fjall reopen checks do not establish universal shutdown guarantees. Inherited blocking broker drop and potentially masked secondary cleanup diagnostics remain.
