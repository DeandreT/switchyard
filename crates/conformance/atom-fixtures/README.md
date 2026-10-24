# Atom collection profiles

This is a **source-derived, synthetic, scrubbed fixture contract**, not a wire
capture, SDK execution result, running administration endpoint, or compatibility
claim. All names, dates, property values and statistics are invented test inputs.
Nonzero statistics exercise parsing; they are not Switchyard runtime counts.

## Evidence and pins

`manifest.json` identifies public source revisions, exact source-file digests,
request shapes, response files, namespaces, and refusal expectations. The .NET
selectors refer to the existing `../pins/{declared-current,previous}` inputs:

| Selector | ServiceBus / Core | Default administration API |
| --- | --- | --- |
| declared-current | 7.21.0 / 1.62.0 | 2024-05 |
| previous | 7.20.2 / 1.60.0 | 2021-05 |

The matching Azure SDK release tags resolve to immutable public commits recorded
in the manifest. This establishes source provenance, **not** that the approved
NuGet DLL bytes were built from those commits or that a transport emitted these
bytes. Query ordering/escaping is source-derived; ordinary HTTP headers and
authorization are deliberately absent. No credentials were generated or used.

The selected **source-only Sift fixture pin** is public commit
`5ee62ada4ac8901d5756c73c551dd42243a07c32`. This pins the request/profile
research, not an executed compatibility gate or a promise of runtime support.
Sift's `client.rs` and `atom.rs` establish its request construction and parser
defaults; upstream comments calling examples “captured” do not make our newly
constructed fixtures observed captures. Actual Sift interception remains
unverified until that client is executed under a separate approved harness.

Public source anchors (the manifest also records SHA-256 of the full files):

- [Current SDK paging and query construction](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/HttpRequestAndResponse.cs#L123-L180).
  The previous pinned version has byte-identical helper and client-method files.
- [Definition collection](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/ServiceBusAdministrationClient.cs#L1072-L1085)
  and [runtime collection](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/ServiceBusAdministrationClient.cs#L1338-L1350).
- Default API selections: [current](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/ServiceBusAdministrationClientOptions.cs#L22),
  [previous](https://github.com/Azure/azure-sdk-for-net/blob/f81988005453a91be7a974bba8c1b69012758e0f/sdk/servicebus/Azure.Messaging.ServiceBus/src/Administration/ServiceBusAdministrationClientOptions.cs#L22).
- [Sift paths, query encoding and paging](https://github.com/DeandreT/sift/blob/5ee62ada4ac8901d5756c73c551dd42243a07c32/crates/mgmt/src/client.rs),
  [missing-count defaults](https://github.com/DeandreT/sift/blob/5ee62ada4ac8901d5756c73c551dd42243a07c32/crates/mgmt/src/atom.rs#L94-L119),
  and [count namespace example](https://github.com/DeandreT/sift/blob/5ee62ada4ac8901d5756c73c551dd42243a07c32/crates/mgmt/src/atom.rs#L533-L539).

## Decision: do not infer caller intent

For **both** SDK versions, `GetQueuesAsync` and
`GetQueuesRuntimePropertiesAsync` call the same `GetEntitiesPageAsync` with the
same path. That helper always calls `GetEntityAsync(..., enrich: false)`.
Topics and subscriptions do the same. Thus even the complete URI cannot tell
definition collection calls from runtime collection calls. This is not fixed by
checking for `enrich=True`: that distinguishes singleton methods only.

Sift list/get methods omit `enrich` altogether and consume definitions and
runtime values together. Missing counts become zero in `runtime_info`,
`count_details`, and the topic parser. The .NET runtime parsers likewise leave
unseen numeric fields at defaults. A successfully parsed definition-only feed
therefore does not prove a truthful runtime response.

**Selected refusal rule:** until complete, authoritative runtime fields can be
materialized, refuse every ambiguous queue/topic/subscription collection,
including empty collections and later pages. Refuse Sift's combined singleton
requests and explicit singleton runtime requests too. Do not use user-agent,
authorization, timing, “empty namespace”, or client method names to distinguish
otherwise identical requests. Never replace unavailable data with zeros, omit
statistics and rely on client defaults, or return a partial successful page.
The manifest records a future HTTP 400 refusal, not existing server behavior.

Definition singleton requests with explicit `enrich=False` are distinguishable;
they are recorded as such, **not activated or certified here**. An adapter must
prove truthful metadata and authorization separately. Definition and combined
response grammars are test shapes, not permission to synthesize values.

## Closed XML shapes

`profiles/definition.rng` and `profiles/combined.rng` accept only the documented
queue/topic/subscription fields, in the selected order. Atom envelope names and
description names are namespace-qualified; prefix spelling is irrelevant.
`CountDetails` itself is in the connect namespace, but each count child is in
`http://schemas.microsoft.com/netservices/2011/06/servicebus`. It is **not** in
the connect namespace. This spelling comes from Sift's public queue-feed
example; SDK parsers' local-name tolerance is not a namespace specification.

The combined grammar requires every chosen runtime field. Current topic
fixtures include `SqlFilterCount` and `CorrelationFilterCount`, which 7.21.0
parses and 7.20.2 does not. Subscription runtime has no SDK `SizeInBytes` or
scheduled-count property; the combined shape includes those Sift fields
explicitly rather than claiming the previous/current SDK exposes them.

These deliberately narrow grammars are not Azure's full XSD. Unknown elements,
attributes, duplicate fields, misplaced children, foreign namespaces, invalid
numbers, negative counts and nil/structured scalar values refuse. Definition
shapes do not accept runtime fields. The paired collection fixtures demonstrate
why selecting either grammar solely from the ambiguous URI is unsound.

All write operations, rule/namespace operations, forwarding, partitioning,
express entities, authorization-rule edits, and fields outside these shapes are
outside this contract. Unknown query keys, duplicated singleton keys, unsupported
API versions, invalid paging and malformed XML refuse, without fallback or
mutation. `controls.json` specifies these checks before an adapter is written;
it is not an implemented HTTP parser. XML processors must disable DTD/entity
expansion and apply input/depth/count bounds before validation. Relax NG alone
does not enforce those resource or transport constraints.

## Local structural checks (no build or network)

Using existing `jq` and `xmllint`, from the repository root:

```sh
set -eu
jq -e '.schema == 1 and .evidence_kind == "source-derived-synthetic"' crates/conformance/atom-fixtures/manifest.json
jq -e '.schema == 1' crates/conformance/atom-fixtures/controls.json
for profile in definition combined; do
  for file in crates/conformance/atom-fixtures/responses/$profile-*.xml; do
    xmllint --nonet --noout --relaxng crates/conformance/atom-fixtures/profiles/$profile.rng "$file" || exit 1
  done
done
for file in crates/conformance/atom-fixtures/rejected/*.xml; do
  if xmllint --nonet --noout --relaxng crates/conformance/atom-fixtures/profiles/combined.rng "$file"; then
    exit 1
  fi
done
xmllint --nonet --noout --relaxng crates/conformance/atom-fixtures/profiles/refusal.rng crates/conformance/atom-fixtures/responses/refusal.xml
```

These are fixture-shape checks only. They do not test URI routing, instantiate a
pinned client, compare loaded DLL identities, call Sift, or prove mutation
absence. The [standalone .NET interception harness](dotnet/README.md) covers
client-side collection/singleton construction, nonzero synthetic parsing, and
HTTP refusal handling with an in-memory transport. It imports the existing
current/previous pin inputs and links the existing SDK custody recorder.
Its invocation records are separate from the source-derived fixture manifest.
`controls.json` also supplies future adapter vectors; no Cargo test is discovered
merely by adding this directory.

## Remaining acceptance boundaries

- Execute current/previous SDK request interception using the approved binaries,
  without live service provisioning. Retain those observations separately from
  these source-derived fixtures. Sift interception belongs to a separately scoped
  client gate; the approved source-only pin does not make its execution a #10
  acceptance condition.
- Integrate the standalone .NET harness with a qualified parent launcher if
  automatic workspace discovery is required. Unknown query, strict XML,
  resource-limit and no-mutation adapter controls remain separate; the
  client-side harness is not a server parser or an adapter test.
- Implement truthful catalog/runtime materialization and bounded strict parsing
  in a separately scoped adapter change. No listener, domain write, Topic CRUD,
  runtime-counter, or full administration support is established here.
