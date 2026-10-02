# Native Rule Administration

The optional `--admin-listen` gRPC listener serves `RuleService` alongside
`EntityService`. It creates, gets, lists, and deletes subscription rules through
the existing broker owner. It does not provide Azure Atom/XML administration,
rule actions, updates, or upserts.

The separate trusted [SQL action command](sql-actions.md) can create
action-bearing definitions. Until this API can represent them, Get returns
Unimplemented for such a rule and List returns Unimplemented for a set containing
one; neither silently emits an action-free definition. Delete remains available.

## Requests and Authorization

Every request names the configured namespace and a subscription path such as
`events/subscriptions/audit`. The structural `subscriptions` separator is
canonicalized; parent, subscription, and rule names retain their exact spelling.
Primary entities and dead-letter paths are refused.
Binding uses native topology validation, not AMQP address parsing. Literal
parent segments such as `$Management` retain their spelling; this does not make
those paths available over AMQP.

When authentication is configured, requests require TLS and a SAS token in
`authorization` metadata with Manage permission on the canonical subscription
path. Namespace and parent grants inherit to that child. An exact child grant
does not authorize siblings, and differently cased user names do not alias.
Authorization precedes rule parsing, SQL compilation, and broker/store access.
Authenticated paths must also fit the existing SAS resource grammar. A leading
empty segment, as in `/events/subscriptions/audit`, is refused with
InvalidArgument even for a valid namespace grant. Such literal leading-slash
names can be administered only when authentication is not configured; they are
not aliases for paths without that slash.

Each request binds the current subscription incarnation once. Its subsequent
read or mutation is fenced by that binding, so deletion and recreation while
the request is in flight cannot redirect its work into the replacement. These
are stateless RPCs: a new request can bind a newly created subscription at the
same path. There is no cross-request lease, retry deduplication, or lost-response
recovery guarantee.

## Operations

`CreateRule` is create-only. Duplicate names return AlreadyExists.
`DeleteRule` removes exactly the named rule; missing names return NotFound.
Both return an empty `RuleMutationResponse` only after the expected mutation
outcome, without rereading after commit. No asynchronous operation is implied.

`GetRule` returns the exact named definition. `ListRules` returns the complete,
sorted rule set, including persisted `$Default` if it has not been deleted.
There is no pagination, cursor, or implicit fallback. Subscription creation
persists `$Default` as a true filter; removing every rule matches nothing.
Reads validate the complete rule set before returning a result. Corrupt stored
metadata or physical storage failures return static Internal errors without a
partial list, SQL source, or storage diagnostic.

A returned `Rule` includes namespace, canonical subscription path, exact name,
filter, and its committed creation timestamp in Unix milliseconds. Reading does
not consult or advance the applied clock or propose a command.

## Filter Values

`RuleFilter` is a required oneof: true, false, correlation, or SQL. Missing filter
constructors are not treated as true. SQL carries the original expression and an
optional semantic version. Omission selects version 1; responses include the
stored version. Other versions return Unimplemented before compilation. The
predicate language and evaluation policy remain those in [SQL Rules](sql-rules.md).

Correlation has eight optional system strings and repeated custom-property
entries. Absent system strings differ from present empty strings. Conditions
AND together; an empty correlation filter has no conditions. Duplicate exact
custom-property names are refused rather than overwritten.

`RuleScalarValue` preserves all 21 scalar constructors without numeric coercion:
null, Boolean, unsigned and signed integers at each AMQP width, float, double,
decimal32/64/128, character, timestamp, UUID, binary, string, and symbol. Floating
values use fixed-width IEEE-754 bit fields, preserving NaNs, infinities, and
negative zero. Decimal and UUID byte fields require exactly 4/8/16 and 16 bytes
respectively. Narrow integers must fit their declared width; characters must be
Unicode scalar values and symbols must be ASCII. Missing values differ from
explicit null. Compound values are not part of this contract.

## Bounds and Errors

The existing domain limits remain authoritative: 32 rules per subscription,
32 total conditions per correlation filter, 64 KiB per versioned stored rule,
and 256 KiB per complete subscription rule set. Existing SQL compilation and
evaluation budgets are unchanged. This additive API changes neither command
encoding nor stored-record versions.

Rule and entity services share listener admission and transport bounds:
128 concurrent requests across service clones, at most 32 HTTP/2 streams per
connection, a 30-second request deadline, 64 KiB decoded requests, and 1 MiB
encoded replies. A list is rejected rather than truncated if its encoded reply
exceeds the response bound. These are local policies, not Azure quotas.

Malformed rule inputs and SQL syntax return InvalidArgument; unsupported SQL or
semantic versions return Unimplemented; domain and compilation limits return
ResourceExhausted. Missing or stale targets return NotFound. A stopped owner or
unavailable applied clock returns Unavailable. No refusal is represented as a
successful rule mutation.

## Command Line

`switchyardctl` exposes the same four operations:

```text
rule create <topic> <subscription> <name> --filter-file <path>
rule get <topic> <subscription> <name>
rule list <topic> <subscription>
rule delete <topic> <subscription> <name>
```

Connection options, explicit CA verification, bounded token files, sensitive
authorization metadata, and timeouts are shared with entity commands. Insecure
HTTP remains opt-in, loopback-only, and cannot carry a token. Rule lists are
complete; there are no paging options. Create/delete emit
`{namespace, subscription_path, name, completed: true}` after the successful RPC,
not a fetched definition or an asynchronous operation. Get/list validate the
complete reply before writing JSON. Creation timestamps are decimal strings to
preserve the full unsigned 64-bit range.

Filter files are bounded regular files of at most 512 KiB. Symlinks to regular
files retain the existing file-helper behavior; FIFOs and other nonregular
opened files are refused. The complete encoded protobuf request is checked
against 64 KiB before connecting. Filter-file and conversion errors, and remote
RPC statuses, produce nonzero exits and static errors without echoing file paths,
filter contents, tokens, or remote diagnostic text. SQL is not compiled locally:
the server remains authoritative for syntax, semantic versions, and compilation
budgets.

Filters use an explicit `type` field. True and false are `{"type":"true"}` and
`{"type":"false"}`. SQL uses `expression` and optional `semantic_version`; the
checked-in [red filter](../examples/rules/red.json) is a complete example.
Correlation uses the eight optional system strings and repeated `properties`
entries with `name` and a typed `value`:

```json
{
  "type": "correlation",
  "subject": "",
  "properties": [
    {"name": "priority", "value": {"type": "uint", "value": 3}},
    {"name": "absent", "value": {"type": "null"}},
    {"name": "bits", "value": {"type": "float_bits", "value": "80000000"}}
  ]
}
```

Unknown and duplicate JSON fields, duplicate property names, missing
constructors/values, invalid widths, and more than 32 conditions are refused.
Null has no `value`; it is not a missing constructor. The `ulong`, `long`, and
`timestamp` constructors use canonical decimal strings, not JSON numbers.
`float_bits` and `double_bits` use exactly 8 and 16 lowercase hex digits.
`decimal32`, `decimal64`, `decimal128`, and `uuid` use exactly 8, 16, 32, and 32
lowercase hex digits; `binary` uses any even-length lowercase hex string,
including empty. Hex has no prefix. `char` is a numeric Unicode codepoint.
Other integer widths and Boolean values use JSON numbers and Boolean values;
`string` and ASCII `symbol` use strings. Output filters use the same typed shape,
preserving empty strings, explicit null, octets, and floating-point bit payloads.
