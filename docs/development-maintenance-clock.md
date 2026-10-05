# Development Maintenance Clock Assessment

An optional development query observes whether the existing command-stamping
rule accepted a clock/floor sample during one broker-owner turn. The result can
already be stale when received. It is not whole-node readiness, trustworthy wall
time, timer progress, storage health or permission to submit work. Later commands
independently validate and stamp as before.

## Broker Owner

`BrokerHandle::maintenance_clock_assessment().await` queues one read-only query
on the existing owner. `maintenance_clock_assessment_blocking()` uses the same
queue and waits synchronously. Both return `MaintenanceClockAssessment`:

| State | Meaning |
| --- | --- |
| Unknown | No observed assessment, including a default caller/protobuf value. |
| Ready | The unchanged stamp rule returned success in that owner turn. |
| Unsafe | Clock regression exceeded the existing configured allowance. |
| Unavailable | The floor read or another non-clock probe step returned an error. |
| Stopped | Request admission or reply disconnected, including owner unwind. |

The result contains no sampled timestamp, applied floor, threshold, backend
error, receipt or writer. Debug and Display use fixed names. Ready is publicly
constructible descriptive data, not an admission capability. Stopped is not an
orderly-shutdown or actual-join receipt.

The query calls the unchanged local stamp rule once: Clock::now is read before
the existing typed applied floor. A nonregressed reading is accepted; a backward
reading within the configured tolerance, default 500 ms, is also accepted.
Ordinary commands clamp a small regression to the existing floor. A larger
regression is refused, and later clock catch-up permits subsequent commands.
The query discards the successful Timestamp and writes nothing. It adds no
latch, reset, background sample, health cache or timer hook.

An empty floor is zero. SystemClock's existing pre-epoch zero and overflow
saturation policies are unchanged. Ready does not detect forward jumps. Owner
ordering does not create one physical atomic clock/store sample or exclude an
external privileged store clone.

An unpolled async query is inert. Cancellation while the queue is full discards
an unadmitted request without reading the clock. After admission, observer loss
may leave the read-only query in its accepted FIFO position; later requests
still drain. The assessment creates no task or native worker. Custom Clock/store
panic follows the existing owner behavior, not a new recovery protocol.

## Native Administration

The additive `switchyard.admin.v1.MaintenanceService/GetClockReadiness` RPC uses
the existing administration endpoint. The request carries namespace tag 1; the
response carries only state tag 1, with Unknown=0, Ready=1, Unsafe=2,
Unavailable=3 and Stopped=4. Existing services, methods and field numbers are
unchanged. The route is absent unless explicitly enabled.

The binary requires both `--development-maintenance-readiness` and
`--admin-listen`. Production refuses the new flag before credential, storage or
listener I/O. When both development-only options are requested, the existing
experimental atomic-listener refusal retains precedence. Disabled maintenance
readiness does not change existing startup paths.

Trusted embedders can call
`NativeAdminService::with_development_maintenance_readiness()`. They remain
responsible for deployment policy; the builder name is not a compiler proof of
development mode. Direct trait calls are a trusted embedding boundary, not proof
that the network route was enabled.

The RPC performs existing admission, namespace and authorization checks before
queuing the owner probe. With shared-access policy configured, namespace Manage
and TLS are required. Missing, invalid or expired tokens, Send/Listen-only grants,
entity-only Manage and foreign namespaces cannot reach the clock query.
Policy-free development administration remains unauthenticated as before.
Authentication's own time sampling and token safety are outside this assessment.
No public health socket or weakened authentication path is added.

## CLI

`switchyardctl maintenance-clock` reuses the existing endpoint, namespace, CA,
TLS-name and token-file settings. HTTPS retains certificate verification. HTTP
requires explicit `--allow-insecure` and loopback, and forbids token/TLS options.
There is no inline token argument. Connect/request limits remain 5 s/10 s.

For an explicitly enabled, policy-free development listener:

```sh
cargo run -p switchyardctl -- \
  --endpoint http://127.0.0.1:9080 --allow-insecure \
  maintenance-clock
```

Known states print JSON containing only the fixed scope
`development_maintenance_clock` and a fixed lowercase state string. Only Ready
exits zero. Other known states print JSON followed by a fixed local nonready
error and exit one. Invalid numeric states and transport/auth/deadline errors
produce static or code-only errors, not remote details or fabricated assessments.
JSON is below 128 UTF-8 bytes and contains no namespace, hostname, token,
timestamp or threshold.

## Boundaries

Queue depth 1,024, reply capacity one, native admission 128 and existing listener
limits are unchanged. Known state payloads use at most two protobuf bytes.
These are logical/count limits, not framing, aggregate-memory or allocation
guarantees. The existing floor get may allocate the complete stored value before
typed decoding; backend/auth/protobuf allocation, RSS and OOM are excluded.

Test-owned broker/listener/child cleanup is separate from hidden tonic
descendants, whole-server custody or universal OS retirement. This API does not
establish native/database safety, physical completeness, safe reopen, production
or quorum readiness, SDK behavior or any historical failure cause. Sticky
unsafe-clock timer pause and operator recovery remain a separate policy decision.

## Verification

The 21-source import was exact. Formatting affected eleven new files only; the
eight existing source files retained their old bytes after removing the approved
additions. Independent review confirmed that the 138 protected old check bodies
and 24 protected whole files were unchanged. No source correction or Rust gate
failure was needed for this increment.

The first maintenance-filter run passed 21 new unit cases; eight new integration
and descriptor cases were filtered out. Separate targeted runs passed those
eight cases, and the three new documentation checks passed: one compilation-only
example and two compile-fail examples. Ten subsequent paired focus runs each
passed all 29 new regular cases, giving 290 executions with unchanged sources.

Both default and all-feature workspace runs passed 5,003 checks in 138 physical
groups, retaining ten existing ignored tests. Target-aware comparison against
the preceding 4,971-check increment found exactly 29 regular and three
documentation additions, with no old identities or statuses removed or changed.
It preserved all 131 old logical harnesses while allowing the three new
integration targets and the additional server documentation group.

The all-feature engine suite passed 840 checks, protocol 514, storage 239,
domain 1,371 and cluster 797. Their complete registries and statuses were
unchanged from the preceding increment. Formatting passed. Strict workspace
Clippy and workspace builds passed in both feature configurations. Protobuf
descriptor generation and whitespace checks passed.

Verification ran serially on two low-priority cores and reused one target
directory. Final resource checks left about 150 GiB free at home and 893 GiB
on the build volume; the target directory occupied about 112 GiB. No SDK gate
ran for this increment. These checks do not make Ready write authority,
establish whole-node readiness or expand the native/task custody boundaries above.
