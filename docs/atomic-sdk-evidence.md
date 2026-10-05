# Atomic SDK Diagnostic Evidence

The opt-in [.NET transaction-scope gates](dotnet-transaction-scopes.md) preserve
bounded operation and AMQP lifecycle context without changing their transaction
workflows, success markers, zero-retry policy, or deadlines. This is diagnostic
coverage, not additional transaction compatibility or a timeout fix.

The separate [typed server recorder foundation](server-diagnostics.md) is not
attached to these fixtures, connections, or collectors. Its manual typed rows
cannot supply missing server evidence, whole-task coverage, or stronger cleanup.

## Bounded Context

Each atomic client run emits at most 128 operation rows and one truncation
marker. Rows identify a fixed test stage, UTC time, elapsed milliseconds,
cancellation state, exception type, and Service Bus failure reason. Stage and
type names are limited to 160 and 64 characters respectively. A saturating
reservation counter cannot wrap and resume publication, including under
concurrent calls. Endpoint-disposal failures get their own context without
replacing the first workflow failure.

A temporary lifecycle observer emits at most 64 rows and one truncation marker
for remote Detach, End, Close, or terminal exceptions. It records UTC time,
bounded object and exception type names, an ephemeral object identifier, and an
80-character sanitized AMQP condition. The added rows do not render message
bodies, addresses, tokens, credentials, raw frames, error descriptions, or
exception messages. The existing final primary-exception rendering is unchanged;
these bounds are not a claim that all SDK or externally configured logging is
content-private.

The observer forwards all 27 public callbacks on the pinned AMQP trace surface,
with their original arguments, exactly once to the previous provider. Failures
while publishing its own diagnostics are contained; failures from the previous
provider still propagate. Disposal is idempotent and restores the previous
provider only while the observer still owns the global slot. This does not
repair arbitrary out-of-order observer stacks or guarantee safe concurrent
replacement of the global provider.

The pre-existing refusal observer is unchanged. Its nested state callback still
reaches the lifecycle observer, but its other callbacks do not forward through
that observer. Synthetic forwarding tests are not proof of a decoded wire
refusal; the real socket gate still requires the exact disabled-ingress error.

## Failure Preservation

Operation metadata annotation and lookup tolerate throwing or read-only
exception dictionaries. Only the expected string, integer, and Boolean metadata
types reach final formatting; arbitrary dictionary values are never rendered.
The original operation exception is rethrown, not replaced by diagnostic work.
The primary exception's own rendering is outside this containment contract.

The Rust harness labels each SDK and backend run and reports client and cleanup
elapsed times. It awaits fixture cleanup after client failure and preserves both
errors when both fail; the client remains the primary error source. Existing
state and reopen checks run only after successful client and cleanup results.
There is no new failure-path state audit, joined connection collection, stronger
broker shutdown result, or proof of native-owner health.

The existing child runner still bounds each captured stream to 4 MiB, with a
bounded diagnostic tail on overflow, 180-second build and run deadlines, and a
five-second cleanup bound. Process-group cleanup, direct-child reaping, and
their existing limitations are unchanged.

## Self-Tests And Limits

Each built SDK pin first runs `atomic-evidence-selftest`, without opening a
network connection or accepting endpoint, trust, or credential arguments. A
successful exit and an exact completed marker are required before either
backend starts. The self-tests check every callback and original argument,
provider restoration and replacement ownership, nested state forwarding,
event and field bounds, concurrent and integer-boundary counter saturation,
throwing output sinks, exception-dictionary failures, typed metadata filtering,
and unchanged successful results and original exceptions.

Both pinned clients then run the existing actual TLS transaction workflows on
Memory and Fjall. These checks do not prove why a previous 7.20.2 gate failed at
`committed same-original Complete`: that label denotes provisional Complete
before scope completion. The captured SDK result was an aborted-receiver
`ServiceTimeout`, not an outer child deadline. Unchanged reruns passed, but no
server-close evidence or established root cause was captured. Added diagnostics
do not turn that failure into a confirmed fix or justify retries.

The diagnostic checkpoint passed all 16 nonignored Rust harness checks, the two
focused atomic SDK gates, and then all ten opt-in SDK gates. Both pins passed
their no-network self-tests and real Memory/Fjall transaction workflows. Both
workspace configurations passed 3,968 tests with the ten opt-in gates ignored
there. Both strict workspace lint configurations and builds, formatting,
protocol descriptor validation, and whitespace checks passed. These results
cover the unchanged timeout policy, not a reproduced or explained prior timeout.
