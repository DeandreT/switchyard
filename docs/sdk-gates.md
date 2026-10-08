# SDK Gates

The .NET TCP/WSS workflows are experimental, Memory-only data-plane gates.
Selectors are fixed: `declared-current` is ServiceBus 7.21.0 / Core 1.62.0;
`previous` is 7.20.2 / 1.60.0. Neither means latest. Exact ranges, locked graphs
and selected runtime DLL approvals are checked in under `crates/conformance/pins`.
The durable matrix remains [#101](https://github.com/DeandreT/switchyard/issues/101).
Ordinary workspace tests ignore four workflows and one restored-pin control.
Selecting one requires
Linux, .NET 10 and NuGet restore; missing prerequisites fail rather than skip.

The four exact batch checks accumulate arrival-order short batches: the
[SDK receive contract](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceiver.receivemessagesasync)
does not promise `maxMessages` results, even when available. Each check requests
only its remainder under one monotonic 10s budget and linked cancellation.
Empty returns pause up to 100ms; 100 total empty returns or budget exhaustion
diagnose a partial result, which still fails the unchanged exact assertions.
Over-return and late full completion fail; caller/unrelated cancellation
propagates. Nothing is filtered, reordered, reread or settled by the helper.
Original fetch tasks are directly awaited: cancellation cooperation is required,
not a finite-progress promise for uncooperative delegates. Pins, custody and
parent deadlines are unchanged.

```sh
cargo test --locked -p server --test sdk_child_custody -j2
cargo test --locked -p server --test sdk_pin_controls -j2 -- --include-ignored --nocapture
cargo test --locked -p server --test amqp_dotnet_current -j2 -- --ignored --nocapture
cargo test --locked -p server --test amqp_dotnet_websockets -j2 -- --ignored --nocapture
```

Standalone fake-receiver controls use .NET 10 without NuGet packages; they are
separate from ordinary Rust CI and do not certify SDK/broker behavior:

```sh
DOTNET_PROCESSOR_COUNT=2 dotnet run --project crates/conformance/batch-receive-controls/Switchyard.Conformance.BatchReceiveControls.csproj --configuration Release
```

## Custody Contract

```mermaid
flowchart LR
    Pins["Exact selector + locked graph + DLL approvals"] --> Build["Isolated restore + fresh project/bin/obj"]
    Build --> Assets["Resolved runtime assets + package/output hashes"]
    Assets --> Files["Pre-hashed absolute DLLs"]
    Files --> Start["Original child: nonce + loaded identities"]
    Start --> Work["Unchanged workflow + async disposal"]
    Work --> Complete["Post-disposal completion record"]
    Complete --> Reap["Group termination before original leader reap"]
    Reap --> Verify["Parent matches records + post-hashes"]
```

| Boundary | Limit or evidence |
| --- | --- |
| Restore + build/run | Combined 300s restore/build and separate 180s run deadlines; .NET processor count/MSBuild workers set to two |
| Output | Owned nonblocking stdout/stderr, each capped at 1 MiB; records at 16 KiB |
| Identity | Entry, ServiceBus and Core: FullName, informational version, actual Location and SHA256; nonce-bound start/completion must match launched files |
| Artifacts | 64 MiB/file ceiling; invocation-owned project/output, package, HTTP/scratch/plugin caches and CLI home; no global package fallback |
| Pins | Wrong selector/range/graph, ambiguous runtime assets, mixed output and poisoned cache/output bytes refuse before launch |
| Cleanup | Original child retained; non-reaping leader check, group kill before wait; failed wait retries never signal a possibly reused PID |
| Controls | Original-PID retry, exact leader reap and captured descendant pidfd exit; watchdog intervention fails, not certifies, cleanup |

Completion is emitted only after the original workflow's async disposals return.
Locked restore fixes the graph; byte custody covers the entry and selected
ServiceBus/Core DLLs, not every loaded dependency or independent PE metadata.
Controls require Linux pidfd support and prove descendant exit,
not descendant reap. Escaped groups and uninterruptible kernel waits are excluded;
synchronous cleanup is not a sandbox or universal finite-progress guarantee.

TCP retains its permissive test certificate callback; WSS retains child-scoped
`SSL_CERT_FILE` trust. Neither certifies production trust, administration or
whole-test listener/timer task-tree shutdown. See [compatibility](compatibility.md).
