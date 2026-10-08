# SDK Gates

The .NET TCP/WSS workflows are experimental, Memory-only data-plane gates.
Their projects declare 7.20.2; exact package pins and the durable matrix remain
[#100](https://github.com/DeandreT/switchyard/issues/100)/[#101](https://github.com/DeandreT/switchyard/issues/101).
Ordinary workspace tests leave both selectors ignored. Selecting one requires
Linux, .NET 10 and NuGet restore; missing prerequisites fail rather than skip.

```sh
cargo test --locked -p server --test sdk_child_custody -j2
cargo test --locked -p server --test amqp_dotnet_current -j2 -- --ignored --nocapture
cargo test --locked -p server --test amqp_dotnet_websockets -j2 -- --ignored --nocapture
```

## Custody Contract

```mermaid
flowchart LR
    Build["Fresh project/bin/obj"] --> Files["Pre-hashed absolute DLLs"]
    Files --> Start["Original child: nonce + loaded identities"]
    Start --> Work["Unchanged workflow + async disposal"]
    Work --> Complete["Post-disposal completion record"]
    Complete --> Reap["Group termination before original leader reap"]
    Reap --> Verify["Parent matches records + post-hashes"]
```

| Boundary | Limit or evidence |
| --- | --- |
| Build/run | Separate 300s/180s deadlines; .NET processor count/MSBuild workers set to two |
| Output | Owned nonblocking stdout/stderr, each capped at 1 MiB; records at 16 KiB |
| Identity | Entry, ServiceBus and Core: FullName, informational version, actual Location and SHA256; nonce-bound start/completion must match launched files |
| Artifacts | 64 MiB/file ceiling; fresh generated directories; existing global NuGet package cache reused |
| Cleanup | Original child retained; non-reaping leader check, group kill before wait; failed wait retries never signal a possibly reused PID |
| Controls | Original-PID retry, exact leader reap and captured descendant pidfd exit; watchdog intervention fails, not certifies, cleanup |

Completion is emitted only after the original workflow's async disposals return.
File identity is not exact NuGet graph certification or independent PE metadata
verification. Controls require Linux pidfd support and prove descendant exit,
not descendant reap. Escaped groups and uninterruptible kernel waits are excluded;
synchronous cleanup is not a sandbox or universal finite-progress guarantee.

TCP retains its permissive test certificate callback; WSS retains child-scoped
`SSL_CERT_FILE` trust. Neither certifies production trust, administration or
whole-test listener/timer task-tree shutdown. See [compatibility](compatibility.md).
