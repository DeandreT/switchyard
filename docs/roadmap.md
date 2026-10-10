# Completion Roadmap

Pre-alpha: only verified, merged `main` is complete.
[Issues](https://github.com/DeandreT/switchyard/issues) own scope;
[compatibility](compatibility.md) records guarantees.

## Main Progress

- [x] AMQP/SASL/CBS; settlement, DLQ, deferral, peek, sessions, scheduling, queue duplicates.
- [x] Actionless topics/rules, bounded topology and selected profile integrity.
- [x] Memory/Fjall format-2 and opt-in journal/indexed apply/local replay.
- [x] Local authority, JWT/SQL kernels and native/link/session/connection custody.
- [x] Source-bound builds and pinned Memory SDK TCP/WSS gates.
- [ ] Remaining semantics, administration and conserved physical-byte capacity.
- [ ] Full recovery, quorum, multi-tenant security and measured release.

## Next Main Increments

#222/#224/#225 verified; #134 custody audit closed; #7 open. #248 wire bounds Blocked/unassigned.
#135/#136/#65 own admission/deadlines/process; joins have no Close/latency guarantee.
Reference branches are not main evidence; #15 is entity retirement. #255's qualified history audit is closed.

```mermaid
flowchart TD
    Main["Main"] --> Lifecycle["#7 Shutdown -> #12 Authority -> #15 Entity retirement"] --> Semantics["#20/#21 Rules -> #26-#28 Capacity"]
    Main --> Domain["#14/#16/#23 Domain"] --> Semantics
    Main --> Recovery["#37 Snapshots -> #38 Quorum"] --> Runtime["#41 Runtime"]
    Lifecycle --> Security["#71 Grants -> #17 JWT; #42/#43/#49 Identity"] --> Runtime
    Atom["#10 Atom"] --> Gates["#53 Release"]
    Admin["#18 Coordinator"] --> Catalog["#243 Catalog"] --> Broker["#244 Broker"] --> Gates
    Allocation["#228 Queue ACKs"] --> Activation["#238 Activation"]
    Allocation --> Topic["#239 Topic"]; Allocation --> Tokens["#240 Tokens"]
    Runtime --> Gates; Semantics --> Gates
```

## Snapshot Recovery

#36 local replay is merged; #37 full recovery is not.

- [x] #203 Record format.
- [x] #204 Catalog validation (partial).
- [x] #206 Backend provenance.
- [ ] #205 State audit: verified partials #229/#230/#231; #232 pending.
- [ ] #207 Coherent capture.
- [ ] #208 Offline atomic install.
- [ ] #209 Applied-safe anchor.
- [ ] #210 Selection/compaction.

#250 API/#253 metadata handoffs are sealed.
#232 Blocked/unassigned: verify #239/#240; #227/#228/#238 and partials verified; #207-#210 blocked.

```mermaid
flowchart LR
    Format["#203 Format"] --> Capture["#207 Capture"]
    Catalog["#204 Catalog"] --> Rows["#229 Messages"] --> Sessions["#230 Sessions"]
    Rows --> Duplicates["#231 Duplicates"]
    Sessions --> State["#232 Allocation/graph"]; Duplicates --> State
    DLQ["#227 DLQ refusal"] --> State
    API["#250 API handoff"] --> Sessions; API --> Duplicates
    Handoff["#253 Metadata"]; Allocators["#228/#238/#239/#240"] --> State; Handoff --> State
    State --> Audit["#205 Audit"] --> Capture; Metadata["#206 Provenance"] --> Capture
    Capture --> Install["#208 Install"]
    Metadata --> Install
    Install --> Anchor["#209 Applied anchor"] --> Compact["#210 Selection/compaction"]
    Install --> Compact
```

Partial validation is not full-state health or install authority. Capture needs
the complete image; install needs all-writer exclusivity. Prune at applied A,
not committed C; counters are not byte quotas.

## Parallel Pickup

Assign before coding. Serialize shared files and two-job builds; merge focused
PRs in dependency order ([workflow](../CONTRIBUTING.md)). Ready is not evidence.

| Lane | Pickup / overlap |
| --- | --- |
| Snapshots | #230 `validate_session_rows`; #231 `validate_duplicate_rows` ([PR260](https://github.com/DeandreT/switchyard/pull/260)): verified partials. Serialize state.rs/mod.rs. |
| Domain | [#14](https://github.com/DeandreT/switchyard/issues/14)/[#16](https://github.com/DeandreT/switchyard/issues/16)/[#23](https://github.com/DeandreT/switchyard/issues/23) Ready/unassigned; serialize command/codec/keys. |
| Atom | [#10](https://github.com/DeandreT/switchyard/issues/10) Ready/unassigned. |
| Admin | #18 Blocked coordinator; [#243](https://github.com/DeandreT/switchyard/issues/243) Ready/unassigned -> #244 Blocked/unassigned. |
| Allocators | [#228](https://github.com/DeandreT/switchyard/issues/228) and #238 ([PR258](https://github.com/DeandreT/switchyard/pull/258)) verified; #239 Ready/unassigned; #240 assigned; controls unexecuted. |
| SDK/security | #71/#101 blocked on #7. |

Shared: #243 keys.rs (released by #229); machine.rs overlaps #240. #244 serializes broker
ownership. Allocator error/condition/indexed edits serialize; #239 also
coordinates #14/#23.

SDK evidence is experimental current/previous Memory, not latest/durable.
Production still refuses; quorum/recovery/runtime remain pending.
#234 and [#236](https://github.com/DeandreT/switchyard/issues/236) are closed/verified; PR246 split the lifecycle reference.

## Milestones

| Gate | Remaining work |
| --- | --- |
| [M1](https://github.com/DeandreT/switchyard/milestone/1) | Lifecycle, authority, profiles, entity retirement, typed content. |
| [M2](https://github.com/DeandreT/switchyard/milestone/2) | SDK/admin semantics, sustained Flow, capacity, same-group transactions. |
| [M3](https://github.com/DeandreT/switchyard/milestone/3) | Recovery/quorum, identity/fairness, encrypted audit/backup, measured release. |

## Completion Checks

- [ ] **Foundation:** live owner health, retained-authority fencing, bounded lifecycle
  and format refusal/reopen gates are merged; no stale path can mutate a new entity.
- [ ] **Compatibility:** declared current/previous SDK and administration clients
  exercise supported semantics, rules, sessions, TTL, modes and transactions;
  remaining deviations are explicit and unsupported requests refuse.
- [ ] **Capacity:** physical-byte conservation covers every queue/topic lifecycle
  and copy/error route; no public finite profile exists without its ledger.
- [ ] **Production:** all proposals/timers/consistent reads use real quorum routing,
  with no local fallback; quotas, identity, KMS encryption and committed audit pass.
- [ ] **Recovery:** full-state restart/install/compaction, partitions, key rotation,
  corrupt-backup refusal and empty-cluster restore pass under faults.
- [ ] **Release:** signed Linux x64/arm64 artifacts/SBOMs plus published three-replica,
  encrypted/audited 100k 1KiB messages/s and under-20ms p99 evidence; 5TiB soak
  includes compaction, failover, backup and restore on declared hardware.

[#53](https://github.com/DeandreT/switchyard/issues/53) reaches every lane. Executed
evidence, not code presence or skipped tests, closes gates; 1.0 stays reserved.
[Architecture](../ARCHITECTURE.md) retains non-goals: Premium, cross-group atomic
transactions, geo replication and regulatory certification.
