# Local Transaction Registry

`protocol-amqp::AtomicTransactionRegistry` supplies a bounded, serialized local
lifecycle for future connection transactions. It is a trusted Rust API, not an
enabled AMQP coordinator: Service Bus listeners and default connection drivers
still [refuse transaction traffic](amqp-transaction-types.md). The separate
opt-in [native posting lifecycle](native-transactional-ingress.md) is not yet
bound to this registry or the broker owner.

## Ownership And Identity

One non-clonable registry owns staged commands. Its mutations require exclusive
access. A clonable registry handle observes connection activity and shared work
usage without retaining commands or gaining admission authority. Dropping an
observer or controller clone is inert; closing or dropping the owning registry
revokes pending work and destroys locally staged payloads.

Controllers have opaque connection provenance, a checked generation, and an
explicit active lifetime. A matching numeric channel, link handle, or name is
not a controller identity. Retiring a controller invalidates all its clones;
controllers from another registry cannot control its groups. These runtime
identities are not native delivery receipts, credentials, or authorization.
An own closed controller can still query retained state diagnostically, but
cannot declare, stage, or discharge more work.

Generated transaction IDs are eight-byte binary values. A checked, shared
process-local counter never wraps or reuses an issued value in that process.
This is a local policy, not a durable or cross-restart uniqueness guarantee.
Only generated IDs belonging to the exact controlling registry and controller
are usable. Arbitrary legal AMQP binary IDs are still representable by the
native codec, but are not issued by this registry.

## Admission And Bounds

Declaration immediately reserves one of the shared budget's 32 group slots,
including for a group that never stages an action. The connection budget also
retains the [work reservation limits](atomic-work-reservations.md): 8 MiB of
command-content tally and 131,072 message value items. The existing
[per-group input limits](atomic-queue-operations.md#local-resource-bounds) apply
to every append. Live registry entries are independently capped at 32, even if
custom trusted owner code prematurely drops its work reservation while retaining
an undecided ticket. These are accounting limits, not process-RSS or serialized-size
guarantees; caller-owned observers, controllers, and rejected candidates are not
bounded by them.

The first successfully admitted action binds a group to one exact primary queue
binding, including its namespace, owner, target, kind, and generation. Later
actions must match it. A refused first action leaves the group unbound and its
content accounting unchanged. A successful empty send batch still counts as an
action and binds the group; zero messages or bytes does not make it an empty
transaction.

Pure input admission does not prove message shape, live queue configuration,
non-session configuration, held-lock validity, or authorization. The ordinary
owner validation still applies to bound work. Data-link grants and exact native
receipt authority must be supplied by a future wire adapter.

## Discharge And Decisions

A commit discharge moves the unique ticket, payloads, and lease into an owned
submission once. Repeating the same discharge flag reports the current permit
state without creating another submission. The opposite flag is a conflict and
cannot reverse a prior decision. An abort discharge destroys locally staged
work without submitting owner work. Submitted work retains its reservation until
the owner destroys it, even if the registry observes that it has been aborted.
The exact flag-conflict behavior is a local foundation policy, not a claim that
wire rollback Discharge behavior is implemented.

A group with no staged actions has no queue binding. Its distinct empty owned
submission uses the broker owner to claim and publish a commit decision without
entity validation, clock sampling, storage access, or receiver wakeups. It still
holds its slot through reply publication, including a refused claim. No fake
queue or controller address is used as a binding.

Each declaration has an immutable two-minute monotonic deadline starting at
declaration. This is an explicit local policy, not Azure's documented
[first-operation timeout](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-transactions).
Declare, stage, discharge, state, and explicit expiry processing retire expired
pending work;
there is no registry-owned background timer. The owner also checks the deadline
when claiming a submission. Expiry, controller retirement, and connection close
can abort only `Pending` authority, never undo `Started` work or promise rollback.

Up to 32 terminal records are retained in FIFO order. Records contain only
compact identity, state, discharge flag, and static abort information, not
commands, bindings, resource leases, application results, or error strings.
Evicted IDs become unknown. This history is neither durable deduplication nor a
retry-safe transaction log. Physical storage uncertainty remains
[indeterminate](atomic-commit-permits.md#result-boundaries).

Serialized registry mutation alone does not order transfers across native
sessions or establish a provisional acknowledgment barrier. No successful wire
Declare/Discharge, transactional settlement, SDK transaction scope, replication,
or persistent transaction format is added by this local API.
