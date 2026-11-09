# Consensus Peer Transport

`cluster::transport` is an opt-in bounded TCP carrier for the pinned raft-rs
0.7 `eraftpb::Message` codec. It runs no election, consensus driver, primary
routing or domain projection. Ordinary server startup does not activate it and
the production refusal remains closed. See the separate
[persistence contract](consensus-persistence.md) for storage profile 4/F2.

## Wire Profile

Each connection carries exactly one one-way frame. There is no transport RPC,
generated Raft response, automatic retry, durable receipt or connection cache.
An actual Raft response is another ordinary core message on another connection;
the future driver owns term/correlation/quorum processing. Repeated or delayed
valid responses are delivered as messages, not counted as durable voters by this
layer. The receiver dispatches one frame and closes the whole socket; further
connection bytes are not additional messages.

The fixed 60-byte header is big-endian:

- `SWCP` magic (4), frame version 1 (2), reserved zero (2);
- the exact fixed-three configuration/namespace fingerprint (32);
- configured source voter ID (8), destination voter ID (8);
- protobuf body length (4).

Both IDs must be configured, distinct, and match the protobuf `from`/`to`; the
destination must be this receiver. Outbound `from` must be this sender. The
fingerprint includes the unchanged cluster/group/namespace/voter/endpoints
identity. Check the header before allocating a body and check message identity
before dispatch. Unsupported versions/reserved fields, foreign identities,
unknown protobuf fields, duplicate singular fields, noncanonical encodings,
local-only core messages, snapshots and configuration entries refuse. Snapshot
requests and all configuration log entries remain outside the fixed bootstrap
profile. Transport does not reinterpret entry data as commands or fabricate
domain no-ops; exact proposal bytes/time/authority remain the persistence layer's
responsibility. Read-index entry/context data is still opaque core data.

These are **untrusted protocol identity checks, not peer authentication**.
Plain TCP, socket addresses, fingerprints and loopback tests do not establish
production peer trust. Production mutual authentication, secure activation and
release/hardware gates remain separate. There is no mixed-version or automatic
capability negotiation; callers configure compatible explicit bounds.

## Bounds And Backpressure

The default full-frame limit is 4 MiB, explicitly configurable from
`MIN_FRAME_BYTES` through 16 MiB. The minimum derives from the existing 1 MiB
proposal-data ceiling plus 1 MiB entry context, 1 MiB message context, header and
pinned protobuf overhead. A complete legal maximum-size proposal is not silently
reduced to a smaller network payload. At most 64 entries are accepted per frame;
smaller configured entry counts remain explicit. A future driver must batch or
chunk within these limits before submission; transport never splits or truncates
a message behind its back.

An allocation-free structural pass uses a checked borrowed-slice cursor before
the maintained generated protobuf decoder. It caps repeated-entry growth, nested
entry lengths, data/context lengths, field count/work and nesting. Unknown
fields/groups and duplicate singular fields cannot trigger repeated allocation. Every length must
fit both its profile cap and the actual enclosing slice before a sub-slice is
borrowed; no declaration-sized buffer or generic protobuf skip/read helper is
used in preflight. Fixed-width/group/unknown wire paths refuse without skipping,
and scalar/tag/length varints have bounded, overflow-checked canonical reads.
Generated decoding and canonical serialization then operate only on this bounded
shape. There is no custom Raft algorithm or replacement protobuf semantic codec.

Private operation-local probes check actual scalar reads, rejected declarations
and sub-slice aliasing without unsafe code or a global allocator hook. These are
read-path observations, not a process-wide allocation counter; the allocation-free
preflight argument also depends on its slice/scalar-only implementation. Generated
decoding intentionally allocates only after the enclosing-byte checks.

Every accepted operation reserves global connection/operation credit. Once a
header identifies a peer, it must also acquire that peer's credits and its full
byte charge before any body allocation. Unknown/partial headers use the global
admission cap and absolute IO deadline. Outbound admission reserves all credits
before encoding/queueing and uses nonwaiting `try_send`: exhausted quotas return
`Busy`, not an unbounded waiter or retry queue.

Defaults are 16 global and 4 per-peer connections/operations, 128 MiB global and
32 MiB per-peer logical allocation credits. Queued outbound work and completed
but not-yet-joined tasks still count. Byte charges reserve input/encoding,
decoded variable data, canonical serialization and bounded repeated-container
capacity, with checked arithmetic. Global byte capacity must leave room for one
maximum-size frame beyond a fully occupied peer quota; connection/operation caps
also leave another admission slot. A held known peer cannot consume every slot
reserved for independent peer progress. Anonymous header flooding is not peer
authentication or a blanket denial-of-service availability guarantee.

Connection/task overhead is separately capped by concurrency. These are logical
transport allocation bounds, not exact RSS/allocator limits or a bound on a
trusted handler's own storage/core allocations. The handler must not retain
unaccounted clones after returning. Public status is a bounded latest-value watch
and fixed counters for the three configured peers, never a per-message history.

## Deadlines And Original Custody

One absolute IO deadline starts at accept or outbound admission. It spans queued
time, connect, partial header/body and backpressured writes without resetting on
retries, progress, canceled callers or resumed serving. Retirement interrupts
read-only IO and closes the **whole** `TcpStream`. No reader/writer halves or
detached readers survive a terminal path. Local connect/write errors do not
retire independent peers.

`PeerTransport` owns the listener, bounded outbound receiver, original tasks,
completion senders and a task-ID registry of allocation/operation leases. The
registry retains leases until actual original join, including panic/unexpected
cancellation; task completion alone does not return credits. Fair selection
reconsiders reaping, outbound work, accept and retirement after each event. There
are no peer-created detached workers or cleanup streams.

The trusted `PeerHandler` admission gate is atomic with retirement. Once admitted,
its original future is awaited without applying the IO deadline or aborting it
because the remote disconnected, a receipt was dropped or retirement began.
The handler must await its original persistence/work rather than spawn detached
effects. A slow begun handler can therefore delay drain indefinitely; a deadline
or process kill is not successful drain or permission to reopen its store.

`serve(&mut self)` and `finish(&mut self)` are borrowed, cancellation-safe owner
operations. Canceling either leaves the same listener/tasks/registry and fixed
deadlines in the surviving owner. `retire()` closes all new admission gates;
`finish()` requests retirement, closes the listener/queue, refuses unstarted
queued work, then joins every begun original. Repeating a canceled finish does
not restart budgets. A caller must retain the owner through finish. Dropping or
aborting the owner is explicitly **not** supported graceful drain; Drop requests
retirement but does not claim to join asynchronous work or shield an aborted
ancestor. There is no detached Drop-time supervisor.

`SendReceipt::wait(&mut self)` reports only local write completion **after join**,
not remote dispatch/persistence. Canceling the waiter or losing the connection
does not retry or replace an accepted original. Even successful local writing
does not prove delivery; an error can have an unknown remote outcome. Only the
future consensus/application owner can decide durable acknowledgements and retry
policy. Inbound failures and handler/panic/cancellation outcomes remain in the
bounded report; they never fabricate a successful peer response.

## Local Verification Fixture

The separate-process fixture starts three original copies of its compiled test
executable, each with a distinct real `ConsensusFjallStore`. Public configuration,
exact PIDs, nonce, endpoints and store paths identify ownership. A bounded newline
JSON stdin/stdout protocol carries named started/release/persisted/drained
barriers and fixed status snapshots, not application payloads or transport acks.
Callbacks explicitly persist supplied bounded fixture updates through the landed
owner; they implement no election, fabricated quorum or domain projection.

Original handler joins precede process exit; original PID waits precede store
reopen. Failure cleanup targets captured children only, records forced cleanup
as failure, and never counts termination as drain. Tests retain bounded evidence
while removing disposable stores after successful joined completion. Published
loopback results establish neither production peer trust nor hardware fsync,
power-cut, live-quorum or performance certification.
