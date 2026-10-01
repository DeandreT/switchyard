# Entity Incarnations

Every AMQP data and management endpoint is bound to the committed entity that
existed when it was admitted. Reusing a name does not give an old endpoint
authority to send, receive, browse, schedule, settle, renew, manage rules, or
release a session against its replacement. This is a local identity contract,
not a claim of Azure's immediate link-detach timing.

## Persistent Owners

Key tag `0x11` holds one incarnation record per primary queue, primary topic,
or subscription backing. A queue and a topic at the same primary path share
that key across kind changes. A subscription owns its own record independently
of its parent. A dead-letter queue uses its queue or subscription owner's
identity; it never allocates a separate one.

Records retain a nonzero `u64` generation, entity kind, and live/retired status.
Successful creation allocates the next checked generation in the same batch as
all topology records. Deletion marks the existing generation retired in its
atomic purge batch. Failed creation, deletion, or storage commit changes neither
topology nor identity. Configuration updates, equal patches, and pure reads do
not allocate a generation. Exhaustion refuses creation rather than wrapping.

An admission binding contains the namespace, exact canonical physical target,
owner, kind, and generation. A base target and its shadow share an owner but
are not interchangeable bindings. Namespace, literal parent/member spelling,
and case remain part of identity; structural address canonicalization is
unchanged. The generation is not an authorization secret or a SAS grant.

## Serialized Checks

Authorization still precedes topology access. Admission validates topology
and captures its identity in one clock-free broker-owner turn. A session
acceptance made after that turn is already guarded, so a replacement created
between admission and acceptance cannot acquire a hold on the old link's behalf.

Every subsequent action uses the captured binding. The owner checks the exact
logical target and current identity before consulting the host clock, and
executes the operation in that same serialized turn. Stale identities are
refused before clock regression, stamping, writes, or receiver notifications.
Native administration and trusted name-based operations intentionally operate
on the current entity named by their request.

The independently serialized `FencedCommand` envelope includes the binding and
ordinary command. The deterministic state machine checks it again before its
usual command-clock validation and single atomic batch. This preserves replay
checks without changing existing `Command` fields or `CommandKind` ordinals.
Rule operations guard the exact subscription even though the command names its
parent topic; pure rule enumeration is also guarded without a command stamp.

Live metadata with a missing, retired, invalid, or wrong-kind incarnation is
corruption, not permission to invent an identity. Admission, successful creation,
deletion, and guarded operations validate the records they require. A create
request for an occupied path retains its existing already-exists refusal and
cannot reset that identity. Ordinary trusted topology reads retain their
existing validation contract. No operation scans
historical incarnation tombstones or reconstructs generations from messages.

## Connection State

Managed delivery receipts and session references retain the same exact binding
as their data link. A fresh management endpoint cannot borrow a reference from
an older incarnation merely because its path, link name, session identifier,
or token otherwise matches. Such associated-link mismatches return lock-lost
status before owner work.

Management reply routes also retain their admission binding. A request cannot
use a response link admitted for another incarnation, even on the same
connection and at the same address. That transfer is refused before processing
or emitting a response; current matching pairs continue to work. CBS routes
are independent and unchanged. Existing channel-identity checks still prevent
an old route's cleanup from unregistering a newer channel at the same address.

Deletion wakes all registered waiting receivers. They discover the stale
identity on their next guarded operation even if recreation already committed.
Idle producers and links currently delivering a message are not promised an
immediate global detach. Already committed old-incarnation replies may drain;
they do not perform an operation against a replacement. Session cleanup also
keeps the original binding and cannot release a replacement's hold.

## Storage Boundary

Value format remains 10 because existing record shapes are unchanged. Durable
layout 13 requires the new identity contract and refuses older directories;
older builds likewise refuse the newer layout rather than ignore its fences.
There is no directory migration tooling or incarnation-tombstone garbage
collection yet. Retained owner records may accumulate as names are deleted.
Sequence and lock counters remain separate fences and stay lazy where allowed.
These guarantees require the retained records to remain intact; arbitrary manual
tombstone erasure or state replacement is not supported.
