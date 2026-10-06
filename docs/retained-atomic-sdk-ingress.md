# Retained Atomic SDK Ingress

This opt-in library contract retains the original tasks for one already accepted
raw TCP socket. A separate ignored server fixture targets a current .NET client
and direct Memory storage. Ordinary listeners, server startup and the existing
two-pin transaction-scope gates keep their previous policies.

## Root Before Start

The caller creates `RetainedAtomicMessagingOwner<A, B>` and its unique
`RetainedAtomicMessagingStarter<B>` before launching the accepted socket.
`AmqpListener::start_retained_collected_atomic_messaging` consumes the starter
and reuses the existing retained Wrapper and native acceptor. It does not create
a second socket actor, acceptance supervisor or aggregate-owning task.

The owner keeps the external anchor, original socket owner, session admission
histories, original Session and routing-worker tokens, restoring packets and raw
outcomes. The anchor need not be Send or 'static and never enters a spawned task.
The caller must retain and actually drive the captured Runtime A, including I/O
and timers. A runtime Handle alone does not keep a Runtime alive.

`RetainedAtomicMessagingLimits::new` checks lifetime session attempts in 1..=32
and global worker history in 1..=128. Construction reserves history/result/restore
capacity before publishing the starter; a build error retains the original
anchor and reserve cause without rendering their payloads. These are logical
identity/history limits, not complete allocation, byte, RSS or OOM guarantees.

Every native session acceptance attempt consumes one historical slot, including
an error or refusal. Ending or joining a session does not reuse it. Normally
returned worker launches share one connection-wide history across Controller,
CbsRequests, CbsReplies, Consumer and Producer. Joined workers do not refund or
evict committed history. Uncommitted reservations can refund their claims
without erasing the corresponding admission attempt.

Session exhaustion retains one actual overflow IncomingSession receipt, stops
discovery, closes logical authority and requests the existing resource-limit
native Close without creating another acceptance future or Session task. Worker
history reservation precedes native link acceptance. This does not certify
disposal of unaccepted native endpoint or frame buffers.

## Authority And Progress

The existing Wrapper captures the one initial authorization deadline immediately
after Open, before publishing its complete bind context. External binding delay,
repeated drive, cancellation and later sessions cannot reset that deadline.
The root binds one logical atomic Owner and event receiver. The existing initial
control grace permits bounded declaration/rollback metadata only; it grants no
Send, queue commit, Manage or ordinary management authority.

`drive_step` makes one borrowed progress step. The root pumps the same Owner's
events, operations, completions and Skip tick alongside original admissions and
Session/worker joins. WorkerStopped acknowledgments continue through that Owner;
there is no separate event-pump task or reconstructed owner.

Discovery borrows the live connection in the same original Wrapper frame.
Incoming Ready is rooted before the completed discovery future or its captures
can drop. The original cold acceptance future and its result remain in restoring
packets; no lifetime is widened into a fabricated 'static future. Original task
IDs and connection ordinals identify retained launches, not globally permanent
Tokio identities.

`control` returns cloneable data-only request/progress cells. They carry no
runtime, anchor, broker/store authority, transport, task token or hard-abort
capability. A stop request takes effect only when the external root is driven.
A request flag, closed bit, actor-retirement notification or task count is not
authority closure, an actual join or native-resource completion.

`stop` seals admission, closes the logical Owner, publishes authority closure,
then requests the current descendant cancellation. Natural expiry, exhaustion
and peer termination use the same Wrapper's native Close boundary after logical
closure. Explicit stop or finish can interrupt that close; a stop observation
does not establish that the requested Close reached the peer.

## Borrowed Finish And Abandonment

`finish` is borrowed and one-shot. It pumps original admissions, Sessions, workers
and pending classifications beside the original socket finish future. The socket
barrier preserves its internal Wrapper -> Actor -> Reader order; no sequential
cross-role join order is promised. Every created covered role, creator obligation
and pending classification must complete before Report publication.
Ready results enter retained storage before callbacks, awaits or completed-future
disposal. A completed Session result retains its pending classification index
until classification returns.

Cancelling, timing out or unwinding a borrowed drive/finish waiter restores the
same unfinished loans and retains already observed results in the live root.
Resume that owner on captured live Runtime A. No replacement work is submitted,
no completed original join is repolled, and cancellation is not completion.

`RetainedAtomicMessagingReport<A>` is available only after every CREATED covered
role actually joins. It retains the original anchor, admission outcomes,
Session/worker results and original socket report. Fields and constructors are
private; borrowed accessors expose original errors rather than copied diagnostic
strings or invented successes. A subsequent completed finish returns None and
performs no new health assessment.

Dropping an unreported owner is different from losing a borrowed waiter. Drop
seals launch, closes bound logical authority, requests cancellation and
permanently retains one preallocated custody holder with the original anchor,
tokens, restore cells and outcomes. Even a cold abandoned owner retains its
anchor. It does not release a reusable admission permit while descendants may
remain, spawn a rescuer or promise future progress. The library places no global
count bound on caller-created abandoned roots. Explicit finish is required to
return an anchor normally, even when no role was created.

## Raw Failure Boundary

Admission, routing, worker, join, panic and native Close outcomes remain separate.
A returned driver Ok does not erase the optional original native Close result.
None means no completed result, not an invented success.

The native report additionally retains the original decoded peer Close, actual
channel and payload, native connection identity and original reply-write Result.
Received-only, Pending, AbandonedBeforeReady and NotRequired are distinct from an
installed Ready Result. Ready is stored in the original write future's Ready
poll, before that completed future can drop. Local Close acknowledgment does not
invent a second successful reply write. Ordinary native close errors are never
changed to Ok by this observation surface.

Per-role observations retain the original task ID and actual abort-request
sources. ActorReaderShutdown and OwnerFinish describe requests on that same
original token; they do not establish cancellation cause. Cached joins never
acquire retroactive request facts. Native and retained socket reports move the
whole observation carrier through their existing into_parts tuple shapes beside
all original results and the anchor. The new public parts fields are a source
break for external exhaustive struct literals or patterns; these manipulable
parts are data, not report authority. Opaque Debug does not render raw errors,
panic payloads or anchors.

The SDK fixture requires Wrapper and Actor Ok and an actual completed,
error-free peer-initiated Close on channel zero with an empty original payload
for every connection report. Its actual reply-write Result must be Ready Ok.
EOF, missing/local/pending/abandoned receipts, malformed framing, peer errors and
reply-write errors cannot supply that condition. Reader Ok remains ordinarily
acceptable. A raw Reader cancellation is expected only when its original task
ID matches the Reader observation and ActorReaderShutdown was actually requested
on that token. OwnerFinish alone, unknown cancellation and panic still fail.

Only under that completed peer-Close condition and same Reader shutdown-request
fact may the fixture accept a separately retained raw native Close Stopped.
Reader Ok can accompany that observation when an abort races completed work.
RemoteClosed, I/O, timeout and every other native error remain fixture failures.
This conjunction is specific to SDK disposal; final report fields alone do not
prove a general call-start ordering, cancellation cause or whole-fixture health.
Accepted observations do not erase raw errors or make has_failures return false.
That convenience summary does not currently inspect the new peer-reply write
cell; the SDK oracle checks the original write Result separately.

Only the fixture's narrowly observed Session/worker cancellation may be accepted
when the SAME token has an actual abort-request fact and its drain observation
is External. Those observations do not prove cancellation cause. Original
routing/worker errors and panic payloads still fail; natural or unknown
cancellation is not silently excused. Client, setup, primary, WebSocket and
history checks remain independent of every cancellation observation.

## Recorder Tests And Memory SDK Fixture

Protocol-level controlled cases use recorder-backed owner observations to check
custody, deadlines, lifetime budgets, acknowledgment pumping, cancellation and
join restoration. They do not establish a real MemoryStore publication, SDK
interoperability or whole-listener health. A controlled gate in the original
Wrapper proves join Pending, not real native shutdown Pending; the earlier
private collector's separate TCP shutdown coverage remains unchanged.

The distinct server fixture uses direct MemoryStore, StateMachine, LocalProposer
and Broker. It targets SDK 7.21.0 over isolated trusted localhost raw TLS with a
Manage SAS policy. No StoreProvider, Fjall reopen, durable catalog, snapshot,
replication or protected-store activation is involved.

Its current-client workflow warms one immediate Send, rolls back two fixed IDs
in an incomplete Serializable/AsyncFlow scope, then commits two other fixed IDs
in a completed scope. It performs no receive, settlement, management, CreateBatch,
cold-first enlistment, session/topic work, cross-queue work, retries or fallback
listener selection. Existing operation/scope deadlines and zero-retry policy
remain unchanged.

The inline controller retains four lifetime socket roots/anchors and never
reuses a slot after a join. It roots actual TCP accept Ready before disposal,
installs returned original Wrapper tokens synchronously and drives borrowed root
steps beside the original bounded child future. A fifth actual socket occupies
one retained overflow/refusal slot, closes acceptance and drains prior roots.
No unbounded accepted-socket list or spawned root-owning coordinator is added.

The original child Result and any raw poll-panic payload are retained separately;
a completed child future is not repolled. All covered roots are drained before
propagating those outcomes. The fixture keeps reports and anchors through exact
canonical Memory message/body/property and ready/lock/expiry-index assertions:
the warm and committed IDs are present, and rollback IDs are absent. The actual
queue counter record must decode to next_sequence 4 and next_lock_token 1; missing
or malformed counters are not defaulted. Business state is not transport health,
native cleanup or durable persistence evidence.

The ignored case is
`current_stable_dotnet_client_retained_memory_send_commit_and_rollback`.
It reuses the existing bounded two-core current-client build/run and isolated
trust helpers. Child/output-reader process ownership is separate from the
listener-role report; a successful child exit and exact final marker are required,
not inferred from a request or cleanup signal.

## Exclusions

The report covers one socket's created Wrapper, Actor, Reader, Sessions and
returned routing workers, not sibling connections, an acceptance parent,
certificates, the Broker owner thread/native jobs, arbitrary retained operation
futures, the child/output readers, stores/providers or the whole fixture.

Existing Broker Drop sends Stop and discards its owner-thread join result.
Neither its return nor a socket report certifies successful Broker cleanup,
native-buffer disposal, source health, safe reopen, physical durability or
production adoption. Post-report error/anchor disposal can still panic.

Root/runtime loss, undriven runtimes, uncooperative work/destructors, OOM/process
abort, double panic and spawn hooks preventing original token return remain
unsupported. Ordinary TCP/TLS/WebSocket listeners, posting-only ingress, server
startup and existing SDK gates are not activated or widened by this surface.

## Verification

The final formatted source passed both default and all-feature workspace suites:
5,159 passing checks and 11 ignored cases in each, across 140 physical result
groups and 134 logical harness owners. All 5,096 preceding passes and ten ignored
cases retain their identities, statuses, relative order and ignore reasons.
Six pre-existing compile-fail locations moved because of added fields/comments;
their complete original fences remained byte-exact. The additions are 54 regular
cases, eight compile-fail checks, one no-run example and the ignored current-client
gate below. The no-run example is compiled, not executed.

Focused native ownership checks passed all 56 cases, including the 38 unchanged
cases and 18 new close/abort observation cases. The 13 connection-identity cases,
two new actual-TCP retained-protocol observation cases and nine retained server
unit cases also passed; the real-client case remained ignored in that unit run.
These checks exercise the selected custody and disposal predicates, not arbitrary
actor I/O errors, universal destructor behavior or whole-fixture health.

All 11 selected real .NET gates passed in one serial run, including the existing
current/previous message-session, receiving-batch, REMOVE-action, WebSocket and
warmed/cold transaction cases, plus this current-only retained Memory case.
The retained case also passed a separate real-client run. Its passing runs reached
the exact three-message canonical state and strict queue-counter assertions.
The current-only gate does not add previous-client, Fjall, receive or cold-first
coverage to this retained fixture; those existing gates use their separate paths.

Two initial real-client attempts failed before canonical assertions. The diagnostic
attempt observed a successful child and marker beside raw Reader cancellation
and native Close Stopped. Those original errors remain retained; the final
consumer accepts only the completed peer-reply/same-token request conjunction
above, without inferring cancellation cause. Early formatting, test compilation,
refused-Attach expectation and strict-lint failures were corrected within the new
source/tests; their logs remain retained, not relabeled as passing runs.

Final formatting, strict all-target workspace lint in both feature configurations,
both workspace builds, protobuf validation and whitespace checks passed. The
39 source digests remained unchanged throughout the closed final verification
group. Compilation and child processes were restricted to two CPUs at low
priority; Rust tests ran serially. Disk headroom was checked before and after the
large runs, and the existing shared build cache was reused.

Historical SDK/cluster failures recorded by the earlier guides remain unexplained.
These green checks neither identify their causes nor certify native health,
whole-descendant cleanup, production adoption, safe reopen or durable publication.
