# Initial Transaction Authorization

The explicit [atomic messaging listener](atomic-messaging-ingress.md) permits
bounded transaction-control metadata before the first connection grant. This
supports clients that declare a transaction before opening the queue link that
supplies CBS authorization. It does not authorize queue access or a commit, and
is not general SDK transaction compatibility. The separate
[SDK gates](dotnet-transaction-scopes.md) establish cold-first immediate
same-queue Send and the existing warmed workflows over trusted TLS, not
transactional acquisition or cold-first Complete.

## Fixed Initial Window

With shared-access authentication configured, this listener installs one
connection-owned initial window at the existing post-Open authorization timer.
Its default is 20 seconds, configurable through the existing authentication
builder. The deadline never restarts for a controller attach, Declare, rollback,
failed token, or another attempt to install the window. Deadline equality is
expired. An unrepresentable deadline fails closed before session dispatch.

During this window, only an admitted coordinator, bounded Declare metadata,
and explicit `fail=true` rollback can proceed without a grant. The existing
32-controller/transaction, 128-link/operation, and 32-session limits still
apply. Declarations and empty rollback do not bind a queue, read storage, sample
the broker clock, or hand work to the broker owner. A `fail=false` discharge,
including an empty commit, requires a currently valid connection grant.
Unauthorized control closes its coordinator and cancels pending authority.
If no authorization arrives before the fixed deadline, the connection closes
with `amqp:unauthorized-access`.

## Authorization History

Successful live grant publication permanently ends the initial window, under
the same lock used to publish grants. A short-lived grant cannot disappear
between driver polls and leave the window open. Prior successful PLAIN
authentication also ends the window, even if that grant expires before the
transaction driver starts. Invalid tokens never end or extend the window.

Authorization history is not authorization authority. After the first grant,
controllers still require currently valid grants and close when none remain.
There is no second initial grace period. The connection retains its existing
reauthorization behavior: a later valid CBS grant can admit a new coordinator,
without reviving an old coordinator or its transactions. Initial expiry cannot
be undone by a late token.

## Queue And Commit Boundaries

Exact Send or Listen authorization remains mandatory before reading queue
topology or binding a producer or consumer. Control grace never removes the
connection's authorization context. Each receipt is checked, and a control
request is checked again after waiting for owner-event capacity. Final handoff
still checks the controller's current grant and every participating exact
resource grant; the minimum required expiry restricts the unique owner claim.
Queue-incarnation, live-lock, content, action, and transaction limits are
unchanged.

The ordinary listener, posting-only listener, strict native APIs, TLS/SASL
handshake deadline, and transport watchdogs retain their existing policies.
Connections with authentication disabled retain their explicit development
behavior; this window does not make an unauthenticated listener safe.
