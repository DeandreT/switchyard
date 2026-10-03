# Native Outgoing Admission

Ordinary native `Sender` endpoints expose local outgoing reservations and an
owned send-result future. These APIs separate admission from the later peer
outcome without changing AMQP flow control or settlement.

## Reserve And Claim

`reserve_send()` returns an owned `Send + 'static` future. Creating it does not
enqueue a command or perform transport IO. The connection actor grants its
unique `OutgoingSendReservation` against current link credit and existing
outgoing metadata limits: 32 queued slots, 1,024 outstanding deliveries per
link, and 4,096 across a session. Waiting, granted, and claimed reservations
participate in those limits alongside admitted sends.

`try_claim()` consumes that token and returns a unique
`ClaimedOutgoingSendReservation`. A processed peer credit withdrawal can revoke
an unclaimed slot; claim and revocation race through shared state, and the loser
cannot authorize another send. `SendReservationRevoked` is a local refusal, not
a peer outcome. Neither token is cloneable or reconstructible from a link name,
channel, handle, delivery ID, or tag.

Reservations retain their original active link and connection. Link, session,
or connection retirement revokes their authority. Reusing a numeric alias does
not transfer it to the replacement. Wrong-origin checks precede native message
encoding and content admission.

Reserve and claim do not advance a wire delivery count or allocate a delivery
ID. A claimed slot is local admission, not irrevocable permission to transfer:
the first Transfer still requires current link credit and session window.
Credit withdrawn after claim can delay the send until a later grant.

Reserved capacity is protected from ordinary queued sends through the reserved
send's first Transfer. A reserved queued row can therefore precede an older
ordinary row that lacks unreserved credit. Without reservations, the existing
queued-send ordering is unchanged. An unused lookup slot blocks drain until it
is returned; an empty broker read must drop its slot before waiting for new
deliverability.

## Owned Outcome Future

`send_reserved_with_settlement_owned(reservation, message, tag)` consumes the
claimed token and returns an owned `Send + 'static` future. It does not borrow
the sender, so the caller can keep another outcome future, request another
reservation, or watch `on_detach()` independently.

The factory performs no transport IO and arms its cancellation guard before
returning. Its owned message is caller-retained content, not a precharged native
payload allowance. Native tag, size, encoding, frame, and content-budget checks
still run when the actor admits the command.

On an unsettled sending link, the future resolves to the existing
`PendingSettlement` after the peer terminal outcome or applicable default.
Completing a Transfer alone does not supply a peer outcome. Initially
sender-settled messages retain their local Accepted result after final flush.
First and second receiver-settle modes, defaults, and peer settlement without
an outcome also retain their existing rules. Concurrent futures can observe
outcomes out of send order; each result retains its exact original delivery and
acknowledgement generation.

`PendingSettlement::accept()` or `reject()` performs the existing final
acknowledgement when required. The owned future does not perform it
automatically. A broker adapter must commit its canonical logical settlement
before acknowledging it to the peer. An observer of delivery provenance is not
new settlement, transaction, or broker authority.

The existing borrowed `send_reserved_with_settlement()` delegates to the same
path when polled. `send_reserved()` retains its outcome-plus-final-ACK behavior;
ordinary unreserved send methods retain their existing interfaces.

## Cancellation Boundary

Dropping a waiting reservation future, an unused token, or a send-result future
before the actor consumes its token cancels its unique slot synchronously
through shared state. Actor cleanup is woken independently of command-channel
capacity. A lost reservation reply cannot strand its slot. No wire counter has
been spent at that point, so local reuse is possible without advancing it.

Once the actor consumes the token at native admission, dropping its result
waiter cannot undo native admission. The send, tag, and acknowledgement state
retain their ordinary lifetime until transport completion, settlement, or exact
teardown. A dropped waiter does not settle a delivery, auto-acknowledge a pending
second-mode result, abandon a broker message, or refund spent wire credit.

These APIs do not make broker Receive cancellable. A committed receive-delete
remains deleted, and a committed unsettled lock retains the existing expiry
fallback. They do not provide a final owner-claim authorization guard, durable
recovery, transactional admission, or a connection-wide bound on arbitrary
caller-owned futures and messages.

The ordinary Service Bus listener adds its own
[bounded receiving pipeline](ordinary-receiving-pipeline.md) around these APIs.
Its held-work budget is separate from native admission and encoded-content
limits. Separate [SDK batch gates](dotnet-receiving-batches.md) exercise held
action copies and rolling prefetch replenishment; the experimental transactional
receiver is unchanged.
