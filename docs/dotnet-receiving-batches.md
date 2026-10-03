# .NET Receiving Batches

The ordinary TLS listener has separate opt-in receive-batch gates for the pinned
.NET Service Bus clients 7.21.0 and 7.20.2. Each pin runs against both memory and
Fjall. Existing message, WebSocket, transaction, and SQL-action gates remain
separate and unchanged.

## Held Action Copies

The SDK creates two REMOVE action rules and two overlapping action-free rules,
then sends one rich topic message. One Alpha receiver with `PrefetchCount = 0`
collects all three copies through `ReceiveMessagesAsync` before completing any
of them. The gate requires distinct canonical sequences, exact rule names,
distinct nonempty lock tokens, and delivery count one. Body, system properties,
footer, scalar types, and private property removals are checked against the
original.

The second transported copy is completed first. After each completion, Peek
must contain exactly the remaining Alpha sequences, while the independent beta
subscription retains its unchanged original. The beta original is then drained
separately. This proves simultaneous held copies on one receiver and independent
completion; sequential Receive-Complete calls or multiple receiver links cannot
satisfy it.

## Prefetch And Replenishment

One PeekLock queue receiver uses `PrefetchCount = 3` with six unique messages.
It holds messages 1, 2, and 3 before any completion, completes 2 then 1, and
collects 4 and 5 while 3 remains held. It then completes 3 and 5, collects 6
while 4 remains held, and finishes 6 then 4. All six sequences, bodies,
identifiers, lock tokens, and delivery counts are checked; each awaited
completion is followed by an exact remaining-sequence Peek.

Both pinned SDKs configure count prefetch as automatic link credit and PeekLock
as settlement on disposition, rather than settlement on arrival.
See the [7.21.0 receiving profile](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpConnectionScope.cs#L653-L662)
and [7.20.2 receiving profile](https://github.com/Azure/azure-sdk-for-net/blob/f81988005453a91be7a974bba8c1b69012758e0f/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpConnectionScope.cs#L627-L635).
Their AMQP implementation restores count credit on settlement and batches Flow
at a [threshold of two for credit three](https://github.com/Azure/azure-amqp/blob/175d93e37b93b035e258c6b0cc3d877d43a0f110/Microsoft.Azure.Amqp/Amqp/AmqpLinkSettings.cs#L16-L27).
Ordinary dequeue does not request more credit when automatic Flow is enabled.
See [count-credit replenishment](https://github.com/Azure/azure-amqp/blob/175d93e37b93b035e258c6b0cc3d877d43a0f110/Microsoft.Azure.Amqp/Amqp/AmqpLink.cs#L718-L742)
and [on-demand receiving](https://github.com/Azure/azure-amqp/blob/175d93e37b93b035e258c6b0cc3d877d43a0f110/Microsoft.Azure.Amqp/Amqp/ReceivingAmqpLink.cs#L169-L223).

The rolling workflow exercises that pinned policy. It does not require six
unsettled messages under a three-credit grant, a full first SDK batch, or
replenishment after only one completion. It establishes neither general Azure
prefetch parity nor a new application-held-message quota.

## Bounds And Cleanup

Each receiver's collector shares one 20-second deadline across at most 12
calls, including all rolling queue windows and intervening settlement and Peek
operations. Each call has a positive wait of at most three seconds and requests
only the remaining count. Legitimate short or empty returns may be retried
within those bounds. Missing, repeated, or excess messages fail the gate. The SDK workflow
has a separate overall deadline and zero retries.

The client restores explicit action-free default rules and verifies empty
primary and dead-letter entities. Endpoint and client disposal runs in reverse
ownership order with bounded waits before the exact success line is emitted.
The Rust gate uses isolated child certificate trust and the existing bounded
process runner: two-job .NET builds, 180-second build/run deadlines, bounded
output capture, and owned-process cleanup.

After stopping the listener and joining the broker owner, Rust checks exact
configurations, default rules, topic sequence counter 4, queue sequence counter
7, and empty message, ready, lock, expiry, scheduled, session, and duplicate
indexes. It drops the store handle, reopens the provider, compares the complete
snapshot, and repeats the typed checks. Final empty state is cleanup evidence,
not an independent proof of receive count or operation ordering.

Run only these client gates on Linux with:

```sh
CARGO_BUILD_JOBS=2 DOTNET_PROCESSOR_COUNT=2 TOKIO_WORKER_THREADS=2 \
  cargo test -p server --test amqp_dotnet_current receive_batch \
  --all-features --locked -j 2 -- --ignored --test-threads=1
```

These are local interoperability checks over ordinary AMQP/TLS. They do not
extend the experimental transactional receiver, add session batching, prove
fair sharing between receiver links, or change the cancellation boundaries of
the [ordinary receiving pipeline](ordinary-receiving-pipeline.md).
