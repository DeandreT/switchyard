using Azure;
using Azure.Messaging.ServiceBus;

if (args.Length != 12)
{
    Console.Error.WriteLine(
        "usage: <namespace> <custom-endpoint> <queue> <batch-queue> <schedule-queue> <dedupe-queue> <session-queue> <topic> <subscription-a> <subscription-b> <key-name> <key>");
    return 2;
}

string fullyQualifiedNamespace = args[0];
var customEndpoint = new Uri(args[1]);
string queue = args[2];
string batchQueue = args[3];
string scheduleQueue = args[4];
string dedupeQueue = args[5];
string sessionQueue = args[6];
string topic = args[7];
string firstSubscription = args[8];
string secondSubscription = args[9];
string keyName = args[10];
string key = args[11];

var options = new ServiceBusClientOptions
{
    CustomEndpointAddress = customEndpoint,
    TransportType = ServiceBusTransportType.AmqpWebSockets,
    RetryOptions =
    {
        MaxRetries = 0,
        TryTimeout = TimeSpan.FromSeconds(10),
    },
};

await using var client = new ServiceBusClient(
    fullyQualifiedNamespace,
    new AzureNamedKeyCredential(keyName, key),
    options);

await using ServiceBusSender sender = client.CreateSender(queue);
await using ServiceBusReceiver receiver = client.CreateReceiver(queue);
const string body = "official-dotnet-websockets";
await sender.SendMessageAsync(new ServiceBusMessage(body)
{
    MessageId = "official-dotnet-websockets",
});
ServiceBusReceivedMessage? received =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (received?.Body.ToString() != body)
{
    Console.Error.WriteLine($"unexpected WebSocket message: {received?.Body}");
    return 3;
}
await receiver.CompleteMessageAsync(received);

const string deferredBody = "official-websocket-deferred";
await sender.SendMessageAsync(new ServiceBusMessage(deferredBody)
{
    MessageId = "official-websocket-deferred",
});
ServiceBusReceivedMessage? receivedToDefer =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (receivedToDefer?.Body.ToString() != deferredBody)
{
    Console.Error.WriteLine(
        $"unexpected WebSocket message selected for deferral: {receivedToDefer?.Body}");
    return 8;
}
long deferredSequence = receivedToDefer.SequenceNumber;
await receiver.DeferMessageAsync(receivedToDefer);
if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
{
    Console.Error.WriteLine("a deferred WebSocket message remained ordinarily visible");
    return 9;
}
ServiceBusReceivedMessage? peekedDeferred =
    await receiver.PeekMessageAsync(deferredSequence);
if (peekedDeferred?.Body.ToString() != deferredBody ||
    peekedDeferred.SequenceNumber != deferredSequence ||
    peekedDeferred.State != ServiceBusMessageState.Deferred ||
    peekedDeferred.DeliveryCount != 1)
{
    Console.Error.WriteLine(
        $"unexpected deferred WebSocket peek: body={peekedDeferred?.Body}, " +
        $"sequence={peekedDeferred?.SequenceNumber}, state={peekedDeferred?.State}, " +
        $"delivery={peekedDeferred?.DeliveryCount}");
    return 11;
}
var rawPeekedDeferred = peekedDeferred.GetRawAmqpMessage();
if (rawPeekedDeferred.MessageAnnotations.ContainsKey("x-opt-locked-until") ||
    rawPeekedDeferred.DeliveryAnnotations.ContainsKey("x-opt-lock-token"))
{
    Console.Error.WriteLine("a peeked WebSocket message exposed settlement authority");
    return 12;
}
await using ServiceBusReceiver deferredReceiver = client.CreateReceiver(queue);
ServiceBusReceivedMessage? deferred =
    await deferredReceiver.ReceiveDeferredMessageAsync(deferredSequence);
if (deferred?.Body.ToString() != deferredBody ||
    deferred.SequenceNumber != deferredSequence ||
    deferred.State != ServiceBusMessageState.Deferred)
{
    Console.Error.WriteLine(
        $"unexpected deferred WebSocket message: body={deferred?.Body}, " +
        $"sequence={deferred?.SequenceNumber}, state={deferred?.State}");
    return 10;
}
await deferredReceiver.CompleteMessageAsync(deferred);

await using ServiceBusSender scheduleSender = client.CreateSender(scheduleQueue);
await using ServiceBusReceiver scheduleReceiver = client.CreateReceiver(scheduleQueue);
DateTimeOffset scheduledAt = DateTimeOffset.UtcNow.AddHours(1);
long scheduledSequence = await scheduleSender.ScheduleMessageAsync(
    new ServiceBusMessage("websocket-scheduled-cancelled")
    {
        MessageId = "websocket-scheduled-cancelled",
    },
    scheduledAt);
ServiceBusReceivedMessage? peekedScheduled =
    await scheduleReceiver.PeekMessageAsync(scheduledSequence);
if (peekedScheduled?.Body.ToString() != "websocket-scheduled-cancelled" ||
    peekedScheduled.SequenceNumber != scheduledSequence ||
    peekedScheduled.State != ServiceBusMessageState.Scheduled ||
    (peekedScheduled.ScheduledEnqueueTime - scheduledAt).Duration() >
        TimeSpan.FromSeconds(1))
{
    Console.Error.WriteLine(
        $"unexpected scheduled WebSocket peek: body={peekedScheduled?.Body}, " +
        $"sequence={peekedScheduled?.SequenceNumber}, " +
        $"state={peekedScheduled?.State}, " +
        $"scheduled={peekedScheduled?.ScheduledEnqueueTime:o}");
    return 13;
}
await scheduleSender.CancelScheduledMessageAsync(scheduledSequence);
await using ServiceBusReceiver afterScheduleCancel = client.CreateReceiver(scheduleQueue);
if (await afterScheduleCancel.PeekMessageAsync(scheduledSequence) is not null)
{
    Console.Error.WriteLine("the WebSocket scheduled cancellation left a message behind");
    return 14;
}

await using ServiceBusSender batchSender = client.CreateSender(batchQueue);
using ServiceBusMessageBatch batch = await batchSender.CreateMessageBatchAsync();
for (int index = 0; index < 3; index++)
{
    if (!batch.TryAddMessage(new ServiceBusMessage($"websocket-batch-{index}")
    {
        MessageId = $"websocket-batch-{index}",
    }))
    {
        Console.Error.WriteLine($"WebSocket batch child {index} did not fit");
        return 5;
    }
}
await batchSender.SendMessagesAsync(batch);
await using ServiceBusReceiver batchReceiver = client.CreateReceiver(
    batchQueue,
    new ServiceBusReceiverOptions { PrefetchCount = 3 });
IReadOnlyList<ServiceBusReceivedMessage> batchReceived =
    await batchReceiver.ReceiveMessagesAsync(3, TimeSpan.FromSeconds(10));
if (batchReceived.Count != 3)
{
    Console.Error.WriteLine(
        $"expected three WebSocket batch messages, received {batchReceived.Count}");
    return 6;
}
for (int index = batchReceived.Count - 1; index >= 0; index--)
{
    if (batchReceived[index].Body.ToString() != $"websocket-batch-{index}")
    {
        Console.Error.WriteLine($"unexpected WebSocket batch child {index}");
        return 7;
    }
    await batchReceiver.CompleteMessageAsync(batchReceived[index]);
}

await using ServiceBusSender dedupeSender = client.CreateSender(dedupeQueue);
await using ServiceBusReceiver dedupeReceiver = client.CreateReceiver(dedupeQueue);
await dedupeSender.SendMessageAsync(new ServiceBusMessage("websocket-dedupe-first")
{
    MessageId = "websocket-dedupe",
});
await dedupeSender.SendMessageAsync(new ServiceBusMessage("websocket-dedupe-second")
{
    MessageId = "websocket-dedupe",
});
ServiceBusReceivedMessage? deduplicated =
    await dedupeReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deduplicated?.Body.ToString() != "websocket-dedupe-first")
{
    Console.Error.WriteLine($"unexpected WebSocket dedupe winner: {deduplicated?.Body}");
    return 15;
}
await dedupeReceiver.CompleteMessageAsync(deduplicated);
if (await dedupeReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null ||
    await dedupeReceiver.PeekMessageAsync() is not null)
{
    Console.Error.WriteLine("a duplicate WebSocket message remained in the queue");
    return 16;
}

await using ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-websocket-session")
{
    MessageId = "official-websocket-session",
    SessionId = "websocket-session-1",
});
await using ServiceBusSessionReceiver sessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "websocket-session-1");
ServiceBusReceivedMessage? sessionMessage =
    await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (sessionMessage?.Body.ToString() != "official-websocket-session")
{
    Console.Error.WriteLine($"unexpected WebSocket session message: {sessionMessage?.Body}");
    return 4;
}
await sessionReceiver.CompleteMessageAsync(sessionMessage);

if (!await TopicConformance.RunAsync(client, topic, firstSubscription, secondSubscription))
{
    Console.Error.WriteLine("WebSocket topic fan-out did not preserve both copies");
    return 17;
}

Console.WriteLine(
    "official .NET Service Bus client AMQP-over-WebSockets batch/prefetch, " +
    "send/receive/complete, defer/peek/deferred-receive, schedule/cancel, " +
    "duplicate detection, topic fan-out, and session attach passed");
return 0;
