using Azure;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;

if (args.Length != 9)
{
    Console.Error.WriteLine(
        "usage: <namespace> <custom-endpoint> <queue> <batch-queue> <peek-queue> <schedule-queue> <session-queue> <key-name> <key>");
    return 2;
}

string fullyQualifiedNamespace = args[0];
var customEndpoint = new Uri(args[1]);
string queue = args[2];
string batchQueue = args[3];
string peekQueue = args[4];
string scheduleQueue = args[5];
string sessionQueue = args[6];
string keyName = args[7];
string key = args[8];

var options = new ServiceBusClientOptions
{
    CustomEndpointAddress = customEndpoint,
    TransportType = ServiceBusTransportType.AmqpTcp,
    CertificateValidationCallback = (_, _, _, _) => true,
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

await sender.SendMessageAsync(new ServiceBusMessage("official-dotnet-current"));
ServiceBusReceivedMessage? received =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (received is null)
{
    Console.Error.WriteLine("the official client did not receive its message");
    return 3;
}
if (received.Body.ToString() != "official-dotnet-current")
{
    Console.Error.WriteLine($"unexpected body: {received.Body}");
    return 4;
}

DateTimeOffset lockedUntilBeforeRenewal = received.LockedUntil;
await receiver.RenewMessageLockAsync(received);
if (received.LockedUntil < lockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"renewal moved the lock backward: {lockedUntilBeforeRenewal:o} -> {received.LockedUntil:o}");
    return 5;
}

await receiver.CompleteMessageAsync(received);

DateTimeOffset createdAt = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
var fidelityMessage = new ServiceBusMessage("official-envelope-current")
{
    MessageId = "official-envelope-current",
    CorrelationId = "correlation-42",
    Subject = "orders.created",
    ContentType = "application/json",
    To = "logical-orders",
    ReplyTo = "order-replies",
    ReplyToSessionId = "reply-session-1",
    TimeToLive = TimeSpan.FromMinutes(2),
};
fidelityMessage.ApplicationProperties["tenant"] = "northwind";
fidelityMessage.ApplicationProperties["attempt"] = 3;
AmqpAnnotatedMessage outboundEnvelope = fidelityMessage.GetRawAmqpMessage();
outboundEnvelope.Header.Durable = true;
outboundEnvelope.Header.Priority = 7;
outboundEnvelope.Properties.UserId = new byte[] { 1, 2, 3, 4 };
outboundEnvelope.Properties.ContentEncoding = "utf-8";
outboundEnvelope.Properties.CreationTime = createdAt;
outboundEnvelope.DeliveryAnnotations["x-switchyard-delivery"] = "delivery-value";
outboundEnvelope.MessageAnnotations["x-switchyard-message"] = "message-value";
outboundEnvelope.Footer["x-switchyard-footer"] = "footer-value";

await sender.SendMessageAsync(fidelityMessage);
ServiceBusReceivedMessage? fidelity =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (fidelity is null)
{
    Console.Error.WriteLine("the official client did not receive the envelope message");
    return 18;
}
if (fidelity.Body.ToString() != "official-envelope-current" ||
    fidelity.MessageId != "official-envelope-current" ||
    fidelity.CorrelationId != "correlation-42" ||
    fidelity.Subject != "orders.created" ||
    fidelity.ContentType != "application/json" ||
    fidelity.To != "logical-orders" ||
    fidelity.ReplyTo != "order-replies" ||
    fidelity.ReplyToSessionId != "reply-session-1")
{
    Console.Error.WriteLine("standard message properties did not round-trip");
    return 19;
}
if (!Equals(fidelity.ApplicationProperties["tenant"], "northwind") ||
    Convert.ToInt32(fidelity.ApplicationProperties["attempt"]) != 3)
{
    Console.Error.WriteLine("application properties did not round-trip");
    return 20;
}
if (fidelity.DeliveryCount != 1 || fidelity.SequenceNumber <= 0 ||
    fidelity.EnqueuedTime == default || fidelity.LockedUntil <= fidelity.EnqueuedTime ||
    fidelity.ExpiresAt <= fidelity.EnqueuedTime ||
    fidelity.ExpiresAt - fidelity.EnqueuedTime != TimeSpan.FromMinutes(2))
{
    Console.Error.WriteLine(
        $"invalid broker fields: delivery={fidelity.DeliveryCount}, " +
        $"sequence={fidelity.SequenceNumber}, enqueued={fidelity.EnqueuedTime:o}, " +
        $"locked={fidelity.LockedUntil:o}, expires={fidelity.ExpiresAt:o}");
    return 21;
}

AmqpAnnotatedMessage inboundEnvelope = fidelity.GetRawAmqpMessage();
if (inboundEnvelope.Header.Durable != true || inboundEnvelope.Header.Priority != 7 ||
    inboundEnvelope.Header.DeliveryCount != 1 ||
    inboundEnvelope.Header.TimeToLive is not { } rawTimeToLive ||
    rawTimeToLive <= TimeSpan.Zero ||
    (rawTimeToLive - TimeSpan.FromMinutes(2)).Duration() >
        TimeSpan.FromSeconds(1))
{
    Console.Error.WriteLine(
        "the AMQP header did not round-trip with broker delivery count: " +
        $"durable={inboundEnvelope.Header.Durable}, " +
        $"priority={inboundEnvelope.Header.Priority}, " +
        $"deliveryCount={inboundEnvelope.Header.DeliveryCount}, " +
        $"ttl={inboundEnvelope.Header.TimeToLive}");
    return 22;
}
var observedUserId = inboundEnvelope.Properties.UserId;
if (inboundEnvelope.Properties.ContentEncoding != "utf-8" ||
    inboundEnvelope.Properties.CreationTime is null ||
    observedUserId is not { } userId ||
    !userId.Span.SequenceEqual(new byte[] { 1, 2, 3, 4 }))
{
    Console.Error.WriteLine(
        "raw AMQP properties did not round-trip: " +
        $"contentEncoding={inboundEnvelope.Properties.ContentEncoding}, " +
        $"creationTime={inboundEnvelope.Properties.CreationTime:o}, " +
        $"userId={observedUserId}");
    return 23;
}
if (!inboundEnvelope.DeliveryAnnotations.TryGetValue(
        "x-switchyard-delivery", out object? deliveryAnnotation) ||
    !Equals(deliveryAnnotation, "delivery-value") ||
    !inboundEnvelope.MessageAnnotations.TryGetValue(
        "x-switchyard-message", out object? messageAnnotation) ||
    !Equals(messageAnnotation, "message-value") ||
    !inboundEnvelope.Footer.TryGetValue("x-switchyard-footer", out object? footer) ||
    !Equals(footer, "footer-value"))
{
    Console.Error.WriteLine("raw AMQP annotations or footer did not round-trip");
    return 24;
}
foreach (string brokerAnnotation in new[]
{
    "x-opt-sequence-number",
    "x-opt-enqueue-sequence-number",
    "x-opt-enqueued-time",
    "x-opt-locked-until",
})
{
    if (!inboundEnvelope.MessageAnnotations.ContainsKey(brokerAnnotation))
    {
        Console.Error.WriteLine($"broker annotation {brokerAnnotation} is missing");
        return 25;
    }
}
await receiver.CompleteMessageAsync(fidelity);

await using ServiceBusSender batchSender = client.CreateSender(batchQueue);
var enumerableBatch = Enumerable.Range(0, 3)
    .Select(index =>
    {
        var message = new ServiceBusMessage($"enumerable-batch-{index}")
        {
            MessageId = $"enumerable-batch-{index}",
            Subject = "batch.enumerable",
        };
        message.ApplicationProperties["batch-index"] = index;
        return message;
    })
    .ToArray();
await batchSender.SendMessagesAsync(enumerableBatch);

using ServiceBusMessageBatch explicitBatch = await batchSender.CreateMessageBatchAsync();
for (int index = 0; index < 3; index++)
{
    var message = new ServiceBusMessage($"explicit-batch-{index}")
    {
        MessageId = $"explicit-batch-{index}",
        Subject = "batch.explicit",
    };
    message.ApplicationProperties["batch-index"] = index + 3;
    if (!explicitBatch.TryAddMessage(message))
    {
        Console.Error.WriteLine($"message {index} did not fit in an empty SDK batch");
        return 29;
    }
}
await batchSender.SendMessagesAsync(explicitBatch);

ServiceBusReceiver batchReceiver = client.CreateReceiver(
    batchQueue,
    new ServiceBusReceiverOptions { PrefetchCount = 8 });
IReadOnlyList<ServiceBusReceivedMessage> batched =
    await batchReceiver.ReceiveMessagesAsync(6, TimeSpan.FromSeconds(10));
if (batched.Count != 6)
{
    Console.Error.WriteLine($"expected six batched messages, received {batched.Count}");
    return 30;
}
for (int index = 0; index < batched.Count; index++)
{
    string prefix = index < 3 ? "enumerable" : "explicit";
    int childIndex = index % 3;
    ServiceBusReceivedMessage message = batched[index];
    if (message.Body.ToString() != $"{prefix}-batch-{childIndex}" ||
        message.MessageId != $"{prefix}-batch-{childIndex}" ||
        message.Subject != $"batch.{prefix}" ||
        Convert.ToInt32(message.ApplicationProperties["batch-index"]) != index)
    {
        Console.Error.WriteLine($"batch child {index} lost content or envelope fields");
        return 31;
    }
    if (index > 0 && message.SequenceNumber != batched[index - 1].SequenceNumber + 1)
    {
        Console.Error.WriteLine(
            $"batch sequence is not consecutive at {index}: " +
            $"{batched[index - 1].SequenceNumber} -> {message.SequenceNumber}");
        return 32;
    }
}
await Task.WhenAll(
    batched.Reverse().Select(message => batchReceiver.CompleteMessageAsync(message)));
await batchReceiver.DisposeAsync();

var receiveAndDeleteMessages = Enumerable.Range(0, 3)
    .Select(index => new ServiceBusMessage($"receive-delete-batch-{index}")
    {
        MessageId = $"receive-delete-batch-{index}",
    });
await batchSender.SendMessagesAsync(receiveAndDeleteMessages);
await using (ServiceBusReceiver receiveAndDelete = client.CreateReceiver(
    batchQueue,
    new ServiceBusReceiverOptions
    {
        PrefetchCount = 3,
        ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
    }))
{
    IReadOnlyList<ServiceBusReceivedMessage> deleted =
        await receiveAndDelete.ReceiveMessagesAsync(3, TimeSpan.FromSeconds(10));
    if (deleted.Count != 3 ||
        deleted.Select(message => message.Body.ToString()).ToArray() is not
        ["receive-delete-batch-0", "receive-delete-batch-1", "receive-delete-batch-2"])
    {
        Console.Error.WriteLine("receive-and-delete did not return the complete batch in order");
        return 33;
    }
}

var creditDrainMessages = Enumerable.Range(0, 5)
    .Select(index => new ServiceBusMessage($"credit-drain-{index}")
    {
        MessageId = $"credit-drain-{index}",
    })
    .ToArray();
await batchSender.SendMessagesAsync(creditDrainMessages);
await using (ServiceBusReceiver limitedCredit = client.CreateReceiver(
    batchQueue,
    new ServiceBusReceiverOptions
    {
        PrefetchCount = 0,
        ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
    }))
{
    ServiceBusReceivedMessage? firstCredited =
        await limitedCredit.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    ServiceBusReceivedMessage? secondCredited =
        await limitedCredit.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    if (firstCredited?.Body.ToString() != "credit-drain-0" ||
        secondCredited?.Body.ToString() != "credit-drain-1")
    {
        Console.Error.WriteLine(
            "the two credited receives consumed the wrong messages: " +
            $"first={firstCredited?.Body.ToString() ?? "<none>"}, " +
            $"second={secondCredited?.Body.ToString() ?? "<none>"}");
        return 34;
    }
}
await using (ServiceBusReceiver afterDrain = client.CreateReceiver(
    batchQueue,
    new ServiceBusReceiverOptions
    {
        PrefetchCount = 0,
        ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
    }))
{
    ServiceBusReceivedMessage? third =
        await afterDrain.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    ServiceBusReceivedMessage? fourth =
        await afterDrain.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    ServiceBusReceivedMessage? fifth =
        await afterDrain.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    string[] remainingAfterDrain =
        [
            third?.Body.ToString() ?? "<none>",
            fourth?.Body.ToString() ?? "<none>",
            fifth?.Body.ToString() ?? "<none>",
        ];
    if (remainingAfterDrain is not ["credit-drain-2", "credit-drain-3", "credit-drain-4"])
    {
        Console.Error.WriteLine(
            $"draining a limited receive deleted a message beyond remote link credit: " +
            $"bodies=[{string.Join(", ", remainingAfterDrain)}]");
        return 35;
    }
}

var messagesToPeek = Enumerable.Range(0, 3)
    .Select(index => new ServiceBusMessage($"peek-batch-{index}")
    {
        MessageId = $"peek-batch-{index}",
    })
    .ToArray();
await using ServiceBusSender peekSender = client.CreateSender(peekQueue);
await peekSender.SendMessagesAsync(messagesToPeek);
await using ServiceBusReceiver peekStateReceiver = client.CreateReceiver(peekQueue);
ServiceBusReceivedMessage? lockedForPeek =
    await peekStateReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
ServiceBusReceivedMessage? deferredForPeek =
    await peekStateReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (lockedForPeek?.Body.ToString() != "peek-batch-0" ||
    deferredForPeek?.Body.ToString() != "peek-batch-1")
{
    Console.Error.WriteLine(
        $"peek setup received the wrong messages: " +
        $"locked={lockedForPeek?.Body}, deferred={deferredForPeek?.Body}");
    return 52;
}
await peekStateReceiver.DeferMessageAsync(deferredForPeek);

await using ServiceBusReceiver peekReceiver = client.CreateReceiver(peekQueue);
IReadOnlyList<ServiceBusReceivedMessage> firstPeekPage =
    await peekReceiver.PeekMessagesAsync(2, lockedForPeek.SequenceNumber);
if (firstPeekPage.Count != 2 ||
    firstPeekPage[0].Body.ToString() != "peek-batch-0" ||
    firstPeekPage[1].Body.ToString() != "peek-batch-1" ||
    firstPeekPage[0].SequenceNumber != lockedForPeek.SequenceNumber ||
    firstPeekPage[1].SequenceNumber != deferredForPeek.SequenceNumber ||
    firstPeekPage[0].State != ServiceBusMessageState.Active ||
    firstPeekPage[1].State != ServiceBusMessageState.Deferred ||
    firstPeekPage[0].DeliveryCount != 1 ||
    firstPeekPage[1].DeliveryCount != 1)
{
    Console.Error.WriteLine(
        "peek did not return the inclusive locked/deferred page with exact state and count");
    return 53;
}
foreach (ServiceBusReceivedMessage peeked in firstPeekPage)
{
    AmqpAnnotatedMessage rawPeeked = peeked.GetRawAmqpMessage();
    if (rawPeeked.MessageAnnotations.ContainsKey("x-opt-locked-until") ||
        rawPeeked.DeliveryAnnotations.ContainsKey("x-opt-lock-token"))
    {
        Console.Error.WriteLine("a peeked message exposed settlement authority");
        return 54;
    }
}

IReadOnlyList<ServiceBusReceivedMessage> secondPeekPage =
    await peekReceiver.PeekMessagesAsync(2);
if (secondPeekPage.Count != 1 ||
    secondPeekPage[0].Body.ToString() != "peek-batch-2" ||
    secondPeekPage[0].SequenceNumber != deferredForPeek.SequenceNumber + 1 ||
    secondPeekPage[0].State != ServiceBusMessageState.Active ||
    secondPeekPage[0].DeliveryCount != 0)
{
    Console.Error.WriteLine("peek cursor pagination did not return the remaining active message");
    return 55;
}
if (await peekReceiver.PeekMessageAsync() is not null)
{
    Console.Error.WriteLine("peek did not return an empty page after the final sequence");
    return 56;
}

await peekStateReceiver.CompleteMessageAsync(lockedForPeek);
ServiceBusReceivedMessage? deferredPeekCleanup =
    await peekStateReceiver.ReceiveDeferredMessageAsync(deferredForPeek.SequenceNumber);
if (deferredPeekCleanup is null)
{
    Console.Error.WriteLine("the peeked deferred message disappeared during browsing");
    return 57;
}
await peekStateReceiver.CompleteMessageAsync(deferredPeekCleanup);
ServiceBusReceivedMessage? activePeekCleanup =
    await peekStateReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (activePeekCleanup?.Body.ToString() != "peek-batch-2")
{
    Console.Error.WriteLine("the peeked active message disappeared during browsing");
    return 58;
}
await peekStateReceiver.CompleteMessageAsync(activePeekCleanup);

var oversizedPeekRequestMessages = Enumerable.Range(0, 251)
    .Select(index => new ServiceBusMessage($"peek-cap-{index}")
    {
        MessageId = $"peek-cap-{index}",
    })
    .ToArray();
await peekSender.SendMessagesAsync(oversizedPeekRequestMessages);
await using ServiceBusReceiver cappedPeekReceiver = client.CreateReceiver(peekQueue);
IReadOnlyList<ServiceBusReceivedMessage> cappedPeekPage =
    await cappedPeekReceiver.PeekMessagesAsync(
        500,
        activePeekCleanup.SequenceNumber + 1);
if (cappedPeekPage.Count != 250 ||
    cappedPeekPage[0].Body.ToString() != "peek-cap-0" ||
    cappedPeekPage[249].Body.ToString() != "peek-cap-249")
{
    Console.Error.WriteLine(
        $"a 500-message peek request was not capped to the first 250 results: " +
        $"count={cappedPeekPage.Count}");
    return 62;
}
IReadOnlyList<ServiceBusReceivedMessage> cappedPeekRemainder =
    await cappedPeekReceiver.PeekMessagesAsync(500);
if (cappedPeekRemainder.Count != 1 ||
    cappedPeekRemainder[0].Body.ToString() != "peek-cap-250")
{
    Console.Error.WriteLine("the capped peek cursor did not expose the remaining message");
    return 63;
}
if (await cappedPeekReceiver.PeekMessageAsync() is not null)
{
    Console.Error.WriteLine("the capped peek cursor did not reach true end of entity");
    return 64;
}

await using ServiceBusSender scheduleSender = client.CreateSender(scheduleQueue);
await using ServiceBusReceiver schedulePeekReceiver = client.CreateReceiver(scheduleQueue);
DateTimeOffset scheduledAt = DateTimeOffset.UtcNow.AddSeconds(15);
var scheduledMessages = new[]
{
    new ServiceBusMessage("scheduled-management-active")
    {
        MessageId = "scheduled-management-active",
        Subject = "schedule.management",
        TimeToLive = TimeSpan.FromMinutes(2),
    },
    new ServiceBusMessage("scheduled-management-cancelled")
    {
        MessageId = "scheduled-management-cancelled",
        Subject = "schedule.management",
        TimeToLive = TimeSpan.FromMinutes(2),
    },
};
scheduledMessages[0].ApplicationProperties["schedule-path"] = "management";
IReadOnlyList<long> scheduledSequences =
    await scheduleSender.ScheduleMessagesAsync(scheduledMessages, scheduledAt);
if (scheduledSequences.Count != 2 ||
    scheduledSequences[0] <= 0 ||
    scheduledSequences[1] != scheduledSequences[0] + 1)
{
    Console.Error.WriteLine(
        $"schedule management returned invalid placeholder sequences: " +
        $"[{string.Join(", ", scheduledSequences)}]");
    return 65;
}
await scheduleSender.CancelScheduledMessageAsync(scheduledSequences[1]);

var directScheduled = new ServiceBusMessage("scheduled-direct-cancelled")
{
    MessageId = "scheduled-direct-cancelled",
    Subject = "schedule.direct",
    ScheduledEnqueueTime = scheduledAt,
};
directScheduled.ApplicationProperties["schedule-path"] = "direct";
await scheduleSender.SendMessageAsync(directScheduled);

IReadOnlyList<ServiceBusReceivedMessage> scheduledPeek =
    await schedulePeekReceiver.PeekMessagesAsync(10, scheduledSequences[0]);
if (scheduledPeek.Count != 2 ||
    scheduledPeek[0].Body.ToString() != "scheduled-management-active" ||
    scheduledPeek[0].SequenceNumber != scheduledSequences[0] ||
    scheduledPeek[0].State != ServiceBusMessageState.Scheduled ||
    scheduledPeek[0].DeliveryCount != 0 ||
    scheduledPeek[1].Body.ToString() != "scheduled-direct-cancelled" ||
    scheduledPeek[1].State != ServiceBusMessageState.Scheduled ||
    scheduledPeek[1].SequenceNumber <= scheduledSequences[1])
{
    Console.Error.WriteLine(
        "peek did not expose the management/direct scheduled placeholders in order");
    return 66;
}
foreach (ServiceBusReceivedMessage scheduled in scheduledPeek)
{
    if ((scheduled.ScheduledEnqueueTime - scheduledAt).Duration() >
            TimeSpan.FromSeconds(1) ||
        scheduled.GetRawAmqpMessage().MessageAnnotations.ContainsKey("x-opt-locked-until") ||
        scheduled.GetRawAmqpMessage().DeliveryAnnotations.ContainsKey("x-opt-lock-token"))
    {
        Console.Error.WriteLine(
            "a scheduled peek lost its enqueue timestamp or exposed settlement authority");
        return 67;
    }
}
await scheduleSender.CancelScheduledMessageAsync(scheduledPeek[1].SequenceNumber);

await using (ServiceBusReceiver earlyScheduleReceiver = client.CreateReceiver(scheduleQueue))
{
    if (await earlyScheduleReceiver.ReceiveMessageAsync(
            TimeSpan.FromMilliseconds(500)) is not null)
    {
        Console.Error.WriteLine("a scheduled message was receivable before its enqueue time");
        return 68;
    }
}

await using ServiceBusReceiver activatedScheduleReceiver =
    client.CreateReceiver(scheduleQueue);
ServiceBusReceivedMessage? activatedScheduled =
    await activatedScheduleReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30));
if (activatedScheduled?.Body.ToString() != "scheduled-management-active" ||
    activatedScheduled.MessageId != "scheduled-management-active" ||
    activatedScheduled.SequenceNumber == scheduledSequences[0] ||
    activatedScheduled.State != ServiceBusMessageState.Active ||
    activatedScheduled.DeliveryCount != 1 ||
    !Equals(activatedScheduled.ApplicationProperties["schedule-path"], "management"))
{
    Console.Error.WriteLine(
        $"scheduled activation mismatch: body={activatedScheduled?.Body}, " +
        $"sequence={activatedScheduled?.SequenceNumber}, " +
        $"placeholder={scheduledSequences[0]}, state={activatedScheduled?.State}, " +
        $"delivery={activatedScheduled?.DeliveryCount}");
    return 69;
}
if ((activatedScheduled.ScheduledEnqueueTime - scheduledAt).Duration() >
        TimeSpan.FromSeconds(1) ||
    activatedScheduled.EnqueuedTime < scheduledAt - TimeSpan.FromSeconds(1) ||
    activatedScheduled.ExpiresAt - activatedScheduled.EnqueuedTime !=
        TimeSpan.FromMinutes(2))
{
    Console.Error.WriteLine(
        $"scheduled activation timestamps are invalid: " +
        $"scheduled={activatedScheduled.ScheduledEnqueueTime:o}, " +
        $"enqueued={activatedScheduled.EnqueuedTime:o}, " +
        $"expires={activatedScheduled.ExpiresAt:o}");
    return 70;
}
await activatedScheduleReceiver.CompleteMessageAsync(activatedScheduled);
if (await activatedScheduleReceiver.ReceiveMessageAsync(
        TimeSpan.FromMilliseconds(500)) is not null)
{
    Console.Error.WriteLine("a cancelled scheduled message became active");
    return 71;
}

const string settlementBody = "official-settlement-current";
await sender.SendMessageAsync(new ServiceBusMessage(settlementBody)
{
    MessageId = "official-settlement-current",
});
ServiceBusReceivedMessage? abandoned =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (abandoned is null)
{
    Console.Error.WriteLine("the official client did not receive the message to abandon");
    return 6;
}
if (abandoned.Body.ToString() != settlementBody)
{
    Console.Error.WriteLine($"unexpected message to abandon: {abandoned.Body}");
    return 7;
}
if (abandoned.DeliveryCount != 1)
{
    Console.Error.WriteLine($"unexpected first delivery count: {abandoned.DeliveryCount}");
    return 26;
}

await receiver.AbandonMessageAsync(
    abandoned,
    new Dictionary<string, object> { ["abandon-stage"] = "returned" });
ServiceBusReceivedMessage? redelivered =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (redelivered is null)
{
    Console.Error.WriteLine("the abandoned message was not redelivered");
    return 8;
}
if (redelivered.Body.ToString() != settlementBody)
{
    Console.Error.WriteLine($"unexpected redelivered message: {redelivered.Body}");
    return 9;
}
if (redelivered.MessageId != abandoned.MessageId)
{
    Console.Error.WriteLine(
        $"abandon returned a different message: {abandoned.MessageId} -> {redelivered.MessageId}");
    return 10;
}
if (redelivered.DeliveryCount != 2)
{
    Console.Error.WriteLine($"unexpected redelivery count: {redelivered.DeliveryCount}");
    return 27;
}
if (!Equals(redelivered.ApplicationProperties["abandon-stage"], "returned"))
{
    Console.Error.WriteLine("direct abandon property update was lost");
    return 43;
}

const string deadLetterReason = "SchemaMismatch";
const string deadLetterDescription = "official .NET validation failed";
var deadLetterProperties = new Dictionary<string, object>
{
    ["deadletter-stage"] = "direct",
};
await receiver.DeadLetterMessageAsync(
    redelivered,
    deadLetterProperties,
    deadLetterReason,
    deadLetterDescription);

await using ServiceBusReceiver deadLetterReceiver = client.CreateReceiver(
    queue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
ServiceBusReceivedMessage? peekedDeadLetter =
    await deadLetterReceiver.PeekMessageAsync();
if (peekedDeadLetter is null ||
    peekedDeadLetter.Body.ToString() != settlementBody ||
    peekedDeadLetter.DeadLetterReason != deadLetterReason ||
    peekedDeadLetter.DeadLetterErrorDescription != deadLetterDescription ||
    peekedDeadLetter.DeadLetterSource != queue ||
    peekedDeadLetter.DeliveryCount != 2)
{
    Console.Error.WriteLine(
        $"dead-letter peek mismatch: body={peekedDeadLetter?.Body}, " +
        $"reason={peekedDeadLetter?.DeadLetterReason}, " +
        $"description={peekedDeadLetter?.DeadLetterErrorDescription}, " +
        $"source={peekedDeadLetter?.DeadLetterSource}, " +
        $"delivery={peekedDeadLetter?.DeliveryCount}");
    return 50;
}
AmqpAnnotatedMessage peekedDeadLetterEnvelope = peekedDeadLetter.GetRawAmqpMessage();
if (peekedDeadLetterEnvelope.MessageAnnotations.ContainsKey("x-opt-locked-until") ||
    peekedDeadLetterEnvelope.DeliveryAnnotations.ContainsKey("x-opt-lock-token"))
{
    Console.Error.WriteLine("a peeked dead-letter message exposed settlement authority");
    return 51;
}
ServiceBusReceivedMessage? deadLettered =
    await deadLetterReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deadLettered is null)
{
    Console.Error.WriteLine("the dead-lettered message was not available in the DLQ");
    return 11;
}
if (deadLettered.Body.ToString() != settlementBody)
{
    Console.Error.WriteLine($"unexpected dead-letter message: {deadLettered.Body}");
    return 12;
}
if (deadLettered.DeadLetterReason != deadLetterReason)
{
    Console.Error.WriteLine(
        $"unexpected dead-letter reason: {deadLettered.DeadLetterReason}");
    return 13;
}
if (deadLettered.DeadLetterErrorDescription != deadLetterDescription)
{
    Console.Error.WriteLine(
        $"unexpected dead-letter description: {deadLettered.DeadLetterErrorDescription}");
    return 14;
}
if (deadLettered.DeadLetterSource != queue)
{
    Console.Error.WriteLine(
        $"unexpected dead-letter source: {deadLettered.DeadLetterSource}");
    return 28;
}
if (!Equals(deadLettered.ApplicationProperties["deadletter-stage"], "direct"))
{
    Console.Error.WriteLine("direct dead-letter property update was lost");
    return 44;
}
await deadLetterReceiver.CompleteMessageAsync(deadLettered);

const string deferredBody = "official-deferred-current";
var messageToDefer = new ServiceBusMessage(deferredBody)
{
    MessageId = "official-deferred-current",
};
messageToDefer.ApplicationProperties["defer-stage"] = "received";
messageToDefer.ApplicationProperties["defer-preserved"] = "original";
var followingMessage = new ServiceBusMessage("official-after-deferred-current")
{
    MessageId = "official-after-deferred-current",
};
await sender.SendMessagesAsync([messageToDefer, followingMessage]);

ServiceBusReceivedMessage? receivedToDefer =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (receivedToDefer?.Body.ToString() != deferredBody ||
    receivedToDefer.MessageId != "official-deferred-current" ||
    receivedToDefer.SequenceNumber <= 0)
{
    Console.Error.WriteLine(
        $"unexpected message selected for deferral: " +
        $"body={receivedToDefer?.Body}, id={receivedToDefer?.MessageId}, " +
        $"sequence={receivedToDefer?.SequenceNumber}");
    return 36;
}
long deferredSequence = receivedToDefer.SequenceNumber;
var deferredProperties = new Dictionary<string, object>
{
    ["defer-stage"] = "parked",
    ["defer-attempt"] = 2,
};
await receiver.DeferMessageAsync(receivedToDefer, deferredProperties);

ServiceBusReceivedMessage? receivedAfterDeferred =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (receivedAfterDeferred?.Body.ToString() != "official-after-deferred-current")
{
    Console.Error.WriteLine(
        "ordinary receive did not skip the deferred message: " +
        $"{receivedAfterDeferred?.Body}");
    return 37;
}
await receiver.CompleteMessageAsync(receivedAfterDeferred);
if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
{
    Console.Error.WriteLine("a deferred message remained visible to ordinary receive");
    return 38;
}

// Deferred retrieval uses request/response management without first opening
// this receiver's ordinary AMQP data link. This catches implementations that
// accidentally depend on delivery state registered by a prior receive.
await using ServiceBusReceiver deferredReceiver = client.CreateReceiver(queue);
ServiceBusReceivedMessage? deferred =
    await deferredReceiver.ReceiveDeferredMessageAsync(deferredSequence);
if (deferred is null || deferred.Body.ToString() != deferredBody ||
    deferred.MessageId != "official-deferred-current" ||
    deferred.SequenceNumber != deferredSequence ||
    deferred.State != ServiceBusMessageState.Deferred)
{
    Console.Error.WriteLine(
        $"deferred receive returned the wrong message: body={deferred?.Body}, " +
        $"id={deferred?.MessageId}, sequence={deferred?.SequenceNumber}, " +
        $"state={deferred?.State}");
    return 39;
}
if (!Equals(deferred.ApplicationProperties["defer-stage"], "parked") ||
    Convert.ToInt32(deferred.ApplicationProperties["defer-attempt"]) != 2 ||
    !Equals(deferred.ApplicationProperties["defer-preserved"], "original"))
{
    Console.Error.WriteLine("defer property updates did not survive deferred receive");
    return 40;
}
if (deferred.DeliveryCount != 2 || deferred.LockedUntil <= DateTimeOffset.UtcNow)
{
    Console.Error.WriteLine(
        $"deferred receive did not acquire a live second-delivery lock: " +
        $"delivery={deferred.DeliveryCount}, locked={deferred.LockedUntil:o}");
    return 41;
}
DateTimeOffset deferredLockedUntilBeforeRenewal = deferred.LockedUntil;
await deferredReceiver.RenewMessageLockAsync(deferred);
if (deferred.LockedUntil < deferredLockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"deferred lock renewal moved backward: " +
        $"{deferredLockedUntilBeforeRenewal:o} -> {deferred.LockedUntil:o}");
    return 42;
}
await deferredReceiver.AbandonMessageAsync(
    deferred,
    new Dictionary<string, object>
    {
        ["defer-stage"] = "abandoned",
        ["abandon-attempt"] = 3,
    });
if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
{
    Console.Error.WriteLine("abandoning a deferred message made it ordinarily visible");
    return 45;
}

ServiceBusReceivedMessage? deferredAfterAbandon =
    await deferredReceiver.ReceiveDeferredMessageAsync(deferredSequence);
if (deferredAfterAbandon is null ||
    deferredAfterAbandon.State != ServiceBusMessageState.Deferred ||
    deferredAfterAbandon.DeliveryCount != 3)
{
    Console.Error.WriteLine(
        $"deferred abandon did not restore the message: " +
        $"state={deferredAfterAbandon?.State}, delivery={deferredAfterAbandon?.DeliveryCount}");
    return 46;
}
if (!Equals(deferredAfterAbandon.ApplicationProperties["defer-stage"], "abandoned") ||
    Convert.ToInt32(deferredAfterAbandon.ApplicationProperties["abandon-attempt"]) != 3 ||
    !Equals(deferredAfterAbandon.ApplicationProperties["defer-preserved"], "original"))
{
    Console.Error.WriteLine("deferred abandon property updates were not durable");
    return 47;
}

const string deferredDeadLetterReason = "DeferredRejected";
const string deferredDeadLetterDescription = "deferred validation failed";
await deferredReceiver.DeadLetterMessageAsync(
    deferredAfterAbandon,
    new Dictionary<string, object> { ["defer-stage"] = "deadlettered" },
    deferredDeadLetterReason,
    deferredDeadLetterDescription);
ServiceBusReceivedMessage? deadLetteredDeferred =
    await deadLetterReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deadLetteredDeferred?.Body.ToString() != deferredBody ||
    deadLetteredDeferred.DeadLetterReason != deferredDeadLetterReason ||
    deadLetteredDeferred.DeadLetterErrorDescription != deferredDeadLetterDescription ||
    deadLetteredDeferred.DeadLetterSource != queue)
{
    Console.Error.WriteLine(
        $"deferred dead-letter mismatch: body={deadLetteredDeferred?.Body}, " +
        $"reason={deadLetteredDeferred?.DeadLetterReason}, " +
        $"description={deadLetteredDeferred?.DeadLetterErrorDescription}, " +
        $"source={deadLetteredDeferred?.DeadLetterSource}");
    return 48;
}
if (!Equals(deadLetteredDeferred.ApplicationProperties["defer-stage"], "deadlettered") ||
    Convert.ToInt32(deadLetteredDeferred.ApplicationProperties["abandon-attempt"]) != 3 ||
    !Equals(deadLetteredDeferred.ApplicationProperties["defer-preserved"], "original"))
{
    Console.Error.WriteLine("deferred dead-letter property updates were not durable");
    return 49;
}
await deadLetterReceiver.CompleteMessageAsync(deadLetteredDeferred);

await using ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-session-current")
{
    MessageId = "official-session-current",
    SessionId = "session-1",
});
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-session-other")
{
    MessageId = "official-session-other",
    SessionId = "session-2",
});

await using (ServiceBusReceiver crossSessionPeek = client.CreateReceiver(sessionQueue))
{
    IReadOnlyList<ServiceBusReceivedMessage> sessionMessages =
        await crossSessionPeek.PeekMessagesAsync(2);
    if (sessionMessages.Count != 2 ||
        sessionMessages[0].Body.ToString() != "official-session-current" ||
        sessionMessages[0].SessionId != "session-1" ||
        sessionMessages[1].Body.ToString() != "official-session-other" ||
        sessionMessages[1].SessionId != "session-2")
    {
        Console.Error.WriteLine("regular peek did not browse across all queue sessions");
        return 59;
    }
}

await using ServiceBusSessionReceiver sessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "session-1");

IReadOnlyList<ServiceBusReceivedMessage> sessionPeek =
    await sessionReceiver.PeekMessagesAsync(2);
if (sessionPeek.Count != 1 ||
    sessionPeek[0].Body.ToString() != "official-session-current" ||
    sessionPeek[0].SessionId != "session-1")
{
    Console.Error.WriteLine("session peek escaped the session whose lock is held");
    return 60;
}

await sessionReceiver.SetSessionStateAsync(BinaryData.FromString("checkout-step-2"));
BinaryData sessionState = await sessionReceiver.GetSessionStateAsync();
if (sessionState.ToString() != "checkout-step-2")
{
    Console.Error.WriteLine($"unexpected session state: {sessionState}");
    return 15;
}

DateTimeOffset sessionLockedUntilBeforeRenewal = sessionReceiver.SessionLockedUntil;
await sessionReceiver.RenewSessionLockAsync();
if (sessionReceiver.SessionLockedUntil < sessionLockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"session renewal moved the lock backward: {sessionLockedUntilBeforeRenewal:o} -> {sessionReceiver.SessionLockedUntil:o}");
    return 16;
}

ServiceBusReceivedMessage? sessionMessage =
    await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (sessionMessage?.Body.ToString() != "official-session-current")
{
    Console.Error.WriteLine($"unexpected session message: {sessionMessage?.Body}");
    return 17;
}
await sessionReceiver.CompleteMessageAsync(sessionMessage);

await using ServiceBusSessionReceiver otherSessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "session-2");
ServiceBusReceivedMessage? otherSessionMessage =
    await otherSessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (otherSessionMessage?.Body.ToString() != "official-session-other")
{
    Console.Error.WriteLine($"unexpected second session message: {otherSessionMessage?.Body}");
    return 61;
}
await otherSessionReceiver.CompleteMessageAsync(otherSessionMessage);

Console.WriteLine(
    "official .NET Service Bus client batch send/prefetch/concurrent settlement, " +
    "envelope fidelity, send/receive/renew/complete, " +
    "abandon/redelivery/property-update, dead-letter/DLQ receive/complete, " +
    "defer/deferred-receive/management-disposition, peek/browse pagination, " +
    "schedule/cancel/timer activation, and session renew/state/peek passed");
return 0;
