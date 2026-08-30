using Azure;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;

if (args.Length != 6)
{
    Console.Error.WriteLine(
        "usage: <namespace> <custom-endpoint> <queue> <session-queue> <key-name> <key>");
    return 2;
}

string fullyQualifiedNamespace = args[0];
var customEndpoint = new Uri(args[1]);
string queue = args[2];
string sessionQueue = args[3];
string keyName = args[4];
string key = args[5];

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

await receiver.AbandonMessageAsync(abandoned);
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

const string deadLetterReason = "SchemaMismatch";
const string deadLetterDescription = "official .NET validation failed";
await receiver.DeadLetterMessageAsync(
    redelivered,
    deadLetterReason,
    deadLetterDescription);

await using ServiceBusReceiver deadLetterReceiver = client.CreateReceiver(
    queue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
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
await deadLetterReceiver.CompleteMessageAsync(deadLettered);

await using ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-session-current")
{
    SessionId = "session-1",
});
await using ServiceBusSessionReceiver sessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "session-1");

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

Console.WriteLine(
    "official .NET Service Bus client envelope fidelity, send/receive/renew/complete, " +
    "abandon/redelivery, dead-letter/DLQ receive/complete, and session renew/state passed");
return 0;
