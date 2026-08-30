using Azure;
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
    "official .NET Service Bus client send/receive/renew/complete, abandon/redelivery, " +
    "dead-letter/DLQ receive/complete, and session renew/state passed");
return 0;
