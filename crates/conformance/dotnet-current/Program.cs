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
IReadOnlyList<ServiceBusReceivedMessage> peeked =
    await receiver.PeekMessagesAsync(maxMessages: 1);
if (peeked.Count != 1 || peeked[0].Body.ToString() != "official-dotnet-current")
{
    Console.Error.WriteLine(
        $"unexpected peek result: count={peeked.Count}, body={peeked.FirstOrDefault()?.Body}");
    return 3;
}

ServiceBusReceivedMessage? received =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (received is null)
{
    Console.Error.WriteLine("the official client did not receive its message");
    return 4;
}
if (received.Body.ToString() != "official-dotnet-current")
{
    Console.Error.WriteLine($"unexpected body: {received.Body}");
    return 5;
}

DateTimeOffset lockedUntilBeforeRenewal = received.LockedUntil;
await receiver.RenewMessageLockAsync(received);
if (received.LockedUntil < lockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"renewal moved the lock backward: {lockedUntilBeforeRenewal:o} -> {received.LockedUntil:o}");
    return 6;
}

await receiver.CompleteMessageAsync(received);

await using ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-session-current")
{
    SessionId = "session-1",
});
await using ServiceBusSessionReceiver sessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "session-1");

IReadOnlyList<ServiceBusReceivedMessage> peekedSession =
    await sessionReceiver.PeekMessagesAsync(maxMessages: 1);
if (peekedSession.Count != 1 || peekedSession[0].Body.ToString() != "official-session-current")
{
    Console.Error.WriteLine(
        $"unexpected session peek result: count={peekedSession.Count}, body={peekedSession.FirstOrDefault()?.Body}");
    return 7;
}

await sessionReceiver.SetSessionStateAsync(BinaryData.FromString("checkout-step-2"));
BinaryData sessionState = await sessionReceiver.GetSessionStateAsync();
if (sessionState.ToString() != "checkout-step-2")
{
    Console.Error.WriteLine($"unexpected session state: {sessionState}");
    return 8;
}

DateTimeOffset sessionLockedUntilBeforeRenewal = sessionReceiver.SessionLockedUntil;
await sessionReceiver.RenewSessionLockAsync();
if (sessionReceiver.SessionLockedUntil < sessionLockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"session renewal moved the lock backward: {sessionLockedUntilBeforeRenewal:o} -> {sessionReceiver.SessionLockedUntil:o}");
    return 9;
}

ServiceBusReceivedMessage? sessionMessage =
    await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (sessionMessage?.Body.ToString() != "official-session-current")
{
    Console.Error.WriteLine($"unexpected session message: {sessionMessage?.Body}");
    return 10;
}
await sessionReceiver.CompleteMessageAsync(sessionMessage);

Console.WriteLine(
    "official .NET Service Bus client send/peek/receive/renew/complete and session renew/state passed");
return 0;
