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

Console.WriteLine(
    "official .NET Service Bus client AMQP-over-WebSockets send/receive/complete " +
    "and session attach passed");
return 0;
