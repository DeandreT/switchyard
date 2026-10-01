using System.Security.Authentication;
using Azure;
using Azure.Messaging.ServiceBus;

internal static class WebSocketCases
{
    internal static async Task<int> RunAsync(string[] args)
    {
        var endpoint = new Uri(args[2]);
        Require(endpoint.Scheme == "wss", "The public Service Bus client requires secure WebSockets.");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var options = new ServiceBusClientOptions
        {
            CustomEndpointAddress = endpoint,
            TransportType = ServiceBusTransportType.AmqpWebSockets,
            RetryOptions =
            {
                MaxRetries = 0,
                TryTimeout = TimeSpan.FromSeconds(8),
            },
        };
        var client = new ServiceBusClient(args[1], new AzureNamedKeyCredential(args[6], args[7]), options);
        var endpoints = new List<IAsyncDisposable>();
        try
        {
            ServiceBusSender sender = client.CreateSender(args[3]);
            endpoints.Add(sender);
            if (args[0] == "websocket-untrusted")
            {
                try
                {
                    await sender.SendMessageAsync(new ServiceBusMessage("must-not-arrive"), deadline.Token);
                    throw new InvalidOperationException("WSS accepted an untrusted certificate.");
                }
                catch (Exception error) when (ContainsCertificateFailure(error))
                {
                    Console.WriteLine("official .NET WSS untrusted certificate rejected passed");
                    return 0;
                }
            }

            ServiceBusReceiver receiver = client.CreateReceiver(args[3]);
            endpoints.Add(receiver);
            byte[] body = Enumerable.Range(0, 20_000).Select(index => (byte)(index % 251)).ToArray();
            var original = new ServiceBusMessage(new BinaryData(body))
            {
                MessageId = "websocket-preserved-id",
                CorrelationId = "websocket-correlation",
                Subject = "websocket-subject",
                ContentType = "application/octet-stream",
            };
            original.ApplicationProperties["transport-number"] = 42L;
            await sender.SendMessageAsync(original, deadline.Token);
            IReadOnlyList<ServiceBusReceivedMessage> peeked =
                await receiver.PeekMessagesAsync(1, cancellationToken: deadline.Token);
            Require(peeked.Count == 1 && peeked[0].Body.ToArray().SequenceEqual(body), "Queue peek fidelity.");
            ServiceBusReceivedMessage received = await ReceiveAsync(receiver, deadline.Token);
            Require(received.Body.ToArray().SequenceEqual(body)
                && received.MessageId == original.MessageId
                && received.CorrelationId == original.CorrelationId
                && received.Subject == original.Subject
                && received.ContentType == original.ContentType
                && received.ApplicationProperties["transport-number"] is long number && number == 42,
                "Queue receive fidelity.");
            DateTimeOffset lockedUntil = received.LockedUntil;
            await receiver.RenewMessageLockAsync(received, deadline.Token);
            Require(received.LockedUntil >= lockedUntil, "Queue renewal moved backward.");
            await receiver.AbandonMessageAsync(received, cancellationToken: deadline.Token);
            received = await ReceiveAsync(receiver, deadline.Token);
            Require(received.DeliveryCount == 2 && received.Body.ToArray().SequenceEqual(body), "Queue redelivery.");
            await receiver.CompleteMessageAsync(received, deadline.Token);

            ServiceBusSender sessionSender = client.CreateSender(args[4]);
            endpoints.Add(sessionSender);
            await sessionSender.SendMessagesAsync(new[]
            {
                new ServiceBusMessage("websocket-session-1") { SessionId = "websocket-A" },
                new ServiceBusMessage("websocket-session-2") { SessionId = "websocket-A" },
            }, deadline.Token);
            ServiceBusSessionReceiver session = await client.AcceptSessionAsync(
                args[4], "websocket-A", cancellationToken: deadline.Token);
            endpoints.Add(session);
            await session.SetSessionStateAsync(new BinaryData("websocket-state"), deadline.Token);
            BinaryData? state = await session.GetSessionStateAsync(deadline.Token);
            Require(state?.ToString() == "websocket-state", "Session state.");
            await session.RenewSessionLockAsync(deadline.Token);
            foreach (string expected in new[] { "websocket-session-1", "websocket-session-2" })
            {
                ServiceBusReceivedMessage delivery = await ReceiveAsync(session, deadline.Token);
                Require(delivery.SessionId == "websocket-A" && delivery.Body.ToString() == expected,
                    "Session FIFO.");
                await session.CompleteMessageAsync(delivery, deadline.Token);
            }
            await session.SetSessionStateAsync(null, deadline.Token);

            ServiceBusSender topicSender = client.CreateSender(args[5]);
            endpoints.Add(topicSender);
            ServiceBusReceiver alpha = client.CreateReceiver(args[5], "Alpha");
            ServiceBusReceiver beta = client.CreateReceiver(args[5], "beta");
            endpoints.Add(alpha);
            endpoints.Add(beta);
            await topicSender.SendMessageAsync(new ServiceBusMessage("websocket-topic-copy")
            {
                MessageId = "websocket-topic-id",
                SessionId = "preserved-ordinary-session",
            }, deadline.Token);
            ServiceBusReceivedMessage alphaCopy = await ReceiveAsync(alpha, deadline.Token);
            ServiceBusReceivedMessage betaCopy = await ReceiveAsync(beta, deadline.Token);
            Require(alphaCopy.Body.ToString() == "websocket-topic-copy"
                && betaCopy.Body.ToString() == "websocket-topic-copy"
                && alphaCopy.SequenceNumber == betaCopy.SequenceNumber
                && alphaCopy.SessionId == "preserved-ordinary-session"
                && betaCopy.SessionId == alphaCopy.SessionId, "Topic independent copies.");
            await alpha.CompleteMessageAsync(alphaCopy, deadline.Token);
            await beta.CompleteMessageAsync(betaCopy, deadline.Token);
            Console.WriteLine("official .NET WSS CBS/queue/peek/renew/redelivery/session/state/FIFO/topic passed");
            return 0;
        }
        finally
        {
            for (int index = endpoints.Count - 1; index >= 0; index--)
            {
                await CleanupAsync(endpoints[index]);
            }
            await CleanupAsync(client);
        }
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), token);
        return message ?? throw new InvalidOperationException("WebSocket receive returned no message.");
    }

    private static bool ContainsCertificateFailure(Exception error)
    {
        if (error is AuthenticationException)
        {
            return true;
        }
        if (error is AggregateException aggregate)
        {
            return aggregate.InnerExceptions.Any(ContainsCertificateFailure);
        }
        return error.InnerException is not null && ContainsCertificateFailure(error.InnerException);
    }

    private static async Task CleanupAsync(IAsyncDisposable endpoint)
    {
        try
        {
            await endpoint.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            // Broker postconditions separately verify committed release and removal.
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
