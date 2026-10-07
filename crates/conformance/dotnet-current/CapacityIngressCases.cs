using Azure;
using Azure.Messaging.ServiceBus;

internal static class CapacityIngressCases
{
    private const int CreditBytes = 8 * 1024;
    private const int RetainedBytes = 20 * 1024;
    private const int NegotiatedOversizeBytes = 300 * 1024;
    private const int SmallBytes = 512;

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 8 || args[0] != "capacity-ingress"
            || args[1] is not ("seed" or "quota" or "abandon" or "complete" or "retry"
                or "size-seed" or "broker-size" or "negotiated-size" or "small" or "drain-size")
            || args[5] is not ("named" or "connection")
            || Uri.CheckHostName(args[2]) != UriHostNameType.Dns
            || !Uri.TryCreate(args[3], UriKind.Absolute, out Uri? endpoint)
            || endpoint.Scheme != "wss"
            || !string.Equals(endpoint.Host, "localhost", StringComparison.OrdinalIgnoreCase)
            || endpoint.Port <= 0 || endpoint.UserInfo.Length != 0
            || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0
            || string.IsNullOrWhiteSpace(args[4])
            || string.IsNullOrWhiteSpace(args[6]) || string.IsNullOrWhiteSpace(args[7])
            || args[6].IndexOfAny(new[] { ';', '\r', '\n' }) >= 0
            || args[7].IndexOfAny(new[] { ';', '\r', '\n' }) >= 0)
        {
            Console.Error.WriteLine("capacity ingress arguments rejected");
            return 2;
        }

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        ServiceBusClient? client = null;
        var endpoints = new List<(IAsyncDisposable Endpoint, string Role)>();
        var cleanupFailures = new List<(string Role, Exception Error)>();
        Exception? primaryFailure = null;
        try
        {
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
            client = args[5] == "named"
                ? new ServiceBusClient(args[2], new AzureNamedKeyCredential(args[6], args[7]), options)
                : new ServiceBusClient(
                    $"Endpoint=sb://{args[2]}/;SharedAccessKeyName={args[6]};SharedAccessKey={args[7]}",
                    options);
            AtomAdministrationCases.EmitLoadedAssemblyEvidence();
            await RunStageAsync(client, args[1], args[4], endpoints, deadline.Token);
        }
        catch (Exception error)
        {
            primaryFailure = error;
        }
        finally
        {
            for (int index = endpoints.Count - 1; index >= 0; index--)
            {
                await DisposeAsync(endpoints[index].Endpoint, endpoints[index].Role, cleanupFailures);
            }
            if (client is not null)
            {
                await DisposeAsync(client, "client", cleanupFailures);
            }
        }

        if (primaryFailure is not null)
        {
            Console.Error.WriteLine($"capacity ingress primary failure class={Classify(primaryFailure)}");
        }
        foreach (var failure in cleanupFailures)
        {
            Console.Error.WriteLine(
                $"capacity ingress cleanup failure role={failure.Role} class={Classify(failure.Error)}");
        }
        if (primaryFailure is not null || cleanupFailures.Count != 0)
        {
            return primaryFailure is not null ? 3 : 4;
        }

        Console.WriteLine($"official .NET capacity ingress {args[1]} {args[5]} passed");
        return 0;
    }

    private static async Task RunStageAsync(
        ServiceBusClient client, string stage, string queue,
        List<(IAsyncDisposable Endpoint, string Role)> endpoints, CancellationToken token)
    {
        if (stage is "abandon" or "complete" or "drain-size")
        {
            ServiceBusReceiver receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = 0,
            });
            endpoints.Add((receiver, "receiver"));
            if (stage == "drain-size")
            {
                await DrainSizeAsync(receiver, token);
                return;
            }

            ServiceBusReceivedMessage delivery = await ReceiveAsync(receiver, token);
            RequireMessage(delivery, "capacity-credit", CreditBytes);
            if (stage == "abandon")
            {
                await receiver.AbandonMessageAsync(delivery, cancellationToken: token);
            }
            else
            {
                await receiver.CompleteMessageAsync(delivery, token);
            }
            return;
        }

        ServiceBusSender sender = client.CreateSender(queue);
        endpoints.Add((sender, "sender"));
        switch (stage)
        {
            case "seed":
            case "retry":
                await sender.SendMessageAsync(CreateMessage("capacity-credit", CreditBytes), token);
                break;
            case "quota":
                await ExpectFailureAsync(
                    () => sender.SendMessageAsync(CreateMessage("capacity-credit", CreditBytes), token),
                    ServiceBusFailureReason.QuotaExceeded);
                break;
            case "size-seed":
                await sender.SendMessageAsync(CreateMessage("capacity-retained", RetainedBytes), token);
                break;
            case "broker-size":
                await ExpectFailureAsync(
                    () => sender.SendMessageAsync(CreateMessage("capacity-retained", RetainedBytes), token),
                    ServiceBusFailureReason.MessageSizeExceeded);
                break;
            case "negotiated-size":
                await ExpectFailureAsync(
                    () => sender.SendMessageAsync(
                        CreateMessage("capacity-retained", NegotiatedOversizeBytes), token),
                    ServiceBusFailureReason.MessageSizeExceeded);
                break;
            case "small":
                await sender.SendMessageAsync(CreateMessage("capacity-small", SmallBytes), token);
                break;
            default:
                throw new InvalidOperationException("Capacity ingress stage unavailable.");
        }
    }

    private static ServiceBusMessage CreateMessage(string id, int length)
    {
        var message = new ServiceBusMessage(new BinaryData(CreateBody(length)))
        {
            MessageId = id,
            Subject = id,
            ContentType = "application/octet-stream",
        };
        message.ApplicationProperties["capacity-source"] = "official-sdk";
        return message;
    }

    private static byte[] CreateBody(int length)
    {
        var body = new byte[length];
        for (int index = 0; index < body.Length; index++)
        {
            body[index] = (byte)(index % 251);
        }
        return body;
    }

    private static void RequireMessage(ServiceBusReceivedMessage message, string id, int length)
    {
        Require(message.MessageId == id && message.Subject == id
            && message.ContentType == "application/octet-stream"
            && message.ApplicationProperties.TryGetValue("capacity-source", out object? source)
            && source is string text && text == "official-sdk"
            && message.Body.ToArray().SequenceEqual(CreateBody(length)));
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? message =
            await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), token);
        return message ?? throw new InvalidOperationException("Capacity ingress receive returned no message.");
    }

    private static async Task DrainSizeAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (int index = 0; index < 2; index++)
        {
            ServiceBusReceivedMessage message = await ReceiveAsync(receiver, token);
            int length = message.MessageId switch
            {
                "capacity-retained" => RetainedBytes,
                "capacity-small" => SmallBytes,
                _ => throw new InvalidOperationException("Capacity ingress drain identity unavailable."),
            };
            RequireMessage(message, message.MessageId, length);
            Require(seen.Add(message.MessageId));
            await receiver.CompleteMessageAsync(message, token);
        }
        Require(seen.SetEquals(new[] { "capacity-retained", "capacity-small" }));
    }

    private static async Task ExpectFailureAsync(Func<Task> operation, ServiceBusFailureReason reason)
    {
        try
        {
            await operation();
        }
        catch (ServiceBusException error) when (error.Reason == reason)
        {
            return;
        }
        throw new InvalidOperationException("Capacity ingress expected refusal unavailable.");
    }

    private static async Task DisposeAsync(
        IAsyncDisposable endpoint, string role, List<(string Role, Exception Error)> failures)
    {
        try
        {
            await endpoint.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception error)
        {
            failures.Add((role, error));
        }
    }

    private static string Classify(Exception error) => error switch
    {
        OperationCanceledException => "cancelled",
        TimeoutException => "timeout",
        ServiceBusException serviceBus => serviceBus.Reason switch
        {
            ServiceBusFailureReason.QuotaExceeded => "quota-exceeded",
            ServiceBusFailureReason.MessageSizeExceeded => "message-size-exceeded",
            _ => "unexpected-service-bus",
        },
        InvalidOperationException => "verification",
        _ => "unexpected",
    };

    private static void Require(bool condition)
    {
        if (!condition)
        {
            throw new InvalidOperationException("Capacity ingress message fidelity failed.");
        }
    }
}
