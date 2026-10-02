using Azure;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp;
using System.Transactions;

internal static partial class AtomicMessagingCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ScopeTimeout = TimeSpan.FromSeconds(30);
    private const string DisabledDescription = "native transactional ingress is disabled";
    private const string Success =
        "official .NET warmed same-queue transaction batch/rollback/complete/rearm/default refusal passed";

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 9)
        {
            Console.Error.WriteLine(
                "usage: atomic-messaging <namespace> <atomic-endpoint> <ordinary-endpoint> <send-queue> <held-queue> <ordinary-control-queue> <rule> <key>");
            return 2;
        }

        var endpoints = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            var credential = new AzureNamedKeyCredential(args[7], args[8]);
            ServiceBusClient atomic = Keep(
                endpoints,
                Client(args[1], args[2], credential));
            ServiceBusClient ordinary = Keep(
                endpoints,
                Client(args[1], args[3], credential));
            ServiceBusSender sends = Keep(endpoints, atomic.CreateSender(args[4]));
            ServiceBusSender heldSends = Keep(endpoints, atomic.CreateSender(args[5]));
            ServiceBusReceiver held = Keep(endpoints, atomic.CreateReceiver(args[5], ReceiverOptions()));
            ServiceBusReceiver sendPeek = Keep(endpoints, ordinary.CreateReceiver(args[4], ReceiverOptions()));
            ServiceBusReceiver heldPeek = Keep(endpoints, ordinary.CreateReceiver(args[5], ReceiverOptions()));
            ServiceBusSender controlSends = Keep(endpoints, ordinary.CreateSender(args[6]));
            ServiceBusReceiver control = Keep(endpoints, ordinary.CreateReceiver(args[6], ReceiverOptions()));

            await SendBatchesAsync(sends, sendPeek);
            await CompleteAndSendAsync(heldSends, held, heldPeek);
            await OrdinaryRefusalAsync(controlSends, control);
            await ColdSendsAsync(args[1], args[2], args[4], credential, sendPeek);
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            for (int index = endpoints.Count - 1; index >= 0; index--)
            {
                try
                {
                    await endpoints[index].DisposeAsync().AsTask().WaitAsync(OperationTimeout);
                }
                catch (Exception exception)
                {
                    failure ??= exception;
                }
            }
        }

        if (failure is not null)
        {
            Console.Error.WriteLine(
                $"atomic messaging gate failed at {failure.Data["atomic-messaging-stage"] ?? "scope or endpoint disposal"}: {failure}");
            return 1;
        }

        Console.WriteLine(Success);
        Console.WriteLine(ColdSuccess);
        return 0;
    }

    private static ServiceBusClient Client(
        string host,
        string endpoint,
        AzureNamedKeyCredential credential) =>
        new(host, credential, new ServiceBusClientOptions
        {
            CustomEndpointAddress = new Uri(endpoint),
            TransportType = ServiceBusTransportType.AmqpTcp,
            EnableCrossEntityTransactions = false,
            RetryOptions =
            {
                MaxRetries = 0,
                TryTimeout = OperationTimeout,
            },
        });

    private static ServiceBusReceiverOptions ReceiverOptions() => new()
    {
        ReceiveMode = ServiceBusReceiveMode.PeekLock,
        PrefetchCount = 0,
    };

    private static T Keep<T>(List<IAsyncDisposable> endpoints, T endpoint)
        where T : IAsyncDisposable
    {
        endpoints.Add(endpoint);
        return endpoint;
    }

    private static TransactionScope Scope() => new(
        TransactionScopeOption.RequiresNew,
        new TransactionOptions
        {
            IsolationLevel = IsolationLevel.Serializable,
            Timeout = ScopeTimeout,
        },
        TransactionScopeAsyncFlowOption.Enabled);

    private static ServiceBusMessage Message(string id, string body, string phase)
    {
        var message = new ServiceBusMessage(body)
        {
            MessageId = id,
            CorrelationId = id + "-correlation",
            Subject = "atomic-sdk",
            ContentType = "text/plain",
        };
        message.ApplicationProperties["phase"] = phase;
        message.ApplicationProperties["number"] = 42L;
        message.ApplicationProperties["enabled"] = true;
        return message;
    }

    private static async Task<ServiceBusMessageBatch> BatchAsync(
        ServiceBusSender sender,
        string phase)
    {
        // Opening the batch first supplies Send authorization before the coordinator is opened.
        ServiceBusMessageBatch batch = await OperationAsync(
            $"create {phase} batch",
            cancellation => sender.CreateMessageBatchAsync(cancellation).AsTask());
        try
        {
            foreach (string suffix in new[] { "a", "b" })
            {
                Require(
                    batch.TryAddMessage(Message($"atomic-send-{phase}-{suffix}", $"send-{phase}-{suffix}", phase)),
                    "a tiny transaction message did not fit the SDK batch");
            }
            Require(batch.Count == 2, "the SDK transaction must contain a true two-member batch");
            return batch;
        }
        catch
        {
            batch.Dispose();
            throw;
        }
    }

    private static async Task SendBatchesAsync(ServiceBusSender sender, ServiceBusReceiver peek)
    {
        using (ServiceBusMessageBatch rollback = await BatchAsync(sender, "rollback"))
        {
            using (Scope())
            {
                await OperationAsync("send rollback batch", cancellation => sender.SendMessagesAsync(rollback, cancellation));
            }
        }
        await AssertContentsAsync(peek, Array.Empty<(string, string)>());

        using (ServiceBusMessageBatch commit = await BatchAsync(sender, "commit"))
        {
            using (TransactionScope scope = Scope())
            {
                await OperationAsync("send commit batch", cancellation => sender.SendMessagesAsync(commit, cancellation));
                scope.Complete();
            }
        }
        await AssertContentsAsync(peek, new[]
        {
            ("atomic-send-commit-a", "send-commit-a"),
            ("atomic-send-commit-b", "send-commit-b"),
        });
    }

    private static async Task CompleteAndSendAsync(
        ServiceBusSender sender,
        ServiceBusReceiver receiver,
        ServiceBusReceiver peek)
    {
        await OperationAsync("seed held original", cancellation => sender.SendMessageAsync(
            Message("atomic-held-original", "held-original", "seed"), cancellation));
        ServiceBusReceivedMessage original = await ReceiveAsync(receiver, "held original");
        Require(original.MessageId == "atomic-held-original" && original.Body.ToString() == "held-original",
            "the experimental receiver did not return the seeded original");
        Require(original.DeliveryCount == 1, "the original must be acquired exactly once");
        string lockToken = original.LockToken;
        long sequence = original.SequenceNumber;
        Require(sequence > 0 && Guid.TryParse(lockToken, out Guid token) && token != Guid.Empty,
            "the held message lacks its real sequence or lock token");

        using (Scope())
        {
            await OperationAsync("rollback Complete", cancellation => receiver.CompleteMessageAsync(original, cancellation));
            await OperationAsync("rollback replacement send", cancellation => sender.SendMessageAsync(
                Message("atomic-held-rollback", "held-rollback", "rollback"), cancellation));
        }
        IReadOnlyList<ServiceBusReceivedMessage> retained = await AssertContentsAsync(peek, new[]
        {
            ("atomic-held-original", "held-original"),
        });
        Require(retained[0].SequenceNumber == sequence, "rollback replaced the canonical original");
        Require(original.SequenceNumber == sequence && original.LockToken == lockToken && original.DeliveryCount == 1,
            "rollback changed the same received object's acquisition identity");

        // The same object and receiver must still address the original live delivery after rollback.
        using (TransactionScope scope = Scope())
        {
            await OperationAsync("committed replacement send", cancellation => sender.SendMessageAsync(
                Message("atomic-held-commit", "held-commit", "commit"), cancellation));
            await OperationAsync("committed same-original Complete", cancellation => receiver.CompleteMessageAsync(original, cancellation));
            scope.Complete();
        }
        await AssertContentsAsync(peek, new[]
        {
            ("atomic-held-commit", "held-commit"),
        });
    }

    private static async Task OrdinaryRefusalAsync(ServiceBusSender sender, ServiceBusReceiver receiver)
    {
        await OperationAsync("ordinary warm send", cancellation => sender.SendMessageAsync(
            Message("atomic-ordinary-warm", "ordinary-warm", "warm"), cancellation));
        ServiceBusReceivedMessage warm = await ReceiveAsync(receiver, "ordinary warm message");
        Require(warm.MessageId == "atomic-ordinary-warm", "ordinary warm receive returned a different message");
        await OperationAsync("ordinary warm Complete", cancellation => receiver.CompleteMessageAsync(warm, cancellation));
        await AssertContentsAsync(receiver, Array.Empty<(string, string)>());

        bool refused = false;
        using (Scope())
        {
            using var trace = new AtomicMessagingRefusalTrace(DisabledDescription);
            try
            {
                await OperationAsync("ordinary transactional refusal", cancellation => sender.SendMessageAsync(
                    Message("atomic-ordinary-forbidden", "ordinary-forbidden", "forbidden"), cancellation));
            }
            catch (NotSupportedException exception) when (
                exception.Message.StartsWith(DisabledDescription, StringComparison.Ordinal))
            {
                refused = true;
            }
            catch (AmqpException exception) when (
                exception.Error.Condition.Value == "amqp:not-implemented")
            {
                refused = true;
            }
            catch (ServiceBusException exception) when (
                trace.ProofCount == 1 && AtomicMessagingRefusalTrace.IsTranslatedAbort(exception))
            {
                refused = true;
            }
        }
        Require(refused, "the ordinary endpoint did not explicitly refuse transactional Send");
        await AssertContentsAsync(receiver, Array.Empty<(string, string)>());

        await OperationAsync("ordinary healthy send after refusal", cancellation => sender.SendMessageAsync(
            Message("atomic-ordinary-healthy", "ordinary-healthy", "ordinary"), cancellation));
        ServiceBusReceivedMessage healthy = await ReceiveAsync(receiver, "ordinary healthy message");
        Require(healthy.MessageId == "atomic-ordinary-healthy" && healthy.Body.ToString() == "ordinary-healthy",
            "the ordinary endpoint was not usable after scoped transaction refusal");
        await OperationAsync("ordinary healthy Complete", cancellation => receiver.CompleteMessageAsync(healthy, cancellation));
        await AssertContentsAsync(receiver, Array.Empty<(string, string)>());
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, string stage)
    {
        Require(Transaction.Current is null, "acquisition must happen outside any transaction scope");
        ServiceBusReceivedMessage? message = await OperationAsync(stage,
            cancellation => receiver.ReceiveMessageAsync(OperationTimeout, cancellation));
        return message ?? throw new InvalidOperationException($"{stage} returned no message");
    }

    private static async Task<IReadOnlyList<ServiceBusReceivedMessage>> AssertContentsAsync(
        ServiceBusReceiver receiver,
        IReadOnlyList<(string Id, string Body)> expected)
    {
        Require(Transaction.Current is null, "management verification must happen outside any transaction scope");
        IReadOnlyList<ServiceBusReceivedMessage> messages = await OperationAsync("ordinary Peek verification",
            cancellation => receiver.PeekMessagesAsync(16, fromSequenceNumber: 1, cancellationToken: cancellation));
        Require(messages.Count == expected.Count, $"unexpected retained count: {messages.Count}, expected {expected.Count}");
        for (int index = 0; index < expected.Count; index++)
        {
            Require(messages[index].MessageId == expected[index].Id && messages[index].Body.ToString() == expected[index].Body,
                $"unexpected retained message at index {index}");
        }
        return messages;
    }

    private static async Task OperationAsync(string stage, Func<CancellationToken, Task> operation)
    {
        using var cancellation = new CancellationTokenSource(OperationTimeout);
        try
        {
            await operation(cancellation.Token).WaitAsync(OperationTimeout);
        }
        catch (Exception exception)
        {
            exception.Data["atomic-messaging-stage"] = stage;
            throw;
        }
    }

    private static async Task<T> OperationAsync<T>(string stage, Func<CancellationToken, Task<T>> operation)
    {
        using var cancellation = new CancellationTokenSource(OperationTimeout);
        try
        {
            return await operation(cancellation.Token).WaitAsync(OperationTimeout);
        }
        catch (Exception exception)
        {
            exception.Data["atomic-messaging-stage"] = stage;
            throw;
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
