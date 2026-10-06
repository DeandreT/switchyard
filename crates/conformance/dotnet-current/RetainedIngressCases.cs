using Azure;
using Azure.Messaging.ServiceBus;
using System.Transactions;

internal static class RetainedIngressCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ScopeTimeout = TimeSpan.FromSeconds(30);
    private const string Success =
        "official .NET retained Memory warmed immediate-send rollback/commit passed";

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 6)
        {
            Console.Error.WriteLine(
                "usage: retained-ingress <namespace> <endpoint> <queue> <rule> <key>");
            return 2;
        }

        var endpoints = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            var client = new ServiceBusClient(
                args[1], new AzureNamedKeyCredential(args[4], args[5]),
                new ServiceBusClientOptions
                {
                    CustomEndpointAddress = new Uri(args[2]),
                    TransportType = ServiceBusTransportType.AmqpTcp,
                    EnableCrossEntityTransactions = false,
                    RetryOptions =
                    {
                        MaxRetries = 0,
                        TryTimeout = OperationTimeout,
                    },
                });
            endpoints.Add(client);
            ServiceBusSender sender = client.CreateSender(args[3]);
            endpoints.Add(sender);

            if (Transaction.Current is not null)
            {
                throw new InvalidOperationException("warmup must be outside an ambient scope");
            }
            await SendAsync(sender, Message("retained-warm", "warm", "warm"));

            using (Scope())
            {
                await SendAsync(sender, Message("retained-rollback-a", "rollback-a", "rollback"));
                await SendAsync(sender, Message("retained-rollback-b", "rollback-b", "rollback"));
            }
            using (TransactionScope scope = Scope())
            {
                await SendAsync(sender, Message("retained-commit-a", "commit-a", "commit"));
                await SendAsync(sender, Message("retained-commit-b", "commit-b", "commit"));
                scope.Complete();
            }
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
            Console.Error.WriteLine("retained SDK workflow or endpoint disposal failed");
            return 1;
        }
        Console.WriteLine(Success);
        return 0;
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
            Subject = "retained-sdk",
            ContentType = "text/plain",
        };
        message.ApplicationProperties["phase"] = phase;
        message.ApplicationProperties["number"] = 42L;
        message.ApplicationProperties["enabled"] = true;
        return message;
    }

    private static async Task SendAsync(ServiceBusSender sender, ServiceBusMessage message)
    {
        using var cancellation = new CancellationTokenSource(OperationTimeout);
        await sender.SendMessageAsync(message, cancellation.Token).WaitAsync(OperationTimeout);
    }
}
