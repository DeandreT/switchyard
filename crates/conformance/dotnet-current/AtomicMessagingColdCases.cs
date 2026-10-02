using Azure;
using Azure.Messaging.ServiceBus;
using System.Runtime.ExceptionServices;
using System.Transactions;

internal static partial class AtomicMessagingCases
{
    private const string ColdSuccess =
        "official .NET cold-first same-queue transaction rollback/commit passed";

    private static async Task ColdSendsAsync(
        string host,
        string endpoint,
        string queue,
        AzureNamedKeyCredential credential,
        ServiceBusReceiver peek)
    {
        await ColdSendAsync(host, endpoint, queue, credential, commit: false);
        await AssertContentsAsync(peek, new[]
        {
            ("atomic-send-commit-a", "send-commit-a"),
            ("atomic-send-commit-b", "send-commit-b"),
        });

        await ColdSendAsync(host, endpoint, queue, credential, commit: true);
        await AssertContentsAsync(peek, new[]
        {
            ("atomic-send-commit-a", "send-commit-a"),
            ("atomic-send-commit-b", "send-commit-b"),
            ("atomic-cold-send-commit-a", "cold-send-commit-a"),
            ("atomic-cold-send-commit-b", "cold-send-commit-b"),
        });
    }

    private static async Task ColdSendAsync(
        string host,
        string endpoint,
        string queue,
        AzureNamedKeyCredential credential,
        bool commit)
    {
        Require(Transaction.Current is null, "the fresh client must be created outside any transaction scope");
        string phase = commit ? "commit" : "rollback";
        var endpoints = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            ServiceBusClient client = Keep(endpoints, Client(host, endpoint, credential));
            ServiceBusSender sender = Keep(endpoints, client.CreateSender(queue));
            IEnumerable<ServiceBusMessage> messages = new[]
            {
                Message($"atomic-cold-send-{phase}-a", $"cold-send-{phase}-a", phase),
                Message($"atomic-cold-send-{phase}-b", $"cold-send-{phase}-b", phase),
            };

            // The first network operation on this client enlists before its producer link is opened.
            using (TransactionScope scope = Scope())
            {
                await OperationAsync($"cold-first {phase} enumerable send",
                    cancellation => sender.SendMessagesAsync(messages, cancellation));
                if (commit)
                {
                    scope.Complete();
                }
            }
        }
        catch (Exception exception)
        {
            exception.Data["atomic-messaging-stage"] ??= $"cold-first {phase} scope";
            failure = exception;
        }
        finally
        {
            for (int index = endpoints.Count - 1; index >= 0; index--)
            {
                try
                {
                    await OperationAsync($"cold-first {phase} endpoint disposal",
                        _ => endpoints[index].DisposeAsync().AsTask());
                }
                catch (Exception exception)
                {
                    failure ??= exception;
                }
            }
        }

        if (failure is not null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }
}
