using Azure.Messaging.ServiceBus;

internal static class SessionPeekCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(ServiceBusClient client, string queue,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"session-peek-{Guid.NewGuid():N}";
        string sessionA = run + "-A";
        string sessionB = run + "-B";
        ServiceBusMessage[] messages =
        {
            Message(run + "-A-first", sessionA),
            Message(run + "-B", sessionB),
            Message(run + "-A-second", sessionA),
        };
        var resources = new List<IAsyncDisposable>();
        ServiceBusSender sender = client.CreateSender(queue);
        resources.Add(sender);
        ServiceBusReceiver browser = client.CreateReceiver(queue);
        resources.Add(browser);
        try
        {
            foreach (ServiceBusMessage message in messages)
            {
                await BoundedAsync(ct => sender.SendMessageAsync(message, ct), token);
            }
            // Peek opens only the management node. It never accepts a session
            // or opens this ordinary receiver's filterless data link.
            await AssertBrowseAsync(browser, messages, null, token);
            ServiceBusSessionReceiver owner = await BoundedAsync(
                ct => client.AcceptSessionAsync(queue, sessionA, cancellationToken: ct), token);
            resources.Add(owner);
            ServiceBusReceivedMessage first = await ReceiveAsync(owner, token);
            CheckContent(first, messages[0]);
            await BoundedAsync(ct => owner.DeferMessageAsync(first, cancellationToken: ct), token);
            await AssertBrowseAsync(browser, messages, messages[0].MessageId, token);
            await AssertBrowseAsync(owner, new[] { messages[0], messages[2] }, messages[0].MessageId, token);
            ServiceBusReceivedMessage deferred = await BoundedAsync(
                ct => owner.ReceiveDeferredMessageAsync(first.SequenceNumber, ct), token);
            CheckContent(deferred, messages[0]);
            Require(deferred.SequenceNumber == first.SequenceNumber, "deferred browse changed the original sequence");
            await BoundedAsync(ct => owner.CompleteMessageAsync(deferred, ct), token);
            ServiceBusReceivedMessage second = await ReceiveAsync(owner, token);
            CheckContent(second, messages[2]);
            Require(second.SequenceNumber > first.SequenceNumber, "global browse changed session FIFO order");
            await BoundedAsync(ct => owner.CompleteMessageAsync(second, ct), token);
            await DisposeBoundedAsync(owner);
            resources.Remove(owner);
            await AssertBrowseAsync(browser, new[] { messages[1] }, null, token);
            ServiceBusSessionReceiver other = await BoundedAsync(
                ct => client.AcceptSessionAsync(queue, sessionB, cancellationToken: ct), token);
            resources.Add(other);
            ServiceBusReceivedMessage delivery = await ReceiveAsync(other, token);
            CheckContent(delivery, messages[1]);
            await BoundedAsync(ct => other.CompleteMessageAsync(delivery, ct), token);
            await DisposeBoundedAsync(other);
            resources.Remove(other);
            await AssertBrowseAsync(browser, Array.Empty<ServiceBusMessage>(), null, token);
            Console.WriteLine("official .NET required queue management-only global/session-filtered peek passed");
        }
        finally
        {
            Exception? failure = null;
            foreach (IAsyncDisposable resource in resources.AsEnumerable().Reverse())
            {
                try { await DisposeBoundedAsync(resource); }
                catch (Exception error) { failure ??= error; }
            }
            if (failure is not null) { throw new InvalidOperationException("session peek cleanup failed", failure); }
        }
    }

    private static ServiceBusMessage Message(string id, string session) => new(id)
    {
        MessageId = id,
        SessionId = session,
        TimeToLive = TimeSpan.FromMinutes(2),
    };

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.SessionId == expected.SessionId,
            "a session browse changed body, identity or session");
    }

    private static async Task AssertBrowseAsync(ServiceBusReceiver receiver,
        IReadOnlyList<ServiceBusMessage> expected, string? deferredMessageId, CancellationToken token)
    {
        var browsed = new List<ServiceBusReceivedMessage>();
        long nextSequence = 1;
        bool exhausted = false;
        for (int page = 0; page < 4; page++)
        {
            IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
                ct => receiver.PeekMessagesAsync(2, fromSequenceNumber: nextSequence, cancellationToken: ct), token);
            Require(messages.Count <= 2, "peek exceeded its requested page size");
            if (messages.Count == 0)
            {
                exhausted = true;
                break;
            }
            foreach (ServiceBusReceivedMessage message in messages)
            {
                Require(message.SequenceNumber >= nextSequence, "peek repeated or reordered a sequence");
                nextSequence = message.SequenceNumber + 1;
                browsed.Add(message);
            }
        }
        Require(exhausted && browsed.Count == expected.Count, "peek lost or repeated a session message");
        for (int index = 0; index < expected.Count; index++)
        {
            CheckContent(browsed[index], expected[index]);
            ServiceBusMessageState state = expected[index].MessageId == deferredMessageId
                ? ServiceBusMessageState.Deferred : ServiceBusMessageState.Active;
            Require(browsed[index].State == state, "peek omitted a deferred message or crossed session state");
        }
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? delivery = await BoundedAsync(
            ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
        Require(delivery is not null, "browse consumed a message or left its session unavailable");
        return delivery!;
    }

    private static async Task BoundedAsync(Func<CancellationToken, Task> operation, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task<T> BoundedAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        return await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static Task DisposeBoundedAsync(IAsyncDisposable resource) => resource.DisposeAsync().AsTask().WaitAsync(CleanupTimeout);

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"session peek conformance failed: {detail}"); }
    }
}
