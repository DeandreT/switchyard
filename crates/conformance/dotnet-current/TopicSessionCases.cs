using Azure.Messaging.ServiceBus;

internal static class TopicSessionCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(
        ServiceBusClient client,
        string topic,
        string firstSubscription,
        string secondSubscription,
        string ordinarySubscription,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"topic-session-{Guid.NewGuid():N}";
        var resources = new List<IAsyncDisposable>();
        ServiceBusSender sender = client.CreateSender(topic);
        resources.Add(sender);
        ServiceBusReceiver ordinary = client.CreateReceiver(topic, ordinarySubscription);
        resources.Add(ordinary);
        ServiceBusReceiver firstDeadLetters = client.CreateReceiver(topic, firstSubscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        resources.Add(firstDeadLetters);
        ServiceBusReceiver secondDeadLetters = client.CreateReceiver(topic, secondSubscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        resources.Add(secondDeadLetters);
        try
        {
            foreach (bool safeBatch in new[] { false, true })
            {
                string scope = $"{run}-{safeBatch}";
                string sessionA = scope + "-A";
                string sessionB = scope + "-B";
                ServiceBusMessage[] messages =
                {
                    Message(scope + "-missing", null),
                    Message(scope + "-A-first", sessionA),
                    Message(scope + "-B", sessionB),
                    Message(scope + "-A-second", sessionA),
                };
                // A null first member leaves the outer batch session absent;
                // each nested message remains the authority for its own copy.
                await SendBatchAsync(sender, messages, safeBatch, token);
                ServiceBusSessionReceiver first = await BoundedAsync(
                    ct => client.AcceptSessionAsync(topic, firstSubscription, sessionA,
                        cancellationToken: ct), token);
                resources.Add(first);
                ServiceBusSessionReceiver second = await BoundedAsync(
                    ct => client.AcceptSessionAsync(topic, secondSubscription, sessionA,
                        cancellationToken: ct), token);
                resources.Add(second);
                Require(first.SessionId == sessionA && second.SessionId == sessionA,
                    "the same ID was not independently accepted in both subscriptions");
                string firstState = scope + "-first-state";
                string secondState = scope + "-second-state";
                await BoundedAsync(ct => first.SetSessionStateAsync(BinaryData.FromString(firstState), ct), token);
                await BoundedAsync(ct => second.SetSessionStateAsync(BinaryData.FromString(secondState), ct), token);
                BinaryData? firstStoredState = await BoundedAsync(ct => first.GetSessionStateAsync(ct), token);
                BinaryData? secondStoredState = await BoundedAsync(ct => second.GetSessionStateAsync(ct), token);
                Require(firstStoredState?.ToString() == firstState
                    && secondStoredState?.ToString() == secondState,
                    "session state crossed subscription ownership");
                DateTimeOffset firstUntil = first.SessionLockedUntil;
                DateTimeOffset secondUntil = second.SessionLockedUntil;
                await BoundedAsync(ct => first.RenewSessionLockAsync(ct), token);
                Require(first.SessionLockedUntil >= firstUntil && second.SessionLockedUntil == secondUntil,
                    "renewal moved backward or changed the sibling receiver");

                ServiceBusReceivedMessage a = await ReceiveAsync(first, token);
                ServiceBusReceivedMessage b = await ReceiveAsync(second, token);
                CheckContent(a, messages[1]);
                CheckContent(b, messages[1]);
                Require(a.SequenceNumber == b.SequenceNumber, "session copies did not share the topic sequence");
                long firstSequence = a.SequenceNumber;
                var updates = new Dictionary<string, object> { ["stage"] = "topic-session-deferred" };
                await BoundedAsync(ct => first.DeferMessageAsync(a, updates, ct), token);
                ServiceBusReceivedMessage deferred = await BoundedAsync(
                    ct => first.ReceiveDeferredMessageAsync(a.SequenceNumber, ct), token);
                CheckContent(deferred, messages[1]);
                Require(deferred.SequenceNumber == firstSequence
                    && Equals(deferred.ApplicationProperties["stage"], "topic-session-deferred"),
                    "the owning session lost its deferred copy or updates");
                await BoundedAsync(ct => first.CompleteMessageAsync(deferred, ct), token);
                Require(!b.ApplicationProperties.ContainsKey("stage"), "defer mutated the sibling copy");
                var deadLetterUpdates = new Dictionary<string, object>
                {
                    ["DeadLetterReason"] = "topic-session-rejected",
                    ["DeadLetterErrorDescription"] = "only the second subscription rejected this copy",
                };
                await BoundedAsync(ct => second.DeadLetterMessageAsync(b, deadLetterUpdates, ct), token);
                a = await ReceiveAsync(first, token);
                b = await ReceiveAsync(second, token);
                CheckContent(a, messages[3]);
                CheckContent(b, messages[3]);
                Require(a.SequenceNumber == b.SequenceNumber && a.SequenceNumber > firstSequence,
                    "a session lost FIFO order or shared copy sequences");
                long secondSequence = a.SequenceNumber;
                await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
                await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
                await AssertEmptyAsync(first, token);
                await AssertEmptyAsync(second, token);

                var ordinarySequences = new Dictionary<string, long>();
                foreach (ServiceBusMessage source in messages)
                {
                    ServiceBusReceivedMessage delivery = await ReceiveAsync(ordinary, token);
                    CheckContent(delivery, source);
                    Require(ordinarySequences.TryAdd(delivery.MessageId, delivery.SequenceNumber),
                        "the ordinary sibling repeated a copy");
                    await BoundedAsync(ct => ordinary.CompleteMessageAsync(delivery, ct), token);
                }
                Require(ordinarySequences[messages[1].MessageId] == firstSequence
                    && ordinarySequences[messages[3].MessageId] == secondSequence,
                    "ordinary and required subscriptions received different topic sequences");
                ServiceBusReceivedMessage missingFirst = await ReceiveAsync(firstDeadLetters, token);
                ServiceBusReceivedMessage missingSecond = await ReceiveAsync(secondDeadLetters, token);
                foreach (ServiceBusReceivedMessage missing in new[] { missingFirst, missingSecond })
                {
                    CheckDeadLetter(missing, messages[0], ordinarySequences[messages[0].MessageId]);
                    Require(missing.DeadLetterReason == "Session ID is null"
                        && missing.DeadLetterErrorDescription
                            == "Session enabled entity doesn't allow a message whose session identifier is null.",
                        "a missing-session copy did not retain the chosen local reason");
                }
                await BoundedAsync(ct => firstDeadLetters.CompleteMessageAsync(missingFirst, ct), token);
                await BoundedAsync(ct => secondDeadLetters.CompleteMessageAsync(missingSecond, ct), token);
                ServiceBusReceivedMessage rejected = await ReceiveAsync(secondDeadLetters, token);
                CheckDeadLetter(rejected, messages[1], firstSequence);
                Require(rejected.DeadLetterReason == "topic-session-rejected"
                    && rejected.DeadLetterErrorDescription == (string)deadLetterUpdates["DeadLetterErrorDescription"],
                    "an explicitly rejected session copy lost its reason");
                await BoundedAsync(ct => secondDeadLetters.CompleteMessageAsync(rejected, ct), token);
                await AssertEmptyAsync(firstDeadLetters, token);
                await AssertEmptyAsync(secondDeadLetters, token);
                await AssertEmptyAsync(ordinary, token);

                await DisposeBoundedAsync(first);
                resources.Remove(first);
                await DisposeBoundedAsync(second);
                resources.Remove(second);
                ServiceBusSessionReceiver nextFirst = await BoundedAsync(
                    ct => client.AcceptNextSessionAsync(topic, firstSubscription, cancellationToken: ct), token);
                resources.Add(nextFirst);
                ServiceBusSessionReceiver nextSecond = await BoundedAsync(
                    ct => client.AcceptNextSessionAsync(topic, secondSubscription, cancellationToken: ct), token);
                resources.Add(nextSecond);
                Require(nextFirst.SessionId == sessionB && nextSecond.SessionId == sessionB,
                    "the next-available subscription receiver did not learn its echoed grant");
                a = await ReceiveAsync(nextFirst, token);
                b = await ReceiveAsync(nextSecond, token);
                CheckContent(a, messages[2]);
                CheckContent(b, messages[2]);
                Require(a.SequenceNumber == ordinarySequences[messages[2].MessageId]
                    && b.SequenceNumber == a.SequenceNumber, "the next session lost its copy sequence");
                await BoundedAsync(ct => nextFirst.CompleteMessageAsync(a, ct), token);
                await BoundedAsync(ct => nextSecond.CompleteMessageAsync(b, ct), token);
                await DisposeBoundedAsync(nextFirst);
                resources.Remove(nextFirst);
                await DisposeBoundedAsync(nextSecond);
                resources.Remove(nextSecond);

                foreach ((string subscription, string state) in new[]
                    { (firstSubscription, firstState), (secondSubscription, secondState) })
                {
                    ServiceBusSessionReceiver reopened = await BoundedAsync(
                        ct => client.AcceptSessionAsync(topic, subscription, sessionA,
                            cancellationToken: ct), token);
                    resources.Add(reopened);
                    BinaryData? restoredState = await BoundedAsync(ct => reopened.GetSessionStateAsync(ct), token);
                    Require(restoredState?.ToString() == state,
                        "closing or a sibling operation erased the persisted state");
                    await BoundedAsync(ct => reopened.SetSessionStateAsync(new BinaryData(Array.Empty<byte>()), ct), token);
                    BinaryData? clearedState = await BoundedAsync(ct => reopened.GetSessionStateAsync(ct), token);
                    Require(clearedState is null || clearedState.ToArray().Length == 0,
                        "empty session state was not cleared");
                    await AssertEmptyAsync(reopened, token);
                    await DisposeBoundedAsync(reopened);
                    resources.Remove(reopened);
                }
            }
            Console.WriteLine("official .NET topic session accept/next/FIFO/renew/state/deferred/SDLQ/isolation passed");
        }
        finally
        {
            await DisposeAllAsync(resources);
        }
    }

    private static ServiceBusMessage Message(string id, string? session) => new(id)
    {
        MessageId = id,
        SessionId = session,
        TimeToLive = TimeSpan.FromMinutes(2),
        CorrelationId = id + "-correlation",
    };

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.SessionId == expected.SessionId
            && actual.CorrelationId == expected.CorrelationId && actual.TimeToLive == expected.TimeToLive,
            "a subscription copy changed its body, identity, session, or lifetime");
    }

    private static void CheckDeadLetter(ServiceBusReceivedMessage actual, ServiceBusMessage expected, long sequence)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.CorrelationId == expected.CorrelationId
            && actual.SequenceNumber == sequence && actual.SessionId is null,
            "a session SDLQ changed the copy identity or retained a session");
        Require(actual.GetRawAmqpMessage().Header.TimeToLive is null
            && actual.GetRawAmqpMessage().Properties.AbsoluteExpiryTime is null,
            "a session SDLQ retained an expiring lifetime");
    }

    private static async Task SendBatchAsync(ServiceBusSender sender,
        IReadOnlyList<ServiceBusMessage> messages, bool safeBatch, CancellationToken token)
    {
        if (!safeBatch)
        {
            await BoundedAsync(ct => sender.SendMessagesAsync(messages, ct), token);
            return;
        }
        using ServiceBusMessageBatch batch = await BoundedAsync(
            ct => sender.CreateMessageBatchAsync(ct).AsTask(), token);
        foreach (ServiceBusMessage message in messages)
        {
            Require(batch.TryAddMessage(message), "a small session topic member did not fit its batch");
        }
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? delivery = await BoundedAsync(
            ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
        Require(delivery is not null, "a topic session publication lost its copy");
        return delivery!;
    }

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(1, fromSequenceNumber: 1, cancellationToken: ct), token);
        Require(messages.Count == 0, "the topic session workflow left a retained copy");
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

    private static async Task DisposeAllAsync(IEnumerable<IAsyncDisposable> resources)
    {
        Exception? failure = null;
        foreach (IAsyncDisposable resource in resources.Reverse())
        {
            try { await DisposeBoundedAsync(resource); }
            catch (Exception error) { failure ??= error; }
        }
        if (failure is not null) { throw new InvalidOperationException("topic session cleanup failed", failure); }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"topic session conformance failed: {detail}"); }
    }
}
