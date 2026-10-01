using Azure.Messaging.ServiceBus;

internal static class TopicSchedulingCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(ServiceBusClient client, string topic,
        string sessionTopic, CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        string run = $"topic-scheduling-{Guid.NewGuid():N}";
        await OrdinaryCopiesAsync(client, topic, run, deadline.Token);
        await SessionCopiesAsync(client, sessionTopic, run, deadline.Token);
        Console.WriteLine("official .NET topic schedule/cancel/parent browse/timer/TTL/new sequence/dedup passed");
        Console.WriteLine("official .NET scheduled topic session/ordinary/independent SDLQ copies passed");
    }

    private static async Task OrdinaryCopiesAsync(ServiceBusClient client, string topic,
        string run, CancellationToken token)
    {
        var resources = new List<IAsyncDisposable>();
        ServiceBusSender sender = client.CreateSender(topic);
        ServiceBusReceiver parent = client.CreateReceiver(topic);
        ServiceBusReceiver first = client.CreateReceiver(topic, "Alpha");
        ServiceBusReceiver second = client.CreateReceiver(topic, "beta");
        resources.AddRange(new IAsyncDisposable[] { sender, parent, first, second });
        try
        {
            DateTimeOffset future = DueAfter(TimeSpan.FromMinutes(1));
            ServiceBusMessage[] cancelled = Enumerable.Range(0, 6)
                .Select(index => Message($"{run}-cancel-{index}", null, future)).ToArray();
            long single = await BoundedAsync(
                ct => sender.ScheduleMessageAsync(cancelled[0], future, ct), token);
            IReadOnlyList<long> batch = await BoundedAsync(
                ct => sender.ScheduleMessagesAsync(cancelled[1..3], future, ct), token);
            Require(batch.Count == 2, "the management batch did not return both handles");
            await BoundedAsync(ct => sender.SendMessageAsync(cancelled[3], ct), token);
            using (ServiceBusMessageBatch outgoing = await BoundedAsync(
                ct => sender.CreateMessageBatchAsync(ct).AsTask(), token))
            {
                foreach (ServiceBusMessage message in cancelled[4..])
                {
                    Require(outgoing.TryAddMessage(message), "a timestamped topic member did not fit");
                }
                await BoundedAsync(ct => sender.SendMessagesAsync(outgoing, ct), token);
            }
            List<ServiceBusReceivedMessage> pending = await BrowseAsync(parent, token);
            Require(pending.Count == cancelled.Length, "the parent lost a scheduled ingress member");
            for (int index = 0; index < cancelled.Length; index++)
            {
                CheckContent(pending[index], cancelled[index]);
                Require(pending[index].State == ServiceBusMessageState.Scheduled
                    && pending[index].ScheduledEnqueueTime == future,
                    "parent browse did not preserve Scheduled state or the requested timestamp");
            }
            Require(pending[0].SequenceNumber == single
                && pending[1].SequenceNumber == batch[0] && pending[2].SequenceNumber == batch[1],
                "parent browse and scheduling returned different handles");
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            await BoundedAsync(ct => sender.CancelScheduledMessageAsync(single, ct), token);
            await BoundedAsync(ct => sender.CancelScheduledMessagesAsync(
                pending.Skip(1).Select(message => message.SequenceNumber), ct), token);
            await AssertEmptyAsync(parent, token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            await BoundedAsync(ct => sender.SendMessageAsync(Message(cancelled[0].MessageId), ct), token);
            IReadOnlyList<long> duplicateHandles = await BoundedAsync(
                ct => sender.ScheduleMessagesAsync(new[] { cancelled[1] }, future, ct), token);
            Require(duplicateHandles.Count == 1, "duplicate scheduling omitted its response handle");
            await AssertEmptyAsync(parent, token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);

            DateTimeOffset due = DueAfter(TimeSpan.FromSeconds(5));
            ServiceBusMessage[] scheduled =
            {
                Message(run + "-activate-first", null, due),
                Message(run + "-activate-second", null, due),
            };
            IReadOnlyList<long> handles = await BoundedAsync(
                ct => sender.ScheduleMessagesAsync(scheduled, due, ct), token);
            Require(handles.Count == 2, "activation scheduling did not return both handles");
            ServiceBusMessage immediate = Message(run + "-interleaved");
            await BoundedAsync(ct => sender.SendMessageAsync(immediate, ct), token);
            ServiceBusReceivedMessage a = await ReceiveAsync(first, token);
            ServiceBusReceivedMessage b = await ReceiveAsync(second, token);
            CheckContent(a, immediate);
            CheckContent(b, immediate);
            Require(a.SequenceNumber == b.SequenceNumber && a.SequenceNumber > handles.Max(),
                "the interleaved immediate copy did not get its own shared sequence");
            long previous = a.SequenceNumber;
            await CompleteBothAsync(first, a, second, b, token);
            foreach (ServiceBusMessage original in scheduled)
            {
                a = await ReceiveAsync(first, token);
                b = await ReceiveAsync(second, token);
                CheckActivated(a, original, due);
                CheckActivated(b, original, due);
                Require(a.SequenceNumber == b.SequenceNumber && a.SequenceNumber > previous,
                    "activation reused a schedule handle or lost shared FIFO sequences");
                previous = a.SequenceNumber;
                await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
                List<ServiceBusReceivedMessage> remaining = await BrowseAsync(second, token);
                Require(remaining.Any(message => message.SequenceNumber == b.SequenceNumber),
                    "completing one activated copy consumed the sibling copy");
                await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
            }
            await AssertEmptyAsync(parent, token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
        }
        finally
        {
            await DisposeAllAsync(resources);
        }
    }

    private static async Task SessionCopiesAsync(ServiceBusClient client, string topic,
        string run, CancellationToken token)
    {
        var resources = new List<IAsyncDisposable>();
        ServiceBusSender sender = client.CreateSender(topic);
        ServiceBusReceiver parent = client.CreateReceiver(topic);
        ServiceBusReceiver ordinary = client.CreateReceiver(topic, "ordinary");
        resources.AddRange(new IAsyncDisposable[] { sender, parent, ordinary });
        try
        {
            DateTimeOffset due = DueAfter(TimeSpan.FromSeconds(5));
            string sessionA = run + "-A";
            string sessionB = run + "-B";
            ServiceBusMessage[] sources =
            {
                Message(run + "-missing", null, due),
                Message(run + "-A", sessionA, due),
                Message(run + "-B", sessionB, due),
            };
            IReadOnlyList<long> handles = await BoundedAsync(
                ct => sender.ScheduleMessagesAsync(sources, due, ct), token);
            Require(handles.Count == sources.Length, "mixed scheduling omitted a parent handle");
            var sequences = new Dictionary<string, long>();
            foreach (ServiceBusMessage source in sources)
            {
                ServiceBusReceivedMessage delivery = await ReceiveAsync(ordinary, token);
                CheckActivated(delivery, source, due);
                Require(delivery.SequenceNumber > handles.Max()
                    && sequences.TryAdd(delivery.MessageId, delivery.SequenceNumber),
                    "the ordinary scheduled sibling reused a handle or repeated a copy");
                await BoundedAsync(ct => ordinary.CompleteMessageAsync(delivery, ct), token);
            }
            Require(sequences[sources[0].MessageId] < sequences[sources[1].MessageId]
                && sequences[sources[1].MessageId] < sequences[sources[2].MessageId],
                "mixed scheduled activation lost shared ingress order");
            foreach (string subscription in new[] { "Alpha", "beta" })
            {
                foreach (ServiceBusMessage source in sources.Skip(1))
                {
                    ServiceBusSessionReceiver receiver = await BoundedAsync(
                        ct => client.AcceptSessionAsync(topic, subscription, source.SessionId,
                            cancellationToken: ct), token);
                    resources.Add(receiver);
                    ServiceBusReceivedMessage delivery = await ReceiveAsync(receiver, token);
                    CheckActivated(delivery, source, due);
                    Require(receiver.SessionId == source.SessionId
                        && delivery.SequenceNumber == sequences[source.MessageId],
                        "required subscription scheduling crossed session or sequence ownership");
                    await BoundedAsync(ct => receiver.CompleteMessageAsync(delivery, ct), token);
                    await DisposeBoundedAsync(receiver);
                    resources.Remove(receiver);
                }
                ServiceBusReceiver deadLetters = client.CreateReceiver(topic, subscription,
                    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
                resources.Add(deadLetters);
                ServiceBusReceivedMessage missing = await ReceiveAsync(deadLetters, token);
                Require(missing.MessageId == sources[0].MessageId
                    && missing.Body.ToArray().SequenceEqual(sources[0].Body.ToArray())
                    && missing.CorrelationId == sources[0].CorrelationId
                    && missing.SequenceNumber == sequences[sources[0].MessageId]
                    && missing.SessionId is null && missing.ScheduledEnqueueTime == due,
                    "a scheduled missing-session shadow lost its identity or requested timestamp");
                Require(missing.DeadLetterReason == "Session ID is null"
                    && missing.DeadLetterErrorDescription
                        == "Session enabled entity doesn't allow a message whose session identifier is null."
                    && missing.GetRawAmqpMessage().Header.TimeToLive is null
                    && missing.GetRawAmqpMessage().Properties.AbsoluteExpiryTime is null,
                    "a scheduled missing-session shadow retained TTL or lost the local reason");
                await BoundedAsync(ct => deadLetters.CompleteMessageAsync(missing, ct), token);
                await AssertEmptyAsync(deadLetters, token);
                await DisposeBoundedAsync(deadLetters);
                resources.Remove(deadLetters);
                ServiceBusReceiver browser = client.CreateReceiver(topic, subscription);
                resources.Add(browser);
                await AssertEmptyAsync(browser, token);
                await DisposeBoundedAsync(browser);
                resources.Remove(browser);
            }
            await AssertEmptyAsync(parent, token);
            await AssertEmptyAsync(ordinary, token);
        }
        finally
        {
            await DisposeAllAsync(resources);
        }
    }

    private static ServiceBusMessage Message(string id, string? session = null,
        DateTimeOffset scheduled = default)
    {
        var message = new ServiceBusMessage(id)
        {
            MessageId = id,
            SessionId = session,
            CorrelationId = id + "-correlation",
            Subject = "scheduled-topic-copy",
            ContentType = "text/plain",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        if (scheduled != default)
        {
            message.ScheduledEnqueueTime = scheduled;
        }
        message.ApplicationProperties["origin"] = "topic-scheduling";
        message.ApplicationProperties["attempt"] = 1L;
        return message;
    }

    private static DateTimeOffset DueAfter(TimeSpan delay) =>
        DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds());

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.SessionId == expected.SessionId
            && actual.CorrelationId == expected.CorrelationId && actual.Subject == expected.Subject
            && actual.ContentType == expected.ContentType && actual.TimeToLive == expected.TimeToLive
            && Equals(actual.ApplicationProperties["origin"], expected.ApplicationProperties["origin"])
            && Equals(actual.ApplicationProperties["attempt"], expected.ApplicationProperties["attempt"]),
            "a scheduled topic copy changed its content, identity, session, or TTL");
    }

    private static void CheckActivated(ServiceBusReceivedMessage actual,
        ServiceBusMessage expected, DateTimeOffset due)
    {
        CheckContent(actual, expected);
        Require(actual.State == ServiceBusMessageState.Active && actual.ScheduledEnqueueTime == due
            && actual.EnqueuedTime >= due && actual.ExpiresAt == actual.EnqueuedTime + expected.TimeToLive,
            "activation lost its timestamp or started TTL before the actual enqueue");
    }

    private static async Task<List<ServiceBusReceivedMessage>> BrowseAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        var result = new List<ServiceBusReceivedMessage>();
        long next = 1;
        for (int page = 0; page < 8; page++)
        {
            IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
                ct => receiver.PeekMessagesAsync(2, fromSequenceNumber: next, cancellationToken: ct), token);
            Require(messages.Count <= 2, "browse exceeded its requested page size");
            if (messages.Count == 0) { return result; }
            foreach (ServiceBusReceivedMessage message in messages)
            {
                Require(message.SequenceNumber >= next, "browse repeated or reversed a sequence");
                next = message.SequenceNumber + 1;
                result.Add(message);
            }
        }
        throw new InvalidOperationException("topic scheduling browse exceeded its page bound");
    }

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(1, fromSequenceNumber: 1, cancellationToken: ct), token);
        Require(messages.Count == 0, "the topic scheduling workflow left a retained copy");
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            ServiceBusReceivedMessage? delivery = await BoundedAsync(
                ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
            if (delivery is not null) { return delivery; }
        }
        throw new InvalidOperationException("the topic scheduling timer did not publish a copy");
    }

    private static async Task CompleteBothAsync(ServiceBusReceiver first,
        ServiceBusReceivedMessage a, ServiceBusReceiver second, ServiceBusReceivedMessage b,
        CancellationToken token)
    {
        await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
        await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
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

    private static Task DisposeBoundedAsync(IAsyncDisposable resource) =>
        resource.DisposeAsync().AsTask().WaitAsync(CleanupTimeout);

    private static async Task DisposeAllAsync(IEnumerable<IAsyncDisposable> resources)
    {
        Exception? failure = null;
        foreach (IAsyncDisposable resource in resources.Reverse())
        {
            try { await DisposeBoundedAsync(resource); }
            catch (Exception error) { failure ??= error; }
        }
        if (failure is not null) { throw new InvalidOperationException("topic scheduling cleanup failed", failure); }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"topic scheduling conformance failed: {detail}"); }
    }
}
