using Azure.Messaging.ServiceBus;

internal static class TopicCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(
        ServiceBusClient client,
        string topic,
        string firstSubscription,
        string secondSubscription,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"topic-{Guid.NewGuid():N}";
        ServiceBusSender sender = client.CreateSender(topic);
        ServiceBusReceiver first = client.CreateReceiver(topic, firstSubscription);
        ServiceBusReceiver second = client.CreateReceiver(topic, secondSubscription);
        ServiceBusReceiver deadLetters = client.CreateReceiver(topic, secondSubscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        try
        {
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            await AssertEmptyAsync(deadLetters, token);
            ServiceBusMessage original = RichMessage(run + "-single", 0);
            await BoundedAsync(ct => sender.SendMessageAsync(original, ct), token);
            ServiceBusReceivedMessage firstPeek = await PeekOneAsync(first, token);
            ServiceBusReceivedMessage secondPeek = await PeekOneAsync(second, token);
            CheckContent(firstPeek, original);
            CheckContent(secondPeek, original);
            Require(firstPeek.SequenceNumber == secondPeek.SequenceNumber,
                "subscription copies did not share the topic sequence");
            ServiceBusReceivedMessage a = await ReceiveAsync(first, token);
            ServiceBusReceivedMessage b = await ReceiveAsync(second, token);
            CheckContent(a, original);
            CheckContent(b, original);
            Require(a.SequenceNumber == b.SequenceNumber, "received copy sequence changed");
            DateTimeOffset before = a.LockedUntil;
            await BoundedAsync(ct => first.RenewMessageLockAsync(a, ct), token);
            Require(a.LockedUntil >= before, "subscription lock renewal moved backward");
            await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
            await AssertEmptyAsync(first, token);
            Require((await PeekOneAsync(second, token)).SequenceNumber == b.SequenceNumber,
                "completing one copy removed its sibling");
            var reason = new Dictionary<string, object>
            {
                ["DeadLetterReason"] = "topic-copy-rejected",
                ["DeadLetterErrorDescription"] = "only the second subscription rejected it",
                ["copy-route"] = secondSubscription,
            };
            await BoundedAsync(ct => second.DeadLetterMessageAsync(b, reason, ct), token);
            await AssertEmptyAsync(second, token);
            ServiceBusReceivedMessage dead = await ReceiveAsync(deadLetters, token);
            Require(dead.SequenceNumber == b.SequenceNumber
                && dead.Body.ToArray().SequenceEqual(original.Body.ToArray())
                && dead.MessageId == original.MessageId,
                "the subscription DLQ changed the original copy");
            Require(dead.DeadLetterReason == "topic-copy-rejected"
                && dead.DeadLetterErrorDescription == (string)reason["DeadLetterErrorDescription"]
                && Equals(dead.ApplicationProperties["copy-route"], secondSubscription),
                "the subscription DLQ lost its reason or property updates");
            await BoundedAsync(ct => deadLetters.CompleteMessageAsync(dead, ct), token);
            await AssertEmptyAsync(deadLetters, token);

            ServiceBusMessage deferredSource = RichMessage(run + "-deferred", 1);
            await BoundedAsync(ct => sender.SendMessageAsync(deferredSource, ct), token);
            a = await ReceiveAsync(first, token);
            b = await ReceiveAsync(second, token);
            var updates = new Dictionary<string, object> { ["stage"] = "waiting-for-first" };
            await BoundedAsync(ct => first.DeferMessageAsync(a, updates, ct), token);
            ServiceBusReceivedMessage deferred = await BoundedAsync(
                ct => first.ReceiveDeferredMessageAsync(a.SequenceNumber, ct), token);
            CheckContent(deferred, deferredSource);
            Require(deferred.SequenceNumber == b.SequenceNumber
                && Equals(deferred.ApplicationProperties["stage"], "waiting-for-first"),
                "deferred receive changed the copy sequence or updates");
            await BoundedAsync(ct => first.CompleteMessageAsync(deferred, ct), token);
            await AssertEmptyAsync(first, token);
            CheckContent(b, deferredSource);
            Require(!b.ApplicationProperties.ContainsKey("stage"),
                "deferring the first copy mutated the second subscription");
            await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
            await AssertEmptyAsync(second, token);

            foreach (bool safeBatch in new[] { false, true })
            {
                ServiceBusMessage[] messages = Enumerable.Range(0, 3)
                    .Select(index => RichMessage($"{run}-batch-{safeBatch}", index))
                    .ToArray();
                await SendBatchAsync(sender, messages, safeBatch, token);
                IReadOnlyList<ServiceBusReceivedMessage> firstBatch = await BoundedAsync(
                    ct => first.PeekMessagesAsync(messages.Length + 1,
                        fromSequenceNumber: 1, cancellationToken: ct), token);
                IReadOnlyList<ServiceBusReceivedMessage> secondBatch = await BoundedAsync(
                    ct => second.PeekMessagesAsync(messages.Length + 1,
                        fromSequenceNumber: 1, cancellationToken: ct), token);
                Require(firstBatch.Count == messages.Length && secondBatch.Count == messages.Length,
                    "topic batch left a partial fanout or extra copy");
                long previous = 0;
                for (int index = 0; index < messages.Length; index++)
                {
                    CheckContent(firstBatch[index], messages[index]);
                    CheckContent(secondBatch[index], messages[index]);
                    Require(firstBatch[index].SequenceNumber == secondBatch[index].SequenceNumber
                        && firstBatch[index].SequenceNumber > previous,
                        "topic batch copy order or shared sequences changed");
                    previous = firstBatch[index].SequenceNumber;
                    b = await ReceiveAsync(second, token);
                    a = await ReceiveAsync(first, token);
                    CheckContent(a, messages[index]);
                    CheckContent(b, messages[index]);
                    Require(a.SequenceNumber == b.SequenceNumber,
                        "topic batch receive returned different copy sequences");
                    await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
                    await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
                }
                await AssertEmptyAsync(first, token);
                await AssertEmptyAsync(second, token);
                await SendBatchAsync(sender, messages, safeBatch, token);
                await AssertEmptyAsync(first, token);
                await AssertEmptyAsync(second, token);
            }
            await AssertEmptyAsync(deadLetters, token);
            Console.WriteLine("official .NET topic fanout/subscription peek/renew/defer/complete/dead-letter/batch passed");
        }
        finally
        {
            await DisposeAllAsync(deadLetters, second, first, sender);
        }
    }

    private static ServiceBusMessage RichMessage(string scope, int index)
    {
        var message = new ServiceBusMessage($"{scope}:body:{index}")
        {
            MessageId = $"{scope}:id:{index}",
            CorrelationId = $"{scope}:correlation:{index}",
            Subject = $"topic-subject-{index}",
            ContentType = "application/octet-stream",
            To = "logical-topic-destination",
            ReplyTo = "logical-topic-replies",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
        message.GetRawAmqpMessage().Footer["topic-checksum"] = $"checksum-{index}";
        message.ApplicationProperties["member-index"] = index;
        message.ApplicationProperties["nullable"] = null!;
        message.ApplicationProperties["bytes"] = new byte[] { (byte)index, 0, 255 };
        return message;
    }

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray()), "copy body changed");
        Require(actual.MessageId == expected.MessageId
            && actual.CorrelationId == expected.CorrelationId
            && actual.Subject == expected.Subject
            && actual.ContentType == expected.ContentType
            && actual.To == expected.To && actual.ReplyTo == expected.ReplyTo,
            "copy message properties changed");
        Require(actual.TimeToLive == expected.TimeToLive
            && actual.ExpiresAt == actual.EnqueuedTime.Add(expected.TimeToLive),
            "copy lifetime changed");
        Require(actual.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8",
            "copy content encoding changed");
        Require(actual.GetRawAmqpMessage().Footer.TryGetValue("topic-checksum", out object? footer)
            && Equals(footer, expected.GetRawAmqpMessage().Footer["topic-checksum"]),
            "copy footer changed");
        foreach (var pair in expected.ApplicationProperties)
        {
            Require(actual.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && (value is byte[] bytes && pair.Value is byte[] expectedBytes
                    ? bytes.SequenceEqual(expectedBytes) : Equals(value, pair.Value)),
                $"copy application property {pair.Key} changed");
        }
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
            Require(batch.TryAddMessage(message), "a small topic member did not fit its batch");
        }
        Require(batch.Count == messages.Count, "topic safe batch count changed");
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? message = await BoundedAsync(
            ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
        Require(message is not null, "a successful publication lost its subscription copy");
        return message!;
    }

    private static async Task<ServiceBusReceivedMessage> PeekOneAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(2, fromSequenceNumber: 1,
                cancellationToken: ct), token);
        Require(messages.Count == 1, "peek did not return exactly one subscription copy");
        return messages[0];
    }

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(1, fromSequenceNumber: 1,
                cancellationToken: ct), token);
        Require(messages.Count == 0, "topic workflow left a retained subscription copy");
    }

    private static async Task BoundedAsync(Func<CancellationToken, Task> operation,
        CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task<T> BoundedAsync<T>(Func<CancellationToken, Task<T>> operation,
        CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        return await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task DisposeAllAsync(params IAsyncDisposable[] resources)
    {
        Exception? failure = null;
        foreach (IAsyncDisposable resource in resources)
        {
            try { await resource.DisposeAsync().AsTask().WaitAsync(CleanupTimeout); }
            catch (Exception error) { failure ??= error; }
        }
        if (failure is not null) { throw new InvalidOperationException("topic cleanup failed", failure); }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"topic conformance failed: {detail}"); }
    }
}
