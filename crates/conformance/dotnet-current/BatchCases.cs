using Azure.Messaging.ServiceBus;

internal static class BatchCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(
        ServiceBusClient client,
        string ordinaryQueue,
        string sessionQueue,
        string duplicateQueue,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"batch-{Guid.NewGuid():N}";
        ServiceBusSender sender = client.CreateSender(ordinaryQueue);
        ServiceBusReceiver receiver = client.CreateReceiver(ordinaryQueue);
        ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
        ServiceBusSender duplicateSender = client.CreateSender(duplicateQueue);
        ServiceBusReceiver duplicateReceiver = client.CreateReceiver(duplicateQueue);
        try
        {
            await AssertEmptyAsync(receiver, token);
            await AssertEmptyAsync(duplicateReceiver, token);
            foreach (bool safeBatch in new[] { false, true })
            {
                foreach (int count in new[] { 1, 3 })
                {
                    string scope = $"{run}-{safeBatch}-{count}";
                    ServiceBusMessage[] messages = Enumerable.Range(0, count)
                        .Select(index => RichMessage(scope, index))
                        .ToArray();
                    await SendAsync(sender, messages, safeBatch, token);
                    await VerifyAndCompleteAsync(receiver, messages, token);
                    await AssertEmptyAsync(receiver, token);

                    string sessionId = $"{scope}-session";
                    ServiceBusMessage[] sessionMessages = Enumerable.Range(0, count)
                        .Select(index => RichMessage(scope + "-session", index, sessionId))
                        .ToArray();
                    await SendAsync(sessionSender, sessionMessages, safeBatch, token);
                    ServiceBusSessionReceiver sessionReceiver = await BoundedAsync(
                        ct => client.AcceptSessionAsync(sessionQueue, sessionId,
                            cancellationToken: ct), token);
                    try
                    {
                        await VerifyAndCompleteAsync(sessionReceiver, sessionMessages, token);
                        await AssertEmptyAsync(sessionReceiver, token);
                    }
                    finally
                    {
                        await DisposeBoundedAsync(sessionReceiver);
                    }
                }
                await RejectInvalidSessionsAsync(client, sessionQueue, sessionSender,
                    $"{run}-invalid-{safeBatch}", safeBatch, token);
                await VerifyDeduplicationAsync(duplicateSender, duplicateReceiver,
                    $"{run}-dedup-{safeBatch}", safeBatch, token);
            }
            await VerifyOversizedTryAddAsync(sender, receiver, run, token);
        }
        finally
        {
            await DisposeAllAsync(receiver, sender, duplicateReceiver, duplicateSender, sessionSender);
        }
    }

    private static ServiceBusMessage RichMessage(string scope, int index, string? sessionId = null)
    {
        var message = new ServiceBusMessage($"{scope}:body:{index}")
        {
            MessageId = $"{scope}:id:{index}",
            CorrelationId = $"{scope}:correlation:{index}",
            Subject = $"subject-{index}",
            ContentType = index % 2 == 0 ? "application/json" : "text/plain",
            To = $"destination-{index}",
            ReplyTo = $"replies-{index}",
            ReplyToSessionId = $"reply-session-{index}",
            SessionId = sessionId,
            TimeToLive = TimeSpan.FromMinutes(2).Add(TimeSpan.FromSeconds(index)),
        };
        message.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
        message.GetRawAmqpMessage().Footer["batch-checksum"] = $"checksum-{index}";
        message.ApplicationProperties["member-index"] = index;
        message.ApplicationProperties["member-name"] = $"member-{index}";
        message.ApplicationProperties["even"] = index % 2 == 0;
        message.ApplicationProperties["bytes"] = new byte[] { (byte)index, 0, 255 };
        message.ApplicationProperties["nullable"] = null!;
        if (index == 0)
        {
            message.ApplicationProperties["first-only"] = "not inherited";
        }
        return message;
    }

    private static async Task SendAsync(
        ServiceBusSender sender,
        IReadOnlyList<ServiceBusMessage> messages,
        bool safeBatch,
        CancellationToken token)
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
            Require(batch.TryAddMessage(message), "a small message did not fit the safe batch");
        }
        Require(batch.Count == messages.Count, "safe batch count changed unexpectedly");
        Require(batch.SizeInBytes <= batch.MaxSizeInBytes, "safe batch exceeded its size limit");
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
    }

    private static async Task VerifyAndCompleteAsync(
        ServiceBusReceiver receiver,
        IReadOnlyList<ServiceBusMessage> expected,
        CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> peeked = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(expected.Count + 1, fromSequenceNumber: 1,
                cancellationToken: ct), token);
        Require(peeked.Count == expected.Count, "peek returned a partial batch or extra members");
        long previousSequence = 0;
        for (int index = 0; index < expected.Count; index++)
        {
            CheckContent(peeked[index], expected[index], index);
            Require(peeked[index].SequenceNumber > previousSequence, "batch FIFO sequence order changed");
            previousSequence = peeked[index].SequenceNumber;
            ServiceBusReceivedMessage? received = await BoundedAsync(
                ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
            Require(received is not null, "a successful batch lost a member before receive");
            CheckContent(received!, expected[index], index);
            Require(received!.SequenceNumber == peeked[index].SequenceNumber,
                "receive did not preserve the batch FIFO order");
            Require(received.DeliveryCount == 1, "a new member was unexpectedly redelivered");
            await BoundedAsync(ct => receiver.CompleteMessageAsync(received, ct), token);
        }
    }

    private static void CheckContent(
        ServiceBusReceivedMessage actual,
        ServiceBusMessage expected,
        int memberIndex)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray()), "member body changed");
        Require(actual.MessageId == expected.MessageId, "member message ID was inherited or changed");
        Require(actual.CorrelationId == expected.CorrelationId, "member correlation ID changed");
        Require(actual.Subject == expected.Subject && actual.ContentType == expected.ContentType,
            "member subject or content type changed");
        Require(actual.To == expected.To && actual.ReplyTo == expected.ReplyTo
            && actual.ReplyToSessionId == expected.ReplyToSessionId, "member addressing changed");
        Require(actual.SessionId == expected.SessionId, "member session ID changed");
        Require(actual.TimeToLive == expected.TimeToLive
            && actual.ExpiresAt == actual.EnqueuedTime.Add(expected.TimeToLive), "member lifetime changed");
        Require(actual.State == ServiceBusMessageState.Active, "ordinary batch member became scheduled");
        Require(actual.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8",
            "member content encoding changed");
        Require(actual.GetRawAmqpMessage().Footer.TryGetValue("batch-checksum", out object? checksum)
            && Equals(checksum, $"checksum-{memberIndex}"), "member footer changed");
        foreach (string key in new[] { "member-index", "member-name", "even", "bytes", "nullable" })
        {
            Require(actual.ApplicationProperties.TryGetValue(key, out object? value)
                && PropertyEquals(value, expected.ApplicationProperties[key]), $"member property {key} changed");
        }
        Require(actual.ApplicationProperties.ContainsKey("first-only")
            == expected.ApplicationProperties.ContainsKey("first-only"), "first-member properties leaked into another member");
    }

    private static bool PropertyEquals(object? actual, object? expected) =>
        actual is byte[] actualBytes && expected is byte[] expectedBytes
            ? actualBytes.SequenceEqual(expectedBytes)
            : Equals(actual, expected);

    private static async Task VerifyOversizedTryAddAsync(
        ServiceBusSender sender,
        ServiceBusReceiver receiver,
        string run,
        CancellationToken token)
    {
        using ServiceBusMessageBatch batch = await BoundedAsync(
            ct => sender.CreateMessageBatchAsync(new CreateMessageBatchOptions
            {
                MaxSizeInBytes = 4096,
            }, ct).AsTask(), token);
        ServiceBusMessage fitting = RichMessage(run + "-small-limit", 0);
        Require(batch.TryAddMessage(fitting), "a small message did not fit the explicit batch cap");
        int originalCount = batch.Count;
        long originalSize = batch.SizeInBytes;
        Require(!batch.TryAddMessage(new ServiceBusMessage(new byte[4097])),
            "TryAddMessage accepted an oversized message");
        Require(batch.Count == originalCount && batch.SizeInBytes == originalSize,
            "failed TryAddMessage changed the accepted batch");
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
        await VerifyAndCompleteAsync(receiver, new[] { fitting }, token);
        await AssertEmptyAsync(receiver, token);
    }

    private static async Task RejectInvalidSessionsAsync(
        ServiceBusClient client,
        string queue,
        ServiceBusSender sender,
        string scope,
        bool safeBatch,
        CancellationToken token)
    {
        string firstSession = scope + "-first";
        string secondSession = scope + "-second";
        ServiceBusSessionReceiver first = await BoundedAsync(
            ct => client.AcceptSessionAsync(queue, firstSession, cancellationToken: ct), token);
        ServiceBusSessionReceiver? second = null;
        try
        {
            second = await BoundedAsync(
                ct => client.AcceptSessionAsync(queue, secondSession, cancellationToken: ct), token);
            foreach (bool missing in new[] { false, true })
            {
                ServiceBusMessage[] invalid =
                {
                    RichMessage(scope, 0, firstSession),
                    RichMessage(scope, 1, missing ? null : secondSession),
                };
                string expectedReason = missing ? "requires a session" : "same session";
                bool rejected = false;
                try
                {
                    await SendAsync(sender, invalid, safeBatch, token);
                }
                catch (ServiceBusException error) when (
                    error.Reason == ServiceBusFailureReason.GeneralError
                    && error.Message.Contains(expectedReason, StringComparison.OrdinalIgnoreCase))
                {
                    rejected = true;
                }
                catch (InvalidOperationException error) when (
                    missing
                    && error.Message.Contains(expectedReason, StringComparison.OrdinalIgnoreCase))
                {
                    rejected = true;
                }
                Require(rejected, "a mixed or missing-session batch was acknowledged");
                await AssertEmptyAsync(first, token);
                await AssertEmptyAsync(second, token);
                ServiceBusMessage[] retry = Enumerable.Range(0, 3)
                    .Select(index => RichMessage(scope + $"-retry-{missing}", index, firstSession))
                    .ToArray();
                await SendAsync(sender, retry, safeBatch, token);
                await VerifyAndCompleteAsync(first, retry, token);
                await AssertEmptyAsync(first, token);
            }
        }
        finally
        {
            if (second is not null)
            {
                await DisposeAllAsync(second, first);
            }
            else
            {
                await DisposeBoundedAsync(first);
            }
        }
    }

    private static async Task VerifyDeduplicationAsync(
        ServiceBusSender sender,
        ServiceBusReceiver receiver,
        string scope,
        bool safeBatch,
        CancellationToken token)
    {
        ServiceBusMessage first = RichMessage(scope, 0);
        ServiceBusMessage duplicate = RichMessage(scope, 1);
        duplicate.MessageId = first.MessageId;
        ServiceBusMessage distinct = RichMessage(scope, 2);
        await SendAsync(sender, new[] { first, duplicate, distinct }, safeBatch, token);
        ServiceBusMessage[] replay = Enumerable.Range(0, 3)
            .Select(index => RichMessage(scope + "-replay", index))
            .ToArray();
        replay[0].MessageId = first.MessageId;
        replay[1].MessageId = first.MessageId;
        replay[2].MessageId = distinct.MessageId;
        await SendAsync(sender, replay, safeBatch, token);
        IReadOnlyList<ServiceBusReceivedMessage> peeked = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(4, fromSequenceNumber: 1, cancellationToken: ct), token);
        Require(peeked.Count == 2, "within-batch or replayed duplicates were retained");
        ServiceBusMessage[] expected = { first, distinct };
        int[] originalIndices = { 0, 2 };
        for (int index = 0; index < expected.Length; index++)
        {
            CheckContent(peeked[index], expected[index], originalIndices[index]);
            ServiceBusReceivedMessage? received = await BoundedAsync(
                ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
            Require(received is not null, "the original duplicate-detection member disappeared");
            CheckContent(received!, expected[index], originalIndices[index]);
            await BoundedAsync(ct => receiver.CompleteMessageAsync(received!, ct), token);
        }
        await SendAsync(sender, replay, safeBatch, token);
        await AssertEmptyAsync(receiver, token);
    }

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> messages = await BoundedAsync(
            ct => receiver.PeekMessagesAsync(1, fromSequenceNumber: 1, cancellationToken: ct), token);
        Require(messages.Count == 0, "the batch workflow left a retained member");
    }

    private static async Task BoundedAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task<T> BoundedAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        return await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task DisposeBoundedAsync(IAsyncDisposable resource) =>
        await resource.DisposeAsync().AsTask().WaitAsync(CleanupTimeout);

    private static async Task DisposeAllAsync(params IAsyncDisposable[] resources)
    {
        Exception? failure = null;
        foreach (IAsyncDisposable resource in resources)
        {
            try
            {
                await DisposeBoundedAsync(resource);
            }
            catch (Exception error)
            {
                failure ??= error;
            }
        }
        if (failure is not null)
        {
            throw new InvalidOperationException("batch conformance cleanup failed", failure);
        }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"batch conformance failed: {detail}");
        }
    }
}
