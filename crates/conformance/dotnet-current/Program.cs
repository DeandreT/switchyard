using Azure;
using Azure.Messaging.ServiceBus;

if (args.Length != 7)
{
    Console.Error.WriteLine(
        "usage: <namespace> <custom-endpoint> <queue> <session-queue> <duplicate-queue> <key-name> <key>");
    return 2;
}

string fullyQualifiedNamespace = args[0];
var customEndpoint = new Uri(args[1]);
string queue = args[2];
string sessionQueue = args[3];
string duplicateQueue = args[4];
string keyName = args[5];
string key = args[6];

var options = new ServiceBusClientOptions
{
    CustomEndpointAddress = customEndpoint,
    TransportType = ServiceBusTransportType.AmqpTcp,
    CertificateValidationCallback = (_, _, _, _) => true,
    RetryOptions =
    {
        MaxRetries = 0,
        TryTimeout = TimeSpan.FromSeconds(10),
    },
};

await using var client = new ServiceBusClient(
    fullyQualifiedNamespace,
    new AzureNamedKeyCredential(keyName, key),
    options);
await using ServiceBusSender sender = client.CreateSender(queue);
await using ServiceBusReceiver receiver = client.CreateReceiver(queue);

var original = new ServiceBusMessage("official-dotnet-current")
{
    MessageId = "preserved-id",
    CorrelationId = "preserved-correlation",
    Subject = "checkout",
    ContentType = "application/json",
    To = "logical-destination",
    ReplyTo = "replies",
    ReplyToSessionId = "reply-session",
    PartitionKey = "preserved-partition",
    TimeToLive = TimeSpan.FromDays(60),
};
original.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
original.GetRawAmqpMessage().Footer["producer-checksum"] = "checksum-1";
foreach (var pair in PreservedApplicationProperties())
{
    original.ApplicationProperties[pair.Key] = pair.Value;
}
await sender.SendMessageAsync(original);
IReadOnlyList<ServiceBusReceivedMessage> peeked =
    await receiver.PeekMessagesAsync(maxMessages: 1);
if (peeked.Count != 1 || !HasPreservedContent(peeked[0]) || peeked[0].DeliveryCount != 0)
{
    Console.Error.WriteLine(
        $"unexpected peek result: count={peeked.Count}, body={peeked.FirstOrDefault()?.Body}");
    return 3;
}

ServiceBusReceivedMessage? received =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (received is null)
{
    Console.Error.WriteLine("the official client did not receive its message");
    return 4;
}
if (!HasPreservedContent(received) || received.DeliveryCount != 1)
{
    Console.Error.WriteLine($"unexpected first delivery: body={received.Body}, count={received.DeliveryCount}");
    return 5;
}

DateTimeOffset lockedUntilBeforeRenewal = received.LockedUntil;
await receiver.RenewMessageLockAsync(received);
if (received.LockedUntil < lockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"renewal moved the lock backward: {lockedUntilBeforeRenewal:o} -> {received.LockedUntil:o}");
    return 6;
}

var abandonUpdates = new Dictionary<string, object>
{
    ["attempt"] = 2,
    ["nullable"] = null!,
    ["updated-at"] = new DateTimeOffset(2021, 2, 3, 4, 5, 6, TimeSpan.Zero),
};
await receiver.AbandonMessageAsync(received, abandonUpdates);
received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (received is null || !HasPreservedContent(received) || received.DeliveryCount != 2
    || !HasUpdatedProperties(received, abandonUpdates))
{
    Console.Error.WriteLine("message content did not survive redelivery");
    return 26;
}

await receiver.CompleteMessageAsync(received);

await sender.SendMessageAsync(new ServiceBusMessage("official-deferred-current"));
ServiceBusReceivedMessage? deferredSource =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deferredSource?.Body.ToString() != "official-deferred-current")
{
    Console.Error.WriteLine($"unexpected deferred source message: {deferredSource?.Body}");
    return 7;
}
var deferUpdates = new Dictionary<string, object>
{
    ["stage"] = "waiting",
    ["nullable"] = null!,
};
await receiver.DeferMessageAsync(deferredSource, deferUpdates);
ServiceBusReceivedMessage deferred =
    await receiver.ReceiveDeferredMessageAsync(deferredSource.SequenceNumber);
if (deferred.Body.ToString() != "official-deferred-current"
    || !HasUpdatedProperties(deferred, deferUpdates))
{
    Console.Error.WriteLine($"unexpected deferred message: {deferred.Body}");
    return 8;
}
var resumedUpdates = new Dictionary<string, object> { ["resumed"] = true };
await receiver.AbandonMessageAsync(deferred, resumedUpdates);
ServiceBusReceivedMessage? resumed =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (resumed?.Body.ToString() != "official-deferred-current"
    || !HasUpdatedProperties(resumed, deferUpdates)
    || !HasUpdatedProperties(resumed, resumedUpdates))
{
    Console.Error.WriteLine("management abandonment did not preserve property updates");
    return 27;
}
await receiver.DeferMessageAsync(resumed);
deferred = await receiver.ReceiveDeferredMessageAsync(resumed.SequenceNumber);
var managementDeferUpdates = new Dictionary<string, object> { ["stage"] = "deferred-again" };
await receiver.DeferMessageAsync(deferred, managementDeferUpdates);
deferred = await receiver.ReceiveDeferredMessageAsync(deferred.SequenceNumber);
if (!HasUpdatedProperties(deferred, managementDeferUpdates)
    || !HasUpdatedProperties(deferred, resumedUpdates))
{
    Console.Error.WriteLine("management deferral did not preserve property updates");
    return 28;
}
var managementDeadLetterUpdates = new Dictionary<string, object>
{
    ["DeadLetterReason"] = "deferred-by-client",
    ["DeadLetterErrorDescription"] = "the deferred work cannot continue",
    ["dead-letter-route"] = "management",
};
await receiver.DeadLetterMessageAsync(deferred, managementDeadLetterUpdates);
await using ServiceBusReceiver deadLetterReceiver = client.CreateReceiver(queue,
    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
ServiceBusReceivedMessage? deadLetter =
    await deadLetterReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deadLetter?.Body.ToString() != "official-deferred-current"
    || deadLetter.DeadLetterReason != "deferred-by-client"
    || deadLetter.DeadLetterErrorDescription != "the deferred work cannot continue"
    || !HasUpdatedProperties(deadLetter, managementDeadLetterUpdates)
    || !HasUpdatedProperties(deadLetter, managementDeferUpdates)
    || !HasUpdatedProperties(deadLetter, resumedUpdates))
{
    Console.Error.WriteLine("management dead-lettering lost the reason or property updates");
    return 29;
}
await deadLetterReceiver.CompleteMessageAsync(deadLetter);

await sender.SendMessageAsync(new ServiceBusMessage("official-direct-dead-letter-current"));
ServiceBusReceivedMessage? directDeadLetter =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (directDeadLetter?.Body.ToString() != "official-direct-dead-letter-current")
{
    Console.Error.WriteLine("the direct dead-letter source was not received");
    return 30;
}
var directDeadLetterUpdates = new Dictionary<string, object>
{
    ["attempt"] = 3,
    ["dead-letter-route"] = "link",
};
await receiver.DeadLetterMessageAsync(directDeadLetter, directDeadLetterUpdates,
    "invalid-request", "the request is incomplete");
deadLetter = await deadLetterReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (deadLetter?.Body.ToString() != "official-direct-dead-letter-current"
    || deadLetter.DeadLetterReason != "invalid-request"
    || deadLetter.DeadLetterErrorDescription != "the request is incomplete"
    || !HasUpdatedProperties(deadLetter, directDeadLetterUpdates))
{
    Console.Error.WriteLine("link dead-lettering lost the reason or property updates");
    return 31;
}
await deadLetterReceiver.CompleteMessageAsync(deadLetter);

await sender.SendMessageAsync(new ServiceBusMessage("official-expired-deferred-current")
{
    TimeToLive = TimeSpan.FromSeconds(5),
});
ServiceBusReceivedMessage? expiringDeferred =
    await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
if (expiringDeferred?.Body.ToString() != "official-expired-deferred-current")
{
    Console.Error.WriteLine("the expiring deferred source was not received");
    return 32;
}
await receiver.DeferMessageAsync(expiringDeferred);
TimeSpan untilExpired = expiringDeferred.ExpiresAt.AddMilliseconds(250) - DateTimeOffset.UtcNow;
if (untilExpired > TimeSpan.Zero)
{
    await Task.Delay(untilExpired);
}
if ((await receiver.PeekMessageAsync(expiringDeferred.SequenceNumber))?.State
    != ServiceBusMessageState.Deferred)
{
    Console.Error.WriteLine("an expired deferred message disappeared before retrieval");
    return 33;
}
try
{
    await receiver.ReceiveDeferredMessageAsync(expiringDeferred.SequenceNumber);
    Console.Error.WriteLine("an expired deferred message was delivered");
    return 34;
}
catch (ServiceBusException error) when (error.Reason == ServiceBusFailureReason.MessageNotFound)
{
}
if (await receiver.PeekMessageAsync(expiringDeferred.SequenceNumber) is not null
    || await deadLetterReceiver.PeekMessageAsync(expiringDeferred.SequenceNumber) is not null)
{
    Console.Error.WriteLine("default expiration did not remove the deferred message");
    return 35;
}

DateTimeOffset cancelEnqueueTime = DateTimeOffset.UtcNow.AddMinutes(1);
long cancelledSequence = await sender.ScheduleMessageAsync(
    new ServiceBusMessage("official-cancelled-current"), cancelEnqueueTime);
ServiceBusReceivedMessage? cancelledPeek =
    await receiver.PeekMessageAsync(cancelledSequence);
if (cancelledPeek?.Body.ToString() != "official-cancelled-current"
    || cancelledPeek.State != ServiceBusMessageState.Scheduled
    || cancelledPeek.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
        != cancelEnqueueTime.ToUnixTimeMilliseconds())
{
    Console.Error.WriteLine("the scheduled message was not exposed correctly by peek");
    return 13;
}
await sender.CancelScheduledMessageAsync(cancelledSequence);
if (await receiver.PeekMessageAsync(cancelledSequence) is not null)
{
    Console.Error.WriteLine("the cancelled scheduled message remained in the queue");
    return 14;
}

IReadOnlyList<long> cancelledBatchSequences = await sender.ScheduleMessagesAsync(
    new[]
    {
        new ServiceBusMessage("official-cancelled-batch-a-current"),
        new ServiceBusMessage("official-cancelled-batch-b-current"),
    }, cancelEnqueueTime);
IReadOnlyList<ServiceBusReceivedMessage> cancelledBatchPeek =
    await receiver.PeekMessagesAsync(2, cancelledBatchSequences[0]);
if (cancelledBatchSequences.Count != 2
    || cancelledBatchPeek.Count != 2
    || !cancelledBatchPeek.Select(message => message.SequenceNumber)
        .SequenceEqual(cancelledBatchSequences)
    || cancelledBatchPeek.Any(message => message.State != ServiceBusMessageState.Scheduled
        || message.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
            != cancelEnqueueTime.ToUnixTimeMilliseconds()))
{
    Console.Error.WriteLine("the scheduled batch was not exposed correctly by peek");
    return 15;
}
await sender.CancelScheduledMessagesAsync(cancelledBatchSequences);
if (await receiver.PeekMessageAsync(cancelledBatchSequences[0]) is not null)
{
    Console.Error.WriteLine("the cancelled scheduled batch remained in the queue");
    return 16;
}

DateTimeOffset scheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(2);
IReadOnlyList<long> scheduledSequences = await sender.ScheduleMessagesAsync(
    new[]
    {
        new ServiceBusMessage("official-scheduled-batch-a-current"),
        new ServiceBusMessage("official-scheduled-batch-b-current"),
    }, scheduledEnqueueTime);
await sender.SendMessageAsync(new ServiceBusMessage("official-scheduled-transfer-current")
{
    ScheduledEnqueueTime = scheduledEnqueueTime,
});
IReadOnlyList<ServiceBusReceivedMessage> scheduledPeek =
    await receiver.PeekMessagesAsync(3, scheduledSequences[0]);
if (scheduledSequences.Count != 2
    || scheduledPeek.Count != 3
    || !scheduledPeek.Take(2).Select(message => message.SequenceNumber)
        .SequenceEqual(scheduledSequences)
    || scheduledPeek.Any(message => message.State != ServiceBusMessageState.Scheduled
        || message.DeliveryCount != 0
        || message.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
            != scheduledEnqueueTime.ToUnixTimeMilliseconds()))
{
    Console.Error.WriteLine("the pending scheduled messages were not exposed correctly by peek");
    return 17;
}
var pendingSequences = scheduledPeek.ToDictionary(
    message => message.Body.ToString(), message => message.SequenceNumber);
for (int index = 0; index < scheduledPeek.Count; index++)
{
    ServiceBusReceivedMessage? scheduledMessage =
        await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    if (scheduledMessage is null
        || !pendingSequences.Remove(scheduledMessage.Body.ToString(), out long pendingSequence)
        || scheduledMessage.SequenceNumber == pendingSequence
        || scheduledMessage.State != ServiceBusMessageState.Active
        || scheduledMessage.DeliveryCount != 1
        || scheduledMessage.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
            != scheduledEnqueueTime.ToUnixTimeMilliseconds()
        || DateTimeOffset.UtcNow < scheduledMessage.ScheduledEnqueueTime
        || scheduledMessage.EnqueuedTime < scheduledMessage.ScheduledEnqueueTime)
    {
        Console.Error.WriteLine($"unexpected activated scheduled message: {scheduledMessage?.Body}");
        return 18;
    }
    await receiver.CompleteMessageAsync(scheduledMessage);
}

await using ServiceBusSender sessionSender = client.CreateSender(sessionQueue);
await sessionSender.SendMessageAsync(new ServiceBusMessage("official-session-current")
{
    SessionId = "session-1",
});
await using ServiceBusSessionReceiver sessionReceiver =
    await client.AcceptSessionAsync(sessionQueue, "session-1");

IReadOnlyList<ServiceBusReceivedMessage> peekedSession =
    await sessionReceiver.PeekMessagesAsync(maxMessages: 1);
if (peekedSession.Count != 1 || peekedSession[0].Body.ToString() != "official-session-current")
{
    Console.Error.WriteLine(
        $"unexpected session peek result: count={peekedSession.Count}, body={peekedSession.FirstOrDefault()?.Body}");
    return 9;
}

await sessionReceiver.SetSessionStateAsync(BinaryData.FromString("checkout-step-2"));
BinaryData sessionState = await sessionReceiver.GetSessionStateAsync();
if (sessionState.ToString() != "checkout-step-2")
{
    Console.Error.WriteLine($"unexpected session state: {sessionState}");
    return 10;
}

DateTimeOffset sessionLockedUntilBeforeRenewal = sessionReceiver.SessionLockedUntil;
await sessionReceiver.RenewSessionLockAsync();
if (sessionReceiver.SessionLockedUntil < sessionLockedUntilBeforeRenewal)
{
    Console.Error.WriteLine(
        $"session renewal moved the lock backward: {sessionLockedUntilBeforeRenewal:o} -> {sessionReceiver.SessionLockedUntil:o}");
    return 11;
}

ServiceBusReceivedMessage? sessionMessage =
    await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (sessionMessage?.Body.ToString() != "official-session-current"
    || sessionMessage.DeliveryCount != 1)
{
    Console.Error.WriteLine($"unexpected session message: {sessionMessage?.Body}");
    return 12;
}
var sessionDeferUpdates = new Dictionary<string, object> { ["stage"] = "session-waiting" };
await sessionReceiver.DeferMessageAsync(sessionMessage, sessionDeferUpdates);
ServiceBusReceivedMessage sessionDeferred =
    await sessionReceiver.ReceiveDeferredMessageAsync(sessionMessage.SequenceNumber);
if (sessionDeferred.Body.ToString() != "official-session-current"
    || sessionDeferred.SessionId != "session-1"
    || !HasUpdatedProperties(sessionDeferred, sessionDeferUpdates))
{
    Console.Error.WriteLine("the held session could not retrieve its deferred message");
    return 36;
}
await sessionReceiver.RenewSessionLockAsync();
await sessionReceiver.CompleteMessageAsync(sessionDeferred);

DateTimeOffset scheduledSessionEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(2);
long scheduledSessionSequence = await sessionSender.ScheduleMessageAsync(
    new ServiceBusMessage("official-scheduled-session-current")
    {
        SessionId = "session-1",
    }, scheduledSessionEnqueueTime);
ServiceBusReceivedMessage? scheduledSessionPeek =
    await sessionReceiver.PeekMessageAsync(scheduledSessionSequence);
if (scheduledSessionPeek?.Body.ToString() != "official-scheduled-session-current"
    || scheduledSessionPeek.State != ServiceBusMessageState.Scheduled
    || scheduledSessionPeek.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
        != scheduledSessionEnqueueTime.ToUnixTimeMilliseconds())
{
    Console.Error.WriteLine("the scheduled session message was not exposed correctly by peek");
    return 19;
}
ServiceBusReceivedMessage? scheduledSessionMessage =
    await sessionReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (scheduledSessionMessage?.Body.ToString() != "official-scheduled-session-current"
    || scheduledSessionMessage.SessionId != "session-1"
    || scheduledSessionMessage.SequenceNumber == scheduledSessionSequence
    || scheduledSessionMessage.State != ServiceBusMessageState.Active
    || scheduledSessionMessage.DeliveryCount != 1
    || scheduledSessionMessage.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
        != scheduledSessionEnqueueTime.ToUnixTimeMilliseconds()
    || DateTimeOffset.UtcNow < scheduledSessionMessage.ScheduledEnqueueTime
    || scheduledSessionMessage.EnqueuedTime < scheduledSessionMessage.ScheduledEnqueueTime)
{
    Console.Error.WriteLine($"unexpected activated session message: {scheduledSessionMessage?.Body}");
    return 20;
}
await sessionReceiver.CompleteMessageAsync(scheduledSessionMessage);

await using ServiceBusSender duplicateSender = client.CreateSender(duplicateQueue);
await using ServiceBusReceiver duplicateReceiver = client.CreateReceiver(duplicateQueue);
const string duplicateMessageId = "official-duplicate-current";
await duplicateSender.SendMessageAsync(new ServiceBusMessage("official-duplicate-original-current")
{
    MessageId = duplicateMessageId,
});
await duplicateSender.SendMessageAsync(new ServiceBusMessage("official-duplicate-dropped-current")
{
    MessageId = duplicateMessageId,
});
IReadOnlyList<ServiceBusReceivedMessage> duplicatePeek =
    await duplicateReceiver.PeekMessagesAsync(2, fromSequenceNumber: 1);
if (duplicatePeek.Count != 1
    || duplicatePeek[0].Body.ToString() != "official-duplicate-original-current"
    || duplicatePeek[0].MessageId != duplicateMessageId)
{
    Console.Error.WriteLine("duplicate sends were not acknowledged and reduced to the original message");
    return 22;
}
ServiceBusReceivedMessage? duplicateOriginal =
    await duplicateReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (duplicateOriginal?.Body.ToString() != "official-duplicate-original-current")
{
    Console.Error.WriteLine($"unexpected duplicate original message: {duplicateOriginal?.Body}");
    return 23;
}
await duplicateReceiver.CompleteMessageAsync(duplicateOriginal);
await duplicateSender.SendMessageAsync(new ServiceBusMessage("official-duplicate-after-complete-current")
{
    MessageId = duplicateMessageId,
});
await duplicateSender.ScheduleMessageAsync(
    new ServiceBusMessage("official-scheduled-duplicate-of-completed-current")
    {
        MessageId = duplicateMessageId,
    }, DateTimeOffset.UtcNow.AddMinutes(1));
if (await duplicateReceiver.PeekMessageAsync(fromSequenceNumber: 1) is not null)
{
    Console.Error.WriteLine("completing a message erased its duplicate-detection history");
    return 24;
}

const string scheduledDuplicateMessageId = "official-scheduled-duplicate-current";
const string scheduledBatchDuplicateMessageId = "official-scheduled-batch-duplicate-current";
DateTimeOffset duplicateEnqueueTime = DateTimeOffset.UtcNow.AddMinutes(1);
long scheduledDuplicateOriginal = await duplicateSender.ScheduleMessageAsync(
    new ServiceBusMessage("official-scheduled-duplicate-original-current")
    {
        MessageId = scheduledDuplicateMessageId,
    }, duplicateEnqueueTime);
await duplicateSender.SendMessageAsync(new ServiceBusMessage("official-ordinary-duplicate-of-scheduled-current")
{
    MessageId = scheduledDuplicateMessageId,
});
IReadOnlyList<long> duplicateScheduledBatchSequences =
    await duplicateSender.ScheduleMessagesAsync(new[]
    {
        new ServiceBusMessage("official-scheduled-existing-duplicate-a-current")
        {
            MessageId = scheduledDuplicateMessageId,
        },
        new ServiceBusMessage("official-scheduled-existing-duplicate-b-current")
        {
            MessageId = scheduledDuplicateMessageId,
        },
        new ServiceBusMessage("official-scheduled-batch-original-current")
        {
            MessageId = scheduledBatchDuplicateMessageId,
        },
        new ServiceBusMessage("official-scheduled-batch-duplicate-current")
        {
            MessageId = scheduledBatchDuplicateMessageId,
        },
    }, duplicateEnqueueTime);
IReadOnlyList<ServiceBusReceivedMessage> duplicateScheduledPeek =
    await duplicateReceiver.PeekMessagesAsync(5, fromSequenceNumber: 1);
if (duplicateScheduledBatchSequences.Count != 4
    || duplicateScheduledBatchSequences.Distinct().Count() != 4
    || duplicateScheduledBatchSequences.Contains(scheduledDuplicateOriginal)
    || duplicateScheduledPeek.Count != 2
    || duplicateScheduledPeek[0].SequenceNumber != scheduledDuplicateOriginal
    || duplicateScheduledPeek[0].Body.ToString() != "official-scheduled-duplicate-original-current"
    || duplicateScheduledPeek[1].SequenceNumber != duplicateScheduledBatchSequences[2]
    || duplicateScheduledPeek[1].Body.ToString() != "official-scheduled-batch-original-current"
    || duplicateScheduledPeek.Any(message => message.State != ServiceBusMessageState.Scheduled
        || message.ScheduledEnqueueTime.ToUnixTimeMilliseconds()
            != duplicateEnqueueTime.ToUnixTimeMilliseconds()))
{
    Console.Error.WriteLine("scheduled duplicate detection did not preserve only the original messages");
    return 25;
}
await duplicateSender.CancelScheduledMessagesAsync(new[]
{
    scheduledDuplicateOriginal,
    duplicateScheduledBatchSequences[2],
});

if (await receiver.PeekMessageAsync(fromSequenceNumber: 1) is not null
    || await sessionReceiver.PeekMessageAsync(fromSequenceNumber: 1) is not null
    || await duplicateReceiver.PeekMessageAsync(fromSequenceNumber: 1) is not null)
{
    Console.Error.WriteLine("the official client left messages in a queue");
    return 21;
}

await BatchCases.RunAsync(client, queue + "-batches", sessionQueue + "-batches",
    duplicateQueue + "-batches");
await TopicCases.RunAsync(client, queue + "-topics", "Alpha", "beta");
await TopicSessionCases.RunAsync(client, queue + "-topic-sessions", "Alpha", "beta", "ordinary");
await SessionPeekCases.RunAsync(client, sessionQueue + "-peek");
await TopicSchedulingCases.RunAsync(client, queue + "-topic-scheduling",
    queue + "-topic-scheduling-sessions");

Console.WriteLine(
    "official .NET Service Bus client send/batch/peek/receive/settlement updates/defer/dead-letter/expiry/renew/complete/schedule/cancel/duplicate and session renew/state/deferred receive passed");
return 0;

static Dictionary<string, object> PreservedApplicationProperties() => new()
{
    ["byte"] = (byte)7,
    ["sbyte"] = (sbyte)-7,
    ["char"] = 'Q',
    ["short"] = (short)-300,
    ["ushort"] = (ushort)300,
    ["int"] = -70000,
    ["uint"] = 70000U,
    ["long"] = -5000000000L,
    ["ulong"] = 5000000000UL,
    ["float"] = -1.25F,
    ["double"] = 1.125D,
    ["decimal"] = 12.375M,
    ["bool"] = true,
    ["guid"] = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
    ["string"] = "custom-value",
    ["uri"] = new Uri("https://example.org/orders/1"),
    ["date-time"] = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc),
    ["date-time-offset"] = new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.Zero),
    ["time-span"] = TimeSpan.FromTicks(123456789),
    ["binary"] = new byte[] { 1, 2, 3, 4 },
};

static bool HasPreservedContent(ServiceBusReceivedMessage message)
{
    if (message.Body.ToString() != "official-dotnet-current"
        || message.MessageId != "preserved-id"
        || message.CorrelationId != "preserved-correlation"
        || message.Subject != "checkout"
        || message.ContentType != "application/json"
        || message.To != "logical-destination"
        || message.ReplyTo != "replies"
        || message.ReplyToSessionId != "reply-session"
        || message.PartitionKey != "preserved-partition"
        || message.TimeToLive != TimeSpan.FromDays(60)
        || message.ExpiresAt != message.EnqueuedTime.AddDays(60)
        || message.GetRawAmqpMessage().Properties.ContentEncoding != "utf-8"
        || !message.GetRawAmqpMessage().Footer.TryGetValue("producer-checksum", out object? checksum)
        || !Equals(checksum, "checksum-1"))
    {
        return false;
    }
    return HasUpdatedProperties(message, PreservedApplicationProperties());
}

static bool HasUpdatedProperties(ServiceBusReceivedMessage message,
    IDictionary<string, object> expected)
{
    foreach (var pair in expected)
    {
        if (!message.ApplicationProperties.TryGetValue(pair.Key, out object? actual))
        {
            return false;
        }
        if (pair.Value is null)
        {
            if (actual is not null)
            {
                return false;
            }
            continue;
        }
        if (actual is null || actual.GetType() != pair.Value.GetType())
        {
            return false;
        }
        if (pair.Value is byte[] expectedBytes)
        {
            if (!((byte[])actual).SequenceEqual(expectedBytes))
            {
                return false;
            }
        }
        else if (!Equals(actual, pair.Value))
        {
            return false;
        }
    }
    return true;
}
