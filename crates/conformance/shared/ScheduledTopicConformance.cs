using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static class ScheduledTopicConformance
{
    private const string SelectedSubject = "topic.scheduled.selected";

    internal static async Task<string?> RunAsync(
        ServiceBusClient client,
        string topic,
        string filteredSubscription,
        ServiceBusSender sender,
        ServiceBusReceiver defaultReceiver,
        ServiceBusReceiver filteredReceiver)
    {
        await using ServiceBusRuleManager rules =
            client.CreateRuleManager(topic, filteredSubscription);
        await using ServiceBusReceiver topicBrowser = client.CreateReceiver(topic);

        DateTimeOffset dueAt = DateTimeOffset.UtcNow.AddSeconds(10);
        long singleSequence = await sender.ScheduleMessageAsync(
            Message("topic-scheduled-single", SelectedSubject, "management-single"),
            dueAt);

        var scheduledBatch = new[]
        {
            Message("topic-scheduled-batch-selected", SelectedSubject, "management-batch"),
            Message("topic-scheduled-batch-default-only", "topic.scheduled.other", "management-batch"),
        };
        IReadOnlyList<long> batchSequences =
            await sender.ScheduleMessagesAsync(scheduledBatch, dueAt);
        if (batchSequences.Count != scheduledBatch.Length ||
            batchSequences[0] <= singleSequence ||
            batchSequences[1] != batchSequences[0] + 1)
        {
            return "scheduled topic batch did not return consecutive placeholder sequences";
        }

        long cancelledSequence = await sender.ScheduleMessageAsync(
            Message("topic-scheduled-cancelled", SelectedSubject, "cancel-single"),
            dueAt);
        await sender.CancelScheduledMessageAsync(cancelledSequence);

        IReadOnlyList<long> cancelledBatch = await sender.ScheduleMessagesAsync(
            new[]
            {
                Message("topic-scheduled-cancelled-batch-0", SelectedSubject, "cancel-batch"),
                Message("topic-scheduled-cancelled-batch-1", SelectedSubject, "cancel-batch"),
            },
            dueAt);
        await sender.CancelScheduledMessagesAsync(cancelledBatch);

        var annotated = Message(
            "topic-scheduled-annotated",
            SelectedSubject,
            "annotated-transfer");
        annotated.ScheduledEnqueueTime = dueAt;
        await sender.SendMessageAsync(annotated);

        IReadOnlyList<ServiceBusReceivedMessage> placeholders =
            await topicBrowser.PeekMessagesAsync(20, singleSequence);
        string? placeholderFailure = ValidatePlaceholders(
            placeholders,
            singleSequence,
            batchSequences,
            cancelledSequence,
            cancelledBatch,
            dueAt);
        if (placeholderFailure is not null)
        {
            return placeholderFailure;
        }

        // Scheduling owns a topic placeholder, not an eagerly materialized set
        // of subscription copies. Changing this subscription after scheduling
        // therefore proves the rules are evaluated when the timer activates the
        // publication.
        await rules.DeleteRuleAsync(RuleProperties.DefaultRuleName);
        await rules.CreateRuleAsync(
            "Scheduled-Subject",
            new CorrelationRuleFilter { Subject = SelectedSubject });

        if (await defaultReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(250)) is not null ||
            await filteredReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(250)) is not null)
        {
            return "a scheduled topic publication reached a subscription before its due time";
        }

        IReadOnlyList<ServiceBusReceivedMessage> defaultCopies =
            await ReceiveExactlyAsync(defaultReceiver, 4);
        IReadOnlyList<ServiceBusReceivedMessage> filteredCopies =
            await ReceiveExactlyAsync(filteredReceiver, 3);
        string? deliveryFailure = ValidateDeliveries(defaultCopies, filteredCopies, dueAt);
        if (deliveryFailure is not null)
        {
            return deliveryFailure;
        }

        await CompleteAllAsync(defaultReceiver, defaultCopies);
        await CompleteAllAsync(filteredReceiver, filteredCopies);
        if (await defaultReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null ||
            await filteredReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "cancelled or nonmatching scheduled topic publications remained deliverable";
        }

        return null;
    }

    private static ServiceBusMessage Message(string body, string subject, string path)
    {
        var message = new ServiceBusMessage(body)
        {
            MessageId = body,
            Subject = subject,
            CorrelationId = "scheduled-topic-correlation",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.ApplicationProperties["schedule-path"] = path;
        return message;
    }

    private static string? ValidatePlaceholders(
        IReadOnlyList<ServiceBusReceivedMessage> placeholders,
        long singleSequence,
        IReadOnlyList<long> batchSequences,
        long cancelledSequence,
        IReadOnlyList<long> cancelledBatch,
        DateTimeOffset dueAt)
    {
        var byBody = placeholders.ToDictionary(message => message.Body.ToString());
        string[] expected =
        {
            "topic-scheduled-single",
            "topic-scheduled-batch-selected",
            "topic-scheduled-batch-default-only",
            "topic-scheduled-annotated",
        };
        if (!expected.All(byBody.ContainsKey))
        {
            return $"topic peek did not expose every scheduled placeholder: " +
                $"[{string.Join(", ", byBody.Keys)}]";
        }
        string[] cancelled =
        {
            "topic-scheduled-cancelled",
            "topic-scheduled-cancelled-batch-0",
            "topic-scheduled-cancelled-batch-1",
        };
        if (cancelled.Any(byBody.ContainsKey) ||
            placeholders.Any(message => message.SequenceNumber == cancelledSequence ||
                cancelledBatch.Contains(message.SequenceNumber)))
        {
            return "topic cancellation left a scheduled placeholder behind";
        }
        if (byBody["topic-scheduled-single"].SequenceNumber != singleSequence ||
            byBody["topic-scheduled-batch-selected"].SequenceNumber != batchSequences[0] ||
            byBody["topic-scheduled-batch-default-only"].SequenceNumber != batchSequences[1])
        {
            return "topic peek changed a management-scheduled placeholder sequence";
        }
        foreach (string body in expected)
        {
            ServiceBusReceivedMessage placeholder = byBody[body];
            if (placeholder.State != ServiceBusMessageState.Scheduled ||
                placeholder.DeliveryCount != 0 ||
                (placeholder.ScheduledEnqueueTime - dueAt).Duration() > TimeSpan.FromSeconds(1))
            {
                return $"invalid scheduled topic placeholder for {body}: " +
                    $"state={placeholder.State}, delivery={placeholder.DeliveryCount}, " +
                    $"scheduled={placeholder.ScheduledEnqueueTime:o}";
            }
        }
        return null;
    }

    private static string? ValidateDeliveries(
        IReadOnlyList<ServiceBusReceivedMessage> defaultCopies,
        IReadOnlyList<ServiceBusReceivedMessage> filteredCopies,
        DateTimeOffset dueAt)
    {
        string[] allExpected =
        {
            "topic-scheduled-single",
            "topic-scheduled-batch-selected",
            "topic-scheduled-batch-default-only",
            "topic-scheduled-annotated",
        };
        string[] filteredExpected =
        {
            "topic-scheduled-single",
            "topic-scheduled-batch-selected",
            "topic-scheduled-annotated",
        };
        if (!Bodies(defaultCopies).Order().SequenceEqual(allExpected.Order()))
        {
            return $"default subscription received the wrong scheduled topic copies: " +
                $"[{string.Join(", ", Bodies(defaultCopies))}]";
        }
        if (!Bodies(filteredCopies).Order().SequenceEqual(filteredExpected.Order()))
        {
            return $"correlation rule selected the wrong scheduled topic copies: " +
                $"[{string.Join(", ", Bodies(filteredCopies))}]";
        }
        foreach (ServiceBusReceivedMessage delivery in defaultCopies.Concat(filteredCopies))
        {
            if (delivery.State != ServiceBusMessageState.Active ||
                delivery.CorrelationId != "scheduled-topic-correlation" ||
                delivery.EnqueuedTime < dueAt - TimeSpan.FromSeconds(1) ||
                (delivery.ScheduledEnqueueTime - dueAt).Duration() > TimeSpan.FromSeconds(1) ||
                delivery.ExpiresAt - delivery.EnqueuedTime != TimeSpan.FromMinutes(2))
            {
                return $"invalid activated scheduled topic copy {delivery.Body}: " +
                    $"state={delivery.State}, enqueued={delivery.EnqueuedTime:o}, " +
                    $"scheduled={delivery.ScheduledEnqueueTime:o}, expires={delivery.ExpiresAt:o}";
            }
        }
        return null;
    }

    private static IEnumerable<string> Bodies(
        IEnumerable<ServiceBusReceivedMessage> messages) =>
        messages.Select(message => message.Body.ToString());

    private static async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveExactlyAsync(
        ServiceBusReceiver receiver,
        int count)
    {
        var messages = new List<ServiceBusReceivedMessage>(count);
        while (messages.Count < count)
        {
            ServiceBusReceivedMessage? message =
                await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(12));
            if (message is null)
            {
                break;
            }
            messages.Add(message);
        }
        return messages;
    }

    private static Task CompleteAllAsync(
        ServiceBusReceiver receiver,
        IEnumerable<ServiceBusReceivedMessage> messages) =>
        Task.WhenAll(messages.Select(message => receiver.CompleteMessageAsync(message)));
}
