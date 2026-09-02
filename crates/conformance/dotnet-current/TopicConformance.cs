using Azure.Messaging.ServiceBus;

internal static class TopicConformance
{
    internal static async Task<int> RunAsync(
        ServiceBusClient client,
        string topic,
        string firstSubscription,
        string secondSubscription)
    {
        await using ServiceBusSender sender = client.CreateSender(topic);
        await using ServiceBusReceiver first = client.CreateReceiver(topic, firstSubscription);
        await using ServiceBusReceiver second = client.CreateReceiver(topic, secondSubscription);

        var published = new ServiceBusMessage("official-topic-broadcast")
        {
            MessageId = "official-topic-broadcast",
            Subject = "topic.broadcast",
            CorrelationId = "topic-correlation",
        };
        published.ApplicationProperties["fanout"] = "default-rule";
        await sender.SendMessageAsync(published);

        ServiceBusReceivedMessage? firstCopy =
            await first.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        ServiceBusReceivedMessage? secondCopy =
            await second.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (!IsBroadcastCopy(firstCopy) || !IsBroadcastCopy(secondCopy))
        {
            Console.Error.WriteLine(
                $"topic fan-out lost an envelope: first={firstCopy?.Body}, " +
                $"second={secondCopy?.Body}");
            return 90;
        }
        if (firstCopy!.SequenceNumber != secondCopy!.SequenceNumber)
        {
            Console.Error.WriteLine(
                $"topic copies disagree on sequence: {firstCopy.SequenceNumber} and " +
                $"{secondCopy.SequenceNumber}");
            return 91;
        }

        await first.DeadLetterMessageAsync(
            firstCopy,
            new Dictionary<string, object> { ["reviewed-by"] = "accounting" },
            "TopicConformance",
            "subscription copies have independent settlement");
        await second.CompleteMessageAsync(secondCopy);

        await using (ServiceBusReceiver deadLetters = client.CreateReceiver(
            topic,
            firstSubscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }))
        {
            ServiceBusReceivedMessage? deadLetter =
                await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (deadLetter?.Body.ToString() != "official-topic-broadcast" ||
                deadLetter.DeadLetterReason != "TopicConformance" ||
                deadLetter.DeadLetterSource is not null ||
                !Equals(deadLetter.ApplicationProperties["reviewed-by"], "accounting"))
            {
                Console.Error.WriteLine(
                    $"unexpected subscription dead letter: body={deadLetter?.Body}, " +
                    $"reason={deadLetter?.DeadLetterReason}");
                return 92;
            }
            await deadLetters.CompleteMessageAsync(deadLetter);
        }

        using ServiceBusMessageBatch batch = await sender.CreateMessageBatchAsync();
        for (int index = 0; index < 3; index++)
        {
            if (!batch.TryAddMessage(new ServiceBusMessage($"topic-batch-{index}")
            {
                MessageId = $"topic-batch-{index}",
                Subject = "topic.batch",
            }))
            {
                Console.Error.WriteLine($"topic batch child {index} did not fit");
                return 93;
            }
        }
        await sender.SendMessagesAsync(batch);

        IReadOnlyList<ServiceBusReceivedMessage> firstBatch =
            await ReceiveExactlyAsync(first, 3);
        IReadOnlyList<ServiceBusReceivedMessage> secondBatch =
            await ReceiveExactlyAsync(second, 3);
        if (!IsBatchCopy(firstBatch) || !IsBatchCopy(secondBatch))
        {
            Console.Error.WriteLine(
                $"topic batch fan-out was incomplete: first={firstBatch.Count}, " +
                $"second={secondBatch.Count}");
            return 94;
        }
        for (int index = 0; index < 3; index++)
        {
            if (firstBatch[index].SequenceNumber != secondBatch[index].SequenceNumber)
            {
                Console.Error.WriteLine($"topic batch copy {index} has different sequences");
                return 95;
            }
        }
        await Task.WhenAll(firstBatch.Select(message => first.CompleteMessageAsync(message)));
        await Task.WhenAll(
            secondBatch.Reverse().Select(message => second.CompleteMessageAsync(message)));

        return 0;
    }

    private static bool IsBroadcastCopy(ServiceBusReceivedMessage? message) =>
        message?.Body.ToString() == "official-topic-broadcast" &&
        message.MessageId == "official-topic-broadcast" &&
        message.Subject == "topic.broadcast" &&
        message.CorrelationId == "topic-correlation" &&
        Equals(message.ApplicationProperties["fanout"], "default-rule");

    private static bool IsBatchCopy(IReadOnlyList<ServiceBusReceivedMessage> messages) =>
        messages.Count == 3 && messages.Select((message, index) =>
            message.Body.ToString() == $"topic-batch-{index}" &&
            message.MessageId == $"topic-batch-{index}" &&
            message.Subject == "topic.batch").All(matches => matches);

    private static async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveExactlyAsync(
        ServiceBusReceiver receiver,
        int count)
    {
        var messages = new List<ServiceBusReceivedMessage>(count);
        while (messages.Count < count)
        {
            ServiceBusReceivedMessage? message =
                await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (message is null)
            {
                break;
            }
            messages.Add(message);
        }
        return messages;
    }
}
