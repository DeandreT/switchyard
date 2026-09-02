using Azure.Messaging.ServiceBus;

internal static class CaseInsensitiveIdentityConformance
{
    internal static async Task<string?> RunAsync(
        ServiceBusClient client,
        string queue,
        string topic,
        string subscription)
    {
        const string queueBody = "official-case-insensitive-queue";
        await using (ServiceBusSender sender = client.CreateSender(InvertAsciiCase(queue)))
        await using (ServiceBusReceiver receiver = client.CreateReceiver(queue.ToUpperInvariant()))
        {
            await sender.SendMessageAsync(new ServiceBusMessage(queueBody)
            {
                MessageId = queueBody,
            });
            ServiceBusReceivedMessage? received =
                await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (received?.Body.ToString() != queueBody || received.MessageId != queueBody)
            {
                return $"mixed-case queue identity returned {received?.Body}";
            }
            await receiver.CompleteMessageAsync(received);
        }

        const string topicBody = "official-case-insensitive-topic";
        await using (ServiceBusSender sender = client.CreateSender(topic.ToLowerInvariant()))
        await using (ServiceBusReceiver receiver = client.CreateReceiver(
            InvertAsciiCase(topic),
            subscription.ToUpperInvariant()))
        {
            await sender.SendMessageAsync(new ServiceBusMessage(topicBody)
            {
                MessageId = topicBody,
            });
            ServiceBusReceivedMessage? received =
                await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (received?.Body.ToString() != topicBody || received.MessageId != topicBody)
            {
                return $"mixed-case topic/subscription identity returned {received?.Body}";
            }
            await receiver.CompleteMessageAsync(received);
        }

        return null;
    }

    private static string InvertAsciiCase(string value) =>
        string.Concat(value.Select(character => character switch
        {
            >= 'a' and <= 'z' => char.ToUpperInvariant(character),
            >= 'A' and <= 'Z' => char.ToLowerInvariant(character),
            _ => character,
        }));
}
