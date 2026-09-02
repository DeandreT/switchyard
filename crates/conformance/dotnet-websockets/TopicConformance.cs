using Azure.Messaging.ServiceBus;

internal static class TopicConformance
{
    internal static async Task<bool> RunAsync(
        ServiceBusClient client,
        string topic,
        string firstSubscription,
        string secondSubscription)
    {
        await using ServiceBusSender sender = client.CreateSender(topic);
        await using ServiceBusReceiver first = client.CreateReceiver(topic, firstSubscription);
        await using ServiceBusReceiver second = client.CreateReceiver(topic, secondSubscription);
        await sender.SendMessageAsync(new ServiceBusMessage("websocket-topic-broadcast")
        {
            MessageId = "websocket-topic-broadcast",
            Subject = "topic.websocket",
        });

        ServiceBusReceivedMessage? firstCopy =
            await first.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        ServiceBusReceivedMessage? secondCopy =
            await second.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (firstCopy?.Body.ToString() != "websocket-topic-broadcast" ||
            secondCopy?.Body.ToString() != "websocket-topic-broadcast" ||
            firstCopy.Subject != "topic.websocket" ||
            secondCopy.Subject != "topic.websocket" ||
            firstCopy.SequenceNumber != secondCopy.SequenceNumber)
        {
            return false;
        }

        await first.CompleteMessageAsync(firstCopy);
        await second.CompleteMessageAsync(secondCopy);
        return true;
    }
}
