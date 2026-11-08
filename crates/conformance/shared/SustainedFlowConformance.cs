using Azure;
using Azure.Messaging.ServiceBus;

internal static class SustainedFlowConformance
{
    internal const int Count = 2305;

    internal static async Task<int> RunAsync(
        string host, string endpoint, string queue, string keyName, string key)
    {
        // Fixture-only trust bypass, unchanged from the TCP dispatcher. This is
        // not production TLS validation and never changes machine trust.
        var options = new ServiceBusClientOptions
        {
            CustomEndpointAddress = new Uri(endpoint),
            TransportType = ServiceBusTransportType.AmqpTcp,
            CertificateValidationCallback = (_, _, _, _) => true,
            RetryOptions = { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(30) },
        };
        await using var client = new ServiceBusClient(host, new AzureNamedKeyCredential(keyName, key), options);
        // Keep the same sender for every individual (not batched) delivery.
        await using ServiceBusSender sender = client.CreateSender(queue);
        for (int index = 0; index < Count; index++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"sustained-{index}")
            {
                MessageId = $"sustained-{index}",
            });
        }
        Console.WriteLine($"SUSTAINED_SEND_VERIFIED count={Count} senders=1 retries=0");
        return 0;
    }
}
