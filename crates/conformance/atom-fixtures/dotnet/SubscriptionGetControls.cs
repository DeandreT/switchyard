using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using static FixtureInputs;

// Reads actual bounded response bytes/status/media captured by Rust's real
// HTTPS + owner tests. The SDK transport itself never calls a network service.
internal static class SubscriptionGetControls
{
    internal static async Task RunAsync(FixtureInputs inputs)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        foreach (string backend in new[] { "memory", "fjall" })
        {
            foreach (string name in new[] { "normal", "min", "max", "unlimited", "missing", "corrupt", "unsupported", "stopped", "invalid" })
            {
                using var capture = JsonDocument.Parse(inputs.ReadSubscriptionCapture(backend, name));
                JsonElement root = capture.RootElement;
                Require(root.EnumerateObject().Count() == 3, "unexpected capture fields");
                int status = root.GetProperty("status").GetInt32();
                string media = root.GetProperty("content_type").GetString()!;
                string xml = root.GetProperty("body").GetString()!;
                Require(Encoding.UTF8.GetByteCount(xml) <= 4096, "capture body exceeds profile bound");
                int expected = name switch { "missing" => 404, "corrupt" => 500, "unsupported" => 501, "stopped" => 503, "invalid" => 400, _ => 200 };
                Require(status == expected, "wrong real HTTP capture status");
                Require(media == (status == 200 ? "application/atom+xml; charset=utf-8" : status == 400 ? "text/plain; charset=utf-8" : "application/xml; charset=utf-8"), "wrong captured media type");
                using var transport = new InMemoryTransport(status, xml, media);
                var options = new ServiceBusAdministrationClientOptions { Transport = transport, Retry = { MaxRetries = 0 } };
                var client = new ServiceBusAdministrationClient("fixture.invalid", new FixtureCredential(), options);
                bool refused = false;
                try
                {
                    Response<SubscriptionProperties> response = await client.GetSubscriptionAsync("topic", name, deadline.Token);
                    Require(status == 200, "HTTP error became a successful entity");
                    Check(name, response.Value);
                }
                catch (ArgumentException) when (status == 400) { refused = true; }
                catch (ServiceBusException error) when (status != 200)
                {
                    if (status == 404) Require(error.Reason == ServiceBusFailureReason.MessagingEntityNotFound, "missing status lost");
                    if (status == 503) Require(error.Reason == ServiceBusFailureReason.ServiceBusy, "stopped status lost");
                    refused = true;
                }
                Require(status == 200 || refused, "non-success was not refused");
                Require(transport.Requests.Count == 1, "Get retried or paged");
                RequestShape request = transport.Requests[0];
                Require(request.Method == "GET" && request.Path == $"/topic/Subscriptions/{name}", "wrong singleton address");
                var query = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (string pair in request.Query.Split('&'))
                {
                    string[] parts = pair.Split('=', 2);
                    Require(parts.Length == 2 && query.TryAdd(Uri.UnescapeDataString(parts[0]), Uri.UnescapeDataString(parts[1])), "ambiguous query");
                }
                Require(query.Count == 2 && query.GetValueOrDefault("api-version") == inputs.ApiVersion && query.GetValueOrDefault("enrich") == "False", "wrong definition profile");
                Console.WriteLine("SWITCHYARD_ATOM31_OBSERVATION " + JsonSerializer.Serialize(new
                {
                    schema = 1, nonce = Environment.GetEnvironmentVariable("SWITCHYARD_SDK_NONCE"), selector = inputs.Selector,
                    backend, name, status, media, request,
                    body_sha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(xml))).ToLowerInvariant(),
                    evidence = "in-memory-parse-of-rust-test-https-response-not-sdk-network-certification",
                }));
            }
        }
    }

    private static void Check(string name, SubscriptionProperties value)
    {
        Require(value.TopicName == "topic" && value.SubscriptionName == name && !value.RequiresSession, "identity/session mismatch");
        (long Lock, long? Ttl, int Delivery, bool Expiry, bool Filter) expected = name switch
        {
            "normal" => (60001, 3600001, 17, false, false),
            "min" => (5000, 1000, 1, true, true),
            "max" => (300000, 922337203685477, int.MaxValue, false, true),
            "unlimited" => (60000, null, 10, true, true),
            _ => throw new InvalidOperationException("unknown definition case"),
        };
        Require(value.LockDuration == TimeSpan.FromTicks(expected.Lock * 10000L), "lock precision lost");
        Require(value.DefaultMessageTimeToLive == (expected.Ttl is long millis ? TimeSpan.FromTicks(checked(millis * 10000L)) : TimeSpan.MaxValue), "TTL precision/sentinel lost");
        Require(value.MaxDeliveryCount == expected.Delivery && value.DeadLetteringOnMessageExpiration == expected.Expiry
            && value.EnableDeadLetteringOnFilterEvaluationExceptions == expected.Filter, "actual definition fields/defaults lost");
    }
}
