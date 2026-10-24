using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using Azure;
using Azure.Messaging.ServiceBus.Administration;
using static FixtureInputs;

internal static class AdministrationControls
{
    internal static async Task RunAsync(FixtureInputs inputs)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken cancellation = deadline.Token;
        await CollectionPairAsync(inputs, "dotnet-queues",
            (client, token) => client.GetQueuesAsync(token), CheckQueue,
            (client, token) => client.GetQueuesRuntimePropertiesAsync(token), CheckQueueRuntime,
            cancellation);
        await CollectionPairAsync(inputs, "dotnet-topics",
            (client, token) => client.GetTopicsAsync(token), CheckTopic,
            (client, token) => client.GetTopicsRuntimePropertiesAsync(token), value => CheckTopicRuntime(inputs, value),
            cancellation);
        await CollectionPairAsync(inputs, "dotnet-subscriptions",
            (client, token) => client.GetSubscriptionsAsync("fixture-topic", token), CheckSubscription,
            (client, token) => client.GetSubscriptionsRuntimePropertiesAsync("fixture-topic", token), CheckSubscriptionRuntime,
            cancellation);

        await SingletonPairAsync(inputs, "queues", "/fixture-queue",
            (client, token) => client.GetQueueAsync("fixture-queue", token), CheckQueue,
            (client, token) => client.GetQueueRuntimePropertiesAsync("fixture-queue", token), CheckQueueRuntime,
            cancellation);
        await SingletonPairAsync(inputs, "topics", "/fixture-topic",
            (client, token) => client.GetTopicAsync("fixture-topic", token), CheckTopic,
            (client, token) => client.GetTopicRuntimePropertiesAsync("fixture-topic", token), value => CheckTopicRuntime(inputs, value),
            cancellation);
        await SingletonPairAsync(inputs, "subscriptions", "/fixture-topic/Subscriptions/fixture-subscription",
            (client, token) => client.GetSubscriptionAsync("fixture-topic", "fixture-subscription", token), CheckSubscription,
            (client, token) => client.GetSubscriptionRuntimePropertiesAsync("fixture-topic", "fixture-subscription", token), CheckSubscriptionRuntime,
            cancellation);
    }

    private static async Task CollectionPairAsync<TDefinition, TRuntime>(FixtureInputs inputs, string id,
        Func<ServiceBusAdministrationClient, CancellationToken, AsyncPageable<TDefinition>> definition,
        Action<TDefinition> checkDefinition,
        Func<ServiceBusAdministrationClient, CancellationToken, AsyncPageable<TRuntime>> runtime,
        Action<TRuntime> checkRuntime, CancellationToken cancellation)
        where TDefinition : notnull
        where TRuntime : notnull
    {
        JsonElement spec = inputs.Request(id);
        string path = spec.GetProperty("path").GetString()!;
        string definitionXml = inputs.Read(spec.GetProperty("definition_response").GetString()!);
        string runtimeXml = inputs.Read(spec.GetProperty("combined_response").GetString()!);
        RequestShape definitionRequest = await PageAsync(definition, checkDefinition, definitionXml, cancellation);
        RequestShape runtimeRequest = await PageAsync(runtime, checkRuntime, runtimeXml, cancellation);
        Require(definitionRequest == runtimeRequest, "definition/runtime collection requests differ");
        CheckRequest(inputs, definitionRequest, path, "False", paged: true);
        string selectorKey = inputs.Selector == "declared-current" ? "example_target_current" : "example_target_previous";
        Require(definitionRequest.Path + "?" + definitionRequest.Query == spec.GetProperty(selectorKey).GetString(),
            "emitted request differs from the source-derived literal target");
        Observe(inputs, id, "identical-collection-requests", definitionRequest);

        string refusal = inputs.Read("responses/refusal.xml");
        await RefusalAsync(definition, refusal, path, inputs, id + "-definition-refusal", cancellation);
        await RefusalAsync(runtime, refusal, path, inputs, id + "-runtime-refusal", cancellation);
    }

    private static async Task<RequestShape> PageAsync<T>(
        Func<ServiceBusAdministrationClient, CancellationToken, AsyncPageable<T>> operation,
        Action<T> check, string body, CancellationToken cancellation)
        where T : notnull
    {
        using var transport = new InMemoryTransport(200, body);
        ServiceBusAdministrationClient client = Client(transport);
        await using IAsyncEnumerator<Page<T>> pages = operation(client, cancellation).AsPages().GetAsyncEnumerator(cancellation);
        Require(await pages.MoveNextAsync(), "expected one non-empty synthetic page");
        Require(pages.Current.Values.Count == 1, "wrong synthetic page length");
        check(pages.Current.Values[0]);
        Require(!await pages.MoveNextAsync(), "short page did not terminate enumeration");
        Require(transport.Requests.Count == 1, "unexpected page request count");
        return transport.Requests[0];
    }

    private static async Task RefusalAsync<T>(
        Func<ServiceBusAdministrationClient, CancellationToken, AsyncPageable<T>> operation,
        string body, string path, FixtureInputs inputs, string id, CancellationToken cancellation)
        where T : notnull
    {
        using var transport = new InMemoryTransport(400, body);
        ServiceBusAdministrationClient client = Client(transport);
        bool refused = false;
        try
        {
            await foreach (Page<T> page in operation(client, cancellation).AsPages().WithCancellation(cancellation))
            {
                throw new InvalidOperationException("HTTP refusal became a successful page, including an empty page");
            }
        }
        catch (ArgumentException)
        {
            refused = true;
        }
        Require(refused, "HTTP 400 did not produce the expected SDK ArgumentException");
        Require(transport.Requests.Count == 1, "refusal was not tied to exactly one intercepted request");
        CheckRequest(inputs, transport.Requests[0], path, "False", paged: true);
        Observe(inputs, id, "http-400-is-error-not-empty", transport.Requests[0]);
    }

    private static async Task SingletonPairAsync<TDefinition, TRuntime>(FixtureInputs inputs, string kind, string path,
        Func<ServiceBusAdministrationClient, CancellationToken, Task<Response<TDefinition>>> definition,
        Action<TDefinition> checkDefinition,
        Func<ServiceBusAdministrationClient, CancellationToken, Task<Response<TRuntime>>> runtime,
        Action<TRuntime> checkRuntime, CancellationToken cancellation)
    {
        string definitionXml = SingleEntry(inputs.Read($"responses/definition-{kind}.xml"));
        string runtimeXml = SingleEntry(inputs.Read($"responses/combined-{kind}.xml"));
        using var definitionTransport = new InMemoryTransport(200, definitionXml);
        using var runtimeTransport = new InMemoryTransport(200, runtimeXml);
        Response<TDefinition> definitionResponse = await definition(Client(definitionTransport), cancellation);
        Response<TRuntime> runtimeResponse = await runtime(Client(runtimeTransport), cancellation);
        checkDefinition(definitionResponse.Value);
        checkRuntime(runtimeResponse.Value);
        Require(definitionTransport.Requests.Count == 1 && runtimeTransport.Requests.Count == 1,
            "unexpected singleton request count");
        RequestShape definitionRequest = definitionTransport.Requests[0];
        RequestShape runtimeRequest = runtimeTransport.Requests[0];
        CheckRequest(inputs, definitionRequest, path, "False", paged: false);
        CheckRequest(inputs, runtimeRequest, path, "True", paged: false);
        Require(definitionRequest != runtimeRequest, "singleton runtime selector was lost");
        Observe(inputs, kind + "-definition-singleton", "distinct-enrich-selector", definitionRequest);
        Observe(inputs, kind + "-runtime-singleton", "distinct-enrich-selector", runtimeRequest);
    }

    private static ServiceBusAdministrationClient Client(InMemoryTransport transport)
    {
        var options = new ServiceBusAdministrationClientOptions
        {
            Transport = transport,
            Retry = { MaxRetries = 0 },
        };
        return new ServiceBusAdministrationClient("fixture.invalid", new FixtureCredential(), options);
    }

    private static void CheckRequest(FixtureInputs inputs, RequestShape request, string path, string enrich, bool paged)
    {
        Require(request.Method == "GET" && request.Path == path, "unexpected request method/path");
        var query = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string part in request.Query.Split('&'))
        {
            string[] pair = part.Split('=', 2);
            Require(pair.Length == 2, "malformed intercepted query");
            string name = Uri.UnescapeDataString(pair[0].Replace('+', ' '));
            string value = Uri.UnescapeDataString(pair[1].Replace('+', ' '));
            Require(query.TryAdd(name, value), "duplicate intercepted query key");
        }
        Require(query.Count == (paged ? 4 : 2), "unexpected intercepted query fields");
        Require(query.GetValueOrDefault("api-version") == inputs.ApiVersion, "wrong default API version");
        Require(query.GetValueOrDefault("enrich") == enrich, "wrong enrich selector");
        if (paged)
        {
            Require(query.GetValueOrDefault("$skip") == "0" && query.GetValueOrDefault("$top") == "100",
                "unexpected initial paging query");
        }
    }

    private static string SingleEntry(string feed)
    {
        using var reader = XmlReader.Create(new StringReader(feed), new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            MaxCharactersInDocument = 1024 * 1024,
        });
        XDocument document = XDocument.Load(reader);
        XNamespace atom = "http://www.w3.org/2005/Atom";
        return new XElement(document.Root!.Elements(atom + "entry").Single()).ToString(SaveOptions.DisableFormatting);
    }

    private static void CheckQueue(QueueProperties value)
    {
        Require(value.Name == "fixture-queue", "wrong queue name");
        Require(value.LockDuration == TimeSpan.FromMinutes(1) && value.DefaultMessageTimeToLive == TimeSpan.FromHours(1),
            "wrong queue definition durations");
        Require(!value.RequiresSession && !value.RequiresDuplicateDetection && value.MaxDeliveryCount == 10
            && value.DeadLetteringOnMessageExpiration && value.DuplicateDetectionHistoryTimeWindow == TimeSpan.FromMinutes(10),
            "wrong queue definition properties");
    }

    private static void CheckTopic(TopicProperties value)
    {
        Require(value.Name == "fixture-topic" && value.DefaultMessageTimeToLive == TimeSpan.FromHours(1),
            "wrong topic definition");
        Require(!value.RequiresDuplicateDetection && value.DuplicateDetectionHistoryTimeWindow == TimeSpan.FromMinutes(10),
            "wrong topic duplicate setting");
    }

    private static void CheckSubscription(SubscriptionProperties value)
    {
        Require(value.TopicName == "fixture-topic" && value.SubscriptionName == "fixture-subscription",
            "wrong subscription identity");
        Require(value.LockDuration == TimeSpan.FromMinutes(1) && value.DefaultMessageTimeToLive == TimeSpan.FromHours(1)
            && !value.RequiresSession && value.MaxDeliveryCount == 10 && value.DeadLetteringOnMessageExpiration,
            "wrong subscription definition");
    }

    private static void CheckQueueRuntime(QueueRuntimeProperties value)
    {
        Require(value.Name == "fixture-queue" && value.TotalMessageCount == 28 && value.SizeInBytes == 4096,
            "wrong synthetic queue totals");
        Require(value.ActiveMessageCount == 11 && value.DeadLetterMessageCount == 3 && value.ScheduledMessageCount == 5
            && value.TransferMessageCount == 7 && value.TransferDeadLetterMessageCount == 2,
            "nonzero synthetic queue counts were lost or defaulted");
        CheckTimes(value.CreatedAt, value.UpdatedAt, value.AccessedAt);
    }

    private static void CheckTopicRuntime(FixtureInputs inputs, TopicRuntimeProperties value)
    {
        Require(value.Name == "fixture-topic" && value.SizeInBytes == 8192
            && value.SubscriptionCount == 4 && value.ScheduledMessageCount == 5, "wrong synthetic topic runtime");
        foreach ((string name, int expected) in new[] { ("SqlFilterCount", 3), ("CorrelationFilterCount", 2) })
        {
            var property = value.GetType().GetProperty(name);
            if (inputs.Selector == "declared-current")
            {
                Require(property?.GetValue(value) is int actual && actual == expected, "current topic filter count was lost");
            }
            else
            {
                Require(property is null, "previous SDK unexpectedly exposes a current-only filter count");
            }
        }
        CheckTimes(value.CreatedAt, value.UpdatedAt, value.AccessedAt);
    }

    private static void CheckSubscriptionRuntime(SubscriptionRuntimeProperties value)
    {
        Require(value.TopicName == "fixture-topic" && value.SubscriptionName == "fixture-subscription"
            && value.TotalMessageCount == 28, "wrong synthetic subscription runtime");
        Require(value.ActiveMessageCount == 11 && value.DeadLetterMessageCount == 3
            && value.TransferMessageCount == 7 && value.TransferDeadLetterMessageCount == 2,
            "nonzero synthetic subscription counts were lost or defaulted");
        CheckTimes(value.CreatedAt, value.UpdatedAt, value.AccessedAt);
    }

    private static void CheckTimes(DateTimeOffset created, DateTimeOffset updated, DateTimeOffset accessed)
    {
        Require(created == new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
            && updated == new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero)
            && accessed == new DateTimeOffset(2026, 1, 3, 0, 0, 0, TimeSpan.Zero), "wrong synthetic runtime timestamps");
    }

    private static void Observe(FixtureInputs inputs, string id, string assertion, RequestShape request)
    {
        Console.WriteLine("SWITCHYARD_ATOM_OBSERVATION " + JsonSerializer.Serialize(new
        {
            schema = 1,
            nonce = Environment.GetEnvironmentVariable("SWITCHYARD_SDK_NONCE"),
            selector = inputs.Selector,
            id,
            assertion,
            request,
            response_evidence = "synthetic-fixtures-not-server-statistics",
        }));
    }
}
