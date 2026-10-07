using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using static AtomAdministrationCases;

internal static class AtomRuleMessageFlowCases
{
    private const string Success = "official .NET HTTPS rules to AMQP typed action copies passed";
    private const string Topic = "sdk-atom-rule-flow";
    private const string Subject = "bridge caf\u00E9 & <\u03BB>\nsubject";
    private const string FilterSource = " colour = 'red' AND sys.Label = 'bridge caf\u00E9 & <\u03BB>\nsubject' ";
    private const string ActionSource = " /* bridge v2 */ REMOVE audit; REMOVE user.[drop];\nSET [MiXeD Target]=' caf\u00E9 & <\u03BB>\nO''Brien '; SET user.enabled=FALSE; SET added=+23; SET number=-7; SET [RuleName]='ignored'; ";
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 8)
        {
            Console.Error.WriteLine("usage: atom-rule-message-flow <https> <amqp> <ca> <namespace> <key-name> <key> <topic>");
            return 2;
        }
        Require(args[7] == Topic && args[4] == "tenant.servicebus.windows.net",
            "bridge topic or AMQP audience differs from its fixed native fixture");
        var https = new Uri(args[1]);
        var amqp = new Uri(args[2]);
        Require(https.IsAbsoluteUri && https.Scheme == Uri.UriSchemeHttps && https.Host == "localhost"
            && https.UserInfo.Length == 0 && https.Query.Length == 0 && https.Fragment.Length == 0
            && https.AbsolutePath == "/", "bridge HTTPS endpoint is outside its closed local profile");
        Require(amqp.IsAbsoluteUri && amqp.Scheme == "sb" && amqp.Host == "localhost"
            && amqp.UserInfo.Length == 0 && amqp.Query.Length == 0 && amqp.Fragment.Length == 0
            && amqp.AbsolutePath == "/", "bridge AMQP endpoint is outside its closed local profile");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var resources = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            EmitLoadedAssemblyEvidence();
            using var transport = new AtomAdministrationTransport(https, args[3]);
            ServiceBusAdministrationClient named = transport.CreateClient(false, args[5], args[6]);
            ServiceBusAdministrationClient connection = transport.CreateClient(true, args[5], args[6]);
            var options = new ServiceBusClientOptions
            {
                CustomEndpointAddress = amqp,
                TransportType = ServiceBusTransportType.AmqpTcp,
                RetryOptions = { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
            };
            ServiceBusClient client = Own(resources, new ServiceBusClient(args[4],
                new AzureNamedKeyCredential(args[5], args[6]), options));
            ServiceBusSender sender = Own(resources, client.CreateSender(Topic));
            ServiceBusReceiver alpha = Own(resources, client.CreateReceiver(Topic, "Alpha"));
            ServiceBusReceiver beta = Own(resources, client.CreateReceiver(Topic, "beta"));
            ServiceBusReceiver alphaDead = Own(resources, client.CreateReceiver(Topic, "Alpha",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            ServiceBusReceiver betaDead = Own(resources, client.CreateReceiver(Topic, "beta",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            int cycle = 0;
            foreach (bool useConnectionString in new[] { false, true })
            {
                ServiceBusAdministrationClient owner = useConnectionString ? connection : named;
                ServiceBusAdministrationClient other = useConnectionString ? named : connection;
                await CycleAsync(owner, other, sender, alpha, beta, alphaDead, betaDead,
                    useConnectionString ? "connection" : "named", cycle++, deadline.Token);
            }
        }
        catch (Exception error) { failure = error; }
        finally
        {
            for (int index = resources.Count - 1; index >= 0; index--)
            {
                try { await resources[index].DisposeAsync().AsTask().WaitAsync(CleanupTimeout); }
                catch (Exception error)
                {
                    failure ??= new InvalidOperationException("bridge SDK cleanup failed", error);
                }
            }
        }
        if (failure is not null)
        {
            Console.Error.WriteLine(failure);
            return 1;
        }
        Console.WriteLine(Success);
        return 0;
    }

    private static T Own<T>(List<IAsyncDisposable> resources, T resource) where T : IAsyncDisposable
    {
        resources.Add(resource);
        return resource;
    }

    private static async Task CycleAsync(ServiceBusAdministrationClient owner,
        ServiceBusAdministrationClient other, ServiceBusSender sender, ServiceBusReceiver alpha,
        ServiceBusReceiver beta, ServiceBusReceiver alphaDead, ServiceBusReceiver betaDead,
        string suffix, int cycle, CancellationToken token)
    {
        string alphaName = "Bridge-" + suffix;
        string betaName = "Sibling-" + suffix;
        await RequireRulesAsync(other, "Alpha", null, false, token);
        await RequireRulesAsync(other, "beta", null, false, token);
        var alphaOptions = new CreateRuleOptions(alphaName, new SqlRuleFilter(FilterSource))
        {
            Action = new SqlRuleAction(ActionSource),
        };
        var correlation = new CorrelationRuleFilter { Subject = Subject };
        correlation.ApplicationProperties["colour"] = "red";
        Response<RuleProperties> alphaCreated = await BoundedAsync(
            ct => owner.CreateRuleAsync(Topic, "Alpha", alphaOptions, ct), token);
        RequireStatus(alphaCreated.GetRawResponse(), 201);
        CheckRule(alphaCreated.Value, alphaName, true);
        Response<RuleProperties> betaCreated = await BoundedAsync(
            ct => owner.CreateRuleAsync(Topic, "beta", new CreateRuleOptions(betaName, correlation), ct), token);
        RequireStatus(betaCreated.GetRawResponse(), 201);
        CheckRule(betaCreated.Value, betaName, false);
        foreach (var item in new[] { ("Alpha", alphaName, true), ("beta", betaName, false) })
        {
            Response<RuleProperties> got = await BoundedAsync(
                ct => other.GetRuleAsync(Topic, item.Item1, item.Item2, ct), token);
            RequireStatus(got.GetRawResponse(), 200);
            CheckRule(got.Value, item.Item2, item.Item3);
            await RequireRulesAsync(other, item.Item1, item.Item2, item.Item3, token);
        }
        foreach (ServiceBusReceiver receiver in new[] { alpha, beta, alphaDead, betaDead })
        {
            Require((await PeekAsync(receiver, token)).Count == 0, "bridge began with a retained copy");
        }
        await BoundedAsync(ct => sender.SendMessageAsync(Message(suffix, false), ct), token);
        foreach (ServiceBusReceiver receiver in new[] { alpha, beta, alphaDead, betaDead })
        {
            Require((await PeekAsync(receiver, token)).Count == 0, "nonmatching bridge message was delivered");
        }
        ServiceBusMessage source = Message(suffix, true);
        await BoundedAsync(ct => sender.SendMessageAsync(source, ct), token);
        long baseSequence = 2 + cycle * 3;
        IReadOnlyList<ServiceBusReceivedMessage> changed = await PeekAsync(alpha, token);
        IReadOnlyList<ServiceBusReceivedMessage> intact = await PeekAsync(beta, token);
        Require(changed.Count == 1 && changed[0].SequenceNumber == baseSequence + 1
            && intact.Count == 1 && intact[0].SequenceNumber == baseSequence,
            "bridge lost independent parent base/action identities");
        CheckCopy(changed[0], source, alphaName, true);
        CheckCopy(intact[0], source, betaName, false);
        ServiceBusReceivedMessage changedDelivery = await ReceiveAsync(alpha, token);
        ServiceBusReceivedMessage intactDelivery = await ReceiveAsync(beta, token);
        CheckCopy(changedDelivery, source, alphaName, true);
        CheckCopy(intactDelivery, source, betaName, false);
        Require(changedDelivery.SequenceNumber == baseSequence + 1
            && intactDelivery.SequenceNumber == baseSequence,
            "bridge receive changed an independent identity");
        await BoundedAsync(ct => alpha.CompleteMessageAsync(changedDelivery, ct), token);
        Require((await PeekAsync(alpha, token)).Count == 0, "completed action remained active");
        intact = await PeekAsync(beta, token);
        Require(intact.Count == 1 && intact[0].SequenceNumber == baseSequence,
            "action completion settled the sibling original");
        CheckCopy(intact[0], source, betaName, false);
        await BoundedAsync(ct => beta.CompleteMessageAsync(intactDelivery, ct), token);
        foreach (ServiceBusReceiver receiver in new[] { alpha, beta, alphaDead, betaDead })
        {
            Require((await PeekAsync(receiver, token)).Count == 0, "bridge retained an active or DLQ copy");
        }
        foreach (var item in new[] { ("Alpha", alphaName), ("beta", betaName) })
        {
            RequireStatus(await BoundedAsync(ct => owner.DeleteRuleAsync(Topic, item.Item1, item.Item2, ct), token), 200);
            await ExpectServiceBusAsync(() => BoundedAsync(
                ct => other.GetRuleAsync(Topic, item.Item1, item.Item2, ct), token),
                ServiceBusFailureReason.MessagingEntityNotFound);
            await RequireRulesAsync(other, item.Item1, null, false, token);
        }
    }

    private static void CheckRule(RuleProperties rule, string name, bool action)
    {
        Require(string.Equals(rule.Name, name, StringComparison.Ordinal), "bridge rule name changed");
        if (action)
        {
            Require(rule.Filter.GetType() == typeof(SqlRuleFilter)
                && rule.Filter is SqlRuleFilter filter
                && string.Equals(filter.SqlExpression, FilterSource, StringComparison.Ordinal)
                && filter.Parameters.Count == 0 && rule.Action?.GetType() == typeof(SqlRuleAction)
                && rule.Action is SqlRuleAction sql
                && string.Equals(sql.SqlExpression, ActionSource, StringComparison.Ordinal)
                && sql.Parameters.Count == 0, "bridge SQL filter/action type, exact source or parameters changed");
        }
        else
        {
            Require(rule.Filter.GetType() == typeof(CorrelationRuleFilter)
                && rule.Filter is CorrelationRuleFilter filter
                && string.Equals(filter.Subject, Subject, StringComparison.Ordinal)
                && filter.CorrelationId is null && filter.MessageId is null && filter.To is null
                && filter.ReplyTo is null && filter.SessionId is null && filter.ReplyToSessionId is null
                && filter.ContentType is null && filter.ApplicationProperties.Count == 1
                && filter.ApplicationProperties.TryGetValue("colour", out object? colour)
                && colour?.GetType() == typeof(string) && (string)colour == "red" && rule.Action is null,
                "bridge sibling correlation definition changed");
        }
    }

    private static async Task RequireRulesAsync(ServiceBusAdministrationClient client,
        string subscription, string? name, bool action, CancellationToken token)
    {
        await BoundedAsync(async ct =>
        {
            int pages = 0;
            await foreach (Page<RuleProperties> page in client.GetRulesAsync(Topic, subscription, ct).AsPages())
            {
                Require(++pages == 1, "bridge rule list emitted multiple pages");
                RequireStatus(page.GetRawResponse(), 200);
                Require(page.Values.Count == (name is null ? 0 : 1), "bridge rule list is incomplete");
                if (name is not null) { CheckRule(page.Values[0], name, action); }
            }
            Require(pages == 1, "bridge rule list omitted its real 200 page");
        }, token);
    }

    private static ServiceBusMessage Message(string suffix, bool matching)
    {
        var message = new ServiceBusMessage("bridge-body-caf\u00E9-<\u03BB>\n")
        {
            MessageId = "bridge-" + suffix + (matching ? "-match" : "-nonmatch"),
            CorrelationId = "bridge-correlation", Subject = matching ? Subject : "bridge-nonmatch",
            ContentType = "text/plain", To = "bridge-destination", ReplyTo = "bridge-reply",
            ReplyToSessionId = "bridge-reply-session", TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.ApplicationProperties["colour"] = "red";
        message.ApplicationProperties["number"] = 42L;
        message.ApplicationProperties["enabled"] = true;
        message.ApplicationProperties["drop"] = "remove-drop";
        message.ApplicationProperties["audit"] = "remove-audit";
        message.ApplicationProperties["Audit"] = "case-retained";
        message.ApplicationProperties["RuleName"] = "publisher-rule";
        message.ApplicationProperties["rulename"] = "lowercase-retained";
        message.GetRawAmqpMessage().Header.Durable = false;
        message.GetRawAmqpMessage().Header.Priority = 4;
        message.GetRawAmqpMessage().Header.FirstAcquirer = false;
        message.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
        message.GetRawAmqpMessage().Footer["producer-checksum"] = "bridge-checksum";
        return message;
    }

    private static void CheckCopy(ServiceBusReceivedMessage copy, ServiceBusMessage source,
        string name, bool action)
    {
        Require(copy.Body.ToArray().SequenceEqual(source.Body.ToArray())
            && copy.MessageId == source.MessageId && copy.CorrelationId == source.CorrelationId
            && copy.Subject == source.Subject && copy.ContentType == source.ContentType
            && copy.To == source.To && copy.ReplyTo == source.ReplyTo
            && copy.ReplyToSessionId == source.ReplyToSessionId && copy.SessionId is null
            && copy.TimeToLive == source.TimeToLive
            && copy.GetRawAmqpMessage().Header.Durable == false
            && copy.GetRawAmqpMessage().Header.Priority == 4
            && copy.GetRawAmqpMessage().Header.FirstAcquirer == false
            && copy.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8"
            && copy.GetRawAmqpMessage().Footer.Count == 1
            && copy.GetRawAmqpMessage().Footer.TryGetValue("producer-checksum", out object? checksum)
            && checksum?.GetType() == typeof(string) && (string)checksum == "bridge-checksum",
            "bridge copy changed body/system/encoding/footer");
        var expected = new Dictionary<string, object>(source.ApplicationProperties, StringComparer.Ordinal);
        if (action)
        {
            expected.Remove("audit"); expected.Remove("drop");
            expected["MiXeD Target"] = " caf\u00E9 & <\u03BB>\nO'Brien ";
            expected["enabled"] = false; expected["added"] = 23L; expected["number"] = -7L;
            expected["RuleName"] = name;
        }
        Require(copy.ApplicationProperties.Count == expected.Count
            && copy.ApplicationProperties.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(expected.Keys),
            "bridge copy changed its complete ordinal key set");
        foreach (var pair in expected)
        {
            Require(copy.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && value is not null && value.GetType() == pair.Value.GetType() && Equals(value, pair.Value),
                "bridge copy changed a typed application property");
        }
        Require(string.Equals((string)copy.ApplicationProperties["rulename"], "lowercase-retained", StringComparison.Ordinal)
            && string.Equals((string)copy.ApplicationProperties["Audit"], "case-retained", StringComparison.Ordinal),
            "bridge action folded exact-key case");
    }

    private static Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(ServiceBusReceiver receiver,
        CancellationToken token) => BoundedAsync(ct => receiver.PeekMessagesAsync(4,
            fromSequenceNumber: 1, cancellationToken: ct), token);

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? message = await BoundedAsync(
            ct => receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), ct), token);
        Require(message is not null, "bridge expected copy was not delivered");
        return message!;
    }

    private static async Task BoundedAsync(Func<CancellationToken, Task> operation, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }

    private static async Task<T> BoundedAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(OperationTimeout);
        return await operation(deadline.Token).WaitAsync(OperationTimeout, token);
    }
}
