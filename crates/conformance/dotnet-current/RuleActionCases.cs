using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static partial class RuleActionCases
{
    private const string Success =
        "official .NET SQL REMOVE/literal SET actions/source/independent copies/conversion DLQ/unsupported system SET passed";
    private const string ColourRule = "a-remove-colour";
    private const string AuditRule = "b-remove-audit";
    private const string PublisherRule = "publisher-rule-name";
    private const string ColourSource =
        " /* retained source */ REMOVE user.[colour]; REMOVE [missing]; REMOVE [RuleName]; ";
    private const string AuditSource = "\nREMOVE [audit]; REMOVE [RuleName];\n";
    private const string AuditFilter = " colour = 'red' ";
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 6)
        {
            Console.Error.WriteLine("usage: rule-actions <namespace> <endpoint> <topic> <key-name> <key>");
            return 2;
        }
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var resources = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            var options = new ServiceBusClientOptions
            {
                CustomEndpointAddress = new Uri(args[2]),
                TransportType = ServiceBusTransportType.AmqpTcp,
                RetryOptions = { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
            };
            ServiceBusClient client = Own(resources, new ServiceBusClient(args[1],
                new AzureNamedKeyCredential(args[4], args[5]), options));
            ServiceBusRuleManager alphaRules = Own(resources, client.CreateRuleManager(args[3], "Alpha"));
            ServiceBusRuleManager betaRules = Own(resources, client.CreateRuleManager(args[3], "beta"));
            ServiceBusSender sender = Own(resources, client.CreateSender(args[3]));
            ServiceBusReceiver alpha = Own(resources, client.CreateReceiver(args[3], "Alpha"));
            ServiceBusReceiver beta = Own(resources, client.CreateReceiver(args[3], "beta"));
            ServiceBusReceiver alphaDead = Own(resources, client.CreateReceiver(args[3], "Alpha",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            ServiceBusReceiver betaDead = Own(resources, client.CreateReceiver(args[3], "beta",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            await WorkflowAsync(alphaRules, betaRules, sender, alpha, beta,
                alphaDead, betaDead, deadline.Token);
        }
        catch (Exception error) { failure = error; }
        finally
        {
            for (int index = resources.Count - 1; index >= 0; index--)
            {
                try { await resources[index].DisposeAsync().AsTask().WaitAsync(CleanupTimeout); }
                catch (Exception error)
                {
                    failure ??= new InvalidOperationException("rule action SDK cleanup failed", error);
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

    private static async Task WorkflowAsync(ServiceBusRuleManager alphaRules,
        ServiceBusRuleManager betaRules, ServiceBusSender sender,
        ServiceBusReceiver alpha, ServiceBusReceiver beta,
        ServiceBusReceiver alphaDead, ServiceBusReceiver betaDead, CancellationToken token)
    {
        await RequireDefaultAsync(alphaRules, token);
        await RequireDefaultAsync(betaRules, token);
        await BoundedAsync(ct => alphaRules.DeleteRuleAsync(RuleProperties.DefaultRuleName, ct), token);
        foreach (string name in new[] { "plain-a", "plain-b" })
        {
            await BoundedAsync(ct => alphaRules.CreateRuleAsync(name, new TrueRuleFilter(), ct), token);
        }
        var colourFilter = new CorrelationRuleFilter { Subject = "rule-action" };
        colourFilter.ApplicationProperties["colour"] = "red";
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(new CreateRuleOptions(ColourRule, colourFilter)
        {
            Action = new SqlRuleAction(ColourSource),
        }, ct), token);
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(new CreateRuleOptions(AuditRule,
            new SqlRuleFilter(AuditFilter)) { Action = new SqlRuleAction(AuditSource) }, ct), token);
        await RequireRulesAsync(alphaRules, token);

        string run = $"actions-{Guid.NewGuid():N}";
        await CheckPublicationAsync(sender, alpha, beta, Message(run, "first"), token);
        bool refused = false;
        try
        {
            await BoundedAsync(ct => alphaRules.CreateRuleAsync(new CreateRuleOptions("unsupported-set",
                new TrueRuleFilter()) { Action = new SqlRuleAction("SET sys.Subject = 'changed'") }, ct), token);
        }
        catch (NotSupportedException) { refused = true; }
        Require(refused, "unsupported system SET did not produce an awaited NotSupportedException");
        await RequireRulesAsync(alphaRules, token);
        await CheckPublicationAsync(sender, alpha, beta, Message(run, "after-refusal"), token);

        await LiteralWorkflowAsync(alphaRules, sender, alpha, beta, alphaDead, betaDead, run, token);

        foreach (RuleProperties rule in await ListRulesAsync(alphaRules, token))
        {
            await BoundedAsync(ct => alphaRules.DeleteRuleAsync(rule.Name, ct), token);
        }
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(RuleProperties.DefaultRuleName,
            new TrueRuleFilter(), ct), token);
        await RequireDefaultAsync(alphaRules, token);
        await RequireDefaultAsync(betaRules, token);
        foreach (ServiceBusReceiver receiver in new[] { alpha, beta, alphaDead, betaDead })
        {
            Require((await PeekAsync(receiver, token)).Count == 0,
                "rule action cleanup left a retained subscription or dead-letter copy");
        }
    }

    private static async Task RequireRulesAsync(ServiceBusRuleManager manager, CancellationToken token)
    {
        List<RuleProperties> rules = await ListRulesAsync(manager, token);
        Require(rules.Select(rule => rule.Name).SequenceEqual(
            new[] { ColourRule, AuditRule, "plain-a", "plain-b" }),
            "rule enumeration lost, added, or repeated a definition");
        foreach (RuleProperties rule in rules)
        {
            if (rule.Name == ColourRule)
            {
                Require(rule.Filter is CorrelationRuleFilter filter && filter.Subject == "rule-action"
                    && filter.ApplicationProperties.Count == 1
                    && Equals(filter.ApplicationProperties["colour"], "red")
                    && rule.Action is SqlRuleAction action && action.SqlExpression == ColourSource,
                    "enumeration did not preserve the correlation filter and exact colour action source");
            }
            else if (rule.Name == AuditRule)
            {
                Require(rule.Filter is SqlRuleFilter filter && filter.SqlExpression == AuditFilter
                    && rule.Action is SqlRuleAction action && action.SqlExpression == AuditSource,
                    "enumeration did not preserve both exact SQL source strings");
            }
            else
            {
                Require(rule.Filter is TrueRuleFilter && rule.Action is null,
                    "an action-free rule lost its typed filter or acquired an action");
            }
        }
    }

    private static async Task CheckPublicationAsync(ServiceBusSender sender,
        ServiceBusReceiver alpha, ServiceBusReceiver beta, ServiceBusMessage source, CancellationToken token)
    {
        await BoundedAsync(ct => sender.SendMessageAsync(source, ct), token);
        IReadOnlyList<ServiceBusReceivedMessage> copies = await PeekAsync(alpha, token);
        Require(copies.Count == 3, "overlapping no-action OR plus two actions did not produce exactly three copies");
        var remaining = new Dictionary<string, long>();
        foreach (ServiceBusReceivedMessage copy in copies)
        {
            string name = CopyName(copy);
            CheckCopy(copy, source, name);
            Require(remaining.TryAdd(name, copy.SequenceNumber), "an independent action copy was duplicated");
        }
        Require(remaining.Keys.ToHashSet().SetEquals(new[] { PublisherRule, ColourRule, AuditRule })
            && remaining.Values.Distinct().Count() == 3, "action copies do not have independent identities");
        IReadOnlyList<ServiceBusReceivedMessage> intact = await PeekAsync(beta, token);
        Require(intact.Count == 1, "the independent subscription did not retain exactly one original copy");
        CheckCopy(intact[0], source, PublisherRule);
        long betaSequence = intact[0].SequenceNumber;

        for (int index = 0; index < 3; index++)
        {
            ServiceBusReceivedMessage copy = await ReceiveAsync(alpha, token);
            string name = CopyName(copy);
            CheckCopy(copy, source, name);
            Require(remaining.Remove(name, out long sequence) && copy.SequenceNumber == sequence,
                "receive changed or repeated an original browsed copy");
            await BoundedAsync(ct => alpha.CompleteMessageAsync(copy, ct), token);
            IReadOnlyList<ServiceBusReceivedMessage> after = await PeekAsync(alpha, token);
            Require(after.Count == remaining.Count
                && after.Select(item => item.SequenceNumber).ToHashSet().SetEquals(remaining.Values),
                "completion removed a sibling action copy or retained the completed copy");
            intact = await PeekAsync(beta, token);
            Require(intact.Count == 1 && intact[0].SequenceNumber == betaSequence,
                "completion crossed subscription ownership");
            CheckCopy(intact[0], source, PublisherRule);
        }
        ServiceBusReceivedMessage original = await ReceiveAsync(beta, token);
        CheckCopy(original, source, PublisherRule);
        Require(original.SequenceNumber == betaSequence, "the unchanged subscription lost its original copy");
        await BoundedAsync(ct => beta.CompleteMessageAsync(original, ct), token);
        Require((await PeekAsync(alpha, token)).Count == 0 && (await PeekAsync(beta, token)).Count == 0,
            "the publication left retained messages after independent completion");
    }

    private static ServiceBusMessage Message(string run, string label)
    {
        var message = new ServiceBusMessage("rule-action-" + label)
        {
            MessageId = run + "-" + label,
            CorrelationId = "action-correlation",
            Subject = "rule-action",
            ContentType = "text/plain",
            To = "logical-destination",
            ReplyTo = "logical-replies",
            ReplyToSessionId = "reply-session",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.ApplicationProperties["colour"] = "red";
        // Keep the case pair outside SQL's referenced property to avoid ambiguity.
        message.ApplicationProperties["Audit"] = "case-retained";
        message.ApplicationProperties["audit"] = "audit-retained";
        message.ApplicationProperties["RuleName"] = PublisherRule;
        message.ApplicationProperties["rulename"] = "lowercase-retained";
        message.ApplicationProperties["number"] = 42L;
        message.ApplicationProperties["enabled"] = true;
        message.ApplicationProperties["nullable"] = null!;
        message.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
        message.GetRawAmqpMessage().Footer["producer-checksum"] = "rule-action-checksum";
        return message;
    }

    private static string CopyName(ServiceBusReceivedMessage copy)
    {
        Require(copy.ApplicationProperties.TryGetValue("RuleName", out object? value) && value is string,
            "copy lost its exact RuleName property");
        return (string)copy.ApplicationProperties["RuleName"];
    }

    private static void CheckCopy(ServiceBusReceivedMessage copy, ServiceBusMessage source, string name)
    {
        Require(copy.Body.ToArray().SequenceEqual(source.Body.ToArray())
            && copy.MessageId == source.MessageId && copy.CorrelationId == source.CorrelationId
            && copy.Subject == source.Subject && copy.ContentType == source.ContentType
            && copy.To == source.To && copy.ReplyTo == source.ReplyTo
            && copy.ReplyToSessionId == source.ReplyToSessionId && copy.SessionId == source.SessionId
            && copy.TimeToLive == source.TimeToLive
            && copy.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8"
            && copy.GetRawAmqpMessage().Footer.Count == 1
            && copy.GetRawAmqpMessage().Footer.TryGetValue("producer-checksum", out object? checksum)
            && Equals(checksum, "rule-action-checksum"),
            "an action changed body, system properties, encoding, or footer");
        var expected = new Dictionary<string, object>(source.ApplicationProperties);
        if (name == ColourRule) { expected.Remove("colour"); }
        else if (name == AuditRule) { expected.Remove("audit"); }
        else { Require(name == PublisherRule, "an unknown action copy was published"); }
        expected["RuleName"] = name;
        Require(copy.ApplicationProperties.Count == expected.Count,
            "an action removed or added unexpected application properties");
        foreach (var pair in expected)
        {
            Require(copy.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && Equals(value, pair.Value), "an action changed a retained property or scalar type");
        }
    }

    private static async Task RequireDefaultAsync(ServiceBusRuleManager manager, CancellationToken token)
    {
        List<RuleProperties> rules = await ListRulesAsync(manager, token);
        Require(rules.Count == 1 && rules[0].Name == RuleProperties.DefaultRuleName
            && rules[0].Filter is TrueRuleFilter && rules[0].Action is null,
            "cleanup did not preserve the explicit no-action default rule");
    }

    private static Task<List<RuleProperties>> ListRulesAsync(ServiceBusRuleManager manager, CancellationToken token) =>
        BoundedAsync(async ct =>
        {
            var rules = new List<RuleProperties>();
            await foreach (RuleProperties rule in manager.GetRulesAsync(ct))
            {
                Require(rules.Count < 32, "rule enumeration exceeded its local bound");
                rules.Add(rule);
            }
            return rules;
        }, token);

    private static Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(
        ServiceBusReceiver receiver, CancellationToken token) =>
        BoundedAsync(ct => receiver.PeekMessagesAsync(8, fromSequenceNumber: 1, cancellationToken: ct), token);

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(
        ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? message = await BoundedAsync(
            ct => receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), ct), token);
        Require(message is not null, "an expected independent copy was not delivered");
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

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException("rule action SDK conformance failed: " + detail); }
    }
}
