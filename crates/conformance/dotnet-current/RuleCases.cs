using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static class RuleCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public static async Task RunAsync(ServiceBusClient client, string topic,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"rules-{Guid.NewGuid():N}";
        ServiceBusRuleManager firstRules = client.CreateRuleManager(topic, "Alpha");
        ServiceBusRuleManager secondRules = client.CreateRuleManager(topic, "beta");
        ServiceBusSender sender = client.CreateSender(topic);
        ServiceBusReceiver first = client.CreateReceiver(topic, "Alpha");
        ServiceBusReceiver second = client.CreateReceiver(topic, "beta");
        try
        {
            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                List<RuleProperties> defaults = await ListRulesAsync(manager, token);
                Require(defaults.Count == 1 && defaults[0].Name == RuleProperties.DefaultRuleName
                    && defaults[0].Filter is TrueRuleFilter && defaults[0].Action is null,
                    "subscription creation did not persist an explicit no-action default rule");
                await BoundedAsync(ct => manager.DeleteRuleAsync(RuleProperties.DefaultRuleName, ct), token);
                Require((await ListRulesAsync(manager, token)).Count == 0,
                    "deleting the default left an implicit rule behind");
            }
            await BoundedAsync(ct => sender.SendMessageAsync(Message(run, "no-rules"), ct), token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            CorrelationRuleFilter filter = Filter(run);
            await BoundedAsync(ct => firstRules.CreateRuleAsync("full", filter, ct), token);
            await BoundedAsync(ct => firstRules.CreateRuleAsync("overlap", Filter(run), ct), token);
            await BoundedAsync(ct => secondRules.CreateRuleAsync("blocked", new FalseRuleFilter(), ct), token);
            List<RuleProperties> rules = await ListRulesAsync(firstRules, token);
            Require(rules.Select(rule => rule.Name).SequenceEqual(new[] { "full", "overlap" }),
                "enumeration lost, repeated, or reordered a rule");
            foreach (RuleProperties rule in rules)
            {
                Require(rule.Action is null && rule.Filter is CorrelationRuleFilter,
                    "enumeration did not return a typed no-action correlation filter");
                CheckFilter((CorrelationRuleFilter)rule.Filter, filter);
            }
            rules = await ListRulesAsync(secondRules, token);
            Require(rules.Count == 1 && rules[0].Name == "blocked"
                && rules[0].Filter is FalseRuleFilter && rules[0].Action is null,
                "false-filter enumeration used the wrong vendor descriptor");
            await ExpectFailureAsync(ct => firstRules.CreateRuleAsync("full", Filter(run), ct),
                ServiceBusFailureReason.MessagingEntityAlreadyExists, token);
            await ExpectFailureAsync(ct => firstRules.DeleteRuleAsync("absent", ct),
                ServiceBusFailureReason.MessagingEntityNotFound, token);

            ServiceBusMessage matching = Message(run, "matched-once");
            var candidates = new List<ServiceBusMessage> { matching };
            ServiceBusMessage wrongCase = Message(run, "wrong-case");
            wrongCase.Subject = "order";
            candidates.Add(wrongCase);
            ServiceBusMessage wrongType = Message(run, "wrong-width");
            wrongType.ApplicationProperties["attempt"] = 7;
            candidates.Add(wrongType);
            ServiceBusMessage missing = Message(run, "missing-null");
            missing.ApplicationProperties.Remove("nullable");
            candidates.Add(missing);
            ServiceBusMessage wrongSession = Message(run, "wrong-session");
            wrongSession.SessionId = run + "-other-session";
            candidates.Add(wrongSession);
            ServiceBusMessage wrongDestination = Message(run, "wrong-destination");
            wrongDestination.To = "different-destination";
            candidates.Add(wrongDestination);
            foreach (ServiceBusMessage message in candidates)
            {
                await BoundedAsync(ct => sender.SendMessageAsync(message, ct), token);
            }
            IReadOnlyList<ServiceBusReceivedMessage> selected = await PeekAsync(first, token);
            Require(selected.Count == 1, "AND/type/null/case filtering or overlapping no-action OR changed copy count");
            CheckContent(selected[0], matching);
            ServiceBusReceivedMessage delivery = await ReceiveAsync(first, token);
            CheckContent(delivery, matching);
            await BoundedAsync(ct => first.CompleteMessageAsync(delivery, ct), token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);

            foreach (bool safeBatch in new[] { false, true })
            {
                ServiceBusMessage[] batch =
                {
                    Message(run, $"batch-{safeBatch}-first"),
                    Message(run, $"batch-{safeBatch}-rejected"),
                    Message(run, $"batch-{safeBatch}-second"),
                };
                batch[1].ApplicationProperties["colour"] = "blue";
                await SendBatchAsync(sender, batch, safeBatch, token);
                selected = await PeekAsync(first, token);
                Require(selected.Count == 2, "batch filtering retained a rejected member or duplicate overlap copy");
                for (int index = 0; index < 2; index++)
                {
                    ServiceBusMessage source = batch[index * 2];
                    CheckContent(selected[index], source);
                    delivery = await ReceiveAsync(first, token);
                    CheckContent(delivery, source);
                    Require(delivery.SequenceNumber == selected[index].SequenceNumber,
                        "filtered batch browse and receive disagreed");
                    await BoundedAsync(ct => first.CompleteMessageAsync(delivery, ct), token);
                }
                await AssertEmptyAsync(first, token);
                await AssertEmptyAsync(second, token);
            }

            await BoundedAsync(ct => secondRules.DeleteRuleAsync("blocked", ct), token);
            await BoundedAsync(ct => secondRules.CreateRuleAsync("all", new TrueRuleFilter(), ct), token);
            ServiceBusMessage retained = Message(run, "retained-before-rule-removal");
            await BoundedAsync(ct => sender.SendMessageAsync(retained, ct), token);
            await BoundedAsync(ct => firstRules.DeleteRuleAsync("full", ct), token);
            await BoundedAsync(ct => firstRules.DeleteRuleAsync("overlap", ct), token);
            Require((await ListRulesAsync(firstRules, token)).Count == 0, "removed rules remained enumerable");
            ServiceBusReceivedMessage a = await ReceiveAsync(first, token);
            ServiceBusReceivedMessage b = await ReceiveAsync(second, token);
            CheckContent(a, retained);
            CheckContent(b, retained);
            Require(a.SequenceNumber == b.SequenceNumber, "rule removal changed an already-published copy");
            await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
            Require((await PeekAsync(second, token)).Any(message => message.SequenceNumber == b.SequenceNumber),
                "settlement crossed subscription ownership");
            await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
            ServiceBusMessage onlySecond = Message(run, "only-second");
            await BoundedAsync(ct => sender.SendMessageAsync(onlySecond, ct), token);
            await AssertEmptyAsync(first, token);
            b = await ReceiveAsync(second, token);
            CheckContent(b, onlySecond);
            await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);

            await BoundedAsync(ct => firstRules.CreateRuleAsync("before-activation", new FalseRuleFilter(), ct), token);
            DateTimeOffset due = DateTimeOffset.FromUnixTimeMilliseconds(
                DateTimeOffset.UtcNow.AddSeconds(10).ToUnixTimeMilliseconds());
            ServiceBusMessage scheduled = Message(run, "current-rules-at-activation");
            scheduled.ScheduledEnqueueTime = due;
            long handle = await BoundedAsync(ct => sender.ScheduleMessageAsync(scheduled, due, ct), token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            // This implementation evaluates current rules at activation;
            // this timing policy has not been compared with a live namespace.
            await BoundedAsync(ct => firstRules.DeleteRuleAsync("before-activation", ct), token);
            await BoundedAsync(ct => firstRules.CreateRuleAsync("after-activation", new TrueRuleFilter(), ct), token);
            a = await ReceiveActivatedAsync(first, token);
            b = await ReceiveActivatedAsync(second, token);
            CheckContent(a, scheduled);
            CheckContent(b, scheduled);
            Require(a.SequenceNumber == b.SequenceNumber && a.SequenceNumber > handle
                && a.ScheduledEnqueueTime == due && b.ScheduledEnqueueTime == due
                && a.EnqueuedTime >= due && b.EnqueuedTime >= due,
                "scheduled activation did not apply current rules with a new shared sequence");
            await BoundedAsync(ct => first.CompleteMessageAsync(a, ct), token);
            await BoundedAsync(ct => second.CompleteMessageAsync(b, ct), token);
            await BoundedAsync(ct => firstRules.DeleteRuleAsync("after-activation", ct), token);
            await BoundedAsync(ct => secondRules.DeleteRuleAsync("all", ct), token);
            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await BoundedAsync(ct => manager.CreateRuleAsync(RuleProperties.DefaultRuleName, new TrueRuleFilter(), ct), token);
                rules = await ListRulesAsync(manager, token);
                Require(rules.Count == 1 && rules[0].Name == RuleProperties.DefaultRuleName
                    && rules[0].Filter is TrueRuleFilter, "cleanup did not restore the explicit default");
            }
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            Console.WriteLine("official .NET rule manager/default/true/false/correlation/types/null/OR/batch/current activation rules passed");
        }
        finally
        {
            await DisposeAllAsync(second, first, sender, secondRules, firstRules);
        }
    }

    private static CorrelationRuleFilter Filter(string run)
    {
        var filter = new CorrelationRuleFilter
        {
            CorrelationId = "wanted-correlation",
            MessageId = run + "-candidate",
            To = "logical-destination",
            ReplyTo = "logical-replies",
            Subject = "Order",
            SessionId = run + "-session",
            ReplyToSessionId = run + "-reply-session",
            ContentType = "text/plain",
        };
        filter.ApplicationProperties["colour"] = "red";
        filter.ApplicationProperties["nullable"] = null!;
        filter.ApplicationProperties["attempt"] = 7L;
        return filter;
    }

    private static ServiceBusMessage Message(string run, string text)
    {
        var message = new ServiceBusMessage(text)
        {
            MessageId = run + "-candidate",
            CorrelationId = "wanted-correlation",
            To = "logical-destination",
            ReplyTo = "logical-replies",
            Subject = "Order",
            SessionId = run + "-session",
            ReplyToSessionId = run + "-reply-session",
            ContentType = "text/plain",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.ApplicationProperties["colour"] = "red";
        message.ApplicationProperties["nullable"] = null!;
        message.ApplicationProperties["attempt"] = 7L;
        return message;
    }

    private static void CheckFilter(CorrelationRuleFilter actual, CorrelationRuleFilter expected)
    {
        Require(actual.CorrelationId == expected.CorrelationId && actual.MessageId == expected.MessageId
            && actual.To == expected.To && actual.ReplyTo == expected.ReplyTo
            && actual.Subject == expected.Subject && actual.SessionId == expected.SessionId
            && actual.ReplyToSessionId == expected.ReplyToSessionId && actual.ContentType == expected.ContentType
            && actual.ApplicationProperties.Count == expected.ApplicationProperties.Count,
            "enumeration changed correlation system fields");
        foreach (var pair in expected.ApplicationProperties)
        {
            Require(actual.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && Equals(value, pair.Value), $"enumeration changed scalar condition {pair.Key}");
        }
    }

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.CorrelationId == expected.CorrelationId
            && actual.To == expected.To && actual.ReplyTo == expected.ReplyTo && actual.Subject == expected.Subject
            && actual.SessionId == expected.SessionId && actual.ReplyToSessionId == expected.ReplyToSessionId
            && actual.ContentType == expected.ContentType && actual.TimeToLive == expected.TimeToLive,
            "filtered publication changed retained content or properties");
        foreach (var pair in expected.ApplicationProperties)
        {
            Require(actual.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && Equals(value, pair.Value), $"filtered publication changed property {pair.Key}");
        }
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

    private static async Task SendBatchAsync(ServiceBusSender sender, ServiceBusMessage[] messages,
        bool safeBatch, CancellationToken token)
    {
        if (!safeBatch)
        {
            await BoundedAsync(ct => sender.SendMessagesAsync(messages, ct), token);
            return;
        }
        using ServiceBusMessageBatch batch = await BoundedAsync(ct => sender.CreateMessageBatchAsync(ct).AsTask(), token);
        foreach (ServiceBusMessage message in messages)
        {
            Require(batch.TryAddMessage(message), "a filtered message did not fit its small batch");
        }
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
    }

    private static Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(ServiceBusReceiver receiver, CancellationToken token) =>
        BoundedAsync(ct => receiver.PeekMessagesAsync(16, fromSequenceNumber: 1, cancellationToken: ct), token);

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token) =>
        Require((await PeekAsync(receiver, token)).Count == 0, "a filtered subscription retained an unexpected copy");

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? delivery = await BoundedAsync(ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
        Require(delivery is not null, "a matching rule lost a published copy");
        return delivery!;
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveActivatedAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            ServiceBusReceivedMessage? delivery = await BoundedAsync(ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
            if (delivery is not null) { return delivery; }
        }
        throw new InvalidOperationException("rule workflow scheduled activation did not arrive");
    }

    private static async Task ExpectFailureAsync(Func<CancellationToken, Task> operation,
        ServiceBusFailureReason reason, CancellationToken token)
    {
        try { await BoundedAsync(operation, token); }
        catch (ServiceBusException error)
        {
            Require(error.Reason == reason, $"rule refusal reported {error.Reason}, expected {reason}");
            return;
        }
        throw new InvalidOperationException("the SDK rule manager accepted an invalid mutation");
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

    private static async Task DisposeAllAsync(params IAsyncDisposable[] resources)
    {
        Exception? failure = null;
        foreach (IAsyncDisposable resource in resources)
        {
            try { await resource.DisposeAsync().AsTask().WaitAsync(CleanupTimeout); }
            catch (Exception error) { failure ??= error; }
        }
        if (failure is not null) { throw new InvalidOperationException("rule SDK cleanup failed", failure); }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"rule SDK conformance failed: {detail}"); }
    }
}
