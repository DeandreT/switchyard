using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static class SqlCases
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);
    private const string Selection = " sys.Subject='Order' AND score >= 2 AND colour LIKE 'r_d' AND EXISTS(nullable) AND nullable IS NULL AND NOT EXISTS(blocked) AND sys.SessionId IS NOT NULL ";
    private const string ErrorExpression = "10/denominator>1";

    public static async Task RunAsync(ServiceBusClient client, string topic,
        CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        CancellationToken token = deadline.Token;
        string run = $"sql-{Guid.NewGuid():N}";
        ServiceBusRuleManager firstRules = client.CreateRuleManager(topic, "Alpha");
        ServiceBusRuleManager secondRules = client.CreateRuleManager(topic, "beta");
        ServiceBusSender sender = client.CreateSender(topic);
        ServiceBusReceiver first = client.CreateReceiver(topic, "Alpha");
        ServiceBusReceiver second = client.CreateReceiver(topic, "beta");
        ServiceBusReceiver healthy = client.CreateReceiver(topic, "healthy");
        ServiceBusReceiver firstDead = client.CreateReceiver(topic, "Alpha",
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        ServiceBusReceiver secondDead = client.CreateReceiver(topic, "beta",
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        try
        {
            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await RequireDefaultAsync(manager, token);
                await BoundedAsync(ct => manager.DeleteRuleAsync(RuleProperties.DefaultRuleName, ct), token);
                await BoundedAsync(ct => manager.CreateRuleAsync("selection", new SqlRuleFilter(Selection), ct), token);
                await RequireSqlAsync(manager, "selection", Selection, token);
            }
            var overlap = new CorrelationRuleFilter { Subject = "Order" };
            overlap.ApplicationProperties["overlap"] = true;
            await BoundedAsync(ct => firstRules.CreateRuleAsync("overlap", overlap, ct), token);
            List<RuleProperties> enumerated = await ListRulesAsync(firstRules, token);
            Require(enumerated.Count == 2 && enumerated[0].Name == "overlap"
                && enumerated[0].Filter is CorrelationRuleFilter
                && enumerated[1].Name == "selection" && enumerated[1].Filter is SqlRuleFilter,
                "enumeration did not restore distinct correlation and SQL descriptors");

            ServiceBusMessage selected = Message(run, "selected");
            selected.ApplicationProperties["overlap"] = true;
            ServiceBusMessage wrongCase = Message(run, "wrong-case");
            wrongCase.Subject = "order";
            ServiceBusMessage wrongLike = Message(run, "wrong-like");
            wrongLike.ApplicationProperties["CoLoUr"] = "RED";
            ServiceBusMessage below = Message(run, "below");
            below.ApplicationProperties["score"] = 1L;
            ServiceBusMessage missingNull = Message(run, "missing-null");
            missingNull.ApplicationProperties.Remove("nullable");
            ServiceBusMessage blocked = Message(run, "blocked");
            blocked.ApplicationProperties["blocked"] = null!;
            ServiceBusMessage missingSession = Message(run, "missing-session");
            missingSession.SessionId = null;
            ServiceBusMessage unknown = Message(run, "unknown-score");
            unknown.ApplicationProperties.Remove("score");
            ServiceBusMessage[] candidates =
                { selected, wrongCase, wrongLike, below, missingNull, blocked, missingSession, unknown };
            foreach (ServiceBusMessage message in candidates)
            {
                await BoundedAsync(ct => sender.SendMessageAsync(message, ct), token);
            }
            long[] a = await DrainAsync(first, new[] { selected }, token);
            long[] b = await DrainAsync(second, new[] { selected }, token);
            long[] h = await DrainAsync(healthy, candidates, token);
            Require(a.SequenceEqual(b) && a[0] == h[0],
                "SQL selection or overlapping no-action rules changed shared copy ownership");
            await AssertEmptyAsync(firstDead, token);
            await AssertEmptyAsync(secondDead, token);

            foreach (bool safeBatch in new[] { false, true })
            {
                ServiceBusMessage[] batch =
                {
                    Message(run, $"batch-{safeBatch}-int"),
                    Message(run, $"batch-{safeBatch}-rejected"),
                    Message(run, $"batch-{safeBatch}-double"),
                };
                batch[0].ApplicationProperties["score"] = 3;
                batch[0].ApplicationProperties["overlap"] = true;
                batch[1].ApplicationProperties["score"] = 1L;
                batch[2].ApplicationProperties["score"] = 4.0D;
                batch[2].ApplicationProperties["overlap"] = true;
                await SendBatchAsync(sender, batch, safeBatch, token);
                a = await DrainAsync(first, new[] { batch[0], batch[2] }, token);
                b = await DrainAsync(second, new[] { batch[0], batch[2] }, token);
                h = await DrainAsync(healthy, batch, token);
                Require(a.SequenceEqual(b) && a[0] == h[0] && a[1] == h[2],
                    "SQL batch selection, numeric promotion, or overlap changed copies");
            }

            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await DeleteAllRulesAsync(manager, token);
                await BoundedAsync(ct => manager.CreateRuleAsync("before", new SqlRuleFilter("score < 0"), ct), token);
            }
            DateTimeOffset due = DateTimeOffset.FromUnixTimeMilliseconds(
                DateTimeOffset.UtcNow.AddSeconds(10).ToUnixTimeMilliseconds());
            ServiceBusMessage scheduled = Message(run, "current-sql-at-activation");
            scheduled.ApplicationProperties["score"] = 9L;
            scheduled.ScheduledEnqueueTime = due;
            long handle = await BoundedAsync(ct => sender.ScheduleMessageAsync(scheduled, due, ct), token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            await AssertEmptyAsync(healthy, token);
            // Current-rule evaluation at activation is a local policy, not a
            // claim about rule changes against a live Service Bus namespace.
            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await DeleteAllRulesAsync(manager, token);
                await BoundedAsync(ct => manager.CreateRuleAsync("after", new SqlRuleFilter("score = 9"), ct), token);
                await RequireSqlAsync(manager, "after", "score = 9", token);
            }
            ServiceBusReceivedMessage activeA = await ReceiveActivatedAsync(first, token);
            ServiceBusReceivedMessage activeB = await ReceiveActivatedAsync(second, token);
            ServiceBusReceivedMessage activeHealthy = await ReceiveActivatedAsync(healthy, token);
            foreach (ServiceBusReceivedMessage delivery in new[] { activeA, activeB, activeHealthy })
            {
                CheckContent(delivery, scheduled);
                Require(delivery.SequenceNumber > handle && delivery.ScheduledEnqueueTime == due
                    && delivery.EnqueuedTime >= due,
                    "SQL scheduled activation lost its original deadline or new sequence");
            }
            Require(activeA.SequenceNumber == activeB.SequenceNumber
                && activeA.SequenceNumber == activeHealthy.SequenceNumber,
                "current SQL rules did not use one activation sequence");
            await BoundedAsync(ct => first.CompleteMessageAsync(activeA, ct), token);
            await BoundedAsync(ct => second.CompleteMessageAsync(activeB, ct), token);
            await BoundedAsync(ct => healthy.CompleteMessageAsync(activeHealthy, ct), token);

            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await DeleteAllRulesAsync(manager, token);
                await BoundedAsync(ct => manager.CreateRuleAsync("finite-error", new SqlRuleFilter(ErrorExpression), ct), token);
                await RequireSqlAsync(manager, "finite-error", ErrorExpression, token);
            }
            ServiceBusMessage failing = Message(run, "division-by-zero");
            failing.ApplicationProperties["denominator"] = 0L;
            await BoundedAsync(ct => sender.SendMessageAsync(failing, ct), token);
            await AssertEmptyAsync(first, token);
            await AssertEmptyAsync(second, token);
            await AssertEmptyAsync(secondDead, token);
            ServiceBusReceivedMessage intact = await ReceiveAsync(healthy, token);
            CheckContent(intact, failing);
            ServiceBusReceivedMessage dead = await ReceiveAsync(firstDead, token);
            CheckContent(dead, failing, stripped: true);
            // This reason and static description are implementation policy;
            // they are not asserted as Azure's canonical filter-error text.
            Require(dead.DeadLetterReason == "SwitchyardSqlFilterError"
                && dead.DeadLetterErrorDescription == "SQL filter integer arithmetic divided by zero."
                && dead.SequenceNumber == intact.SequenceNumber && dead.SessionId is null
                && dead.GetRawAmqpMessage().Header.TimeToLive is null
                && dead.GetRawAmqpMessage().Properties.AbsoluteExpiryTime is null,
                "finite SQL error did not preserve the bounded local SDLQ metadata or strip lifetime/session");
            await BoundedAsync(ct => firstDead.CompleteMessageAsync(dead, ct), token);
            Require((await PeekAsync(healthy, token)).Any(message => message.SequenceNumber == intact.SequenceNumber),
                "SQL SDLQ completion crossed healthy subscription ownership");
            await BoundedAsync(ct => healthy.CompleteMessageAsync(intact, ct), token);

            foreach (ServiceBusRuleManager manager in new[] { firstRules, secondRules })
            {
                await DeleteAllRulesAsync(manager, token);
                await BoundedAsync(ct => manager.CreateRuleAsync(RuleProperties.DefaultRuleName, new TrueRuleFilter(), ct), token);
                await RequireDefaultAsync(manager, token);
            }
            foreach (ServiceBusReceiver receiver in new[] { first, second, healthy, firstDead, secondDead })
            {
                await AssertEmptyAsync(receiver, token);
            }
            Console.WriteLine("official .NET SQL source/descriptors/numeric/case/LIKE/Unknown/OR/batches/current activation/local filter-error policy passed");
        }
        finally
        {
            await DisposeAllAsync(secondDead, firstDead, healthy, second, first, sender, secondRules, firstRules);
        }
    }

    private static ServiceBusMessage Message(string run, string text)
    {
        var message = new ServiceBusMessage(text)
        {
            MessageId = run + "-" + text,
            CorrelationId = "sql-correlation",
            To = "logical-destination",
            ReplyTo = "logical-replies",
            Subject = "Order",
            SessionId = run + "-session",
            ReplyToSessionId = run + "-reply-session",
            ContentType = "text/plain",
            TimeToLive = TimeSpan.FromMinutes(2),
        };
        message.ApplicationProperties["CoLoUr"] = "red";
        message.ApplicationProperties["nullable"] = null!;
        message.ApplicationProperties["score"] = 2L;
        message.ApplicationProperties["overlap"] = false;
        message.GetRawAmqpMessage().Properties.ContentEncoding = "utf-8";
        message.GetRawAmqpMessage().Footer["sql-checksum"] = "sql-content";
        return message;
    }

    private static void CheckContent(ServiceBusReceivedMessage actual, ServiceBusMessage expected, bool stripped = false)
    {
        Require(actual.Body.ToArray().SequenceEqual(expected.Body.ToArray())
            && actual.MessageId == expected.MessageId && actual.CorrelationId == expected.CorrelationId
            && actual.To == expected.To && actual.ReplyTo == expected.ReplyTo && actual.Subject == expected.Subject
            && (stripped || actual.SessionId == expected.SessionId)
            && actual.ReplyToSessionId == expected.ReplyToSessionId && actual.ContentType == expected.ContentType
            && (stripped || actual.TimeToLive == expected.TimeToLive)
            && actual.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8"
            && actual.GetRawAmqpMessage().Footer.TryGetValue("sql-checksum", out object? checksum)
            && Equals(checksum, "sql-content"), "SQL routing changed content, message fields, or footer");
        Require(actual.ApplicationProperties.Count == expected.ApplicationProperties.Count + (stripped ? 2 : 0),
            "SQL routing added or removed application properties");
        foreach (var pair in expected.ApplicationProperties)
        {
            Require(actual.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && Equals(value, pair.Value), $"SQL routing changed property {pair.Key}");
        }
    }

    private static async Task<long[]> DrainAsync(ServiceBusReceiver receiver, ServiceBusMessage[] expected,
        CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> browsed = await PeekAsync(receiver, token);
        Require(browsed.Count == expected.Length, "SQL filtering retained a rejected or duplicate copy");
        var sequences = new long[expected.Length];
        for (int index = 0; index < expected.Length; index++)
        {
            CheckContent(browsed[index], expected[index]);
            ServiceBusReceivedMessage delivery = await ReceiveAsync(receiver, token);
            CheckContent(delivery, expected[index]);
            Require(delivery.SequenceNumber == browsed[index].SequenceNumber,
                "SQL browse and receive disagreed on order");
            sequences[index] = delivery.SequenceNumber;
            await BoundedAsync(ct => receiver.CompleteMessageAsync(delivery, ct), token);
        }
        await AssertEmptyAsync(receiver, token);
        return sequences;
    }

    private static async Task RequireSqlAsync(ServiceBusRuleManager manager, string name, string expression,
        CancellationToken token)
    {
        List<RuleProperties> rules = await ListRulesAsync(manager, token);
        Require(rules.Count == 1 && rules[0].Name == name && rules[0].Action is null
            && rules[0].Filter is SqlRuleFilter sql && sql.SqlExpression == expression,
            "enumeration did not preserve the original SQL expression and typed descriptor");
    }

    private static async Task RequireDefaultAsync(ServiceBusRuleManager manager, CancellationToken token)
    {
        List<RuleProperties> rules = await ListRulesAsync(manager, token);
        Require(rules.Count == 1 && rules[0].Name == RuleProperties.DefaultRuleName
            && rules[0].Filter is TrueRuleFilter && rules[0].Action is null,
            "SQL workflow did not preserve or restore the explicit default rule");
    }

    private static async Task DeleteAllRulesAsync(ServiceBusRuleManager manager, CancellationToken token)
    {
        foreach (RuleProperties rule in await ListRulesAsync(manager, token))
        {
            await BoundedAsync(ct => manager.DeleteRuleAsync(rule.Name, ct), token);
        }
    }

    private static Task<List<RuleProperties>> ListRulesAsync(ServiceBusRuleManager manager, CancellationToken token) =>
        BoundedAsync(async ct =>
        {
            var rules = new List<RuleProperties>();
            await foreach (RuleProperties rule in manager.GetRulesAsync(ct))
            {
                Require(rules.Count < 32, "SQL rule enumeration exceeded its local bound");
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
            Require(batch.TryAddMessage(message), "a SQL candidate did not fit its small batch");
        }
        await BoundedAsync(ct => sender.SendMessagesAsync(batch, ct), token);
    }

    private static Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(ServiceBusReceiver receiver, CancellationToken token) =>
        BoundedAsync(ct => receiver.PeekMessagesAsync(16, fromSequenceNumber: 1, cancellationToken: ct), token);

    private static async Task AssertEmptyAsync(ServiceBusReceiver receiver, CancellationToken token) =>
        Require((await PeekAsync(receiver, token)).Count == 0, "SQL workflow left an unexpected retained copy");

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        ServiceBusReceivedMessage? delivery = await BoundedAsync(ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
        Require(delivery is not null, "SQL workflow lost an expected copy");
        return delivery!;
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveActivatedAsync(ServiceBusReceiver receiver, CancellationToken token)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            ServiceBusReceivedMessage? delivery = await BoundedAsync(ct => receiver.ReceiveMessageAsync(ReceiveWait, ct), token);
            if (delivery is not null) { return delivery; }
        }
        throw new InvalidOperationException("SQL workflow scheduled activation did not arrive");
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
        if (failure is not null) { throw new InvalidOperationException("SQL SDK cleanup failed", failure); }
    }

    private static void Require(bool condition, string detail)
    {
        if (!condition) { throw new InvalidOperationException($"SQL SDK conformance failed: {detail}"); }
    }
}
