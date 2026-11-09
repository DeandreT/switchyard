using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static class RuleConformance
{
    internal static async Task<string?> RunAsync(
        ServiceBusClient client,
        string topic,
        string subscription)
    {
        const string firstRule = "Priority-Correlation";
        const string secondRule = "Priority-Correlation-Backup";

        await using ServiceBusRuleManager rules = client.CreateRuleManager(topic, subscription);
        IReadOnlyList<RuleProperties> initial = await ReadRulesAsync(rules);
        if (initial.Count != 1 ||
            initial[0].Name != RuleProperties.DefaultRuleName ||
            initial[0].Filter is not TrueRuleFilter)
        {
            return $"unexpected initial rule set: {Describe(initial)}";
        }

        await rules.DeleteRuleAsync(RuleProperties.DefaultRuleName);
        await rules.CreateRuleAsync("Blocked", new FalseRuleFilter());
        IReadOnlyList<RuleProperties> blocked = await ReadRulesAsync(rules);
        if (blocked.Count != 1 ||
            blocked[0].Name != "Blocked" ||
            blocked[0].Filter is not FalseRuleFilter)
        {
            return $"false rule did not round-trip: {Describe(blocked)}";
        }

        await using ServiceBusSender sender = client.CreateSender(topic);
        await using ServiceBusReceiver receiver = client.CreateReceiver(topic, subscription);
        await sender.SendMessageAsync(Message("blocked-by-false", priority: 7));
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "a false rule selected a publication";
        }
        await rules.DeleteRuleAsync("Blocked");

        CorrelationRuleFilter filter = Filter();
        await rules.CreateRuleAsync(firstRule, filter);
        await rules.CreateRuleAsync(secondRule, Filter());

        IReadOnlyList<RuleProperties> configured = await ReadRulesAsync(rules);
        if (configured.Count != 2 ||
            !configured.Select(rule => rule.Name).Order().SequenceEqual(
                new[] { firstRule, secondRule }.Order()) ||
            configured.Any(rule => rule.Filter is not CorrelationRuleFilter correlation ||
                !MatchesFilter(correlation)))
        {
            return $"correlation rules did not round-trip: {Describe(configured)}";
        }

        await sender.SendMessageAsync(Message("wrong-priority", priority: 8));
        await sender.SendMessageAsync(Message("wrong-priority-type", priority: 7L));
        await sender.SendMessageAsync(Message("selected-once", priority: 7));
        ServiceBusReceivedMessage? selected =
            await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (selected?.Body.ToString() != "selected-once")
        {
            return $"correlation filtering selected {selected?.Body}";
        }
        await receiver.CompleteMessageAsync(selected);
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "two matching actionless rules produced more than one subscription copy";
        }

        await rules.DeleteRuleAsync(firstRule);
        await rules.DeleteRuleAsync(secondRule);
        if ((await ReadRulesAsync(rules)).Count != 0)
        {
            return "deleting the correlation rules left a durable rule behind";
        }
        await sender.SendMessageAsync(Message("empty-rule-set", priority: 7));
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "an empty rule set selected a publication";
        }

        string? sqlFailure = await SqlAsync(client, rules, sender, receiver, topic, subscription);
        return sqlFailure ?? await ActionsAsync(client, rules, sender, receiver, topic, subscription);
    }

    private static async Task<string?> ActionsAsync(
        ServiceBusClient client,
        ServiceBusRuleManager rules,
        ServiceBusSender sender,
        ServiceBusReceiver receiver,
        string topic,
        string subscription)
    {
        const string first = " SET priority=9;REMOVE region;SET route='alpha'; ";
        const string second = "SET route='beta';SET fresh=3";
        foreach (string name in new[] { "Plain-A", "Plain-B" })
            await rules.CreateRuleAsync(name, new TrueRuleFilter());
        await rules.CreateRuleAsync(new CreateRuleOptions("Action-A", new SqlRuleFilter("priority=7"))
            { Action = new SqlRuleAction(first) });
        await rules.CreateRuleAsync(new CreateRuleOptions("Action-B", new SqlRuleFilter("priority=7 AND region='west'"))
            { Action = new SqlRuleAction(second) });
        await rules.CreateRuleAsync(new CreateRuleOptions("No-Leak", new SqlRuleFilter("route='alpha'"))
            { Action = new SqlRuleAction("SET leaked=TRUE") });
        await rules.CreateRuleAsync(new CreateRuleOptions("Action-Fails", new TrueRuleFilter())
            { Action = new SqlRuleAction("SET priority='incompatible'") });
        IReadOnlyList<RuleProperties> configured = await ReadRulesAsync(rules);
        if (configured.Count != 6 ||
            configured.Single(rule => rule.Name == "Action-A").Action is not SqlRuleAction actionA ||
            actionA.SqlExpression != first ||
            configured.Single(rule => rule.Name == "Action-B").Action is not SqlRuleAction actionB ||
            actionB.SqlExpression != second ||
            configured.Where(rule => rule.Name.StartsWith("Plain-")).Any(rule => rule.Action is not null))
            return "SQL actions did not enumerate exact sources and actionless descriptors";

        await sender.SendMessageAsync(Message("action-copies", 7));
        var copies = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 3; i++)
        {
            ServiceBusReceivedMessage? copy = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (copy is null) return "a matching action copy was missing";
            copies.Add(copy);
            await receiver.CompleteMessageAsync(copy);
        }
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null ||
            copies.Any(copy => copy.Body.ToString() != "action-copies" ||
                copy.MessageId != "rule-message" || copy.CorrelationId != "rule-correlation" ||
                copy.Subject != "rule.subject" || copy.ApplicationProperties.ContainsKey("leaked")))
            return "actions changed body/IDs, leaked a sibling value or emitted an extra copy";
        ServiceBusReceivedMessage plain = copies.Single(copy => !copy.ApplicationProperties.ContainsKey("route"));
        ServiceBusReceivedMessage alpha = copies.Single(copy => Equals(copy.ApplicationProperties.GetValueOrDefault("route"), "alpha"));
        ServiceBusReceivedMessage beta = copies.Single(copy => Equals(copy.ApplicationProperties.GetValueOrDefault("route"), "beta"));
        if (!Equals(plain.ApplicationProperties["priority"], 7) ||
            !Equals(alpha.ApplicationProperties["priority"], 9) || alpha.ApplicationProperties.ContainsKey("region") ||
            !Equals(beta.ApplicationProperties["priority"], 7) || !Equals(beta.ApplicationProperties["region"], "west") ||
            !Equals(beta.ApplicationProperties["fresh"], 3L))
            return "SQL actions lost scalar types, removal, original filtering or copy isolation";
        await using (ServiceBusReceiver deadLetters = client.CreateReceiver(topic, subscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }))
        {
            ServiceBusReceivedMessage? failed = await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
            if (failed?.Body.ToString() != "action-copies" || failed.DeadLetterReason != "ActionException" ||
                failed.DeadLetterErrorDescription != "SQL action failed; no changes were applied" ||
                !Equals(failed.ApplicationProperties["priority"], 7) || failed.ApplicationProperties.ContainsKey("route"))
                return "SQL action failure was not one unchanged fixed-error copy";
            await deadLetters.CompleteMessageAsync(failed);
            if (await deadLetters.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
                return "one failed SQL action produced extra dead letters";
        }

        // Schedule with the old rules, then replace/delete before activation.
        await sender.ScheduleMessageAsync(Message("action-scheduled", 7), DateTimeOffset.UtcNow.AddSeconds(30));
        foreach (string name in new[] { "Action-A", "Action-B", "No-Leak", "Action-Fails" })
            await rules.DeleteRuleAsync(name);
        await rules.CreateRuleAsync(new CreateRuleOptions("Action-Current", new TrueRuleFilter())
            { Action = new SqlRuleAction("SET route='current'") });
        var activated = new List<ServiceBusReceivedMessage>();
        for (int i = 0; i < 2; i++)
        {
            ServiceBusReceivedMessage? copy = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(45));
            if (copy is null) return "a scheduled action copy was missing";
            activated.Add(copy);
            await receiver.CompleteMessageAsync(copy);
        }
        if (activated.Any(copy => copy.Body.ToString() != "action-scheduled" ||
                !Equals(copy.ApplicationProperties["priority"], 7) || !Equals(copy.ApplicationProperties["region"], "west")) ||
            activated.Count(copy => Equals(copy.ApplicationProperties.GetValueOrDefault("route"), "current")) != 1 ||
            activated.Count(copy => !copy.ApplicationProperties.ContainsKey("route")) != 1 ||
            await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
            return "scheduled activation did not select current rules against the original";
        foreach (string name in new[] { "Plain-A", "Plain-B", "Action-Current" })
            await rules.DeleteRuleAsync(name);
        return (await ReadRulesAsync(rules)).Count == 0 ? null : "SQL action deletion left a rule behind";
    }

    private static async Task<string?> SqlAsync(
        ServiceBusClient client,
        ServiceBusRuleManager rules,
        ServiceBusSender sender,
        ServiceBusReceiver receiver,
        string topic,
        string subscription)
    {
        const string sqlRule = "Priority-Sql";
        // Adding zero to a string fails evaluation, so a string priority is
        // dead-lettered rather than skipped.
        const string expression = "priority + 0 > 7 AND region = 'west'";

        try
        {
            await rules.CreateRuleAsync("Broken-Sql", new SqlRuleFilter("priority >"));
            return "an uncompilable SQL filter was accepted";
        }
        catch (Exception error) when (error.Message.Contains("SQL filter cannot be compiled"))
        {
        }

        await rules.CreateRuleAsync(sqlRule, new SqlRuleFilter(expression));
        IReadOnlyList<RuleProperties> configured = await ReadRulesAsync(rules);
        if (configured.Count != 1 ||
            configured[0].Name != sqlRule ||
            configured[0].Filter.GetType() != typeof(SqlRuleFilter) ||
            ((SqlRuleFilter)configured[0].Filter).SqlExpression != expression)
        {
            return $"SQL rule did not round-trip: {Describe(configured)}";
        }

        await sender.SendMessageAsync(Message("sql-low-priority", priority: 5));
        await sender.SendMessageAsync(Message("sql-text-priority", priority: "high"));
        await sender.SendMessageAsync(Message("sql-selected", priority: 8L));
        ServiceBusReceivedMessage? selected =
            await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (selected?.Body.ToString() != "sql-selected")
        {
            return $"SQL filtering selected {selected?.Body}";
        }
        await receiver.CompleteMessageAsync(selected);
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "a SQL rule selected a nonmatching or failing publication";
        }

        await using ServiceBusReceiver deadLetters = client.CreateReceiver(
            topic,
            subscription,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        ServiceBusReceivedMessage? failed =
            await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        if (failed?.Body.ToString() != "sql-text-priority" ||
            failed.DeadLetterReason != "FilterException" ||
            failed.DeadLetterErrorDescription !=
                $"rule {sqlRule}: SQL operands have incompatible types")
        {
            return "a SQL filter error was not dead-lettered as FilterException: " +
                $"{failed?.Body} {failed?.DeadLetterReason} {failed?.DeadLetterErrorDescription}";
        }
        await deadLetters.CompleteMessageAsync(failed);
        if (await deadLetters.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "a SQL filter error produced more than one dead-lettered copy";
        }

        await rules.DeleteRuleAsync(sqlRule);
        if ((await ReadRulesAsync(rules)).Count != 0)
        {
            return "deleting the SQL rule left a durable rule behind";
        }
        await sender.SendMessageAsync(Message("sql-after-delete", priority: 8));
        if (await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) is not null)
        {
            return "a deleted SQL rule selected a publication";
        }

        return null;
    }

    private static CorrelationRuleFilter Filter()
    {
        var filter = new CorrelationRuleFilter
        {
            CorrelationId = "rule-correlation",
            MessageId = "rule-message",
            To = "logical-destination",
            ReplyTo = "logical-reply",
            Subject = "rule.subject",
            ReplyToSessionId = "reply-session",
            ContentType = "application/json",
        };
        filter.ApplicationProperties["Priority"] = 7;
        filter.ApplicationProperties["region"] = "west";
        return filter;
    }

    private static ServiceBusMessage Message(string body, object priority)
    {
        var message = new ServiceBusMessage(body)
        {
            MessageId = "rule-message",
            CorrelationId = "rule-correlation",
            To = "logical-destination",
            ReplyTo = "logical-reply",
            Subject = "rule.subject",
            ReplyToSessionId = "reply-session",
            ContentType = "application/json",
        };
        message.ApplicationProperties["priority"] = priority;
        message.ApplicationProperties["region"] = "west";
        return message;
    }

    private static bool MatchesFilter(CorrelationRuleFilter filter) =>
        filter.CorrelationId == "rule-correlation" &&
        filter.MessageId == "rule-message" &&
        filter.To == "logical-destination" &&
        filter.ReplyTo == "logical-reply" &&
        filter.Subject == "rule.subject" &&
        filter.ReplyToSessionId == "reply-session" &&
        filter.ContentType == "application/json" &&
        Equals(filter.ApplicationProperties["priority"], 7) &&
        Equals(filter.ApplicationProperties["region"], "west");

    private static async Task<IReadOnlyList<RuleProperties>> ReadRulesAsync(
        ServiceBusRuleManager manager)
    {
        var rules = new List<RuleProperties>();
        await foreach (RuleProperties rule in manager.GetRulesAsync())
        {
            rules.Add(rule);
        }
        return rules;
    }

    private static string Describe(IEnumerable<RuleProperties> rules) =>
        string.Join(", ", rules.Select(rule => $"{rule.Name}:{rule.Filter.GetType().Name}"));
}
