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
