using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static partial class RuleActionCases
{
    private const string LiteralStringRule = "c-literal-string";
    private const string LiteralIntegerRule = "d-literal-integer";
    private const string LiteralFailureRule = "e-literal-failure";
    private const string LiteralStringSource =
        " /* local v2 */ SET user.[colour]='blue';SET nullable='present';SET [RuleName]='ignored'; ";
    private const string LiteralIntegerSource = " SET number=-7;SET enabled=FALSE;SET added=23; ";
    private const string LiteralFailureSource = "REMOVE audit;SET enabled='wrong-family'";
    private const string LiteralStringFilter = " colour = 'red' AND number = 42 ";

    private static async Task LiteralWorkflowAsync(ServiceBusRuleManager rules,
        ServiceBusSender sender, ServiceBusReceiver alpha, ServiceBusReceiver beta,
        ServiceBusReceiver alphaDead, ServiceBusReceiver betaDead, string run, CancellationToken token)
    {
        foreach (RuleProperties rule in await ListRulesAsync(rules, token))
        {
            await BoundedAsync(ct => rules.DeleteRuleAsync(rule.Name, ct), token);
        }
        await BoundedAsync(ct => rules.CreateRuleAsync("plain-literal", new TrueRuleFilter(), ct), token);
        await BoundedAsync(ct => rules.CreateRuleAsync(new CreateRuleOptions(LiteralStringRule,
            new SqlRuleFilter(LiteralStringFilter)) { Action = new SqlRuleAction(LiteralStringSource) }, ct), token);
        var originalFilter = new CorrelationRuleFilter { Subject = "rule-action" };
        originalFilter.ApplicationProperties["colour"] = "red";
        await BoundedAsync(ct => rules.CreateRuleAsync(new CreateRuleOptions(LiteralIntegerRule,
            originalFilter) { Action = new SqlRuleAction(LiteralIntegerSource) }, ct), token);
        await RequireLiteralRulesAsync(rules, false, token);
        await CheckLiteralPublicationAsync(sender, alpha, beta, alphaDead, betaDead,
            Message(run, "literal-success"), 7, false, token);

        await BoundedAsync(ct => rules.CreateRuleAsync(new CreateRuleOptions(LiteralFailureRule,
            new SqlRuleFilter(" enabled = TRUE AND colour = 'red' "))
        {
            Action = new SqlRuleAction(LiteralFailureSource),
        }, ct), token);
        await RequireLiteralRulesAsync(rules, true, token);
        await CheckLiteralPublicationAsync(sender, alpha, beta, alphaDead, betaDead,
            Message(run, "literal-failure"), 10, true, token);
    }

    private static async Task RequireLiteralRulesAsync(ServiceBusRuleManager manager,
        bool failure, CancellationToken token)
    {
        List<RuleProperties> rules = await ListRulesAsync(manager, token);
        string[] names = failure
            ? new[] { LiteralStringRule, LiteralIntegerRule, LiteralFailureRule, "plain-literal" }
            : new[] { LiteralStringRule, LiteralIntegerRule, "plain-literal" };
        Require(rules.Select(rule => rule.Name).SequenceEqual(names),
            "literal rule enumeration changed the complete sorted definition set");
        foreach (RuleProperties rule in rules)
        {
            if (rule.Name == LiteralStringRule)
            {
                Require(rule.Filter is SqlRuleFilter filter && filter.SqlExpression == LiteralStringFilter
                    && rule.Action is SqlRuleAction action && action.SqlExpression == LiteralStringSource,
                    "literal string rule lost exact source or original-input predicate");
            }
            else if (rule.Name == LiteralIntegerRule)
            {
                Require(rule.Filter is CorrelationRuleFilter filter && filter.Subject == "rule-action"
                    && filter.ApplicationProperties.Count == 1 && Equals(filter.ApplicationProperties["colour"], "red")
                    && rule.Action is SqlRuleAction action && action.SqlExpression == LiteralIntegerSource,
                    "literal integer rule lost its typed original-input filter or exact source");
            }
            else if (rule.Name == LiteralFailureRule)
            {
                Require(rule.Filter is SqlRuleFilter filter
                    && filter.SqlExpression == " enabled = TRUE AND colour = 'red' "
                    && rule.Action is SqlRuleAction action && action.SqlExpression == LiteralFailureSource,
                    "conversion failure rule lost its exact source");
            }
            else
            {
                Require(rule.Name == "plain-literal" && rule.Filter is TrueRuleFilter && rule.Action is null,
                    "literal base acquired an action or lost its typed true filter");
            }
        }
    }

    private static async Task CheckLiteralPublicationAsync(ServiceBusSender sender,
        ServiceBusReceiver alpha, ServiceBusReceiver beta, ServiceBusReceiver alphaDead,
        ServiceBusReceiver betaDead, ServiceBusMessage source, long baseSequence,
        bool failure, CancellationToken token)
    {
        await BoundedAsync(ct => sender.SendMessageAsync(source, ct), token);
        IReadOnlyList<ServiceBusReceivedMessage> copies = await PeekAsync(alpha, token);
        Require(copies.Count == 3, "literal publication did not retain exactly one base and two successful actions");
        var remaining = new Dictionary<string, long>();
        foreach (ServiceBusReceivedMessage copy in copies)
        {
            string name = CopyName(copy);
            CheckLiteralCopy(copy, source, name, false);
            Require(remaining.TryAdd(name, copy.SequenceNumber), "literal publication repeated an action identity");
        }
        Require(remaining.Count == 3 && remaining.GetValueOrDefault(PublisherRule) == baseSequence
            && remaining.GetValueOrDefault(LiteralStringRule) == baseSequence + 1
            && remaining.GetValueOrDefault(LiteralIntegerRule) == baseSequence + 2,
            "literal base/action sequences lost sorted independent parent allocation");
        IReadOnlyList<ServiceBusReceivedMessage> intact = await PeekAsync(beta, token);
        Require(intact.Count == 1 && intact[0].SequenceNumber == baseSequence,
            "literal publication changed the sibling original subscription");
        CheckCopy(intact[0], source, PublisherRule);
        IReadOnlyList<ServiceBusReceivedMessage> dead = await PeekAsync(alphaDead, token);
        Require(dead.Count == (failure ? 1 : 0), "literal conversion emitted the wrong number of dead letters");
        if (failure)
        {
            Require(dead[0].SequenceNumber == baseSequence + 3, "conversion failure lost its separate sorted parent identity");
            CheckLiteralCopy(dead[0], source, LiteralFailureRule, true);
        }
        Require((await PeekAsync(betaDead, token)).Count == 0, "conversion failure crossed to a sibling dead-letter queue");

        var received = new Dictionary<string, ServiceBusReceivedMessage>();
        for (int index = 0; index < 3; index++)
        {
            ServiceBusReceivedMessage copy = await ReceiveAsync(alpha, token);
            string name = CopyName(copy);
            CheckLiteralCopy(copy, source, name, false);
            Require(remaining.TryGetValue(name, out long sequence) && sequence == copy.SequenceNumber
                && received.TryAdd(name, copy), "literal receive changed a browsed identity or repeated a delivery");
        }
        foreach (string name in new[] { LiteralStringRule, PublisherRule, LiteralIntegerRule })
        {
            await BoundedAsync(ct => alpha.CompleteMessageAsync(received[name], ct), token);
            Require(remaining.Remove(name), "literal settlement repeated an identity");
            IReadOnlyList<ServiceBusReceivedMessage> after = await PeekAsync(alpha, token);
            Require(after.Count == remaining.Count
                && after.Select(copy => copy.SequenceNumber).ToHashSet().SetEquals(remaining.Values),
                "completing a literal action removed a base or sibling action copy");
            foreach (ServiceBusReceivedMessage copy in after) { CheckLiteralCopy(copy, source, CopyName(copy), false); }
            intact = await PeekAsync(beta, token);
            Require(intact.Count == 1 && intact[0].SequenceNumber == baseSequence,
                "literal completion crossed subscription ownership");
            CheckCopy(intact[0], source, PublisherRule);
            dead = await PeekAsync(alphaDead, token);
            Require(dead.Count == (failure ? 1 : 0), "active completion removed or added a conversion dead letter");
            if (failure) { CheckLiteralCopy(dead[0], source, LiteralFailureRule, true); }
        }
        ServiceBusReceivedMessage original = await ReceiveAsync(beta, token);
        CheckCopy(original, source, PublisherRule);
        Require(original.SequenceNumber == baseSequence, "sibling delivery lost its original sequence");
        await BoundedAsync(ct => beta.CompleteMessageAsync(original, ct), token);
        if (failure)
        {
            ServiceBusReceivedMessage failed = await ReceiveAsync(alphaDead, token);
            CheckLiteralCopy(failed, source, LiteralFailureRule, true);
            Require(failed.SequenceNumber == baseSequence + 3, "dead-letter delivery lost its browsed identity");
            await BoundedAsync(ct => alphaDead.CompleteMessageAsync(failed, ct), token);
        }
        foreach (ServiceBusReceiver receiver in new[] { alpha, beta, alphaDead, betaDead })
        {
            Require((await PeekAsync(receiver, token)).Count == 0,
                "literal publication retained a completed active or dead-letter copy");
        }
    }

    private static void CheckLiteralCopy(ServiceBusReceivedMessage copy,
        ServiceBusMessage source, string name, bool deadLetter)
    {
        Require(copy.Body.ToArray().SequenceEqual(source.Body.ToArray())
            && copy.MessageId == source.MessageId && copy.CorrelationId == source.CorrelationId
            && copy.Subject == source.Subject && copy.ContentType == source.ContentType
            && copy.To == source.To && copy.ReplyTo == source.ReplyTo
            && copy.ReplyToSessionId == source.ReplyToSessionId && copy.SessionId == source.SessionId
            && (deadLetter || copy.TimeToLive == source.TimeToLive)
            && copy.GetRawAmqpMessage().Properties.ContentEncoding == "utf-8"
            && copy.GetRawAmqpMessage().Footer.Count == 1
            && copy.GetRawAmqpMessage().Footer.TryGetValue("producer-checksum", out object? checksum)
            && Equals(checksum, "rule-action-checksum"),
            "literal action changed body, supported system properties, encoding, or footer");
        var expected = new Dictionary<string, object>(source.ApplicationProperties);
        if (name == LiteralStringRule)
        {
            expected["colour"] = "blue";
            expected["nullable"] = "present";
        }
        else if (name == LiteralIntegerRule)
        {
            expected["number"] = -7L;
            expected["enabled"] = false;
            expected["added"] = 23L;
        }
        else if (name == LiteralFailureRule)
        {
            Require(deadLetter && copy.DeadLetterReason == "SwitchyardSqlActionError"
                && copy.DeadLetterErrorDescription == "TypeMismatch",
                "conversion failure lost its finite local reason or description");
            expected["DeadLetterReason"] = "SwitchyardSqlActionError";
            expected["DeadLetterErrorDescription"] = "TypeMismatch";
        }
        else { Require(name == PublisherRule && !deadLetter, "literal publication produced an unknown copy"); }
        expected["RuleName"] = name;
        Require(copy.ApplicationProperties.Count == expected.Count,
            "literal action removed or added an unexpected property");
        foreach (var pair in expected)
        {
            Require(copy.ApplicationProperties.TryGetValue(pair.Key, out object? value)
                && Equals(value, pair.Value), "literal action changed an original property or checked scalar type");
        }
        Require(Equals(copy.ApplicationProperties["rulename"], "lowercase-retained")
            && Equals(copy.ApplicationProperties["Audit"], "case-retained"),
            "literal action case-folded an exact-key target");
    }
}
