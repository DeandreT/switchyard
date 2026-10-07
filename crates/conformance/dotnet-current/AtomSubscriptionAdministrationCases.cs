using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using static AtomAdministrationCases;

internal static class AtomSubscriptionAdministrationCases
{
    internal const string Topic = "sdk-atom-subscriptions";

    internal static async Task RunAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string scenario, string suffix, CancellationToken token)
    {
        switch (scenario)
        {
            case "subscriptions-empty":
                await RequireMissingAsync(client, "sdk-atom-missing", token);
                break;
            case "subscriptions-create":
                Response<SubscriptionProperties> created =
                    await client.CreateSubscriptionAsync(Topic, DefaultName(suffix), token);
                RequireStatus(created.GetRawResponse(), 201);
                RequireDefault(created.Value, DefaultName(suffix));
                RequireDefault(await GetAsync(other, DefaultName(suffix), token), DefaultName(suffix));
                Require((await other.SubscriptionExistsAsync(Topic, DefaultName(suffix), token)).Value,
                    "Created subscription Exists.");
                var options = new CreateSubscriptionOptions(Topic, DefinitionName(suffix))
                {
                    LockDuration = TimeSpan.FromSeconds(15),
                    MaxDeliveryCount = 3,
                    DefaultMessageTimeToLive = TimeSpan.FromSeconds(45),
                    DeadLetteringOnMessageExpiration = true,
                    EnableDeadLetteringOnFilterEvaluationExceptions = false,
                };
                created = await client.CreateSubscriptionAsync(options, token);
                RequireStatus(created.GetRawResponse(), 201);
                RequireDefinition(created.Value, DefinitionName(suffix));
                RequireDefinition(await GetAsync(other, DefinitionName(suffix), token), DefinitionName(suffix));
                break;
            case "subscriptions-inspect":
                RequireDefault(await GetAsync(client, DefaultName(suffix), token), DefaultName(suffix));
                RequireDefinition(await GetAsync(client, DefinitionName(suffix), token), DefinitionName(suffix));
                break;
            case "subscriptions-update":
                await UpdateAsync(client, other, suffix, token);
                break;
            case "subscriptions-refusals":
                await RefusalsAsync(client, other, suffix, token);
                break;
            case "subscriptions-delete":
                foreach (string name in new[] { DefaultName(suffix), DefinitionName(suffix) })
                {
                    RequireStatus(await client.DeleteSubscriptionAsync(Topic, name, token), 200);
                    await RequireMissingAsync(other, name, token);
                }
                break;
            case "subscriptions-recreate":
                created = await client.CreateSubscriptionAsync(Topic, DefaultName(suffix), token);
                RequireStatus(created.GetRawResponse(), 201);
                RequireDefault(created.Value, DefaultName(suffix));
                RequireDefault(await GetAsync(other, DefaultName(suffix), token), DefaultName(suffix));
                break;
            case "subscriptions-denied":
                await ExpectUnauthorizedAsync(() => client.GetSubscriptionAsync(Topic, DefaultName(suffix), token));
                await ExpectUnauthorizedAsync(() => client.CreateSubscriptionAsync(Topic, DefaultName(suffix), token));
                await ExpectUnauthorizedAsync(() => client.DeleteSubscriptionAsync(Topic, DefaultName(suffix), token));
                break;
            case "subscriptions-tls-refused":
                try
                {
                    await client.GetSubscriptionAsync(Topic, DefaultName(suffix), token);
                }
                catch (Exception error) when (ContainsCertificateFailure(error))
                {
                    break;
                }
                throw new InvalidOperationException("Strict TLS did not refuse the subscription request.");
            default:
                throw new InvalidOperationException("Unknown closed subscription scenario.");
        }
    }

    internal static string DefaultName(string suffix) => $"Default-{suffix}";
    internal static string DefinitionName(string suffix) => $"Definition-{suffix}";

    private static async Task<SubscriptionProperties> GetAsync(
        ServiceBusAdministrationClient client, string name, CancellationToken token)
    {
        Response<SubscriptionProperties> response = await client.GetSubscriptionAsync(Topic, name, token);
        RequireStatus(response.GetRawResponse(), 200);
        Require(string.Equals(response.Value.TopicName, Topic, StringComparison.Ordinal)
            && string.Equals(response.Value.SubscriptionName, name, StringComparison.Ordinal),
            "Ordinal subscription identity.");
        return response.Value;
    }

    private static async Task RequireMissingAsync(
        ServiceBusAdministrationClient client, string name, CancellationToken token)
    {
        await ExpectServiceBusAsync(() => client.GetSubscriptionAsync(Topic, name, token),
            ServiceBusFailureReason.MessagingEntityNotFound);
        Require(!(await client.SubscriptionExistsAsync(Topic, name, token)).Value, "Missing subscription Exists.");
        await ExpectServiceBusAsync(() => client.DeleteSubscriptionAsync(Topic, name, token),
            ServiceBusFailureReason.MessagingEntityNotFound);
    }

    private static void RequireDefault(SubscriptionProperties subscription, string name) =>
        RequireProfile(subscription, name, 60, 10, TimeSpan.MaxValue, false, true);

    private static void RequireDefinition(SubscriptionProperties subscription, string name) =>
        RequireProfile(subscription, name, 15, 3, TimeSpan.FromSeconds(45), true, false);

    private static void RequireProfile(
        SubscriptionProperties subscription, string name, int lockSeconds, int deliveries,
        TimeSpan ttl, bool expiryDeadLetter, bool filterDeadLetter)
    {
        Require(string.Equals(subscription.TopicName, Topic, StringComparison.Ordinal)
            && string.Equals(subscription.SubscriptionName, name, StringComparison.Ordinal),
            "Exact subscription identity.");
        Require(subscription.LockDuration == TimeSpan.FromSeconds(lockSeconds)
            && subscription.MaxDeliveryCount == deliveries && subscription.DefaultMessageTimeToLive == ttl
            && subscription.DeadLetteringOnMessageExpiration == expiryDeadLetter
            && subscription.EnableDeadLetteringOnFilterEvaluationExceptions == filterDeadLetter,
            "Exact subscription static definition.");
        Require(!subscription.RequiresSession && subscription.EnableBatchedOperations
            && subscription.Status == EntityStatus.Active && subscription.AutoDeleteOnIdle == TimeSpan.MaxValue
            && string.IsNullOrEmpty(subscription.ForwardTo)
            && string.IsNullOrEmpty(subscription.ForwardDeadLetteredMessagesTo)
            && string.IsNullOrEmpty(subscription.UserMetadata), "Closed subscription profile.");
    }

    private static async Task UpdateAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string suffix, CancellationToken token)
    {
        foreach (bool reset in new[] { false, true })
        {
            string name = reset ? DefinitionName(suffix) : DefaultName(suffix);
            SubscriptionProperties desired = await GetAsync(client, name, token);
            if (reset)
            {
                RequireDefinition(desired, name);
            }
            else
            {
                RequireDefault(desired, name);
            }
            desired.LockDuration = TimeSpan.FromSeconds(reset ? 60 : 30);
            desired.MaxDeliveryCount = reset ? 10 : 4;
            desired.DefaultMessageTimeToLive = reset ? TimeSpan.MaxValue : TimeSpan.FromSeconds(60);
            desired.RequiresSession = false;
            desired.DeadLetteringOnMessageExpiration = !reset;
            desired.EnableDeadLetteringOnFilterEvaluationExceptions = reset;
            for (int repeat = 0; repeat < 2; repeat++)
            {
                Response<SubscriptionProperties> updated =
                    await client.UpdateSubscriptionAsync(desired, token);
                RequireStatus(updated.GetRawResponse(), 200);
                RequireProfile(updated.Value, name, reset ? 60 : 30, reset ? 10 : 4,
                    reset ? TimeSpan.MaxValue : TimeSpan.FromSeconds(60), !reset, reset);
                RequireProfile(await GetAsync(other, name, token), name, reset ? 60 : 30, reset ? 10 : 4,
                    reset ? TimeSpan.MaxValue : TimeSpan.FromSeconds(60), !reset, reset);
                desired = updated.Value;
            }
        }
    }

    private static async Task RefusalsAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string suffix, CancellationToken token)
    {
        await ExpectServiceBusAsync(() => client.CreateSubscriptionAsync(Topic, DefaultName(suffix), token),
            ServiceBusFailureReason.MessagingEntityAlreadyExists);
        foreach ((string label, Action<CreateSubscriptionOptions> configure) in
            new (string, Action<CreateSubscriptionOptions>)[]
            {
                ("session", options => options.RequiresSession = true),
                ("batch-off", options => options.EnableBatchedOperations = false),
                ("metadata", options => options.UserMetadata = "unsupported-fixture-metadata"),
                ("forward", options => options.ForwardTo = "unsupported-destination"),
                ("forward-dlq", options => options.ForwardDeadLetteredMessagesTo = "unsupported-destination"),
                ("idle", options => options.AutoDeleteOnIdle = TimeSpan.FromMinutes(5)),
                ("status", options => options.Status = EntityStatus.ReceiveDisabled),
                ("lock", options => options.LockDuration = TimeSpan.FromMilliseconds(4_999)),
            })
        {
            string name = $"Refused-{suffix}-{label}";
            var options = new CreateSubscriptionOptions(Topic, name);
            configure(options);
            await ExpectArgumentAsync(() => client.CreateSubscriptionAsync(options, token));
            Require(!(await other.SubscriptionExistsAsync(Topic, name, token)).Value,
                "Refused subscription was created.");
        }
        var parameterized = new TrueRuleFilter();
        parameterized.Parameters.Add("unsupported", 1);
        foreach ((string label, CreateRuleOptions rule) in new (string, CreateRuleOptions)[]
            {
                ("false-filter", new CreateRuleOptions(CreateRuleOptions.DefaultRuleName, new FalseRuleFilter())),
                ("sql-filter", new CreateRuleOptions(CreateRuleOptions.DefaultRuleName, new SqlRuleFilter("1=1"))),
                ("rule-name", new CreateRuleOptions("Custom", new TrueRuleFilter())),
                ("parameters", new CreateRuleOptions(CreateRuleOptions.DefaultRuleName, parameterized)),
                ("action", new CreateRuleOptions { Action = new SqlRuleAction("SET sys.Label = 'changed'") }),
            })
        {
            string name = $"Refused-{suffix}-{label}";
            await ExpectArgumentAsync(() => client.CreateSubscriptionAsync(
                new CreateSubscriptionOptions(Topic, name), rule, token));
            Require(!(await other.SubscriptionExistsAsync(Topic, name, token)).Value,
                "Refused default rule created a subscription.");
        }
        await ExpectArgumentAsync(async () =>
        {
            await foreach (SubscriptionProperties _ in client.GetSubscriptionsAsync(Topic, token)) { }
        });
        await ExpectArgumentAsync(() => client.GetSubscriptionRuntimePropertiesAsync(Topic, DefaultName(suffix), token));
        RuleProperties unchangedRule = (await client.GetRuleAsync(
            Topic, DefaultName(suffix), CreateRuleOptions.DefaultRuleName, token)).Value;
        await ExpectArgumentAsync(() => client.UpdateRuleAsync(
            Topic, DefaultName(suffix), unchangedRule, token));
        RequireDefault(await GetAsync(other, DefaultName(suffix), token), DefaultName(suffix));
        RequireDefinition(await GetAsync(other, DefinitionName(suffix), token), DefinitionName(suffix));
    }
}
