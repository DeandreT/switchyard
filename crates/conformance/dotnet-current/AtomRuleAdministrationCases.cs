using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using static AtomAdministrationCases;

internal static class AtomRuleAdministrationCases
{
    internal const string Topic = "sdk-atom-rules";
    private const string FalseRuleName = "Rules";
    private const string MissingRuleName = "Missing";
    private const string OpaqueSubscription = "NativeOpaque";

    internal static async Task RunAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string scenario, string suffix, CancellationToken token)
    {
        string subscription = OwnedName(suffix);
        switch (scenario)
        {
            case "rules-empty":
                await RequireMissingGetAsync(client, subscription, MissingRuleName, token);
                await RequireRulesAsync(client, subscription, token);
                await ExpectServiceBusAsync(() => client.DeleteRuleAsync(Topic, subscription, MissingRuleName, token),
                    ServiceBusFailureReason.MessagingEntityNotFound);
                break;
            case "rules-create":
                await CreateAsync(client, other, subscription, CreateRuleOptions.DefaultRuleName, true, token);
                await CreateAsync(client, other, subscription, FalseRuleName, false, token);
                await RequireRulesAsync(other, subscription, token,
                    (CreateRuleOptions.DefaultRuleName, true), (FalseRuleName, false));
                break;
            case "rules-inspect":
                await GetAsync(client, subscription, CreateRuleOptions.DefaultRuleName, true, token);
                await GetAsync(client, subscription, FalseRuleName, false, token);
                await RequireRulesAsync(client, subscription, token,
                    (CreateRuleOptions.DefaultRuleName, true), (FalseRuleName, false));
                break;
            case "rules-refusals":
                await RefusalsAsync(client, other, subscription, suffix, token);
                break;
            case "rules-delete":
                foreach (string name in new[] { CreateRuleOptions.DefaultRuleName, FalseRuleName })
                {
                    RequireStatus(await client.DeleteRuleAsync(Topic, subscription, name, token), 200);
                    await RequireMissingGetAsync(other, subscription, name, token);
                    await ExpectServiceBusAsync(() => client.DeleteRuleAsync(Topic, subscription, name, token),
                        ServiceBusFailureReason.MessagingEntityNotFound);
                }
                await RequireRulesAsync(other, subscription, token);
                break;
            case "rules-recreate":
                await CreateAsync(client, other, subscription, CreateRuleOptions.DefaultRuleName, true, token);
                await RequireRulesAsync(other, subscription, token, (CreateRuleOptions.DefaultRuleName, true));
                break;
            case "rules-opaque":
                await OpaqueAsync(client, other, suffix, token);
                break;
            case "rules-denied":
                await ExpectUnauthorizedAsync(() => client.GetRuleAsync(
                    Topic, subscription, CreateRuleOptions.DefaultRuleName, token));
                await ExpectUnauthorizedAsync(() => RequireRulesAsync(client, subscription, token));
                await ExpectUnauthorizedAsync(() => client.CreateRuleAsync(Topic, subscription,
                    new CreateRuleOptions($"Denied-{suffix}", new TrueRuleFilter()), token));
                await ExpectUnauthorizedAsync(() => client.DeleteRuleAsync(
                    Topic, subscription, CreateRuleOptions.DefaultRuleName, token));
                break;
            case "rules-tls-refused":
                try
                {
                    await client.GetRuleAsync(Topic, subscription, CreateRuleOptions.DefaultRuleName, token);
                }
                catch (Exception error) when (ContainsCertificateFailure(error))
                {
                    break;
                }
                throw new InvalidOperationException("Strict TLS did not refuse the rule request.");
            default:
                throw new InvalidOperationException("Unknown closed rule scenario.");
        }
    }

    private static string OwnedName(string suffix) => $"Rules-{suffix}";

    private static async Task CreateAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string subscription, string name, bool trueFilter, CancellationToken token)
    {
        RuleFilter filter = trueFilter ? new TrueRuleFilter() : new FalseRuleFilter();
        Response<RuleProperties> response = await client.CreateRuleAsync(
            Topic, subscription, new CreateRuleOptions(name, filter), token);
        RequireStatus(response.GetRawResponse(), 201);
        RequireRule(response.Value, name, trueFilter);
        await GetAsync(other, subscription, name, trueFilter, token);
    }

    private static async Task<RuleProperties> GetAsync(
        ServiceBusAdministrationClient client, string subscription, string name,
        bool trueFilter, CancellationToken token)
    {
        Response<RuleProperties> response = await client.GetRuleAsync(Topic, subscription, name, token);
        RequireStatus(response.GetRawResponse(), 200);
        RequireRule(response.Value, name, trueFilter);
        return response.Value;
    }

    private static Task RequireMissingGetAsync(
        ServiceBusAdministrationClient client, string subscription, string name, CancellationToken token) =>
        ExpectServiceBusAsync(() => client.GetRuleAsync(Topic, subscription, name, token),
            ServiceBusFailureReason.MessagingEntityNotFound);

    private static void RequireRule(RuleProperties rule, string name, bool trueFilter)
    {
        Require(string.Equals(rule.Name, name, StringComparison.Ordinal), "Ordinal rule name.");
        Require(rule.Filter is SqlRuleFilter filter
            && filter.GetType() == (trueFilter ? typeof(TrueRuleFilter) : typeof(FalseRuleFilter))
            && string.Equals(filter.SqlExpression, trueFilter ? "1=1" : "1=0", StringComparison.Ordinal)
            && filter.Parameters.Count == 0 && rule.Action is null, "Exact constant rule definition.");
    }

    private static async Task RequireRulesAsync(
        ServiceBusAdministrationClient client, string subscription, CancellationToken token,
        params (string Name, bool TrueFilter)[] expected)
    {
        var expectedRules = new Dictionary<string, bool>(StringComparer.Ordinal);
        foreach ((string name, bool trueFilter) in expected)
        {
            Require(expectedRules.TryAdd(name, trueFilter), "Duplicate expected rule name.");
        }
        var names = new HashSet<string>(StringComparer.Ordinal);
        int pages = 0;
        await foreach (Page<RuleProperties> page in client.GetRulesAsync(Topic, subscription, token).AsPages())
        {
            Require(++pages == 1, "Rule page work bound.");
            RequireStatus(page.GetRawResponse(), 200);
            Require(page.Values.Count <= 32 && page.Values.Count == expectedRules.Count,
                "Exact single rule page size.");
            foreach (RuleProperties rule in page.Values)
            {
                Require(expectedRules.TryGetValue(rule.Name, out bool trueFilter), "Unexpected ordinal rule name.");
                Require(names.Add(rule.Name), "Duplicate rule in feed.");
                RequireRule(rule, rule.Name, trueFilter);
            }
        }
        Require(pages == 1 && names.SetEquals(expectedRules.Keys), "Complete single rule page.");
    }

    private static async Task RefusalsAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string subscription, string suffix, CancellationToken token)
    {
        await ExpectServiceBusAsync(() => client.CreateRuleAsync(Topic, subscription,
            new CreateRuleOptions(CreateRuleOptions.DefaultRuleName, new TrueRuleFilter()), token),
            ServiceBusFailureReason.MessagingEntityAlreadyExists);
        var parameterized = new TrueRuleFilter();
        parameterized.Parameters.Add("unsupported", 1);
        foreach ((string label, CreateRuleOptions options) in new (string, CreateRuleOptions)[]
        {
            ("sql", new CreateRuleOptions($"Refused-{suffix}-sql", new SqlRuleFilter("1=1"))),
            ("correlation", new CreateRuleOptions($"Refused-{suffix}-correlation", new CorrelationRuleFilter())),
            ("parameters", new CreateRuleOptions($"Refused-{suffix}-parameters", parameterized)),
            ("action", new CreateRuleOptions($"Refused-{suffix}-action", new TrueRuleFilter())
            {
                Action = new SqlRuleAction("SET sys.Label = 'changed'"),
            }),
        })
        {
            await ExpectArgumentAsync(() => client.CreateRuleAsync(Topic, subscription, options, token));
            await RequireMissingGetAsync(other, subscription, $"Refused-{suffix}-{label}", token);
        }
        RuleProperties unchanged = await GetAsync(
            client, subscription, CreateRuleOptions.DefaultRuleName, true, token);
        await ExpectArgumentAsync(() => client.UpdateRuleAsync(Topic, subscription, unchanged, token));
        await GetAsync(other, subscription, CreateRuleOptions.DefaultRuleName, true, token);
        await GetAsync(other, subscription, FalseRuleName, false, token);
        await RequireRulesAsync(other, subscription, token,
            (CreateRuleOptions.DefaultRuleName, true), (FalseRuleName, false));
    }

    private static async Task OpaqueAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string suffix, CancellationToken token)
    {
        await GetAsync(client, OpaqueSubscription, CreateRuleOptions.DefaultRuleName, true, token);
        await ExpectArgumentAsync(() => RequireRulesAsync(client, OpaqueSubscription, token));
        foreach (string name in new[] { "NativeSql", "NativeAction" })
        {
            await ExpectArgumentAsync(() => client.GetRuleAsync(Topic, OpaqueSubscription, name, token));
        }
        string transient = $"Transient-{suffix}";
        await CreateAsync(client, other, OpaqueSubscription, transient, false, token);
        RequireStatus(await client.DeleteRuleAsync(Topic, OpaqueSubscription, transient, token), 200);
        await RequireMissingGetAsync(other, OpaqueSubscription, transient, token);
        await GetAsync(other, OpaqueSubscription, CreateRuleOptions.DefaultRuleName, true, token);
        await ExpectArgumentAsync(() => RequireRulesAsync(other, OpaqueSubscription, token));
    }
}
