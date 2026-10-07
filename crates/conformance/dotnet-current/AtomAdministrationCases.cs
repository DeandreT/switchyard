using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Authentication;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static class AtomAdministrationCases
{
    private static readonly HashSet<string> Scenarios = new(StringComparer.Ordinal)
    {
        "empty", "create", "update", "noop", "refusals", "quota", "retention",
        "delete", "paging", "denied", "tls-refused",
    };

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 6 || !Scenarios.Contains(args[1])
            || !Uri.TryCreate(args[2], UriKind.Absolute, out Uri? endpoint)
            || endpoint.Scheme != "https" || endpoint.Port <= 0
            || endpoint.UserInfo.Length != 0 || endpoint.Query.Length != 0
            || endpoint.Fragment.Length != 0 || endpoint.AbsolutePath != "/"
            || (endpoint.Host != "localhost"
                && !(args[1] == "tls-refused" && endpoint.Host == "127.0.0.1")))
        {
            Console.Error.WriteLine("Atom SDK diagnostic scenario=arguments exception=argument");
            return 2;
        }
        string scenario = args[1];
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(120));
            using var transport = new AtomAdministrationTransport(endpoint, args[3]);
            ServiceBusAdministrationClient named = transport.CreateClient(false, args[4], args[5]);
            ServiceBusAdministrationClient connection = transport.CreateClient(true, args[4], args[5]);
            EmitLoadedAssemblyEvidence();
            if (scenario == "paging")
            {
                await AtomAdministrationPagingCases.RunAsync(named, connection, deadline.Token);
            }
            else
            {
                foreach (bool fromConnectionString in new[] { false, true })
                {
                    ServiceBusAdministrationClient client = fromConnectionString ? connection : named;
                    ServiceBusAdministrationClient other = fromConnectionString ? named : connection;
                    string suffix = fromConnectionString ? "connection" : "named";
                    await RunScenarioAsync(client, other, scenario, suffix, deadline.Token);
                }
            }
            Console.WriteLine($"official .NET Atom administration {scenario} named-key/connection-string passed");
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine($"Atom SDK diagnostic scenario={scenario} exception={Classify(error)}");
            return 3;
        }
    }

    private static void EmitLoadedAssemblyEvidence()
    {
        Emit(typeof(ServiceBusAdministrationClient).Assembly, "Azure.Messaging.ServiceBus");
        Emit(typeof(Azure.Core.Pipeline.HttpClientTransport).Assembly, "Azure.Core");

        static void Emit(Assembly assembly, string name)
        {
            AssemblyName identity = assembly.GetName();
            Version version = identity.Version ?? throw new InvalidOperationException("Assembly version unavailable.");
            if (!string.Equals(identity.Name, name, StringComparison.Ordinal)
                || new[] { version.Major, version.Minor, version.Build, version.Revision }
                    .Any(value => value < 0 || value > ushort.MaxValue)
                || string.IsNullOrEmpty(assembly.Location))
            {
                throw new InvalidOperationException("Owned assembly identity unavailable.");
            }
            string location = Path.GetFullPath(assembly.Location);
            string expected = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, name + ".dll"));
            if (!string.Equals(location, expected, StringComparison.Ordinal)
                || (File.GetAttributes(location) & (FileAttributes.ReparsePoint | FileAttributes.Directory)) != 0)
            {
                throw new InvalidOperationException("Loaded assembly is not an owned output file.");
            }
            using FileStream stream = File.OpenRead(location);
            long length = stream.Length;
            if (length <= 0 || length > 16 * 1024 * 1024)
            {
                throw new InvalidOperationException("Owned assembly length unsupported.");
            }
            string hash = Convert.ToHexString(SHA256.HashData(stream));
            if (stream.Position != length || stream.Length != length)
            {
                throw new InvalidOperationException("Owned assembly length changed.");
            }
            Console.WriteLine($"Atom SDK loaded assembly={name} version={version.ToString(4)} sha256={hash}");
        }
    }

    private static async Task RunScenarioAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string scenario, string suffix, CancellationToken token)
    {
        switch (scenario)
        {
            case "empty":
                await AtomAdministrationPagingCases.RequireEmptyAsync(client, token);
                await ExpectServiceBusAsync(() => client.GetQueueAsync("sdk-atom-missing", token),
                    ServiceBusFailureReason.MessagingEntityNotFound);
                Require(!(await client.QueueExistsAsync("sdk-atom-missing", token)).Value, "Missing Exists.");
                await ExpectServiceBusAsync(() => client.DeleteQueueAsync("sdk-atom-missing", token),
                    ServiceBusFailureReason.MessagingEntityNotFound);
                break;
            case "create":
                await CreateAsync(client, other, suffix, token);
                break;
            case "update":
                await UpdateAsync(client, suffix, token);
                break;
            case "noop":
                foreach (string name in new[] { DefaultName(suffix), DefinitionName(suffix) })
                {
                    QueueProperties before = await GetAsync(client, name, token);
                    Response<QueueProperties> updated = await client.UpdateQueueAsync(before, token);
                    RequireStatus(updated.GetRawResponse(), 200);
                    RequireSame(before, updated.Value);
                    RequireSame(before, await GetAsync(other, name, token));
                }
                break;
            case "refusals":
                await RefusalsAsync(client, suffix, token);
                break;
            case "quota":
                await QuotaAsync(client, suffix, token);
                break;
            case "retention":
                await RetentionAsync(client, suffix, token);
                break;
            case "delete":
                await DeleteAsync(client, suffix, token);
                break;
            case "denied":
                await ExpectUnauthorizedAsync(() => client.GetQueueAsync("sdk-atom-denied", token));
                await ExpectUnauthorizedAsync(() => client.CreateQueueAsync("sdk-atom-denied", token));
                await ExpectUnauthorizedAsync(() => AtomAdministrationPagingCases.RequireEmptyAsync(client, token));
                break;
            case "tls-refused":
                try
                {
                    await client.GetQueueAsync("sdk-atom-tls-probe", token);
                }
                catch (Exception error) when (ContainsCertificateFailure(error))
                {
                    break;
                }
                throw new InvalidOperationException("Strict TLS did not refuse the certificate.");
            default:
                throw new InvalidOperationException("Unknown closed scenario.");
        }
    }

    private static async Task CreateAsync(
        ServiceBusAdministrationClient client, ServiceBusAdministrationClient other,
        string suffix, CancellationToken token)
    {
        string ordinary = DefaultName(suffix);
        Response<QueueProperties> created = await client.CreateQueueAsync(ordinary, token);
        RequireStatus(created.GetRawResponse(), 201);
        RequireDefault(created.Value, ordinary);
        RequireDefault(await GetAsync(other, ordinary, token), ordinary);
        Require((await other.QueueExistsAsync(ordinary, token)).Value, "Created Exists.");
        string definition = DefinitionName(suffix);
        var options = new CreateQueueOptions(definition)
        {
            LockDuration = TimeSpan.FromSeconds(15),
            MaxSizeInMegabytes = 2,
            MaxMessageSizeInKilobytes = 4,
            MaxDeliveryCount = 3,
            DefaultMessageTimeToLive = TimeSpan.FromSeconds(45),
            DeadLetteringOnMessageExpiration = true,
        };
        created = await client.CreateQueueAsync(options, token);
        RequireStatus(created.GetRawResponse(), 201);
        RequireDefinition(created.Value, definition, 15, 2, 4, 3, TimeSpan.FromSeconds(45), true);
        RequireSame(created.Value, await GetAsync(other, definition, token));
    }

    private static async Task UpdateAsync(
        ServiceBusAdministrationClient client, string suffix, CancellationToken token)
    {
        string name = DefinitionName(suffix);
        QueueProperties desired = await GetAsync(client, name, token);
        RequireDefinition(desired, name, 15, 2, 4, 3, TimeSpan.FromSeconds(45), true);
        desired.LockDuration = TimeSpan.FromSeconds(10);
        desired.MaxSizeInMegabytes = 4;
        desired.MaxMessageSizeInKilobytes = 16;
        desired.MaxDeliveryCount = 6;
        desired.DefaultMessageTimeToLive = TimeSpan.FromSeconds(90);
        desired.DeadLetteringOnMessageExpiration = false;
        Response<QueueProperties> updated = await client.UpdateQueueAsync(desired, token);
        RequireStatus(updated.GetRawResponse(), 200);
        RequireDefinition(updated.Value, name, 10, 4, 16, 6, TimeSpan.FromSeconds(90), false);
        desired = await GetAsync(client, name, token);
        desired.LockDuration = TimeSpan.FromSeconds(20);
        desired.MaxSizeInMegabytes = 3;
        desired.MaxMessageSizeInKilobytes = 8;
        desired.MaxDeliveryCount = 4;
        // Both pins omit this sentinel; the complete PUT must clear finite TTL.
        desired.DefaultMessageTimeToLive = TimeSpan.MaxValue;
        desired.DeadLetteringOnMessageExpiration = true;
        // Disabled duplicate history is omitted even after assigning a valid value.
        desired.DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(2);
        updated = await client.UpdateQueueAsync(desired, token);
        RequireStatus(updated.GetRawResponse(), 200);
        RequireDefinition(updated.Value, name, 20, 3, 8, 4, TimeSpan.MaxValue, true);
        RequireDefinition(await GetAsync(client, name, token), name, 20, 3, 8, 4, TimeSpan.MaxValue, true);
    }

    private static async Task RefusalsAsync(
        ServiceBusAdministrationClient client, string suffix, CancellationToken token)
    {
        await ExpectServiceBusAsync(() => client.CreateQueueAsync(DefaultName(suffix), token),
            ServiceBusFailureReason.MessagingEntityAlreadyExists);
        foreach ((string label, Action<CreateQueueOptions> configure) in new (string, Action<CreateQueueOptions>)[]
        {
            ("session", options => options.RequiresSession = true),
            ("dedup", options => options.RequiresDuplicateDetection = true),
            ("partition", options => options.EnablePartitioning = true),
            ("batch-off", options => options.EnableBatchedOperations = false),
            ("metadata", options => options.UserMetadata = "unsupported-fixture-metadata"),
            ("size", options => options.MaxMessageSizeInKilobytes = 257),
            ("limit", options => options.MaxSizeInMegabytes = 0),
            ("lock", options => options.LockDuration = TimeSpan.FromMilliseconds(4_999)),
        })
        {
            var options = new CreateQueueOptions($"sdk-atom-refused-{suffix}-{label}");
            configure(options);
            await ExpectArgumentAsync(() => client.CreateQueueAsync(options, token));
        }
        RequireDefault(await GetAsync(client, DefaultName(suffix), token), DefaultName(suffix));
    }

    private static async Task QuotaAsync(
        ServiceBusAdministrationClient client, string suffix, CancellationToken token)
    {
        string name = $"sdk-atom-quota-{suffix}";
        QueueProperties before = await GetAsync(client, name, token);
        RequireDefinition(before, name, 60, 2, 256, 10, TimeSpan.MaxValue, false);
        QueueProperties desired = await GetAsync(client, name, token);
        desired.MaxSizeInMegabytes = 1;
        await ExpectServiceBusAsync(() => client.UpdateQueueAsync(desired, token),
            ServiceBusFailureReason.QuotaExceeded);
        RequireSame(before, await GetAsync(client, name, token));
    }

    private static async Task RetentionAsync(
        ServiceBusAdministrationClient client, string suffix, CancellationToken token)
    {
        string name = $"sdk-atom-retention-{suffix}";
        QueueProperties desired = await GetAsync(client, name, token);
        RequireDefinition(desired, name, 60, 2, 256, 10, TimeSpan.FromMinutes(10), false);
        desired.LockDuration = TimeSpan.FromSeconds(10);
        desired.MaxSizeInMegabytes = 3;
        desired.MaxMessageSizeInKilobytes = 4;
        desired.MaxDeliveryCount = 3;
        desired.DefaultMessageTimeToLive = TimeSpan.MaxValue;
        desired.DeadLetteringOnMessageExpiration = true;
        Response<QueueProperties> updated = await client.UpdateQueueAsync(desired, token);
        RequireStatus(updated.GetRawResponse(), 200);
        RequireDefinition(updated.Value, name, 10, 3, 4, 3, TimeSpan.MaxValue, true);
        RequireSame(updated.Value, await GetAsync(client, name, token));
    }

    private static async Task DeleteAsync(
        ServiceBusAdministrationClient client, string suffix, CancellationToken token)
    {
        foreach (string name in new[] { DefaultName(suffix), DefinitionName(suffix) })
        {
            RequireStatus(await client.DeleteQueueAsync(name, token), 200);
            Require(!(await client.QueueExistsAsync(name, token)).Value, "Deleted Exists.");
            await ExpectServiceBusAsync(() => client.GetQueueAsync(name, token),
                ServiceBusFailureReason.MessagingEntityNotFound);
            await ExpectServiceBusAsync(() => client.DeleteQueueAsync(name, token),
                ServiceBusFailureReason.MessagingEntityNotFound);
        }
        Response<QueueProperties> recreated = await client.CreateQueueAsync(DefaultName(suffix), token);
        RequireStatus(recreated.GetRawResponse(), 201);
        RequireDefault(recreated.Value, DefaultName(suffix));
        RequireDefault(await GetAsync(client, DefaultName(suffix), token), DefaultName(suffix));
    }

    private static string DefaultName(string suffix) => $"sdk-atom-default-{suffix}";
    private static string DefinitionName(string suffix) => $"sdk-atom-definition-{suffix}";

    private static async Task<QueueProperties> GetAsync(
        ServiceBusAdministrationClient client, string name, CancellationToken token)
    {
        Response<QueueProperties> response = await client.GetQueueAsync(name, token);
        RequireStatus(response.GetRawResponse(), 200);
        Require(string.Equals(response.Value.Name, name, StringComparison.Ordinal), "Exact queue name.");
        return response.Value;
    }

    internal static void RequireDefault(QueueProperties queue, string name) =>
        RequireDefinition(queue, name, 60, 1024, 256, 10, TimeSpan.MaxValue, false);

    internal static void RequireDefinition(
        QueueProperties queue, string name, int lockSeconds, long limitMiB,
        long maxMessageKiB, int deliveries, TimeSpan ttl, bool expireToDeadLetter)
    {
        Require(string.Equals(queue.Name, name, StringComparison.Ordinal), "Ordinal queue name.");
        Require(queue.LockDuration == TimeSpan.FromSeconds(lockSeconds)
            && queue.MaxSizeInMegabytes == limitMiB && queue.MaxMessageSizeInKilobytes == maxMessageKiB
            && queue.MaxDeliveryCount == deliveries && queue.DefaultMessageTimeToLive == ttl
            && queue.DeadLetteringOnMessageExpiration == expireToDeadLetter
            && queue.DuplicateDetectionHistoryTimeWindow == TimeSpan.FromMinutes(1), "Exact static definition.");
        RequireProfile(queue);
    }

    private static void RequireSame(QueueProperties expected, QueueProperties actual)
    {
        // The SDK equality helper ignores path case and disabled history differences.
        Require(string.Equals(expected.Name, actual.Name, StringComparison.Ordinal)
            && expected.LockDuration == actual.LockDuration
            && expected.MaxSizeInMegabytes == actual.MaxSizeInMegabytes
            && expected.MaxMessageSizeInKilobytes == actual.MaxMessageSizeInKilobytes
            && expected.MaxDeliveryCount == actual.MaxDeliveryCount
            && expected.DefaultMessageTimeToLive == actual.DefaultMessageTimeToLive
            && expected.DeadLetteringOnMessageExpiration == actual.DeadLetteringOnMessageExpiration
            && expected.DuplicateDetectionHistoryTimeWindow == actual.DuplicateDetectionHistoryTimeWindow,
            "Exact static roundtrip.");
        RequireProfile(expected);
        RequireProfile(actual);
    }

    private static void RequireProfile(QueueProperties queue)
    {
        Require(!queue.RequiresSession && !queue.RequiresDuplicateDetection && !queue.EnablePartitioning
            && queue.EnableBatchedOperations && queue.Status == EntityStatus.Active
            && queue.AutoDeleteOnIdle == TimeSpan.MaxValue && queue.AuthorizationRules.Count == 0
            && string.IsNullOrEmpty(queue.ForwardTo) && string.IsNullOrEmpty(queue.ForwardDeadLetteredMessagesTo)
            && string.IsNullOrEmpty(queue.UserMetadata), "Closed supported profile.");
    }

    internal static void RequireStatus(Response response, int status) =>
        Require(response.Status == status, "Exact HTTP status.");

    internal static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private static async Task ExpectServiceBusAsync(Func<Task> action, ServiceBusFailureReason reason)
    {
        try
        {
            await action();
        }
        catch (ServiceBusException error) when (error.Reason == reason)
        {
            return;
        }
        throw new InvalidOperationException("Expected Service Bus refusal was absent.");
    }

    private static async Task ExpectArgumentAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (ArgumentException)
        {
            return;
        }
        throw new InvalidOperationException("Expected unsupported-definition refusal was absent.");
    }

    private static async Task ExpectUnauthorizedAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (UnauthorizedAccessException)
        {
            return;
        }
        throw new InvalidOperationException("Expected Manage refusal was absent.");
    }

    private static bool ContainsCertificateFailure(Exception error)
    {
        static bool Contains<T>(Exception current, Func<T, bool> matches) where T : Exception =>
            (current is T typed && matches(typed))
            || (current is AggregateException aggregate && aggregate.InnerExceptions.Any(child => Contains(child, matches)))
            || (current.InnerException is not null && Contains(current.InnerException, matches));
        return Contains<AuthenticationException>(error, _ => true)
            && Contains<HttpRequestException>(error, value => value.HttpRequestError == HttpRequestError.SecureConnectionError);
    }

    private static string Classify(Exception error) => error switch
    {
        UnauthorizedAccessException => "unauthorized",
        ServiceBusException => "service-bus",
        ArgumentException => "argument",
        OperationCanceledException => "cancelled",
        _ when ContainsCertificateFailure(error) => "tls",
        _ => "other",
    };
}
