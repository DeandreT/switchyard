using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Messaging.ServiceBus;

internal static class SdkCustody
{
    private const string Prefix = "SWITCHYARD_SDK_CUSTODY ";
    private const int RecordBytes = 16 * 1024;
    private const long ArtifactBytes = 64 * 1024 * 1024;

    internal static async Task<int> RunAsync(string[] args, Func<string[], Task<int>> workflow)
    {
        string nonce = Environment.GetEnvironmentVariable("SWITCHYARD_SDK_NONCE") ?? "";
        if (nonce.Length != 64 || !nonce.All(character => "0123456789abcdef".Contains(character)))
        {
            throw new InvalidOperationException("missing or invalid SDK invocation nonce");
        }
        AssemblyIdentity[] identities =
        [
            Read("entry", Assembly.GetEntryAssembly() ?? throw new InvalidOperationException("no entry assembly")),
            Read("service_bus", typeof(ServiceBusClient).Assembly),
            Read("core", typeof(AzureNamedKeyCredential).Assembly),
        ];
        Emit("start", nonce, identities);
        // The original function's await-using scopes all finish before it returns here.
        int result = await workflow(args);
        if (result == 0)
        {
            AssemblyIdentity[] completed =
            [
                Read("entry", Assembly.GetEntryAssembly()!),
                Read("service_bus", typeof(ServiceBusClient).Assembly),
                Read("core", typeof(AzureNamedKeyCredential).Assembly),
            ];
            if (!identities.SequenceEqual(completed))
            {
                throw new InvalidOperationException("loaded assembly identities changed during workflow");
            }
            Emit("complete", nonce, completed);
        }
        return result;
    }

    private static AssemblyIdentity Read(string role, Assembly assembly)
    {
        string fullName = Bound(assembly.FullName ?? "", 1024);
        string informational = Bound(assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "", 1024);
        string location = Bound(assembly.Location, 4096);
        using FileStream file = File.OpenRead(location);
        if (file.Length > ArtifactBytes)
        {
            throw new InvalidOperationException("SDK assembly exceeded the file byte ceiling");
        }
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = new byte[8192];
        long total = 0;
        int count;
        while ((count = file.Read(buffer)) != 0)
        {
            total += count;
            if (total > ArtifactBytes)
            {
                throw new InvalidOperationException("SDK assembly grew beyond the file byte ceiling");
            }
            hash.AppendData(buffer, 0, count);
        }
        string sha256 = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
        return new(role, fullName, informational, location, sha256);
    }

    private static string Bound(string value, int maximum)
    {
        if (value.Length == 0 || Encoding.UTF8.GetByteCount(value) > maximum)
        {
            throw new InvalidOperationException("missing or oversized SDK assembly identity");
        }
        return value;
    }

    private static void Emit(string kind, string nonce, AssemblyIdentity[] identities)
    {
        string line = Prefix + JsonSerializer.Serialize(new CustodyRecord(1, kind, nonce, identities));
        if (Encoding.UTF8.GetByteCount(line) > RecordBytes)
        {
            throw new InvalidOperationException("SDK custody record exceeded its byte ceiling");
        }
        Console.WriteLine(line);
    }

    private sealed record AssemblyIdentity(string role, string full_name,
        string informational_version, string location, string sha256);
    private sealed record CustodyRecord(int schema, string kind, string nonce, AssemblyIdentity[] assemblies);
}
