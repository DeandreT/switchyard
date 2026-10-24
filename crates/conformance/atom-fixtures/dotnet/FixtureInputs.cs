using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure;
using Azure.Messaging.ServiceBus;

internal sealed class FixtureInputs : IDisposable
{
    private const int FileBytes = 1024 * 1024;
    private const long AssemblyBytes = 64 * 1024 * 1024;
    private readonly string root;
    private readonly Dictionary<string, string> fingerprints = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> contents = new(StringComparer.Ordinal);
    private readonly JsonDocument manifest;

    internal string Selector { get; }
    internal string ApiVersion { get; }
    internal IReadOnlyDictionary<string, string> Fingerprints => fingerprints;

    internal FixtureInputs(string[] args)
    {
        Selector = args[0];
        Require(Selector is "declared-current" or "previous", "unsupported SDK selector");
        root = Absolute(args[1]);
        string approvalPath = Absolute(args[2]);
        string expectedEntry = DigestArgument(args[3]);
        string expectedApproval = DigestArgument(args[4]);
        var entry = Assembly.GetEntryAssembly() ?? throw new InvalidOperationException("no entry assembly");
        Require(HashFile(entry.Location, AssemblyBytes) == expectedEntry, "entry assembly does not match prelaunch bytes");
        Require(entry.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(value => value.Key == "SwitchyardSdkSelector").Value == Selector,
            "entry assembly was built for another selector");

        string approved = ReadAndRemember(approvalPath);
        Require(Hash(Encoding.UTF8.GetBytes(approved)) == expectedApproval, "approval file does not match prelaunch bytes");
        using var approval = JsonDocument.Parse(approved);
        Require(approval.RootElement.GetProperty("schema").GetInt32() == 1, "unsupported approval schema");
        Require(approval.RootElement.GetProperty("selector").GetString() == Selector, "wrong approval selector");
        Require(approval.RootElement.GetProperty("framework").GetString() == "net10.0", "wrong approval framework");
        JsonElement[] packages = approval.RootElement.GetProperty("packages").EnumerateArray().ToArray();
        Require(packages.Length == 2, "ambiguous approved packages");
        VerifyAssembly(typeof(ServiceBusClient).Assembly, packages, "Azure.Messaging.ServiceBus", "service_bus");
        VerifyAssembly(typeof(AzureNamedKeyCredential).Assembly, packages, "Azure.Core", "core");
        foreach (Assembly assembly in new[] { typeof(ServiceBusClient).Assembly, typeof(AzureNamedKeyCredential).Assembly })
        {
            Require(Path.GetDirectoryName(Path.GetFullPath(assembly.Location)) == Path.GetDirectoryName(Path.GetFullPath(entry.Location)),
                "loaded dependency is outside the entry assembly output directory");
        }

        manifest = JsonDocument.Parse(Read("manifest.json"));
        Require(manifest.RootElement.GetProperty("schema").GetInt32() == 1, "unsupported fixture schema");
        string client = Selector == "declared-current" ? "dotnet-current" : "dotnet-previous";
        JsonElement source = manifest.RootElement.GetProperty("sources").GetProperty(client);
        ApiVersion = source.GetProperty("api_version").GetString()!;
        Require(ApiVersion == (Selector == "declared-current" ? "2024-05" : "2021-05"), "unexpected default API version");
        Require(source.GetProperty("service_bus").GetString() ==
            packages.Single(value => value.GetProperty("role").GetString() == "service_bus").GetProperty("version").GetString(),
            "fixture source pin differs from approved ServiceBus package");
        Require(source.GetProperty("azure_core").GetString() ==
            packages.Single(value => value.GetProperty("role").GetString() == "core").GetProperty("version").GetString(),
            "fixture source pin differs from approved Core package");
    }

    internal string Read(string relative)
    {
        Require(!Path.IsPathRooted(relative) && !relative.Split('/').Contains(".."), "fixture path escaped root");
        return ReadAndRemember(Path.Combine(root, relative));
    }

    internal JsonElement Request(string id) => manifest.RootElement.GetProperty("requests")
        .EnumerateArray().Single(value => value.GetProperty("id").GetString() == id);

    internal void VerifyUnchanged()
    {
        foreach ((string path, string fingerprint) in fingerprints)
        {
            Require(HashFile(path, FileBytes) == fingerprint, "fixture or approval bytes changed during controls");
        }
    }

    private string ReadAndRemember(string path)
    {
        path = Path.GetFullPath(path);
        if (contents.TryGetValue(path, out string? cached))
        {
            return cached;
        }
        byte[] bytes = ReadBounded(path, FileBytes);
        string text = new UTF8Encoding(false, true).GetString(bytes);
        fingerprints.Add(path, Hash(bytes));
        contents.Add(path, text);
        return text;
    }

    private static void VerifyAssembly(Assembly assembly, JsonElement[] packages, string id, string role)
    {
        JsonElement approved = packages.Single(value => value.GetProperty("id").GetString() == id);
        Require(approved.GetProperty("role").GetString() == role, "incorrect approved package role");
        Require(approved.GetProperty("runtime_asset").GetString() == $"lib/net10.0/{id}.dll", "incorrect runtime asset");
        Require(assembly.GetName().Name == id, "incorrect loaded assembly name");
        Require(HashFile(assembly.Location, AssemblyBytes) == approved.GetProperty("sha256").GetString(),
            "loaded assembly bytes differ from selected approval");
    }

    private static string Absolute(string path)
    {
        Require(Path.IsPathFullyQualified(path), "input paths must be absolute");
        return Path.GetFullPath(path);
    }

    private static string DigestArgument(string value)
    {
        Require(value.Length == 64 && value.All(character => "0123456789abcdef".Contains(character)),
            "expected SHA-256 must be 64 lowercase hex characters");
        return value;
    }

    private static byte[] ReadBounded(string path, long limit)
    {
        using var file = File.OpenRead(path);
        Require(file.Length <= limit, "input exceeds byte limit");
        using var output = new MemoryStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = file.Read(buffer)) != 0)
        {
            Require(output.Length + count <= limit, "input grew beyond byte limit");
            output.Write(buffer, 0, count);
        }
        return output.ToArray();
    }

    private static string HashFile(string path, long limit) => Hash(ReadBounded(path, limit));
    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    internal static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    public void Dispose() => manifest.Dispose();
}
