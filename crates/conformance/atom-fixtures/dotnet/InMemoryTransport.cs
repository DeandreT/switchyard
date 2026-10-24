using System.Diagnostics.CodeAnalysis;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;

internal sealed record RequestShape(string Method, string Path, string Query);

internal sealed class InMemoryTransport(int status, string body) : HttpPipelineTransport, IDisposable
{
    private readonly List<MemoryResponse> responses = [];
    internal List<RequestShape> Requests { get; } = [];

    // Only request construction is delegated. Neither Process method ever calls
    // the real transport, resolves a host, opens a socket, or signs credentials.
    public override Request CreateRequest() => HttpClientTransport.Shared.CreateRequest();

    public override void Process(HttpMessage message)
    {
        message.CancellationToken.ThrowIfCancellationRequested();
        Uri uri = message.Request.Uri.ToUri();
        FixtureInputs.Require(uri.Scheme == "https" && uri.Host == "fixture.invalid" && uri.IsDefaultPort,
            "request left synthetic origin");
        FixtureInputs.Require(message.Request.Method == RequestMethod.Get, "unexpected non-GET request");
        FixtureInputs.Require(message.Request.Content is null, "unexpected GET request body");
        FixtureInputs.Require(Requests.Count == 0, "unexpected second request or retry");
        Requests.Add(new(message.Request.Method.ToString(), uri.AbsolutePath, uri.Query.TrimStart('?')));
        var response = new MemoryResponse(status, body);
        responses.Add(response);
        message.Response = response;
    }

    public override ValueTask ProcessAsync(HttpMessage message)
    {
        Process(message);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        foreach (MemoryResponse response in responses)
        {
            response.Dispose();
        }
    }

    private sealed class MemoryResponse(int status, string body) : Response
    {
        private readonly Dictionary<string, string> headers = new(StringComparer.OrdinalIgnoreCase)
        {
            ["Content-Type"] = status == 200 ? "application/atom+xml" : "application/xml",
        };
        public override int Status => status;
        public override string ReasonPhrase => status == 200 ? "OK" : "Bad Request";
        public override Stream? ContentStream { get; set; } = new MemoryStream(Encoding.UTF8.GetBytes(body), writable: false);
        public override string ClientRequestId { get; set; } = "synthetic-fixture";
        public override void Dispose() => ContentStream?.Dispose();
        protected override bool ContainsHeader(string name) => headers.ContainsKey(name);
        protected override IEnumerable<HttpHeader> EnumerateHeaders() => headers.Select(value => new HttpHeader(value.Key, value.Value));
        protected override bool TryGetHeader(string name, [NotNullWhen(true)] out string? value) => headers.TryGetValue(name, out value);
        protected override bool TryGetHeaderValues(string name, [NotNullWhen(true)] out IEnumerable<string>? values)
        {
            if (headers.TryGetValue(name, out string? value))
            {
                values = [value];
                return true;
            }
            values = null;
            return false;
        }
    }
}

internal sealed class FixtureCredential : TokenCredential
{
    // Deliberately unusable outside this process; never read from a user store.
    private static readonly AccessToken Token = new("synthetic-not-a-real-token", DateTimeOffset.MaxValue);
    public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Token;
    }
    public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        => ValueTask.FromResult(GetToken(requestContext, cancellationToken));
}
