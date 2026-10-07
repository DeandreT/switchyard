using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Azure;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus.Administration;

internal sealed class AtomAdministrationTransport : IDisposable
{
    private readonly Uri _endpoint;
    private readonly X509Certificate2 _root;
    private readonly HttpClient _client;
    private readonly HttpClientTransport _transport;
    private bool _disposed;

    internal AtomAdministrationTransport(Uri endpoint, string caFile)
    {
        _endpoint = endpoint;
        _root = X509Certificate2.CreateFromPem(File.ReadAllText(caFile));
        SocketsHttpHandler? handler = null;
        HttpMessageHandler? versionHandler = null;
        HttpClient? client = null;
        try
        {
            handler = new SocketsHttpHandler();
            handler.UseProxy = false;
            handler.AllowAutoRedirect = false;
            handler.UseCookies = false;
            handler.ConnectTimeout = TimeSpan.FromSeconds(5);
            handler.MaxConnectionsPerServer = 2;
            handler.MaxResponseHeadersLength = 16;
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                VerificationFlags = X509VerificationFlags.NoFlag,
                RevocationMode = X509RevocationMode.NoCheck,
                DisableCertificateDownloads = true,
            };
            policy.CustomTrustStore.Add(_root);
            policy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
            handler.SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = policy,
                RemoteCertificateValidationCallback = null,
                AllowRenegotiation = false,
                AllowTlsResume = false,
            };
            versionHandler = new Http11Handler(handler);
            client = new HttpClient(versionHandler, disposeHandler: true);
            client.Timeout = TimeSpan.FromSeconds(12);
            client.DefaultRequestVersion = HttpVersion.Version11;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            _transport = new HttpClientTransport(client);
            _client = client;
        }
        catch
        {
            client?.Dispose();
            versionHandler?.Dispose();
            handler?.Dispose();
            _root.Dispose();
            throw;
        }
    }

    internal ServiceBusAdministrationClient CreateClient(
        bool connectionString, string keyName, string key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // No API version override: each package retains its own default.
        var options = new ServiceBusAdministrationClientOptions
        {
            Transport = _transport,
            Retry =
            {
                MaxRetries = 0,
                NetworkTimeout = TimeSpan.FromSeconds(10),
            },
        };
        if (!connectionString)
        {
            return new ServiceBusAdministrationClient(
                _endpoint.GetLeftPart(UriPartial.Authority),
                new AzureNamedKeyCredential(keyName, key), options);
        }
        var serviceBusEndpoint = new UriBuilder(_endpoint)
        {
            Scheme = "sb",
            Path = "/",
            Query = string.Empty,
            Fragment = string.Empty,
        }.Uri;
        string value = $"Endpoint={serviceBusEndpoint.AbsoluteUri};SharedAccessKeyName={keyName};SharedAccessKey={key}";
        return new ServiceBusAdministrationClient(value, options);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        try
        {
            _transport.Dispose();
        }
        finally
        {
            _client.Dispose();
            _root.Dispose();
        }
    }

    private sealed class Http11Handler : DelegatingHandler
    {
        internal Http11Handler(HttpMessageHandler inner) : base(inner) { }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // SDK requests are explicit messages, so HttpClient defaults do not apply.
            request.Version = HttpVersion.Version11;
            request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            return base.SendAsync(request, cancellationToken);
        }
    }
}
