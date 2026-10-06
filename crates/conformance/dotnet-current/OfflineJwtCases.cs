using Azure.Core;
using Azure.Messaging.ServiceBus;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

internal static class OfflineJwtCases
{
    private const string Issuer = "https://issuer.example/";
    private const string Audience = "urn:switchyard:tenant";
    private const string Subject = "producer";
    private const string Scope = "https://servicebus.azure.net/.default";
    private const string Marker = "official .NET offline JWT Memory TLS send and LISTEN denial passed";
    // Public test material from jsonwebtoken v11.1.0's tests/rsa PKCS1 fixture.
    private const string PrivateDer = "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKdWUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZDpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7jE0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5JnLnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSSbYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBlxyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp19m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P07mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZmY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIamQOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gnPYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJHUvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==";

    private sealed class FixtureCredential : TokenCredential
    {
        private readonly AccessToken _token;
        private int _requests;
        private int _scopeRefused;

        internal FixtureCredential(string token, long expires)
        {
            _token = new AccessToken(token, DateTimeOffset.FromUnixTimeSeconds(expires));
            if (_token.ExpiresOn.ToUnixTimeSeconds() != expires)
                throw new InvalidOperationException("offline JWT expiry mismatch");
        }

        internal int Requests => Volatile.Read(ref _requests);
        internal bool ScopeRefused => Volatile.Read(ref _scopeRefused) != 0;

        public override AccessToken GetToken(TokenRequestContext context, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (context.Scopes.Length != 1 || context.Scopes[0] != Scope)
            {
                Volatile.Write(ref _scopeRefused, 1);
                throw new InvalidOperationException("offline JWT requested scope refused");
            }
            Interlocked.Increment(ref _requests);
            return _token;
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(context, cancellationToken));
    }

    private static string Base64Url(byte[] value) =>
        Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static FixtureCredential Credential()
    {
        var issued = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var expires = checked(issued + 3300);
        var header = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", kid = "key-1", typ = "at+jwt" }));
        var claims = Base64Url(JsonSerializer.SerializeToUtf8Bytes(new { iss = Issuer, sub = Subject, aud = Audience, iat = issued, exp = expires }));
        var input = header + "." + claims;
        using var rsa = RSA.Create();
        var key = Convert.FromBase64String(PrivateDer);
        rsa.ImportRSAPrivateKey(key, out var consumed);
        if (consumed != key.Length || rsa.KeySize != 2048)
            throw new InvalidOperationException("offline JWT fixture key refused");
        var signature = rsa.SignData(Encoding.ASCII.GetBytes(input), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return new FixtureCredential(input + "." + Base64Url(signature), expires);
    }

    private static string ExceptionKind(Exception error) => error switch
    {
        UnauthorizedAccessException => "unauthorized",
        ServiceBusException => "service-bus",
        OperationCanceledException => "cancelled",
        ArgumentException => "argument",
        InvalidOperationException => "invalid-operation",
        CryptographicException => "cryptographic",
        System.Security.Authentication.AuthenticationException => "tls",
        System.IO.IOException => "io",
        _ => "other"
    };

    internal static async Task<int> RunAsync(string[] args)
    {
        var stage = "arguments";
        FixtureCredential? credential = null;
        try
        {
            if (args.Length != 4 || !Uri.TryCreate(args[2], UriKind.Absolute, out var endpoint) || endpoint.Scheme != "sb")
                throw new InvalidOperationException("offline JWT arguments refused");
            stage = "credential";
            credential = Credential();
            stage = "client";
            var options = new ServiceBusClientOptions
            {
                CustomEndpointAddress = endpoint,
                TransportType = ServiceBusTransportType.AmqpTcp,
                RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) }
            };
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
            await using (var client = new ServiceBusClient(args[1], credential, options))
            {
                stage = "send";
                await using (var sender = client.CreateSender(args[3]))
                {
                    var message = new ServiceBusMessage("offline-jwt-body")
                    {
                        MessageId = "offline-jwt-send",
                        CorrelationId = "offline-jwt-correlation",
                        Subject = "offline-jwt",
                        ContentType = "text/plain"
                    };
                    await sender.SendMessageAsync(message, deadline.Token);
                    stage = "sender-disposal";
                }
                stage = "listen";
                await using (var receiver = client.CreateReceiver(args[3], new ServiceBusReceiverOptions { PrefetchCount = 0 }))
                {
                    var denied = false;
                    try
                    {
                        var unexpected = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), deadline.Token);
                        if (unexpected is not null)
                        {
                            stage = "listen-unexpected-message";
                            throw new InvalidOperationException("offline JWT acquired an unauthorized message");
                        }
                    }
                    catch (UnauthorizedAccessException)
                    {
                        denied = true;
                    }
                    if (!denied)
                    {
                        stage = "listen-denial-missing";
                        throw new InvalidOperationException("offline JWT LISTEN denial missing");
                    }
                    stage = "receiver-disposal";
                }
                stage = "client-disposal";
            }
            stage = "credential-check";
            if (credential.Requests == 0)
                throw new InvalidOperationException("offline JWT credential was not requested");
            Console.WriteLine(Marker);
            return 0;
        }
        catch (Exception error)
        {
            // Only fixed labels and booleans cross the child diagnostic boundary.
            var requested = (credential?.Requests ?? 0) > 0 ? "true" : "false";
            var scopeRefused = credential?.ScopeRefused == true ? "true" : "false";
            Console.Error.WriteLine($"offline JWT SDK diagnostic stage={stage} exception={ExceptionKind(error)} credential_requested={requested} scope_refused={scopeRefused}");
            Console.Error.WriteLine("offline JWT SDK gate failed");
            return 1;
        }
    }
}
