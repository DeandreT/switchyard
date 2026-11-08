using Azure.Core;
using Azure.Messaging.ServiceBus;
using System.Text;

// Deliberate local test credential, not an Azure/OIDC token acquisition client.
internal static class OfflineJwtConformance
{
    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 4)
        {
            Console.Error.WriteLine("usage: offline-jwt <namespace> <sb://trusted-host:port (SDK selects WSS)> <queue> <token-file>");
            return 2;
        }

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var credential = new FileCredential(args[3]);
        var options = new ServiceBusClientOptions
        {
            CustomEndpointAddress = new Uri(args[1]),
            TransportType = ServiceBusTransportType.AmqpWebSockets,
            RetryOptions = { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        };
        // No certificate callback: the child-scoped private CA and the real
        // endpoint hostname must satisfy the platform's normal TLS validation.
        await using var client = new ServiceBusClient(args[0], credential, options);
        await using var sender = client.CreateSender(args[2]);
        await using var receiver = client.CreateReceiver(args[2]);
        const string body = "official-offline-jwt";
        await sender.SendMessageAsync(new ServiceBusMessage(body) { MessageId = body }, deadline.Token);
        ServiceBusReceivedMessage? message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), deadline.Token);
        if (message?.Body.ToString() != body || message.MessageId != body)
            throw new InvalidOperationException("offline JWT SDK delivery did not match");
        await receiver.RenewMessageLockAsync(message, deadline.Token);
        await receiver.CompleteMessageAsync(message, deadline.Token);
        if (await receiver.PeekMessageAsync(cancellationToken: deadline.Token) is not null)
            throw new InvalidOperationException("offline JWT SDK queue was not empty after completion");
        if (credential.Requests == 0)
            throw new InvalidOperationException("SDK did not request the declared credential");
        return 0;
    }

    private sealed class FileCredential(string path) : TokenCredential
    {
        internal int Requests { get; private set; }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (requestContext.Scopes.Length != 1 ||
                requestContext.Scopes[0] != "https://servicebus.azure.net/.default")
                throw new InvalidOperationException("unexpected SDK token request scope");
            using var stream = File.OpenRead(path);
            byte[] buffer = new byte[8193];
            int count = 0;
            while (count < buffer.Length)
            {
                int read = stream.Read(buffer, count, buffer.Length - count);
                if (read == 0) break;
                count += read;
            }
            if (count > 8192) throw new InvalidOperationException("offline credential file exceeds its limit");
            string token = new UTF8Encoding(false, true).GetString(buffer, 0, count).TrimEnd('\r', '\n');
            if (token.Length == 0 || token.Any(char.IsWhiteSpace))
                throw new InvalidOperationException("offline credential file is invalid");
            Requests++;
            // Cache lifetime for this short fixture only. The server independently
            // verifies the signed token's real expiry and all other claims.
            return new AccessToken(token, DateTimeOffset.UtcNow.AddMinutes(1));
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new(GetToken(requestContext, cancellationToken));
    }
}
