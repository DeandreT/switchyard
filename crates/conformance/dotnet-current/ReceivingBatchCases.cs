using System.Diagnostics;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

internal static partial class RuleActionCases
{
    private const string ReceivingBatchSuccess =
        "official .NET same-receiver receive-batch/action copies/prefetch passed";

    public static async Task<int> RunReceivingBatchAsync(string[] args)
    {
        if (args.Length != 7)
        {
            Console.Error.WriteLine(
                "usage: receive-batch <namespace> <endpoint> <topic> <queue> <key-name> <key>");
            return 2;
        }
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var resources = new List<IAsyncDisposable>();
        Exception? failure = null;
        try
        {
            var options = new ServiceBusClientOptions
            {
                CustomEndpointAddress = new Uri(args[2]),
                TransportType = ServiceBusTransportType.AmqpTcp,
                RetryOptions = { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
            };
            ServiceBusClient client = Own(resources, new ServiceBusClient(args[1],
                new AzureNamedKeyCredential(args[5], args[6]), options));
            ServiceBusRuleManager alphaRules = Own(resources, client.CreateRuleManager(args[3], "Alpha"));
            ServiceBusRuleManager betaRules = Own(resources, client.CreateRuleManager(args[3], "beta"));
            ServiceBusSender topicSender = Own(resources, client.CreateSender(args[3]));
            ServiceBusSender queueSender = Own(resources, client.CreateSender(args[4]));
            ServiceBusReceiver alpha = Own(resources, client.CreateReceiver(args[3], "Alpha",
                new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock, PrefetchCount = 0 }));
            ServiceBusReceiver beta = Own(resources, client.CreateReceiver(args[3], "beta",
                new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock, PrefetchCount = 0 }));
            ServiceBusReceiver queue = Own(resources, client.CreateReceiver(args[4],
                new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock, PrefetchCount = 3 }));
            ServiceBusReceiver alphaDead = Own(resources, client.CreateReceiver(args[3], "Alpha",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            ServiceBusReceiver betaDead = Own(resources, client.CreateReceiver(args[3], "beta",
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));
            ServiceBusReceiver queueDead = Own(resources, client.CreateReceiver(args[4],
                new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }));

            string run = $"receive-batch-{Guid.NewGuid():N}";
            await ActionReceivingBatchAsync(alphaRules, betaRules, topicSender,
                alpha, beta, Message(run, "actions"), deadline.Token);
            await QueueReceivingBatchAsync(queueSender, queue, run, deadline.Token);
            await RequireDefaultAsync(alphaRules, deadline.Token);
            await RequireDefaultAsync(betaRules, deadline.Token);
            foreach (ServiceBusReceiver receiver in new[] { alpha, beta, queue, alphaDead, betaDead, queueDead })
            {
                Require((await PeekAsync(receiver, deadline.Token)).Count == 0,
                    "receive-batch cleanup left a primary or dead-letter message");
            }
        }
        catch (Exception error) { failure = error; }
        finally
        {
            for (int index = resources.Count - 1; index >= 0; index--)
            {
                try { await resources[index].DisposeAsync().AsTask().WaitAsync(CleanupTimeout); }
                catch (Exception error)
                {
                    failure ??= new InvalidOperationException("receive-batch SDK cleanup failed", error);
                }
            }
        }
        if (failure is not null)
        {
            Console.Error.WriteLine(failure);
            return 1;
        }
        Console.WriteLine(ReceivingBatchSuccess);
        return 0;
    }

    private static async Task ActionReceivingBatchAsync(ServiceBusRuleManager alphaRules,
        ServiceBusRuleManager betaRules, ServiceBusSender sender, ServiceBusReceiver alpha,
        ServiceBusReceiver beta, ServiceBusMessage source, CancellationToken token)
    {
        await RequireDefaultAsync(alphaRules, token);
        await RequireDefaultAsync(betaRules, token);
        await BoundedAsync(ct => alphaRules.DeleteRuleAsync(RuleProperties.DefaultRuleName, ct), token);
        foreach (string name in new[] { "plain-a", "plain-b" })
        {
            await BoundedAsync(ct => alphaRules.CreateRuleAsync(name, new TrueRuleFilter(), ct), token);
        }
        var colourFilter = new CorrelationRuleFilter { Subject = "rule-action" };
        colourFilter.ApplicationProperties["colour"] = "red";
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(new CreateRuleOptions(ColourRule, colourFilter)
        {
            Action = new SqlRuleAction(ColourSource),
        }, ct), token);
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(new CreateRuleOptions(AuditRule,
            new SqlRuleFilter(AuditFilter)) { Action = new SqlRuleAction(AuditSource) }, ct), token);
        await RequireRulesAsync(alphaRules, token);
        await BoundedAsync(ct => sender.SendMessageAsync(source, ct), token);

        IReadOnlyList<ServiceBusReceivedMessage> browsed = await PeekAsync(alpha, token);
        Require(browsed.Count == 3, "receive-batch action publication did not have exactly three copies");
        var remaining = new Dictionary<string, long>();
        foreach (ServiceBusReceivedMessage copy in browsed)
        {
            string name = CopyName(copy);
            CheckCopy(copy, source, name);
            Require(remaining.TryAdd(name, copy.SequenceNumber), "a browsed action copy was duplicated");
        }
        Require(remaining.Keys.ToHashSet().SetEquals(new[] { PublisherRule, ColourRule, AuditRule })
            && remaining.Values.Distinct().Count() == 3, "browsed action copies lacked independent identities");
        IReadOnlyList<ServiceBusReceivedMessage> original = await PeekAsync(beta, token);
        Require(original.Count == 1, "the independent subscription did not retain one original");
        CheckCopy(original[0], source, PublisherRule);
        long betaSequence = original[0].SequenceNumber;

        using var collector = new ReceivingBatchCollector(alpha, token);
        List<ServiceBusReceivedMessage> held = await collector.CollectAsync(3);
        var receivedNames = new HashSet<string>();
        foreach (ServiceBusReceivedMessage copy in held)
        {
            string name = CopyName(copy);
            CheckCopy(copy, source, name);
            Require(receivedNames.Add(name) && remaining.TryGetValue(name, out long sequence)
                && copy.SequenceNumber == sequence, "batch receive changed or repeated a browsed action copy");
        }
        Require(receivedNames.SetEquals(remaining.Keys), "not all three action copies were held before settlement");
        foreach (ServiceBusReceivedMessage copy in new[] { held[1], held[0], held[2] })
        {
            await BoundedAsync(ct => alpha.CompleteMessageAsync(copy, ct), collector.Token);
            Require(remaining.Remove(CopyName(copy)), "a held action copy was completed twice");
            IReadOnlyList<ServiceBusReceivedMessage> after = await PeekAsync(alpha, collector.Token);
            Require(after.Count == remaining.Count
                && after.Select(item => item.SequenceNumber).ToHashSet().SetEquals(remaining.Values),
                "batch completion removed a sibling action copy or retained the completed copy");
            foreach (ServiceBusReceivedMessage sibling in after)
            {
                CheckCopy(sibling, source, CopyName(sibling));
            }
            original = await PeekAsync(beta, collector.Token);
            Require(original.Count == 1 && original[0].SequenceNumber == betaSequence,
                "batch completion crossed subscription ownership");
            CheckCopy(original[0], source, PublisherRule);
        }
        ServiceBusReceivedMessage betaCopy = await ReceiveAsync(beta, token);
        CheckCopy(betaCopy, source, PublisherRule);
        Require(betaCopy.SequenceNumber == betaSequence, "the independent subscription lost its original copy");
        await BoundedAsync(ct => beta.CompleteMessageAsync(betaCopy, ct), token);

        foreach (RuleProperties rule in await ListRulesAsync(alphaRules, token))
        {
            await BoundedAsync(ct => alphaRules.DeleteRuleAsync(rule.Name, ct), token);
        }
        await BoundedAsync(ct => alphaRules.CreateRuleAsync(RuleProperties.DefaultRuleName,
            new TrueRuleFilter(), ct), token);
        await RequireDefaultAsync(alphaRules, token);
        await RequireDefaultAsync(betaRules, token);
    }

    private static async Task QueueReceivingBatchAsync(ServiceBusSender sender,
        ServiceBusReceiver receiver, string run, CancellationToken token)
    {
        ServiceBusMessage[] sources = Enumerable.Range(1, 6)
            .Select(index => Message(run, $"queue-{index}")).ToArray();
        await BoundedAsync(ct => sender.SendMessagesAsync((IEnumerable<ServiceBusMessage>)sources, ct), token);
        var remaining = Enumerable.Range(1, 6).Select(value => (long)value).ToHashSet();
        await RequireQueueCopiesAsync(receiver, sources, remaining, token);

        using var collector = new ReceivingBatchCollector(receiver, token);
        var held = new Dictionary<long, ServiceBusReceivedMessage>();
        await CollectQueueCopiesAsync(collector, held, sources, 1, 3);
        Require(held.Count == 3, "prefetch did not hold its first three deliveries before settlement");
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 2, collector.Token);
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 1, collector.Token);
        await CollectQueueCopiesAsync(collector, held, sources, 4, 2);
        Require(held.Keys.ToHashSet().SetEquals(new long[] { 3, 4, 5 }),
            "settlement refill did not preserve the still-held original delivery");
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 3, collector.Token);
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 5, collector.Token);
        await CollectQueueCopiesAsync(collector, held, sources, 6, 1);
        Require(held.Keys.ToHashSet().SetEquals(new long[] { 4, 6 }),
            "the second refill replaced or repeated a still-held delivery");
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 6, collector.Token);
        await CompleteQueueCopyAsync(receiver, held, sources, remaining, 4, collector.Token);
        Require(held.Count == 0 && remaining.Count == 0 && collector.TotalReceived == 6,
            "the queue batch did not complete exactly six unique deliveries");
    }

    private static async Task CollectQueueCopiesAsync(ReceivingBatchCollector collector,
        Dictionary<long, ServiceBusReceivedMessage> held, ServiceBusMessage[] sources, long first, int count)
    {
        List<ServiceBusReceivedMessage> copies = await collector.CollectAsync(count);
        for (int index = 0; index < copies.Count; index++)
        {
            ServiceBusReceivedMessage copy = copies[index];
            Require(copy.SequenceNumber == first + index, "queue batch receive lost canonical FIFO identity");
            CheckCopy(copy, sources[checked((int)copy.SequenceNumber - 1)], PublisherRule);
            Require(held.TryAdd(copy.SequenceNumber, copy), "queue batch repeated a still-held delivery");
        }
    }

    private static async Task CompleteQueueCopyAsync(ServiceBusReceiver receiver,
        Dictionary<long, ServiceBusReceivedMessage> held, ServiceBusMessage[] sources,
        HashSet<long> remaining, long sequence, CancellationToken token)
    {
        Require(held.TryGetValue(sequence, out ServiceBusReceivedMessage? copy),
            "queue completion did not refer to an actually held delivery");
        await BoundedAsync(ct => receiver.CompleteMessageAsync(copy!, ct), token);
        Require(held.Remove(sequence) && remaining.Remove(sequence), "a queue delivery was completed twice");
        await RequireQueueCopiesAsync(receiver, sources, remaining, token);
    }

    private static async Task RequireQueueCopiesAsync(ServiceBusReceiver receiver,
        ServiceBusMessage[] sources, HashSet<long> remaining, CancellationToken token)
    {
        IReadOnlyList<ServiceBusReceivedMessage> copies = await PeekAsync(receiver, token);
        Require(copies.Count == remaining.Count
            && copies.Select(copy => copy.SequenceNumber).ToHashSet().SetEquals(remaining),
            "queue completion changed an unrelated canonical message");
        foreach (ServiceBusReceivedMessage copy in copies)
        {
            CheckCopy(copy, sources[checked((int)copy.SequenceNumber - 1)], PublisherRule);
        }
    }

    private sealed class ReceivingBatchCollector : IDisposable
    {
        private static readonly TimeSpan TotalTimeout = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(3);
        private readonly ServiceBusReceiver receiver;
        private readonly CancellationTokenSource deadline;
        private readonly Stopwatch elapsed = Stopwatch.StartNew();
        private readonly HashSet<long> sequences = new();
        private readonly HashSet<Guid> tokens = new();
        private int calls;

        public ReceivingBatchCollector(ServiceBusReceiver receiver, CancellationToken token)
        {
            this.receiver = receiver;
            deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(TotalTimeout);
        }

        public CancellationToken Token => deadline.Token;
        public int TotalReceived => sequences.Count;

        public async Task<List<ServiceBusReceivedMessage>> CollectAsync(int count)
        {
            Require(count > 0 && count <= 3, "batch collection exceeded its requested work bound");
            var held = new List<ServiceBusReceivedMessage>(count);
            while (held.Count < count)
            {
                Token.ThrowIfCancellationRequested();
                Require(++calls <= 12, "batch collection exhausted its finite receive-call bound");
                TimeSpan remainingTime = TotalTimeout - elapsed.Elapsed;
                Require(remainingTime > TimeSpan.Zero, "batch collection exceeded its absolute deadline");
                TimeSpan wait = remainingTime < CallTimeout ? remainingTime : CallTimeout;
                using var call = CancellationTokenSource.CreateLinkedTokenSource(Token);
                call.CancelAfter(wait);
                IReadOnlyList<ServiceBusReceivedMessage> received = await receiver.ReceiveMessagesAsync(
                    count - held.Count, wait, call.Token).WaitAsync(wait, Token);
                Require(received.Count <= count - held.Count, "batch receive exceeded its requested maximum");
                foreach (ServiceBusReceivedMessage copy in received)
                {
                    Require(copy.SequenceNumber > 0 && sequences.Add(copy.SequenceNumber)
                        && Guid.TryParse(copy.LockToken, out Guid lockToken) && lockToken != Guid.Empty
                        && tokens.Add(lockToken) && copy.DeliveryCount == 1,
                        "batch receive lost its unique original sequence, lock token, or first-delivery count");
                    held.Add(copy);
                }
            }
            return held;
        }

        public void Dispose() => deadline.Dispose();
    }
}
