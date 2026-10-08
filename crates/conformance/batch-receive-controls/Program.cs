(string Name, Func<Task> Run)[] controls =
[
    ("PartialOneTwoThreePreservesOrderAndSingleBudget", PartialBatches),
    ("EmptyBatchWaitsBeforeRetryAndRequestsOnlyRemainder", EmptyThenArrival),
    ("UnavailableStopsAtTotalEmptyLimit", EmptyLimit),
    ("PartialArrivalDoesNotResetEmptyLimit", EmptyLimitAcrossPartial),
    ("PartialDeadlineKeepsMissingMessageFailure", PartialDeadline),
    ("OutstandingFetchSucceedsBeforeDeadline", PositiveOutstanding),
    ("OutstandingFetchSettlesOnCallerCancellation", CallerCancellation),
    ("OutstandingFetchSettlesOnDeadlineCancellation", DeadlineCancellation),
    ("PreCancelledCallerNeverFetches", PreCancellation),
    ("UnrelatedFetchCancellationPropagates", UnrelatedCancellation),
    ("OversizedFetchRejectsInsteadOfTruncating", OversizedFetch),
    ("DuplicatesAndOutOfOrderAreNotRepaired", InvalidDeliveries),
    ("LateFullFetchCannotPassExactCount", LateFullFetch),
    ("InvalidArgumentsNeverFetch", InvalidArguments),
];
foreach (var control in controls)
{
    await control.Run();
    Console.WriteLine($"PASS {control.Name}");
}
return 0;

static void Require(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}

static async Task<T> Throws<T>(Func<Task> action) where T : Exception
{
    try
    {
        await action();
    }
    catch (T error)
    {
        return error;
    }
    throw new InvalidOperationException($"expected {typeof(T).Name}");
}

static async Task<string> CaptureDiagnostics(Func<Task> action)
{
    TextWriter previous = Console.Error;
    using var output = new StringWriter();
    Console.SetError(output);
    try
    {
        await action();
        return output.ToString();
    }
    finally
    {
        Console.SetError(previous);
    }
}

static async Task PartialBatches()
{
    var clock = new ManualClock();
    int[][] batches = [[1], [2, 3], [4, 5, 6]];
    int[] advances = [2, 3, 4];
    var requests = new List<int>();
    var waits = new List<TimeSpan>();
    var tokens = new List<CancellationToken>();
    IReadOnlyList<int> result = await BatchReceive.AccumulateAsync<int>(
        6, TimeSpan.FromSeconds(10), (remaining, wait, token) =>
        {
            int index = requests.Count;
            requests.Add(remaining);
            waits.Add(wait);
            tokens.Add(token);
            clock.Advance(TimeSpan.FromSeconds(advances[index]));
            return Task.FromResult<IReadOnlyList<int>>(batches[index]);
        }, timeProvider: clock);
    Require(result.SequenceEqual([1, 2, 3, 4, 5, 6]), "partial arrivals were changed");
    Require(requests.SequenceEqual([6, 5, 3]), "did not request only the remainder");
    Require(waits.SequenceEqual([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(5)]), "total budget restarted");
    Require(tokens.All(token => token.CanBeCanceled && token == tokens[0]), "fetch cancellation budget changed");
    clock.Advance(TimeSpan.FromSeconds(1));
    Require(!tokens[0].IsCancellationRequested && clock.ActiveTimers == 0, "completed operation retained its deadline timer");
}

static async Task EmptyThenArrival()
{
    var clock = new ManualClock();
    int[][] batches = [[], [2], [1]];
    var requests = new List<int>();
    Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
        2, TimeSpan.FromSeconds(10), (remaining, _, _) =>
        {
            int index = requests.Count;
            requests.Add(remaining);
            return Task.FromResult<IReadOnlyList<int>>(batches[index]);
        }, timeProvider: clock);
    clock.Advance(TimeSpan.Zero);
    Require(requests.Count == 1 && !pending.IsCompleted, "empty batch hot-retried");
    await clock.WaitForPauseAsync();
    clock.Advance(BatchReceive.EmptyPause);
    IReadOnlyList<int> result = await pending;
    Require(requests.SequenceEqual([2, 2, 1]), "empty retry changed remainder");
    Require(result.SequenceEqual([2, 1]), "arrival order was repaired");
}

static async Task EmptyLimit()
{
    var clock = new ManualClock();
    int calls = 0;
    string diagnostics = await CaptureDiagnostics(async () =>
    {
        Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
            3, TimeSpan.FromMinutes(1), (remaining, _, _) =>
            {
                Require(remaining == 3, "empty result changed remainder");
                Interlocked.Increment(ref calls);
                return Task.FromResult<IReadOnlyList<int>>(Array.Empty<int>());
            }, timeProvider: clock);
        for (int index = 0; index < BatchReceive.EmptyBatchLimit - 1; index++)
        {
            await clock.WaitForPauseAsync();
            Require(Volatile.Read(ref calls) == index + 1, "empty retry was unbounded");
            clock.Advance(BatchReceive.EmptyPause);
        }
        Require((await pending).Count == 0, "unavailable input fabricated messages");
    });
    Require(calls == BatchReceive.EmptyBatchLimit, "total empty cap was not enforced");
    Require(diagnostics.Contains("0/3") && diagnostics.Contains("empty"), "empty exhaustion lacks count/reason diagnostics");
}

static async Task EmptyLimitAcrossPartial()
{
    var clock = new ManualClock();
    int calls = 0;
    string diagnostics = await CaptureDiagnostics(async () =>
    {
        Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
            3, TimeSpan.FromMinutes(1), (remaining, _, _) =>
            {
                int call = Interlocked.Increment(ref calls);
                Require(remaining == (call <= 51 ? 3 : 2), "partial empty result changed remainder");
                return Task.FromResult<IReadOnlyList<int>>(call == 51 ? new[] { 1 } : Array.Empty<int>());
            }, timeProvider: clock);
        for (int index = 0; index < BatchReceive.EmptyBatchLimit - 1; index++)
        {
            await clock.WaitForPauseAsync();
            clock.Advance(BatchReceive.EmptyPause);
        }
        Require((await pending).SequenceEqual([1]), "empty exhaustion lost its partial prefix");
    });
    Require(calls == BatchReceive.EmptyBatchLimit + 1 && diagnostics.Contains("1/3"), "partial arrival reset the total empty cap");
}

static async Task PartialDeadline()
{
    var clock = new ManualClock();
    var fetch = new PendingFetch(holdCancellation: true);
    int calls = 0;
    string diagnostics = await CaptureDiagnostics(async () =>
    {
        Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
            3, TimeSpan.FromSeconds(10), (remaining, wait, token) =>
            {
                calls++;
                Require(remaining == (calls == 1 ? 3 : 2), "partial result changed remainder");
                return calls == 1
                    ? Task.FromResult<IReadOnlyList<int>>(new[] { 1 })
                    : fetch.ReceiveAsync(remaining, wait, token);
            }, timeProvider: clock);
        Require(calls == 2 && fetch.Calls == 1 && !pending.IsCompleted, "partial original fetch was not retained");
        clock.Advance(TimeSpan.FromSeconds(10));
        await fetch.CancellationObserved.Task;
        Require(calls == 2 && fetch.Calls == 1 && !fetch.Settled && !pending.IsCompleted, "partial deadline abandoned or retried the original fetch");
        fetch.Completion.SetCanceled(fetch.Token);
        IReadOnlyList<int> result = await pending;
        Require(result.SequenceEqual([1]) && result.Count != 3, "missing delivery became an exact-count pass");
        Require(fetch.Settled && fetch.Token.IsCancellationRequested, "partial deadline did not await original cancellation");
    });
    Require(calls == 2 && diagnostics.Contains("1/3"), "partial deadline lacks diagnostics");
}

static async Task PositiveOutstanding()
{
    var clock = new ManualClock();
    var fetch = new PendingFetch();
    Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
        3, TimeSpan.FromSeconds(10), fetch.ReceiveAsync, timeProvider: clock);
    clock.Advance(TimeSpan.FromSeconds(9));
    Require(!pending.IsCompleted && !fetch.Token.IsCancellationRequested, "positive boundary cancelled early");
    fetch.Completion.SetResult([1, 2, 3]);
    Require((await pending).SequenceEqual([1, 2, 3]) && fetch.Settled, "original positive fetch was not awaited");
    clock.Advance(TimeSpan.FromSeconds(1));
    Require(!fetch.Token.IsCancellationRequested, "successful fetch left a live deadline");
}

static async Task CallerCancellation()
{
    var clock = new ManualClock();
    var fetch = new PendingFetch(holdCancellation: true);
    using var caller = new CancellationTokenSource();
    Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
        3, TimeSpan.FromSeconds(10), fetch.ReceiveAsync, caller.Token, clock);
    clock.Advance(TimeSpan.FromSeconds(9));
    caller.Cancel();
    await fetch.CancellationObserved.Task;
    Require(!pending.IsCompleted && !fetch.Settled && fetch.Calls == 1, "caller cancellation abandoned or retried the original fetch");
    fetch.Completion.SetCanceled(fetch.Token);
    await Throws<OperationCanceledException>(async () => await pending);
    Require(fetch.Settled && fetch.Calls == 1 && fetch.Token.IsCancellationRequested, "caller cancellation abandoned the original fetch");
}

static async Task DeadlineCancellation()
{
    var clock = new ManualClock();
    var fetch = new PendingFetch(holdCancellation: true);
    string diagnostics = await CaptureDiagnostics(async () =>
    {
        Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
            3, TimeSpan.FromSeconds(10), fetch.ReceiveAsync, timeProvider: clock);
        clock.Advance(TimeSpan.FromSeconds(10));
        await fetch.CancellationObserved.Task;
        Require(!pending.IsCompleted && !fetch.Settled && fetch.Calls == 1, "deadline abandoned or retried the original fetch");
        fetch.Completion.SetCanceled(fetch.Token);
        Require((await pending).Count == 0, "deadline fabricated messages");
        Require(fetch.Settled && fetch.Calls == 1 && fetch.Token.IsCancellationRequested, "deadline abandoned the original fetch");
    });
    Require(diagnostics.Contains("0/3"), "deadline lacks missing-message diagnostics");
}

static async Task PreCancellation()
{
    int calls = 0;
    using var caller = new CancellationTokenSource();
    caller.Cancel();
    await Throws<OperationCanceledException>(async () =>
        await BatchReceive.AccumulateAsync<int>(3, TimeSpan.FromSeconds(10), (_, _, _) =>
        {
            calls++;
            return Task.FromResult<IReadOnlyList<int>>([1, 2, 3]);
        }, caller.Token, new ManualClock()));
    Require(calls == 0, "pre-cancelled caller fetched messages");
}

static async Task UnrelatedCancellation()
{
    await Throws<OperationCanceledException>(async () =>
        await BatchReceive.AccumulateAsync<int>(3, TimeSpan.FromSeconds(10), (_, _, _) =>
            Task.FromCanceled<IReadOnlyList<int>>(new CancellationToken(true)), timeProvider: new ManualClock()));
}

static async Task OversizedFetch()
{
    int calls = 0;
    await Throws<InvalidOperationException>(async () =>
        await BatchReceive.AccumulateAsync<int>(3, TimeSpan.FromSeconds(10), (remaining, _, _) =>
        {
            calls++;
            Require(remaining == (calls == 1 ? 3 : 1), "over-return control had wrong request");
            return Task.FromResult<IReadOnlyList<int>>(calls == 1 ? new[] { 1, 2 } : new[] { 3, 4 });
        }, timeProvider: new ManualClock()));
    Require(calls == 2, "over-return triggered more fetches");
}

static async Task InvalidDeliveries()
{
    foreach (int[][] batches in new int[][][] { [[1, 1], [3]], [[2], [1, 3]] })
    {
        int calls = 0;
        IReadOnlyList<int> result = await BatchReceive.AccumulateAsync<int>(
            3, TimeSpan.FromSeconds(10), (_, _, _) =>
                Task.FromResult<IReadOnlyList<int>>(batches[calls++]), timeProvider: new ManualClock());
        Require(result.SequenceEqual(batches.SelectMany(batch => batch)), "invalid delivery was filtered/reordered");
        Require(!result.SequenceEqual([1, 2, 3]), "invalid delivery became the expected-order pass");
    }
}

static async Task LateFullFetch()
{
    var clock = new ManualClock();
    var fetch = new PendingFetch(cooperative: false);
    Task<IReadOnlyList<int>> pending = BatchReceive.AccumulateAsync<int>(
        3, TimeSpan.FromSeconds(10), fetch.ReceiveAsync, timeProvider: clock);
    clock.Advance(TimeSpan.FromSeconds(10));
    Require(!pending.IsCompleted, "helper abandoned a non-cooperative original task");
    fetch.Completion.SetResult([1, 2, 3]);
    await Throws<TimeoutException>(async () => await pending);
    Require(fetch.Settled, "late original fetch was not awaited");
}

static async Task InvalidArguments()
{
    int calls = 0;
    Task<IReadOnlyList<int>> Receive(int _, TimeSpan __, CancellationToken ___)
    {
        calls++;
        return Task.FromResult<IReadOnlyList<int>>(Array.Empty<int>());
    }
    foreach (int count in new[] { 0, -1 })
    {
        await Throws<ArgumentOutOfRangeException>(async () =>
            await BatchReceive.AccumulateAsync(count, TimeSpan.FromSeconds(10), Receive));
    }
    foreach (TimeSpan budget in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1) })
    {
        await Throws<ArgumentOutOfRangeException>(async () =>
            await BatchReceive.AccumulateAsync(3, budget, Receive));
    }
    await Throws<ArgumentNullException>(async () =>
        await BatchReceive.AccumulateAsync<int>(3, TimeSpan.FromSeconds(10), null!));
    Require(calls == 0, "invalid arguments fetched messages");
}

sealed class PendingFetch(bool cooperative = true, bool holdCancellation = false)
{
    internal TaskCompletionSource<IReadOnlyList<int>> Completion { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource<bool> CancellationObserved { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal CancellationToken Token { get; private set; }
    internal bool Settled { get; private set; }
    internal int Calls { get; private set; }

    internal async Task<IReadOnlyList<int>> ReceiveAsync(int _, TimeSpan __, CancellationToken token)
    {
        Calls++;
        Token = token;
        using CancellationTokenRegistration registration = cooperative
            ? token.Register(() =>
            {
                CancellationObserved.TrySetResult(true);
                if (!holdCancellation) { Completion.TrySetCanceled(token); }
            })
            : default;
        try
        {
            return await Completion.Task;
        }
        finally
        {
            Settled = true;
        }
    }
}

sealed class ManualClock : TimeProvider
{
    private readonly object gate = new();
    private readonly List<ManualTimer> timers = new();
    private readonly SemaphoreSlim pauses = new(0);
    private long timestamp;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    public override long GetTimestamp() { lock (gate) { return timestamp; } }
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch + TimeSpan.FromTicks(GetTimestamp());
    internal int ActiveTimers { get { lock (gate) { return timers.Count(timer => timer.Due != long.MaxValue); } } }
    internal Task WaitForPauseAsync() => pauses.WaitAsync();

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        lock (gate)
        {
            var timer = new ManualTimer(this, callback, state);
            timers.Add(timer);
            timer.Change(dueTime, period);
            if (dueTime > TimeSpan.Zero && dueTime <= BatchReceive.EmptyPause && period == Timeout.InfiniteTimeSpan)
            {
                pauses.Release();
            }
            return timer;
        }
    }

    internal void Advance(TimeSpan amount)
    {
        if (amount < TimeSpan.Zero) { throw new ArgumentOutOfRangeException(nameof(amount)); }
        lock (gate)
        {
            long target = checked(timestamp + amount.Ticks);
            while (true)
            {
                ManualTimer? next = timers.Where(timer => timer.Due <= target).MinBy(timer => timer.Due);
                if (next is null) { timestamp = target; return; }
                timestamp = next.Due;
                next.Fire();
            }
        }
    }

    private sealed class ManualTimer(ManualClock clock, TimerCallback callback, object? state) : ITimer
    {
        internal long Due { get; private set; } = long.MaxValue;
        private long period;
        private bool disposed;

        public bool Change(TimeSpan dueTime, TimeSpan interval)
        {
            lock (clock.gate)
            {
                if (disposed) { return false; }
                if ((dueTime < TimeSpan.Zero && dueTime != Timeout.InfiniteTimeSpan) ||
                    (interval < TimeSpan.Zero && interval != Timeout.InfiniteTimeSpan))
                {
                    throw new ArgumentOutOfRangeException(nameof(dueTime));
                }
                Due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : checked(clock.timestamp + dueTime.Ticks);
                period = interval > TimeSpan.Zero ? interval.Ticks : 0;
                return true;
            }
        }

        internal void Fire()
        {
            Due = period == 0 ? long.MaxValue : checked(Due + period);
            callback(state);
        }

        public void Dispose() { lock (clock.gate) { disposed = true; Due = long.MaxValue; } }
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }
}
