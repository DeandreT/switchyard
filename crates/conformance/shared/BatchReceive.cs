internal static class BatchReceive
{
    internal const int EmptyBatchLimit = 100;
    internal static readonly TimeSpan EmptyPause = TimeSpan.FromMilliseconds(100);

    internal static async Task<IReadOnlyList<T>> AccumulateAsync<T>(
        int expectedCount,
        TimeSpan totalBudget,
        Func<int, TimeSpan, CancellationToken, Task<IReadOnlyList<T>>> receive,
        CancellationToken cancellationToken = default,
        TimeProvider? timeProvider = null)
    {
        if (expectedCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedCount));
        }
        if (totalBudget <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(totalBudget));
        }
        ArgumentNullException.ThrowIfNull(receive);
        cancellationToken.ThrowIfCancellationRequested();

        TimeProvider clock = timeProvider ?? TimeProvider.System;
        var messages = new List<T>(expectedCount);
        int emptyBatches = 0;
        long startedAt = clock.GetTimestamp();
        using var deadline = new CancellationTokenSource(totalBudget, clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken, deadline.Token);

        TimeSpan Remaining() => totalBudget - clock.GetElapsedTime(startedAt);

        IReadOnlyList<T> Partial(string reason)
        {
            Console.Error.WriteLine(
                $"batch receive {reason}: received {messages.Count}/{expectedCount}, " +
                $"empty batches={emptyBatches}");
            return messages;
        }

        try
        {
            while (messages.Count < expectedCount)
            {
                cancellationToken.ThrowIfCancellationRequested();
                TimeSpan remaining = Remaining();
                if (deadline.IsCancellationRequested || remaining <= TimeSpan.Zero)
                {
                    return Partial("exhausted the total budget");
                }

                int requested = expectedCount - messages.Count;
                // Await the original fetch so no receiver operation outlives this scope.
                IReadOnlyList<T> batch = await receive(requested, remaining, linked.Token);
                cancellationToken.ThrowIfCancellationRequested();
                if (batch.Count > requested)
                {
                    throw new InvalidOperationException(
                        $"batch receive returned {batch.Count} messages for a request of {requested}");
                }
                messages.AddRange(batch);
                if (batch.Count == 0)
                {
                    emptyBatches++;
                }

                cancellationToken.ThrowIfCancellationRequested();
                if (deadline.IsCancellationRequested || Remaining() <= TimeSpan.Zero)
                {
                    Partial("completed a fetch after the total budget");
                    if (messages.Count == expectedCount)
                    {
                        throw new TimeoutException(
                            $"batch receive completed {expectedCount} messages after the total budget");
                    }
                    return messages;
                }
                if (messages.Count == expectedCount)
                {
                    return messages;
                }
                if (batch.Count != 0)
                {
                    continue;
                }
                if (emptyBatches >= EmptyBatchLimit)
                {
                    return Partial("reached the empty-batch limit");
                }

                remaining = Remaining();
                if (remaining <= TimeSpan.Zero)
                {
                    return Partial("exhausted the total budget");
                }
                TimeSpan pause = remaining < EmptyPause ? remaining : EmptyPause;
                await Task.Delay(pause, clock, linked.Token);
            }
            return messages;
        }
        catch (OperationCanceledException error) when (
            deadline.IsCancellationRequested &&
            !cancellationToken.IsCancellationRequested &&
            error.CancellationToken == linked.Token)
        {
            return Partial("exhausted the total budget");
        }
    }
}
