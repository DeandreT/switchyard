internal static class AtomicMessagingDiagnostics
{
    // Zero means no event; maximum + 1 reserves the single truncation marker.
    internal static int Reserve(ref int counter, int maximum)
    {
        if (maximum < 0 || maximum == int.MaxValue)
        {
            return 0;
        }

        int terminal = maximum + 1;
        while (true)
        {
            int observed = Volatile.Read(ref counter);
            if (observed < 0 || observed >= terminal)
            {
                return 0;
            }
            int next = observed + 1;
            if (Interlocked.CompareExchange(ref counter, next, observed) == observed)
            {
                return next;
            }
        }
    }

    internal static void AnnotateFailure(
        Exception failure,
        string stage,
        long elapsedMilliseconds,
        bool cancellationRequested)
    {
        try
        {
            failure.Data["atomic-messaging-stage"] = stage;
            failure.Data["atomic-messaging-stage-elapsed-ms"] = elapsedMilliseconds;
            failure.Data["atomic-messaging-cancellation-requested"] = cancellationRequested;
        }
        catch (Exception)
        {
            // A diagnostic dictionary must not replace the original exception.
        }
    }

    internal static string? Stage(Exception failure) =>
        ReadMetadata(failure, "atomic-messaging-stage") as string;

    internal static long? ElapsedMilliseconds(Exception failure) =>
        ReadMetadata(failure, "atomic-messaging-stage-elapsed-ms") is long value ? value : null;

    internal static bool? CancellationRequested(Exception failure) =>
        ReadMetadata(failure, "atomic-messaging-cancellation-requested") is bool value ? value : null;

    internal static string FormatFailure(Exception failure) =>
        $"atomic messaging gate failed at {Stage(failure) ?? "scope or endpoint disposal"} elapsed_ms={ElapsedMilliseconds(failure)?.ToString() ?? "unavailable"} cancellation_requested={CancellationRequested(failure)?.ToString() ?? "unavailable"}: {failure}";

    private static object? ReadMetadata(Exception failure, string key)
    {
        try
        {
            return failure.Data[key];
        }
        catch (Exception)
        {
            return null;
        }
    }
}
