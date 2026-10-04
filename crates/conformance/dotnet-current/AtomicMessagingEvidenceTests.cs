using Microsoft.Azure.Amqp;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text;

internal static class AtomicMessagingEvidenceTests
{
    internal const string Success =
        "official .NET atomic diagnostic forwarding/restoration/bounds/failure containment passed";

    internal static async Task<int> RunAsync(string[] args)
    {
        if (args.Length != 1 || args[0] != "atomic-evidence-selftest")
        {
            Console.Error.WriteLine("usage: atomic-evidence-selftest");
            return 2;
        }

        AmqpTrace originalProvider = AmqpTrace.Provider;
        TextWriter originalError = Console.Error;
        string stage = "not-started";
        Exception? failure = null;
        try
        {
            stage = "all callbacks";
            AllCallbacksForwardUnchanged();
            stage = "normal restoration";
            NormalAndIdempotentRestoration();
            stage = "replacement ownership";
            OlderTraceDoesNotClobberReplacement();
            stage = "nested refusal forwarding";
            NestedRefusalForwardsState();
            stage = "lifecycle bounds";
            LifecycleEventsAreBounded();
            stage = "counter saturation";
            DiagnosticReservationsSaturate();
            stage = "concurrent counter reservation";
            ConcurrentReservationsAreUnique();
            stage = "operation bounds";
            Isolated((_, output) => AtomicMessagingCases.VerifyOperationDiagnosticBounds(output));
            stage = "publication failures";
            LifecyclePublicationFailureStillForwards();
            await AtomicMessagingCases.VerifyDiagnosticOperationResultsAsync();
            stage = "previous provider failures";
            PreviousProviderFailureStillPropagates();
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            AmqpTrace.Provider = originalProvider;
            Console.SetError(originalError);
        }

        if (failure is not null)
        {
            // The mode uses only synthetic data and never opens a connection.
            Console.Error.WriteLine(
                $"atomic evidence self-test failed stage={stage} failure_type={failure.GetType().Name}: {failure.Message}");
            return 1;
        }

        Console.WriteLine(Success);
        return 0;
    }

    private static void Isolated(Action<RecordingTrace, StringWriter> check)
    {
        AmqpTrace previous = AmqpTrace.Provider;
        TextWriter previousError = Console.Error;
        var recorder = new RecordingTrace();
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        AmqpTrace.Provider = recorder;
        Console.SetError(output);
        try
        {
            check(recorder, output);
        }
        finally
        {
            AmqpTrace.Provider = previous;
            Console.SetError(previousError);
        }
    }

    private static void AllCallbacksForwardUnchanged() => Isolated((recorder, output) =>
    {
        using var trace = new AtomicMessagingLifecycleTrace();
        object source = new PrivateSentinel();
        object connection = new PrivateSentinel();
        object session = new PrivateSentinel();
        object link = new PrivateSentinel();
        object target = new PrivateSentinel();
        object state = new PrivateSentinel();
        var exception = new InvalidOperationException("synthetic private exception text");
        const string text = "synthetic-private-text";
        const TraceOperation operation = (TraceOperation)73;

        CheckForward(recorder, "AmqpOpenConnection", [source, connection],
            () => trace.AmqpOpenConnection(source, connection));
        CheckForward(recorder, "AmqpCloseConnection", [source, connection, true],
            () => trace.AmqpCloseConnection(source, connection, true));
        CheckForward(recorder, "AmqpAddSession", [source, session, (ushort)41, (ushort)43],
            () => trace.AmqpAddSession(source, session, 41, 43));
        CheckForward(recorder, "AmqpAttachLink", [connection, session, link, 47U, 53U, text, "sender", source, target],
            () => trace.AmqpAttachLink(connection, session, link, 47, 53, text, "sender", source, target));
        CheckForward(recorder, "AmqpDeliveryNotFound", [source, text],
            () => trace.AmqpDeliveryNotFound(source, text));
        CheckForward(recorder, "AmqpDispose", [source, 59U, false, state],
            () => trace.AmqpDispose(source, 59, false, state));
        CheckForward(recorder, "AmqpDynamicBufferSizeChange", [source, text, -61, 67],
            () => trace.AmqpDynamicBufferSizeChange(source, text, -61, 67));
        CheckForward(recorder, "AmqpInsecureTransport", [source, target, true, false],
            () => trace.AmqpInsecureTransport(source, target, true, false));
        CheckForward(recorder, "AmqpLinkDetach", [source, text, 71U, "close", "synthetic-private-description"],
            () => trace.AmqpLinkDetach(source, text, 71, "close", "synthetic-private-description"));
        CheckForward(recorder, "AmqpListenSocketAcceptError", [source, true, text],
            () => trace.AmqpListenSocketAcceptError(source, true, text));
        CheckForward(recorder, "AmqpLogError", [source, "send", text],
            () => trace.AmqpLogError(source, "send", text));
        CheckForward(recorder, "AmqpLogOperationInformational", [source, operation, target],
            () => trace.AmqpLogOperationInformational(source, operation, target));
        CheckForward(recorder, "AmqpLogOperationVerbose", [source, operation, state],
            () => trace.AmqpLogOperationVerbose(source, operation, state));
        CheckForward(recorder, "AmqpMissingHandle", [source, text, 79U],
            () => trace.AmqpMissingHandle(source, text, 79));
        CheckForward(recorder, "AmqpOpenEntityFailed", [source, target, text, "entity", "synthetic-private-description"],
            () => trace.AmqpOpenEntityFailed(source, target, text, "entity", "synthetic-private-description"));
        CheckForward(recorder, "AmqpOpenEntitySucceeded", [source, target, text, "entity"],
            () => trace.AmqpOpenEntitySucceeded(source, target, text, "entity"));
        CheckForward(recorder, "AmqpSentMessage", [source, 83U, 9007199254740993L],
            () => trace.AmqpSentMessage(source, 83, 9007199254740993L));
        CheckForward(recorder, "AmqpReceiveMessage", [source, 89U, -97],
            () => trace.AmqpReceiveMessage(source, 89, -97));
        CheckForward(recorder, "AmqpRemoveLink", [connection, session, link, 101U, 103U, text],
            () => trace.AmqpRemoveLink(connection, session, link, 101, 103, text));
        CheckForward(recorder, "AmqpRemoveSession", [source, session, (ushort)107, (ushort)109],
            () => trace.AmqpRemoveSession(source, session, 107, 109));
        CheckForward(recorder, "AmqpSessionWindowClosed", [source, -113],
            () => trace.AmqpSessionWindowClosed(source, -113));
        CheckForward(recorder, "AmqpStateTransition", [source, "transition", state, target],
            () => trace.AmqpStateTransition(source, "transition", state, target));
        CheckForward(recorder, "AmqpUpgradeTransport", [source, connection, target],
            () => trace.AmqpUpgradeTransport(source, connection, target));
        CheckForward(recorder, "AmqpAbortThrowingException", [text],
            () => trace.AmqpAbortThrowingException(text));
        CheckForward(recorder, "AmqpCacheMessage", [source, 127U, -131, true, 9007199254740995L, 137U, 139U],
            () => trace.AmqpCacheMessage(source, 127, -131, true, 9007199254740995L, 137, 139));
        CheckForward(recorder, "AmqpIoEvent", [source, -149, 9007199254740997L],
            () => trace.AmqpIoEvent(source, -149, 9007199254740997L));
        CheckForward(recorder, "AmqpHandleException", [exception, text],
            () => trace.AmqpHandleException(exception, text));

        string[] publicCallbacks = typeof(AmqpTrace)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Where(method => method.IsVirtual)
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
        string[] forwarded = recorder.Calls.Select(call => call.Name)
            .OrderBy(name => name, StringComparer.Ordinal).ToArray();
        Require(publicCallbacks.Length == 27 && publicCallbacks.SequenceEqual(forwarded),
            "the exact pinned callback set was not covered");
        Require(output.ToString().Length == 0,
            "nonqualifying callbacks unexpectedly emitted lifecycle diagnostics");
    });

    private static void CheckForward(RecordingTrace recorder, string name, object[] expected, Action invoke)
    {
        int before = recorder.Calls.Count;
        invoke();
        Require(recorder.Calls.Count == before + 1, $"callback {name} was omitted or duplicated");
        RecordingTrace.Call call = recorder.Calls[^1];
        Require(call.Name == name && call.Arguments.Length == expected.Length,
            $"callback {name} signature or destination changed");
        for (int index = 0; index < expected.Length; index++)
        {
            bool matches = expected[index].GetType().IsValueType
                ? expected[index].Equals(call.Arguments[index])
                : ReferenceEquals(expected[index], call.Arguments[index]);
            Require(matches, $"callback {name} argument {index} changed");
        }
    }

    private static void NormalAndIdempotentRestoration() => Isolated((recorder, _) =>
    {
        var trace = new AtomicMessagingLifecycleTrace();
        Require(ReferenceEquals(AmqpTrace.Provider, trace), "trace was not installed");
        trace.Dispose();
        Require(ReferenceEquals(AmqpTrace.Provider, recorder), "previous provider was not restored");
        var replacement = new RecordingTrace();
        AmqpTrace.Provider = replacement;
        trace.Dispose();
        Require(ReferenceEquals(AmqpTrace.Provider, replacement), "idempotent disposal replaced a later provider");
    });

    private static void OlderTraceDoesNotClobberReplacement() => Isolated((_, output) =>
    {
        var older = new AtomicMessagingLifecycleTrace();
        using var replacement = new AtomicMessagingLifecycleTrace();
        older.Dispose();
        Require(ReferenceEquals(AmqpTrace.Provider, replacement), "older disposal clobbered its replacement");
        replacement.Dispose();
        Require(ReferenceEquals(AmqpTrace.Provider, older), "replacement did not restore its exact predecessor");
        older.AmqpStateTransition(new PrivateSentinel(), "R:END", new object(), new object());
        Require(output.ToString().Length == 0, "a disposed trace emitted diagnostics");
        // Isolated restores the original provider; out-of-order disposal is not stack repair.
    });

    private static void NestedRefusalForwardsState() => Isolated((recorder, output) =>
    {
        using var lifecycle = new AtomicMessagingLifecycleTrace();
        using (var refusal = new AtomicMessagingRefusalTrace("native transactional ingress is disabled"))
        {
            object source = new PrivateSentinel();
            object from = new object();
            object to = new object();
            CheckForward(recorder, "AmqpStateTransition", [source, "R:END", from, to],
                () => AmqpTrace.Provider.AmqpStateTransition(source, "R:END", from, to));
            Require(refusal.ProofCount == 0, "synthetic source was accepted as real refusal evidence");
        }
        Require(ReferenceEquals(AmqpTrace.Provider, lifecycle), "nested refusal did not restore lifecycle provider");
        Require(Lines(output).Length == 1, "nested state transition was not diagnosed exactly once");
    });

    private static void LifecycleEventsAreBounded() => Isolated((recorder, output) =>
    {
        using var trace = new AtomicMessagingLifecycleTrace();
        string[] operations = ["R:DETACH", "R:END", "R:CLOSE"];
        object[] sources = [new PrivateSentinel(), new ThisIsAnIntentionallyLongDiagnosticSourceTypeNameExceedingSixtyFourCharacters()];
        for (int index = 0; index < 67; index++)
        {
            trace.AmqpStateTransition(sources[index % sources.Length], operations[index % operations.Length], new object(), new object());
        }
        string[] lines = Lines(output);
        Require(lines.Length == 65, "lifecycle diagnostic bound or truncation count changed");
        Require(lines[^1] == "atomic-messaging lifecycle diagnostics truncated after 64 events",
            "lifecycle truncation marker changed");
        for (int index = 0; index < 64; index++)
        {
            string line = lines[index];
            string typeName = sources[index % sources.Length].GetType().Name;
            typeName = typeName[..Math.Min(typeName.Length, 64)];
            Require(line.StartsWith("atomic-messaging lifecycle utc=", StringComparison.Ordinal)
                && line.Contains($" object_type={typeName} ", StringComparison.Ordinal)
                && line.EndsWith(" terminal_type=none condition=none", StringComparison.Ordinal),
                "lifecycle row exposed unexpected data");
        }
        Require(recorder.Calls.Count == 67, "truncation stopped previous-provider forwarding");
        trace.Dispose();
        trace.AmqpStateTransition(sources[0], "R:CLOSE", new object(), new object());
        Require(Lines(output).Length == 65 && recorder.Calls.Count == 68,
            "disposed diagnostics changed forwarding or the event bound");
    });

    private static void LifecyclePublicationFailureStillForwards() => Isolated((recorder, _) =>
    {
        var writer = new ThrowingWriter();
        Console.SetError(writer);
        using var trace = new AtomicMessagingLifecycleTrace();
        object source = new PrivateSentinel();
        object from = new object();
        object to = new object();
        CheckForward(recorder, "AmqpStateTransition", [source, "R:END", from, to],
            () => trace.AmqpStateTransition(source, "R:END", from, to));
        Require(writer.Attempts == 1, "diagnostic publication failure was not exercised");
    });

    private static void DiagnosticReservationsSaturate()
    {
        int counter = 0;
        int[] expected = [1, 2, 3, 0, 0, 0];
        foreach (int reservation in expected)
        {
            Require(AtomicMessagingDiagnostics.Reserve(ref counter, 2) == reservation,
                "diagnostic reservations did not stop after the truncation marker");
        }
        Require(counter == 3, "saturated reservations mutated the counter");
        counter = int.MaxValue - 1;
        Require(AtomicMessagingDiagnostics.Reserve(ref counter, int.MaxValue - 1) == int.MaxValue,
            "the final representable reservation was not available");
        Require(AtomicMessagingDiagnostics.Reserve(ref counter, int.MaxValue - 1) == 0
            && AtomicMessagingDiagnostics.Reserve(ref counter, int.MaxValue - 1) == 0
            && counter == int.MaxValue,
            "the diagnostic counter wrapped after saturation");
    }

    private static void ConcurrentReservationsAreUnique()
    {
        int counter = 0;
        var reservations = new ConcurrentBag<int>();
        var failures = new ConcurrentBag<Exception>();
        using var arrived = new CountdownEvent(2);
        using var release = new ManualResetEventSlim(false);
        Thread[] workers = Enumerable.Range(0, 2).Select(_ => new Thread(() =>
        {
            try
            {
                arrived.Signal();
                release.Wait();
                for (int index = 0; index < 128; index++)
                {
                    int reservation = AtomicMessagingDiagnostics.Reserve(ref counter, 8);
                    if (reservation != 0) { reservations.Add(reservation); }
                }
            }
            catch (Exception exception) { failures.Add(exception); }
        }) { IsBackground = true }).ToArray();
        var started = new List<Thread>();
        bool bothArrived = false;
        bool joined = true;
        try
        {
            foreach (Thread worker in workers)
            {
                worker.Start();
                started.Add(worker);
            }
            bothArrived = arrived.Wait(TimeSpan.FromSeconds(5));
        }
        finally
        {
            release.Set();
            foreach (Thread worker in started)
            {
                bool completed = worker.Join(TimeSpan.FromSeconds(5));
                joined &= completed;
            }
        }
        Require(bothArrived && joined && failures.IsEmpty,
            "concurrent diagnostic workers failed to enter or finish");
        Require(reservations.OrderBy(value => value).SequenceEqual(Enumerable.Range(1, 9))
            && counter == 9,
            "concurrent diagnostics duplicated events or the truncation marker");
        Require(AtomicMessagingDiagnostics.Reserve(ref counter, 8) == 0 && counter == 9,
            "concurrent saturation did not remain terminal");
    }

    private static void PreviousProviderFailureStillPropagates() => Isolated((recorder, output) =>
    {
        var expected = new InvalidOperationException("synthetic provider failure");
        recorder.Failure = expected;
        using var trace = new AtomicMessagingLifecycleTrace();
        RequireSameFailure(expected,
            () => trace.AmqpStateTransition(new PrivateSentinel(), "R:END", new object(), new object()));
        RequireSameFailure(expected, () => trace.AmqpOpenConnection(new object(), new object()));
        Require(recorder.Calls.Count == 2 && Lines(output).Length == 1,
            "provider exception propagation changed callback delivery");
        var writer = new ThrowingWriter();
        Console.SetError(writer);
        RequireSameFailure(expected,
            () => trace.AmqpStateTransition(new PrivateSentinel(), "R:DETACH", new object(), new object()));
        Require(recorder.Calls.Count == 3 && writer.Attempts == 1,
            "diagnostic failure swallowed the previous provider's own exception");
    });

    internal static string[] Lines(StringWriter writer) => writer.ToString()
        .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);

    internal static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private static void RequireSameFailure(Exception expected, Action operation)
    {
        Exception? actual = null;
        try { operation(); }
        catch (Exception exception) { actual = exception; }
        Require(ReferenceEquals(expected, actual), "previous-provider exception was swallowed or replaced");
    }

    private sealed class PrivateSentinel
    {
        public override string ToString() => throw new InvalidOperationException("sentinel must not be rendered");
    }

    private sealed class ThisIsAnIntentionallyLongDiagnosticSourceTypeNameExceedingSixtyFourCharacters
    {
        public override string ToString() => throw new InvalidOperationException("sentinel must not be rendered");
    }

    internal sealed class ThrowingWriter : TextWriter
    {
        internal int Attempts { get; private set; }
        public override Encoding Encoding => System.Text.Encoding.UTF8;
        public override void WriteLine(string? value)
        {
            Attempts++;
            throw new IOException("synthetic diagnostic publication failure");
        }
    }

    private sealed class RecordingTrace : AmqpTrace
    {
        internal sealed record Call(string Name, object[] Arguments);
        internal List<Call> Calls { get; } = new();
        internal Exception? Failure { get; set; }

        private void Record(string name, params object[] arguments)
        {
            Calls.Add(new Call(name, arguments));
            if (Failure is not null) { throw Failure; }
        }

        public override void AmqpOpenConnection(object source, object connection) => Record(nameof(AmqpOpenConnection), source, connection);
        public override void AmqpCloseConnection(object source, object connection, bool abort) => Record(nameof(AmqpCloseConnection), source, connection, abort);
        public override void AmqpAddSession(object source, object session, ushort localChannel, ushort remoteChannel) => Record(nameof(AmqpAddSession), source, session, localChannel, remoteChannel);
        public override void AmqpAttachLink(object connection, object session, object link, uint localHandle, uint remoteHandle, string linkName, string role, object source, object target) => Record(nameof(AmqpAttachLink), connection, session, link, localHandle, remoteHandle, linkName, role, source, target);
        public override void AmqpDeliveryNotFound(object source, string deliveryTag) => Record(nameof(AmqpDeliveryNotFound), source, deliveryTag);
        public override void AmqpDispose(object source, uint deliveryId, bool settled, object state) => Record(nameof(AmqpDispose), source, deliveryId, settled, state);
        public override void AmqpDynamicBufferSizeChange(object source, string type, int oldSize, int newSize) => Record(nameof(AmqpDynamicBufferSizeChange), source, type, oldSize, newSize);
        public override void AmqpInsecureTransport(object source, object transport, bool isSecure, bool isAuthenticated) => Record(nameof(AmqpInsecureTransport), source, transport, isSecure, isAuthenticated);
        public override void AmqpLinkDetach(object source, string name, uint handle, string action, string error) => Record(nameof(AmqpLinkDetach), source, name, handle, action, error);
        public override void AmqpListenSocketAcceptError(object source, bool willRetry, string error) => Record(nameof(AmqpListenSocketAcceptError), source, willRetry, error);
        public override void AmqpLogError(object source, string operation, string message) => Record(nameof(AmqpLogError), source, operation, message);
        public override void AmqpLogOperationInformational(object source, TraceOperation operation, object detail) => Record(nameof(AmqpLogOperationInformational), source, operation, detail);
        public override void AmqpLogOperationVerbose(object source, TraceOperation operation, object detail) => Record(nameof(AmqpLogOperationVerbose), source, operation, detail);
        public override void AmqpMissingHandle(object source, string type, uint handle) => Record(nameof(AmqpMissingHandle), source, type, handle);
        public override void AmqpOpenEntityFailed(object source, object obj, string name, string entityName, string error) => Record(nameof(AmqpOpenEntityFailed), source, obj, name, entityName, error);
        public override void AmqpOpenEntitySucceeded(object source, object obj, string name, string entityName) => Record(nameof(AmqpOpenEntitySucceeded), source, obj, name, entityName);
        public override void AmqpSentMessage(object source, uint deliveryId, long bytes) => Record(nameof(AmqpSentMessage), source, deliveryId, bytes);
        public override void AmqpReceiveMessage(object source, uint deliveryId, int transferCount) => Record(nameof(AmqpReceiveMessage), source, deliveryId, transferCount);
        public override void AmqpRemoveLink(object connection, object session, object link, uint localHandle, uint remoteHandle, string linkName) => Record(nameof(AmqpRemoveLink), connection, session, link, localHandle, remoteHandle, linkName);
        public override void AmqpRemoveSession(object source, object session, ushort localChannel, ushort remoteChannel) => Record(nameof(AmqpRemoveSession), source, session, localChannel, remoteChannel);
        public override void AmqpSessionWindowClosed(object source, int nextId) => Record(nameof(AmqpSessionWindowClosed), source, nextId);
        public override void AmqpStateTransition(object source, string operation, object fromState, object toState) => Record(nameof(AmqpStateTransition), source, operation, fromState, toState);
        public override void AmqpUpgradeTransport(object source, object from, object to) => Record(nameof(AmqpUpgradeTransport), source, from, to);
        public override void AmqpAbortThrowingException(string exception) => Record(nameof(AmqpAbortThrowingException), exception);
        public override void AmqpCacheMessage(object source, uint deliveryId, int count, bool isPrefecthingBySize, long totalCacheSizeInBytes, uint totalLinkCredit, uint linkCredit) => Record(nameof(AmqpCacheMessage), source, deliveryId, count, isPrefecthingBySize, totalCacheSizeInBytes, totalLinkCredit, linkCredit);
        public override void AmqpIoEvent(object source, int ioEvent, long queueSize) => Record(nameof(AmqpIoEvent), source, ioEvent, queueSize);
        public override void AmqpHandleException(Exception exception, string traceInfo) => Record(nameof(AmqpHandleException), exception, traceInfo);
    }
}

internal static partial class AtomicMessagingCases
{
    internal static void VerifyOperationDiagnosticBounds(StringWriter output)
    {
        Interlocked.Exchange(ref operationDiagnostics, 0);
        try
        {
            string stage = new('s', 181);
            var failure = new ThisIsAnIntentionallyLongDiagnosticExceptionTypeNameExceedingSixtyFourCharacters();
            var elapsed = Stopwatch.StartNew();
            for (int index = 0; index < 131; index++)
            {
                TraceOperation(stage, "failed", elapsed, cancellationRequested: true, failure: failure);
            }
            string[] lines = AtomicMessagingEvidenceTests.Lines(output);
            AtomicMessagingEvidenceTests.Require(lines.Length == 129,
                "operation diagnostic bound or truncation count changed");
            AtomicMessagingEvidenceTests.Require(lines[^1] == "atomic-messaging operation diagnostics truncated after 128 events",
                "operation truncation marker changed");
            string boundedName = failure.GetType().Name[..64];
            foreach (string line in lines.Take(128))
            {
                AtomicMessagingEvidenceTests.Require(
                    line.StartsWith("atomic-messaging operation utc=", StringComparison.Ordinal)
                    && line.Contains($" stage={stage[..160]} outcome=failed elapsed_ms=", StringComparison.Ordinal)
                    && line.EndsWith($" cancellation_requested=True failure_type={boundedName} servicebus_reason=none", StringComparison.Ordinal)
                    && !line.Contains("synthetic-private-message", StringComparison.Ordinal),
                    "operation row violated its metadata bounds or privacy");
            }
        }
        finally
        {
            Interlocked.Exchange(ref operationDiagnostics, 0);
        }
    }

    internal static async Task VerifyDiagnosticOperationResultsAsync()
    {
        TextWriter originalError = Console.Error;
        var writer = new AtomicMessagingEvidenceTests.ThrowingWriter();
        Console.SetError(writer);
        Interlocked.Exchange(ref operationDiagnostics, 0);
        try
        {
            int value = await OperationAsync("synthetic success", _ => Task.FromResult(42));
            AtomicMessagingEvidenceTests.Require(value == 42, "failed diagnostics changed a successful result");
            await OperationAsync("synthetic void success", _ => Task.CompletedTask);
            var genericFailure = new InvalidOperationException("synthetic-private-message");
            Exception? caught = null;
            try
            {
                await OperationAsync<int>("synthetic failed value", _ => Task.FromException<int>(genericFailure));
            }
            catch (Exception exception) { caught = exception; }
            VerifyOriginalOperationFailure(genericFailure, caught, "synthetic failed value");
            var voidFailure = new InvalidOperationException("synthetic-private-message");
            caught = null;
            try
            {
                await OperationAsync("synthetic failed void", _ => Task.FromException(voidFailure));
            }
            catch (Exception exception) { caught = exception; }
            VerifyOriginalOperationFailure(voidFailure, caught, "synthetic failed void");
            var throwingData = new ThrowingDataException();
            caught = null;
            try
            {
                await OperationAsync<int>("synthetic throwing Data", _ => Task.FromException<int>(throwingData));
            }
            catch (Exception exception) { caught = exception; }
            AtomicMessagingEvidenceTests.Require(ReferenceEquals(throwingData, caught),
                "a throwing Data getter replaced the original operation exception");
            AtomicMessagingEvidenceTests.Require(
                AtomicMessagingDiagnostics.Stage(throwingData) is null
                && AtomicMessagingDiagnostics.ElapsedMilliseconds(throwingData) is null
                && AtomicMessagingDiagnostics.CancellationRequested(throwingData) is null,
                "failed diagnostic metadata lookup was not contained");
            var readOnlyData = new ReadOnlyDataException();
            caught = null;
            try
            {
                await OperationAsync("synthetic read-only Data", _ => Task.FromException(readOnlyData));
            }
            catch (Exception exception) { caught = exception; }
            AtomicMessagingEvidenceTests.Require(ReferenceEquals(readOnlyData, caught),
                "read-only Data replaced the original operation exception");
            AtomicMessagingEvidenceTests.Require(
                AtomicMessagingDiagnostics.Stage(readOnlyData) is null
                && AtomicMessagingDiagnostics.ElapsedMilliseconds(readOnlyData) is null
                && AtomicMessagingDiagnostics.CancellationRequested(readOnlyData) is null,
                "missing read-only diagnostic metadata was not harmless");
            var sentinel = new ThrowingMetadataValue();
            var sentinelFailure = new ReadOnlyDataException(sentinel);
            caught = null;
            try
            {
                await OperationAsync<int>("synthetic sentinel Data", _ => Task.FromException<int>(sentinelFailure));
            }
            catch (Exception exception) { caught = exception; }
            AtomicMessagingEvidenceTests.Require(ReferenceEquals(sentinelFailure, caught),
                "prepopulated read-only Data replaced the original operation exception");
            AtomicMessagingEvidenceTests.Require(
                AtomicMessagingDiagnostics.Stage(sentinelFailure) is null
                && AtomicMessagingDiagnostics.ElapsedMilliseconds(sentinelFailure) is null
                && AtomicMessagingDiagnostics.CancellationRequested(sentinelFailure) is null,
                "unexpected metadata types were exposed to final diagnostic formatting");
            string summary = AtomicMessagingDiagnostics.FormatFailure(sentinelFailure);
            AtomicMessagingEvidenceTests.Require(
                summary.StartsWith("atomic messaging gate failed at scope or endpoint disposal elapsed_ms=unavailable cancellation_requested=unavailable: ", StringComparison.Ordinal)
                && summary.EndsWith(sentinelFailure.ToString(), StringComparison.Ordinal)
                && ReferenceEquals(sentinelFailure, caught)
                && sentinel.Attempts == 0,
                "final diagnostics rendered an arbitrary metadata object or changed the primary failure");
            AtomicMessagingEvidenceTests.Require(writer.Attempts == 14,
                "every immediate operation must exercise both failing diagnostic publications");
        }
        finally
        {
            Console.SetError(originalError);
            Interlocked.Exchange(ref operationDiagnostics, 0);
        }
    }

    private static void VerifyOriginalOperationFailure(Exception expected, Exception? actual, string stage)
    {
        AtomicMessagingEvidenceTests.Require(ReferenceEquals(expected, actual),
            "failed diagnostics swallowed or replaced the original operation exception");
        AtomicMessagingEvidenceTests.Require(
            expected.Data["atomic-messaging-stage"] is string recorded && recorded == stage
            && expected.Data["atomic-messaging-stage-elapsed-ms"] is long elapsed && elapsed >= 0
            && expected.Data["atomic-messaging-cancellation-requested"] is false
            && AtomicMessagingDiagnostics.Stage(expected) == stage
            && AtomicMessagingDiagnostics.ElapsedMilliseconds(expected) == elapsed
            && AtomicMessagingDiagnostics.CancellationRequested(expected) == false,
            "operation failure metadata was not preserved");
    }

    private sealed class ThisIsAnIntentionallyLongDiagnosticExceptionTypeNameExceedingSixtyFourCharacters : Exception
    {
        internal ThisIsAnIntentionallyLongDiagnosticExceptionTypeNameExceedingSixtyFourCharacters()
            : base("synthetic-private-message") { }
    }

    private sealed class ThrowingDataException : Exception
    {
        public override IDictionary Data => throw new InvalidOperationException("synthetic Data getter failure");
    }

    private sealed class ReadOnlyDataException : Exception
    {
        private readonly IDictionary values;

        internal ReadOnlyDataException(ThrowingMetadataValue? sentinel = null)
        {
            var metadata = new Dictionary<string, object>();
            if (sentinel is not null)
            {
                metadata["atomic-messaging-stage"] = sentinel;
                metadata["atomic-messaging-stage-elapsed-ms"] = sentinel;
                metadata["atomic-messaging-cancellation-requested"] = sentinel;
            }
            values = new ReadOnlyDictionary<string, object>(metadata);
        }

        public override IDictionary Data => values;
    }

    private sealed class ThrowingMetadataValue
    {
        internal int Attempts { get; private set; }

        public override string ToString()
        {
            Attempts++;
            throw new InvalidOperationException("synthetic metadata rendering failure");
        }
    }
}
