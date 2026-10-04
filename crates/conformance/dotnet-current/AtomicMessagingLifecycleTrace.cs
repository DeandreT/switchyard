using Microsoft.Azure.Amqp;
using System.Runtime.CompilerServices;

internal sealed class AtomicMessagingLifecycleTrace : AmqpTrace, IDisposable
{
    private const int MaximumEvents = 64;
    private readonly AmqpTrace previous;
    private int events;
    private int disposed;

    internal AtomicMessagingLifecycleTrace()
    {
        previous = AmqpTrace.Provider;
        AmqpTrace.Provider = this;
    }

    public override void AmqpOpenConnection(object source, object connection) =>
        previous.AmqpOpenConnection(source, connection);

    public override void AmqpCloseConnection(object source, object connection, bool abort) =>
        previous.AmqpCloseConnection(source, connection, abort);

    public override void AmqpAddSession(object source, object session, ushort localChannel, ushort remoteChannel) =>
        previous.AmqpAddSession(source, session, localChannel, remoteChannel);

    public override void AmqpAttachLink(object connection, object session, object link, uint localHandle, uint remoteHandle, string linkName, string role, object source, object target) =>
        previous.AmqpAttachLink(connection, session, link, localHandle, remoteHandle, linkName, role, source, target);

    public override void AmqpDeliveryNotFound(object source, string deliveryTag) =>
        previous.AmqpDeliveryNotFound(source, deliveryTag);

    public override void AmqpDispose(object source, uint deliveryId, bool settled, object state) =>
        previous.AmqpDispose(source, deliveryId, settled, state);

    public override void AmqpDynamicBufferSizeChange(object source, string type, int oldSize, int newSize) =>
        previous.AmqpDynamicBufferSizeChange(source, type, oldSize, newSize);

    public override void AmqpInsecureTransport(object source, object transport, bool isSecure, bool isAuthenticated) =>
        previous.AmqpInsecureTransport(source, transport, isSecure, isAuthenticated);

    public override void AmqpLinkDetach(object source, string name, uint handle, string action, string error) =>
        previous.AmqpLinkDetach(source, name, handle, action, error);

    public override void AmqpListenSocketAcceptError(object source, bool willRetry, string error) =>
        previous.AmqpListenSocketAcceptError(source, willRetry, error);

    public override void AmqpLogError(object source, string operation, string message) =>
        previous.AmqpLogError(source, operation, message);

    public override void AmqpLogOperationInformational(object source, TraceOperation operation, object detail) =>
        previous.AmqpLogOperationInformational(source, operation, detail);

    public override void AmqpLogOperationVerbose(object source, TraceOperation operation, object detail) =>
        previous.AmqpLogOperationVerbose(source, operation, detail);

    public override void AmqpMissingHandle(object source, string type, uint handle) =>
        previous.AmqpMissingHandle(source, type, handle);

    public override void AmqpOpenEntityFailed(object source, object obj, string name, string entityName, string error) =>
        previous.AmqpOpenEntityFailed(source, obj, name, entityName, error);

    public override void AmqpOpenEntitySucceeded(object source, object obj, string name, string entityName) =>
        previous.AmqpOpenEntitySucceeded(source, obj, name, entityName);

    public override void AmqpSentMessage(object source, uint deliveryId, long bytes) =>
        previous.AmqpSentMessage(source, deliveryId, bytes);

    public override void AmqpReceiveMessage(object source, uint deliveryId, int transferCount) =>
        previous.AmqpReceiveMessage(source, deliveryId, transferCount);

    public override void AmqpRemoveLink(object connection, object session, object link, uint localHandle, uint remoteHandle, string linkName) =>
        previous.AmqpRemoveLink(connection, session, link, localHandle, remoteHandle, linkName);

    public override void AmqpRemoveSession(object source, object session, ushort localChannel, ushort remoteChannel) =>
        previous.AmqpRemoveSession(source, session, localChannel, remoteChannel);

    public override void AmqpSessionWindowClosed(object source, int nextId) =>
        previous.AmqpSessionWindowClosed(source, nextId);

    public override void AmqpUpgradeTransport(object source, object from, object to) =>
        previous.AmqpUpgradeTransport(source, from, to);

    public override void AmqpAbortThrowingException(string exception) =>
        previous.AmqpAbortThrowingException(exception);

    public override void AmqpCacheMessage(object source, uint deliveryId, int count, bool isPrefecthingBySize, long totalCacheSizeInBytes, uint totalLinkCredit, uint linkCredit) =>
        previous.AmqpCacheMessage(source, deliveryId, count, isPrefecthingBySize, totalCacheSizeInBytes, totalLinkCredit, linkCredit);

    public override void AmqpIoEvent(object source, int ioEvent, long queueSize) =>
        previous.AmqpIoEvent(source, ioEvent, queueSize);

    public override void AmqpHandleException(Exception exception, string traceInfo) =>
        previous.AmqpHandleException(exception, traceInfo);

    public override void AmqpStateTransition(object source, string operation, object fromState, object toState)
    {
        try
        {
            if (Volatile.Read(ref disposed) == 0)
            {
                Exception? terminal = source switch
                {
                    AmqpLink link => link.TerminalException,
                    AmqpSession session => session.TerminalException,
                    AmqpConnection connection => connection.TerminalException,
                    _ => null,
                };
                bool remoteClose = operation is "R:DETACH" or "R:END" or "R:CLOSE";
                if (remoteClose || terminal is not null)
                {
                    int count = AtomicMessagingDiagnostics.Reserve(ref events, MaximumEvents);
                    if (count > 0 && count <= MaximumEvents)
                    {
                        string condition = terminal is AmqpException exception
                            ? SafeSymbol(exception.Error.Condition.Value)
                            : "none";
                        Console.Error.WriteLine(
                            $"atomic-messaging lifecycle utc={DateTime.UtcNow:O} object_type={TypeName(source)} object_id={RuntimeHelpers.GetHashCode(source)} event={(remoteClose ? operation : "terminal")} terminal_type={TypeName(terminal)} condition={condition}");
                    }
                    else if (count == MaximumEvents + 1)
                    {
                        Console.Error.WriteLine("atomic-messaging lifecycle diagnostics truncated after 64 events");
                    }
                }
            }
        }
        catch (Exception)
        {
            // Diagnostic publication must not change the SDK's settlement outcome.
        }

        previous.AmqpStateTransition(source, operation, fromState, toState);
    }

    private static string TypeName(object? value)
    {
        string name = value?.GetType().Name ?? "none";
        return name.Length <= 64 ? name : name[..64];
    }

    private static string SafeSymbol(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return "none";
        }

        return new string(value
            .Where(character => char.IsAsciiLetterOrDigit(character) || character is ':' or '-' or '.')
            .Take(80)
            .ToArray());
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0 && ReferenceEquals(AmqpTrace.Provider, this))
        {
            AmqpTrace.Provider = previous;
        }
    }
}
