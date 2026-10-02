using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp;

internal sealed class AtomicMessagingRefusalTrace : AmqpTrace, IDisposable
{
    private readonly AmqpTrace previous;
    private readonly string disabledDescription;
    private int proofCount;
    private int disposed;

    internal AtomicMessagingRefusalTrace(string disabledDescription)
    {
        this.disabledDescription = disabledDescription;
        previous = AmqpTrace.Provider;
        AmqpTrace.Provider = this;
    }

    internal int ProofCount => Volatile.Read(ref proofCount);

    public override void AmqpStateTransition(object source, string operation, object fromState, object toState)
    {
        // The library assigns the decoded End error before announcing this transition.
        if (Volatile.Read(ref disposed) == 0
            && operation == "R:END"
            && source is AmqpSession session
            && session.TerminalException is AmqpException exception
            && exception.Error.Condition.Value == "amqp:not-implemented"
            && exception.Error.Description == disabledDescription)
        {
            Interlocked.Increment(ref proofCount);
        }
        previous.AmqpStateTransition(source, operation, fromState, toState);
    }

    internal static bool IsTranslatedAbort(ServiceBusException exception)
    {
        if (exception.Reason != ServiceBusFailureReason.ServiceTimeout || exception.InnerException is not null)
        {
            return false;
        }

        const string prefix = "The AMQP object sender";
        if (!exception.Message.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }
        int index = prefix.Length;
        int firstDigit = index;
        while (index < exception.Message.Length && char.IsAsciiDigit(exception.Message[index]))
        {
            index++;
        }
        return index > firstDigit
            && exception.Message.AsSpan(index).StartsWith(" is aborted.", StringComparison.Ordinal);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            AmqpTrace.Provider = previous;
        }
    }
}
