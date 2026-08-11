namespace QueueLoom.Core.ServiceBus;

public sealed record ResubmitDeadLetterRequest
{
    public ResubmitDeadLetterRequest(
        ServiceBusEntityReference source,
        long sequenceNumber,
        ServiceBusEntityReference destination,
        MessageDraft message,
        DeadLetterDisposition disposition = DeadLetterDisposition.KeepOriginal)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentOutOfRangeException.ThrowIfNegative(sequenceNumber);

        if (!source.CanBrowse)
        {
            throw new ArgumentException("Dead-letter messages can only come from queues or subscriptions.", nameof(source));
        }

        if (!destination.CanSend)
        {
            throw new ArgumentException("Messages can only be sent to queues or topics.", nameof(destination));
        }

        Source = source;
        SequenceNumber = sequenceNumber;
        Destination = destination;
        Message = message;
        Disposition = disposition;
    }

    public ServiceBusEntityReference Source { get; }

    public long SequenceNumber { get; }

    public ServiceBusEntityReference Destination { get; }

    public MessageDraft Message { get; }

    public DeadLetterDisposition Disposition { get; }
}
