namespace QueueLoom.Core.ServiceBus;

public sealed record BrowsedMessage
{
    private readonly byte[] _body;

    public BrowsedMessage(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        long sequenceNumber,
        ReadOnlyMemory<byte> body,
        EditableMessageProperties properties,
        IEnumerable<MessageApplicationProperty>? applicationProperties = null,
        ServiceBusMessageState state = ServiceBusMessageState.Unknown,
        long enqueuedSequenceNumber = 0,
        int deliveryCount = 0,
        DateTimeOffset? enqueuedAt = null,
        DateTimeOffset? expiresAt = null,
        DateTimeOffset? lockedUntil = null,
        string? deadLetterReason = null,
        string? deadLetterErrorDescription = null,
        long? originalBodySize = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentOutOfRangeException.ThrowIfNegative(sequenceNumber);
        ArgumentOutOfRangeException.ThrowIfNegative(enqueuedSequenceNumber);
        ArgumentOutOfRangeException.ThrowIfNegative(deliveryCount);

        if (!source.CanBrowse)
        {
            throw new ArgumentException("Messages can only be browsed from queues or subscriptions.", nameof(source));
        }

        Source = source;
        SubQueue = subQueue;
        SequenceNumber = sequenceNumber;
        _body = body.ToArray();
        BodySize = originalBodySize ?? _body.LongLength;
        if (BodySize < _body.LongLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(originalBodySize),
                "The original body size cannot be smaller than the retained body.");
        }
        Properties = properties;
        ApplicationProperties = Array.AsReadOnly((applicationProperties ?? []).ToArray());
        State = state;
        EnqueuedSequenceNumber = enqueuedSequenceNumber;
        DeliveryCount = deliveryCount;
        EnqueuedAt = enqueuedAt;
        ExpiresAt = expiresAt;
        LockedUntil = lockedUntil;
        DeadLetterReason = deadLetterReason;
        DeadLetterErrorDescription = deadLetterErrorDescription;
    }

    public ServiceBusEntityReference Source { get; }

    public ServiceBusSubQueue SubQueue { get; }

    public long SequenceNumber { get; }

    public ReadOnlyMemory<byte> Body => _body;

    public long BodySize { get; }

    public bool IsBodyTruncated => BodySize > _body.LongLength;

    public EditableMessageProperties Properties { get; }

    public IReadOnlyList<MessageApplicationProperty> ApplicationProperties { get; }

    public ServiceBusMessageState State { get; }

    public long EnqueuedSequenceNumber { get; }

    public int DeliveryCount { get; }

    public DateTimeOffset? EnqueuedAt { get; }

    public DateTimeOffset? ExpiresAt { get; }

    public DateTimeOffset? LockedUntil { get; }

    public string? DeadLetterReason { get; }

    public string? DeadLetterErrorDescription { get; }

    public bool IsDeadLetter => SubQueue is ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter;

    public MessageDraft CreateDraft()
    {
        if (IsBodyTruncated)
        {
            throw new InvalidOperationException(
                "This message body exceeds the safe editor limit and was only retained as a preview.");
        }

        return new MessageDraft(EditableMessageBody.FromBytes(_body), Properties, ApplicationProperties);
    }
}
