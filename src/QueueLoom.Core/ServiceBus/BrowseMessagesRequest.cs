namespace QueueLoom.Core.ServiceBus;

public sealed record BrowseMessagesRequest
{
    public const int DefaultMaxMessages = 100;
    public const int MaximumMaxMessages = 1_000;

    public BrowseMessagesRequest(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue = ServiceBusSubQueue.Active,
        int maxMessages = DefaultMaxMessages,
        long? fromSequenceNumber = null,
        bool loadAll = false)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (!source.CanBrowse)
        {
            throw new ArgumentException("Messages can only be browsed from queues or subscriptions.", nameof(source));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessages, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxMessages, MaximumMaxMessages);

        if (fromSequenceNumber is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fromSequenceNumber));
        }

        Source = source;
        SubQueue = subQueue;
        MaxMessages = maxMessages;
        FromSequenceNumber = fromSequenceNumber;
        LoadAll = loadAll;
    }

    public ServiceBusEntityReference Source { get; }

    public ServiceBusSubQueue SubQueue { get; }

    public int MaxMessages { get; }

    public long? FromSequenceNumber { get; }

    public bool LoadAll { get; }

    public BrowseStart Start { get; init; } = BrowseStart.Oldest;
}
