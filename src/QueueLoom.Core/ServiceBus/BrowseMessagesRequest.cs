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

    /// <summary>The longest session id Service Bus accepts.</summary>
    public const int MaximumSessionIdLength = 128;

    /// <summary>
    /// One session of a session-enabled queue or subscription to peek, active, deferred and scheduled messages alike;
    /// null browses the sessions Service Bus hands out. Only Azure Service Bus active queues have sessions.
    /// </summary>
    public string? SessionId
    {
        get;
        init
        {
            if (value is null)
            {
                field = null;
                return;
            }

            var trimmed = value.Trim();
            if (trimmed.Length == 0 || trimmed.Length > MaximumSessionIdLength)
            {
                throw new ArgumentException($"A session id has 1 to {MaximumSessionIdLength} characters.", nameof(SessionId));
            }

            if (SubQueue != ServiceBusSubQueue.Active)
            {
                throw new ArgumentException("Dead-letter queues do not use sessions; open a session of the active queue.", nameof(SessionId));
            }

            field = trimmed;
        }
    }
}
