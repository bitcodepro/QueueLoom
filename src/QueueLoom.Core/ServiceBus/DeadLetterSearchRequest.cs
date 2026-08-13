namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterSearchTarget(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long KnownMessageCount)
{
    public bool IsValid =>
        Source.CanBrowse &&
        SubQueue is ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter &&
        KnownMessageCount > 0;
}

public sealed record DeadLetterSearchRequest
{
    public const int DefaultBatchSize = 100;
    public const int DefaultMaximumMessagesPerTarget = 1_000;
    public const int DefaultMaximumResults = 500;
    public const int MaximumQueryLength = 1_024;

    public DeadLetterSearchRequest(
        string query,
        IEnumerable<DeadLetterSearchTarget> targets,
        int batchSize = DefaultBatchSize,
        int maximumMessagesPerTarget = DefaultMaximumMessagesPerTarget,
        int maximumResults = DefaultMaximumResults)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        ArgumentNullException.ThrowIfNull(targets);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchSize, BrowseMessagesRequest.MaximumMaxMessages);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumMessagesPerTarget, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumMessagesPerTarget, 10_000);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumResults, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumResults, 5_000);

        Query = query.Trim();
        if (Query.Length > MaximumQueryLength)
        {
            throw new ArgumentException(
                $"The search text cannot exceed {MaximumQueryLength:N0} characters.",
                nameof(query));
        }

        Targets = Array.AsReadOnly(targets.Distinct().ToArray());
        if (Targets.Count == 0)
        {
            throw new ArgumentException("At least one non-empty dead-letter source is required.", nameof(targets));
        }
        if (Targets.Any(target => !target.IsValid))
        {
            throw new ArgumentException("Search targets must be non-empty queue or subscription DLQs.", nameof(targets));
        }

        BatchSize = batchSize;
        MaximumMessagesPerTarget = maximumMessagesPerTarget;
        MaximumResults = maximumResults;
    }

    public string Query { get; }

    public IReadOnlyList<DeadLetterSearchTarget> Targets { get; }

    public int BatchSize { get; }

    public int MaximumMessagesPerTarget { get; }

    public int MaximumResults { get; }
}
