namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterSearchTarget(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long KnownMessageCount)
{
    public bool IsValid =>
        Source.CanBrowse &&
        SubQueue is ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter &&
        KnownMessageCount >= 0;
}

public static class DeadLetterSearchTargets
{
    /// <summary>
    /// Every dead-letter and transfer dead-letter queue in the topology. Runtime counters are eventually
    /// consistent, so zero-count sources are included and a fresh message cannot be skipped.
    /// </summary>
    public static DeadLetterSearchTarget[] ForTopology(ServiceBusTopology topology)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var sources = topology.Queues
            .Where(queue => queue.HasDeadLetterQueue)
            .Select(queue => (queue.Reference, queue.Runtime.MessageCounts))
            .Concat(topology.Topics.SelectMany(topic => topic.Subscriptions)
                .Where(subscription => subscription.HasDeadLetterQueue)
                .Select(subscription => (subscription.Reference, subscription.Runtime.MessageCounts)));
        return sources
            .SelectMany(source => new[]
            {
                new DeadLetterSearchTarget(source.Reference, ServiceBusSubQueue.DeadLetter, source.MessageCounts.DeadLetter),
                new DeadLetterSearchTarget(source.Reference, ServiceBusSubQueue.TransferDeadLetter, source.MessageCounts.TransferDeadLetter)
            })
            .Where(target => target.SubQueue != ServiceBusSubQueue.TransferDeadLetter ||
                             (topology.SupportsTransferDeadLetter && !topology.UsesSampledCounts))
            .ToArray();
    }
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
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumMessagesPerTarget, 100_000);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumResults, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumResults, 50_000);

        Query = query.Trim();
        if (Query.Length > MaximumQueryLength)
        {
            throw new ArgumentException(
                $"The search text cannot exceed {MaximumQueryLength:N0} characters.",
                nameof(query));
        }

        // Read once here, so a broken regular expression or JSON path is reported before anything is searched.
        Search = MessageSearchQuery.Parse(Query);

        Targets = Array.AsReadOnly(targets.Distinct().ToArray());
        if (Targets.Count == 0)
        {
            throw new ArgumentException("At least one dead-letter source is required.", nameof(targets));
        }
        if (Targets.Any(target => !target.IsValid))
        {
            throw new ArgumentException("Search targets must be queue or subscription DLQs.", nameof(targets));
        }

        BatchSize = batchSize;
        MaximumMessagesPerTarget = maximumMessagesPerTarget;
        MaximumResults = maximumResults;
    }

    public string Query { get; }

    /// <summary>The query read as text, a /regular expression/ or a $.json.path condition.</summary>
    public MessageSearchQuery Search { get; }

    public IReadOnlyList<DeadLetterSearchTarget> Targets { get; }

    public int BatchSize { get; }

    public int MaximumMessagesPerTarget { get; }

    public int MaximumResults { get; }

    /// <summary>
    /// Only messages enqueued at or after this time are searched ("enqueued within"); a message without an enqueue
    /// time is outside every window. Applied while scanning, before a match counts toward <see cref="MaximumResults"/>,
    /// so older matches (dead letters are read oldest first) cannot use up the cap and hide the recent ones.
    /// </summary>
    public DateTimeOffset? EnqueuedSince { get; init; }

    /// <summary>Whether a message enqueued at <paramref name="enqueuedAt"/> is inside <see cref="EnqueuedSince"/>.</summary>
    public bool IsInWindow(DateTimeOffset? enqueuedAt) =>
        EnqueuedSince is not { } since || enqueuedAt is { } at && at >= since;
}
