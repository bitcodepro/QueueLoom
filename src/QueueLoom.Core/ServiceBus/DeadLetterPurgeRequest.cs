namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterPurgeTarget(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue)
{
    public bool IsValid => Source.CanBrowse &&
                           SubQueue is ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter;
}

public sealed record DeadLetterPurgeRequest
{
    public const int DefaultBatchSize = 10;
    public const int DefaultMaximumMessagesPerSubQueue = 1_000_000;

    public DeadLetterPurgeRequest(
        IEnumerable<ServiceBusEntityReference> sources,
        IEnumerable<ServiceBusSubQueue>? subQueues = null,
        int batchSize = DefaultBatchSize,
        int maximumMessagesPerSubQueue = DefaultMaximumMessagesPerSubQueue)
        : this(CreateTargets(sources, subQueues), batchSize, maximumMessagesPerSubQueue)
    {
    }

    public DeadLetterPurgeRequest(
        IEnumerable<DeadLetterPurgeTarget> targets,
        int batchSize = DefaultBatchSize,
        int maximumMessagesPerSubQueue = DefaultMaximumMessagesPerSubQueue)
    {
        ArgumentNullException.ThrowIfNull(targets);
        var targetArray = targets.Distinct().ToArray();
        if (targetArray.Length == 0)
        {
            throw new ArgumentException("At least one dead-letter source is required.", nameof(targets));
        }
        if (targetArray.Any(target => !target.IsValid))
        {
            throw new ArgumentException("Purge targets must be queue or subscription DLQs.", nameof(targets));
        }
        if (batchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be between 1 and 100.");
        }
        if (maximumMessagesPerSubQueue is < 1 or > DefaultMaximumMessagesPerSubQueue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumMessagesPerSubQueue),
                $"The per-subqueue limit must be between 1 and {DefaultMaximumMessagesPerSubQueue:N0}.");
        }

        Targets = Array.AsReadOnly(targetArray);
        Sources = Array.AsReadOnly(targetArray.Select(target => target.Source).Distinct().ToArray());
        SubQueues = Array.AsReadOnly(targetArray.Select(target => target.SubQueue).Distinct().ToArray());
        BatchSize = batchSize;
        MaximumMessagesPerSubQueue = maximumMessagesPerSubQueue;
    }

    public IReadOnlyList<DeadLetterPurgeTarget> Targets { get; }

    public IReadOnlyList<ServiceBusEntityReference> Sources { get; }

    public IReadOnlyList<ServiceBusSubQueue> SubQueues { get; }

    public int BatchSize { get; }

    public int MaximumMessagesPerSubQueue { get; }

    private static IEnumerable<DeadLetterPurgeTarget> CreateTargets(
        IEnumerable<ServiceBusEntityReference> sources,
        IEnumerable<ServiceBusSubQueue>? subQueues)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var sourceArray = sources.Distinct().ToArray();
        var subQueueArray = (subQueues ??
            [ServiceBusSubQueue.DeadLetter, ServiceBusSubQueue.TransferDeadLetter])
            .Distinct()
            .ToArray();
        return sourceArray.SelectMany(source =>
            subQueueArray.Select(subQueue => new DeadLetterPurgeTarget(source, subQueue)));
    }
}
