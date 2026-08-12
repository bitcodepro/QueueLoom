namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterPurgeRequest
{
    public const int DefaultBatchSize = 1;
    public const int DefaultMaximumMessagesPerSubQueue = 1_000_000;

    public DeadLetterPurgeRequest(
        IEnumerable<ServiceBusEntityReference> sources,
        IEnumerable<ServiceBusSubQueue>? subQueues = null,
        int batchSize = DefaultBatchSize,
        int maximumMessagesPerSubQueue = DefaultMaximumMessagesPerSubQueue)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var sourceArray = sources.Distinct().ToArray();
        if (sourceArray.Length == 0)
        {
            throw new ArgumentException("At least one queue or subscription is required.", nameof(sources));
        }
        if (sourceArray.Any(source => !source.CanBrowse))
        {
            throw new ArgumentException("Only queues and subscriptions can have dead letters purged.", nameof(sources));
        }

        var subQueueArray = (subQueues ??
            [ServiceBusSubQueue.DeadLetter, ServiceBusSubQueue.TransferDeadLetter])
            .Distinct()
            .ToArray();
        if (subQueueArray.Length == 0 ||
            subQueueArray.Any(subQueue => subQueue is not (
                ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter)))
        {
            throw new ArgumentException(
                "Only dead-letter and transfer dead-letter subqueues can be purged.",
                nameof(subQueues));
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

        Sources = Array.AsReadOnly(sourceArray);
        SubQueues = Array.AsReadOnly(subQueueArray);
        BatchSize = batchSize;
        MaximumMessagesPerSubQueue = maximumMessagesPerSubQueue;
    }

    public IReadOnlyList<ServiceBusEntityReference> Sources { get; }

    public IReadOnlyList<ServiceBusSubQueue> SubQueues { get; }

    public int BatchSize { get; }

    public int MaximumMessagesPerSubQueue { get; }
}
