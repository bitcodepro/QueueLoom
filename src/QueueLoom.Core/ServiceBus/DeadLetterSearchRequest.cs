namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterSearchRequest
{
    public const int DefaultBatchSize = 100;
    public const int DefaultMaximumMessagesPerSubQueue = 2_000;
    public const int DefaultMaximumResults = 500;
    public const int MaximumQueryLength = 1_024;

    public DeadLetterSearchRequest(
        string query,
        IEnumerable<ServiceBusEntityReference> sources,
        IEnumerable<ServiceBusSubQueue>? subQueues = null,
        int batchSize = DefaultBatchSize,
        int maximumMessagesPerSubQueue = DefaultMaximumMessagesPerSubQueue,
        int maximumResults = DefaultMaximumResults)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        ArgumentNullException.ThrowIfNull(sources);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchSize, BrowseMessagesRequest.MaximumMaxMessages);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumMessagesPerSubQueue, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumMessagesPerSubQueue, 25_000);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumResults, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumResults, 5_000);

        Query = query.Trim();
        if (Query.Length > MaximumQueryLength)
        {
            throw new ArgumentException(
                $"The search text cannot exceed {MaximumQueryLength:N0} characters.",
                nameof(query));
        }

        Sources = Array.AsReadOnly(sources.Distinct().ToArray());
        if (Sources.Count == 0)
        {
            throw new ArgumentException("At least one queue or subscription is required.", nameof(sources));
        }
        if (Sources.Any(source => !source.CanBrowse))
        {
            throw new ArgumentException("Only queues and subscriptions can be searched.", nameof(sources));
        }

        SubQueues = Array.AsReadOnly((subQueues ??
            [ServiceBusSubQueue.DeadLetter, ServiceBusSubQueue.TransferDeadLetter])
            .Distinct()
            .ToArray());
        if (SubQueues.Count == 0 || SubQueues.Any(subQueue =>
                subQueue is not ServiceBusSubQueue.DeadLetter and not ServiceBusSubQueue.TransferDeadLetter))
        {
            throw new ArgumentException("Search supports dead-letter and transfer dead-letter subqueues only.", nameof(subQueues));
        }

        BatchSize = batchSize;
        MaximumMessagesPerSubQueue = maximumMessagesPerSubQueue;
        MaximumResults = maximumResults;
    }

    public string Query { get; }

    public IReadOnlyList<ServiceBusEntityReference> Sources { get; }

    public IReadOnlyList<ServiceBusSubQueue> SubQueues { get; }

    public int BatchSize { get; }

    public int MaximumMessagesPerSubQueue { get; }

    public int MaximumResults { get; }
}
