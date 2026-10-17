namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Amazon SQS: what a redrive of <paramref name="Source"/>'s dead-letter queue would do. AWS moves every message in
/// <paramref name="DeadLetterQueue"/> back to the queue it came from, which can be any of
/// <paramref name="SourceQueues"/> when several queues share the dead-letter queue. The move happens in AWS: no
/// message is deleted, and no backup is needed.
/// </summary>
public sealed record NativeRedrivePlan(
    ServiceBusEntityReference Source,
    string DeadLetterQueue,
    IReadOnlyList<string> SourceQueues,
    long ApproximateMessages);
