namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Amazon SQS: what a redrive of <paramref name="Source"/>'s dead-letter queue would do. AWS moves every message in
/// <paramref name="DeadLetterQueue"/> back to the queue it came from. <paramref name="SourceQueues"/> are the queues
/// that use the dead-letter queue now; a message can also go back to a queue that used it before, which no reading of
/// the current setup can list. The move happens in AWS: no message is deleted, and no backup is taken.
/// </summary>
public sealed record NativeRedrivePlan(
    ServiceBusEntityReference Source,
    string DeadLetterQueue,
    IReadOnlyList<string> SourceQueues,
    long ApproximateMessages)
{
    /// <summary>The dead-letter queue's ARN. SQS builds it from the account, region and name, so it stays the same
    /// when a queue of that name is deleted and created again; <see cref="DeadLetterCreatedAt"/> tells the two apart.</summary>
    public string DeadLetterArn { get; init; } = string.Empty;

    /// <summary>When the dead-letter queue was created (SQS CreatedTimestamp), or null when SQS did not say.</summary>
    public DateTimeOffset? DeadLetterCreatedAt { get; init; }

    /// <summary>The environment's settings the plan was made with; a changed environment refuses the plan.</summary>
    public string? ConfigurationIdentity { get; init; }
}
