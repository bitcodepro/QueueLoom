namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Scheduled and deferred messages of Azure Service Bus: they sit in a queue or subscription without being delivered.
/// A scheduled message waits for its enqueue time; a deferred one waits for a receiver to ask for it by sequence number.
/// </summary>
public static class PendingMessages
{
    public const int MaximumMessages = DeleteDeadLetterMessagesRequest.MaximumMessages;

    public static bool IsPending(BrowsedMessage message) =>
        message.SubQueue == ServiceBusSubQueue.Active &&
        message.State is ServiceBusMessageState.Scheduled or ServiceBusMessageState.Deferred;

    /// <summary>
    /// Where a scheduled message is held and cancelled: its queue, or the topic it was sent to (subscriptions only see
    /// it once the enqueue time comes).
    /// </summary>
    public static ServiceBusEntityReference ScheduledIn(BrowsedMessage message) =>
        DeadLetterResender.OriginalDestination(message.Source);
}

public sealed record PendingMessageRemovalResult(
    BrowsedMessage Message,
    DeadLetterMessageDeletionOutcome Outcome,
    string? Detail = null);

/// <summary>Scheduled messages cancelled and deferred messages removed, each backed up first.</summary>
public sealed record RemovePendingMessagesResult(IReadOnlyList<PendingMessageRemovalResult> Messages, string BackupDirectory)
{
    public int RemovedCount => Messages.Count(message => message.Outcome == DeadLetterMessageDeletionOutcome.Deleted);

    public int NotFoundCount => Messages.Count(message => message.Outcome == DeadLetterMessageDeletionOutcome.NotFound);

    public int FailedCount => Messages.Count(message => message.Outcome == DeadLetterMessageDeletionOutcome.Failed);

    public int CancelledCount => Messages.Count(message => message.Outcome == DeadLetterMessageDeletionOutcome.Cancelled);
}
