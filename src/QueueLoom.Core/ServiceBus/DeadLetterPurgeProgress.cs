namespace QueueLoom.Core.ServiceBus;

public enum DeadLetterPurgeStage
{
    Starting,
    BackingUp,
    Deleting,
    Verifying,
    Completed
}

public sealed record DeadLetterPurgeProgress(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    int TargetNumber,
    int TargetCount,
    long BackedUpCount,
    long DeletedCount,
    DeadLetterPurgeStage Stage);
