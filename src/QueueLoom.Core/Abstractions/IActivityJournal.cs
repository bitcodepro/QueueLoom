using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

public sealed record ActivityRecord(Guid OperationId, DateTimeOffset Timestamp, string Level,
    string Action, string Details, Guid? ProfileId, string? ProfileName, ServiceBusEntityReference? Source);

public interface IActivityJournal
{
    void Append(ActivityRecord record);
    IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500);
}

/// <summary>A reversible display filter. Journal records are retained.</summary>
public interface IActivityViewJournal : IActivityJournal
{
    DateTimeOffset? ClearViewCutoff { get; }
    void SetClearViewCutoff(DateTimeOffset? cutoff);
}
