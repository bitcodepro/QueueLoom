using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

public sealed record ActivityRecord(Guid OperationId, DateTimeOffset Timestamp, string Level,
    string Action, string Details, Guid? ProfileId, string? ProfileName, ServiceBusEntityReference? Source);

public interface IActivityJournal
{
    /// <summary>Saves the record durably (on disk before returning): used before a destructive operation starts.</summary>
    void Append(ActivityRecord record);

    /// <summary>
    /// Saves an ordinary entry. It need not be forced to disk before returning: losing the newest entries in a power
    /// failure is acceptable, a wait on the disk for every entry on the window's thread is not.
    /// </summary>
    void AppendEntry(ActivityRecord record) => Append(record);

    IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500);
}

/// <summary>A journal that writes ordinary entries later, on its own thread: it reports an entry it could not write.</summary>
public interface IReportsActivityWriteFailures
{
    /// <summary>Raised on the writer's thread for an ordinary entry that could not be saved.</summary>
    event Action<Exception>? EntryWriteFailed;
}

/// <summary>A reversible display filter. Journal records are retained.</summary>
public interface IActivityViewJournal : IActivityJournal
{
    DateTimeOffset? ClearViewCutoff { get; }
    void SetClearViewCutoff(DateTimeOffset? cutoff);
}
