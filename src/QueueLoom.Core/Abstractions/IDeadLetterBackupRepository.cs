using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

public interface IDeadLetterBackupRepository
{
    string RootDirectory { get; }

    Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(
        CancellationToken cancellationToken = default);

    Task<BrowsedMessage> LoadAsync(
        DeadLetterBackupSummary summary,
        CancellationToken cancellationToken = default);

    Task DeleteAsync(
        DeadLetterBackupSummary summary,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes backup session folders that hold nothing but their session.json and have been quiet long enough that no
    /// purge can still be writing into them. Sessions with message files or temporary files are never touched.
    /// Returns how many session folders were removed; stores without session folders remove nothing.
    /// </summary>
    Task<int> RemoveFinishedEmptySessionsAsync(CancellationToken cancellationToken = default) => Task.FromResult(0);
}
