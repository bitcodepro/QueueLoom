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
}
