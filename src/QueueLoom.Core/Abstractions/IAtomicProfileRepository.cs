using QueueLoom.Core.Profiles;

namespace QueueLoom.Core.Abstractions;

/// <summary>Commits profile metadata and selection together, preserving both on failure.</summary>
public interface IAtomicProfileRepository : IProfileRepository
{
    Task UpsertAndSelectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default);
}
