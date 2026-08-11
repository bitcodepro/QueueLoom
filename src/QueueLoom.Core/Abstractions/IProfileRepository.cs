using QueueLoom.Core.Profiles;

namespace QueueLoom.Core.Abstractions;

public interface IProfileRepository
{
    Task<IReadOnlyList<ServiceBusProfile>> ListAsync(CancellationToken cancellationToken = default);

    Task<ServiceBusProfile?> GetAsync(Guid profileId, CancellationToken cancellationToken = default);

    Task UpsertAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(Guid profileId, CancellationToken cancellationToken = default);

    Task<Guid?> GetSelectedProfileIdAsync(CancellationToken cancellationToken = default);

    Task SetSelectedProfileIdAsync(Guid? profileId, CancellationToken cancellationToken = default);
}
