namespace QueueLoom.Core.Abstractions;

/// <summary>Owns a complete profile/credential mutation, separately from individual storage operations.</summary>
public interface IProfileMutationCoordinator
{
    ValueTask<IAsyncDisposable> AcquireProfileMutationAsync(CancellationToken cancellationToken = default);

    /// <summary>Persist before touching credentials; readers fail closed if the writer cannot finish or roll back.</summary>
    Task MarkCredentialUpdatePendingAsync(Guid profileId, CancellationToken cancellationToken = default);

    bool IsCredentialUpdatePending(Guid profileId);

    void CompleteCredentialUpdate(Guid profileId);
}
