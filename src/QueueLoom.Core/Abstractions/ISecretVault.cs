namespace QueueLoom.Core.Abstractions;

/// <summary>
/// Stores secret material outside profile persistence. Implementations must use
/// an OS-backed, current-user/device-bound store and must never log secret values.
/// </summary>
public interface ISecretVault
{
    ValueTask StoreAsync(
        ProfileSecretKey key,
        string secret,
        CancellationToken cancellationToken = default);

    ValueTask<string?> RetrieveAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default);

    ValueTask<bool> ExistsAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default);

    ValueTask<bool> RemoveAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default);
}
