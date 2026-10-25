using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

internal interface IPlatformMasterKeyStore
{
    string BackendName { get; }

    ValueTask<byte[]> GetOrCreateAsync(
        string installationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// How to start the vault over once its installation identity is lost: everything this store keeps bound to the
    /// old identity, so a fresh identity gets a working key. Phrased to follow "To start over, ".
    /// </summary>
    string DescribeReset(QueueLoomPaths paths) => $"remove {paths.SecretsFile}.";
}
