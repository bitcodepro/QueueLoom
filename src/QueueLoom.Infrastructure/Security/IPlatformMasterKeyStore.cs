namespace QueueLoom.Infrastructure.Security;

internal interface IPlatformMasterKeyStore
{
    string BackendName { get; }

    ValueTask<byte[]> GetOrCreateAsync(
        string installationId,
        CancellationToken cancellationToken = default);
}
