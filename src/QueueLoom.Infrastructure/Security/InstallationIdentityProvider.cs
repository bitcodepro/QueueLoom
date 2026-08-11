using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

internal sealed class InstallationIdentityProvider(QueueLoomPaths paths)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cached;

    public async ValueTask<string> GetOrCreateAsync(CancellationToken cancellationToken = default)
    {
        if (_cached is not null)
        {
            return _cached;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cached is not null)
            {
                return _cached;
            }

            paths.EnsureCreated();
            if (File.Exists(paths.InstallationIdFile))
            {
                var value = (await File.ReadAllTextAsync(paths.InstallationIdFile, cancellationToken)
                    .ConfigureAwait(false)).Trim();
                if (!Guid.TryParseExact(value, "N", out _))
                {
                    throw new InvalidDataException("QueueLoom's installation identifier is damaged.");
                }

                return _cached = value;
            }

            var installationId = Guid.NewGuid().ToString("N");
            await AtomicFile.WriteTextAsync(paths.InstallationIdFile, installationId, cancellationToken)
                .ConfigureAwait(false);
            return _cached = installationId;
        }
        finally
        {
            _gate.Release();
        }
    }
}
