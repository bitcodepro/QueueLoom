using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

internal sealed class InstallationIdentityProvider(QueueLoomPaths paths, IPlatformMasterKeyStore masterKeyStore)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cached;

    /// <summary>
    /// Returns the installation id, minting one only when <paramref name="hasStoredSecrets"/> is false: secrets are
    /// bound to the id (AAD, key lookup, DPAPI entropy), so a new id would orphan them. Callers hold the storage lock.
    /// </summary>
    public async ValueTask<string> GetOrCreateAsync(bool hasStoredSecrets, CancellationToken cancellationToken = default)
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
                if (Guid.TryParseExact(value, "N", out _))
                {
                    return _cached = value;
                }

                if (hasStoredSecrets)
                {
                    throw new SecretVaultException(
                        $"The QueueLoom installation identity file is damaged ({paths.InstallationIdFile}) although saved secrets remain. " +
                        RecoveryAdvice());
                }
            }
            else if (hasStoredSecrets)
            {
                // A new id would select a different master key, so the saved secrets would look undecryptable and
                // new ones would be written under a second key. Leave everything as it is for the user to restore.
                throw new SecretVaultException(
                    $"The QueueLoom installation identity file is missing ({paths.InstallationIdFile}) although saved secrets remain. " +
                    RecoveryAdvice());
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

    // Restoring keeps the saved passwords, so it comes first. Starting over must remove whatever the key store bound to
    // the old identity too (on Windows the DPAPI key file), or the fresh identity cannot open it and saving still fails.
    private string RecoveryAdvice() =>
        $"Restore the original {Path.GetFileName(paths.InstallationIdFile)} from a backup to keep the saved passwords. " +
        $"To start over, close QueueLoom and {masterKeyStore.DescribeReset(paths)} " +
        "All saved passwords and connection strings will be lost and must be entered again.";
}
