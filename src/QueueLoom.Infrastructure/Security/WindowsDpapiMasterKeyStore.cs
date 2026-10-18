using System.Security.Cryptography;
using System.Text;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

internal sealed class WindowsDpapiMasterKeyStore(QueueLoomPaths paths) : IPlatformMasterKeyStore
{
    private const int MasterKeySize = 32;

    public string BackendName => "Windows DPAPI (CurrentUser)";

    public async ValueTask<byte[]> GetOrCreateAsync(
        string installationId,
        CancellationToken cancellationToken = default)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException("Windows DPAPI is only available on Windows.");
        }

        paths.EnsureCreated();
        var entropy = SHA256.HashData(Encoding.UTF8.GetBytes($"QueueLoom|vault-v1|{installationId}"));
        try
        {
            if (File.Exists(paths.ProtectedMasterKeyFile))
            {
                var protectedKey = await File.ReadAllBytesAsync(paths.ProtectedMasterKeyFile, cancellationToken)
                    .ConfigureAwait(false);
                try
                {
                    var key = ProtectedData.Unprotect(protectedKey, entropy, DataProtectionScope.CurrentUser);
                    if (key.Length != MasterKeySize)
                    {
                        CryptographicOperations.ZeroMemory(key);
                        throw new SecretVaultException("The QueueLoom vault key has an invalid length.");
                    }

                    return key;
                }
                catch (CryptographicException exception)
                {
                    throw new SecretVaultException(
                        "The QueueLoom vault cannot be unlocked by this Windows user on this computer. " +
                        "The data may have been copied from another device or the user profile changed.",
                        exception);
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(protectedKey);
                }
            }

            // The key file is gone but secrets encrypted with it remain (an antivirus quarantine, a partial restore): a
            // new key would never open them, and secrets saved with it would not open once the old file came back.
            if (HasStoredSecrets(paths.SecretsFile))
            {
                throw new SecretVaultException(
                    $"The QueueLoom vault key file is missing ({paths.ProtectedMasterKeyFile}) although saved secrets remain. " +
                    "Restore the file from a backup, or remove the secrets file to start over and enter the passwords again.");
            }

            var newKey = RandomNumberGenerator.GetBytes(MasterKeySize);
            byte[]? protectedNewKey = null;
            try
            {
                protectedNewKey = ProtectedData.Protect(newKey, entropy, DataProtectionScope.CurrentUser);
                await AtomicFile.WriteBytesAsync(paths.ProtectedMasterKeyFile, protectedNewKey, cancellationToken)
                    .ConfigureAwait(false);
                return newKey;
            }
            catch
            {
                CryptographicOperations.ZeroMemory(newKey);
                throw;
            }
            finally
            {
                if (protectedNewKey is not null)
                {
                    CryptographicOperations.ZeroMemory(protectedNewKey);
                }
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(entropy);
        }
    }

    /// <summary>Whether the secrets file holds any entry; a file that cannot be read counts as holding some.</summary>
    private static bool HasStoredSecrets(string secretsFile)
    {
        if (!File.Exists(secretsFile))
        {
            return false;
        }

        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(secretsFile));
            return !document.RootElement.TryGetProperty("entries", out var entries) ||
                   entries.ValueKind != System.Text.Json.JsonValueKind.Object ||
                   entries.EnumerateObject().Any();
        }
        catch (Exception exception) when (exception is System.Text.Json.JsonException or IOException or UnauthorizedAccessException)
        {
            return true;
        }
    }
}
