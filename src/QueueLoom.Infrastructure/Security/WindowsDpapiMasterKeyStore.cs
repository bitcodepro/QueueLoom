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
}
