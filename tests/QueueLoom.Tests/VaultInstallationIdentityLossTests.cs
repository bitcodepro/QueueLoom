using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// When installation.id vanished while encrypted secrets remained, the vault silently minted a new identity: the platform
// key stores then looked up (or derived) a different master key, every saved password failed with a misleading
// "cannot be decrypted" error and new saves mixed two keys in one file. It now refuses and leaves the files alone.
public sealed class VaultInstallationIdentityLossTests
{
    [Fact]
    public async Task AMissingIdentityWithStoredSecretsIsRefusedAndRestoringItRecovers()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var keys = new KeyStorePerInstallation();
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        using (var vault = new EncryptedFileSecretVault(paths, keys))
        {
            await vault.StoreAsync(key, "synthetic-secret");
        }
        var originalId = await File.ReadAllTextAsync(paths.InstallationIdFile);
        File.Delete(paths.InstallationIdFile);

        using (var vault = new EncryptedFileSecretVault(paths, keys))
        {
            var error = await Assert.ThrowsAsync<SecretVaultException>(async () => await vault.RetrieveAsync(key));
            await Assert.ThrowsAsync<SecretVaultException>(async () =>
                await vault.StoreAsync(ProfileSecretKey.ConnectionString(Guid.NewGuid()), "other"));

            Assert.Contains("installation identity file is missing", error.Message, StringComparison.Ordinal);
            Assert.False(File.Exists(paths.InstallationIdFile), "A new installation identity replaced the missing one.");
            Assert.Equal([originalId], keys.InstallationIds);
        }

        await File.WriteAllTextAsync(paths.InstallationIdFile, originalId);
        using (var vault = new EncryptedFileSecretVault(paths, keys))
        {
            Assert.Equal("synthetic-secret", await vault.RetrieveAsync(key));
        }
    }

    [Fact]
    public async Task ADamagedIdentityWithStoredSecretsIsRefusedAndNotOverwritten()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var keys = new KeyStorePerInstallation();
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        using (var vault = new EncryptedFileSecretVault(paths, keys))
        {
            await vault.StoreAsync(key, "synthetic-secret");
        }
        await File.WriteAllTextAsync(paths.InstallationIdFile, "not-a-guid");

        using var reopened = new EncryptedFileSecretVault(paths, keys);
        var error = await Assert.ThrowsAsync<SecretVaultException>(async () => await reopened.RetrieveAsync(key));
        await Assert.ThrowsAsync<SecretVaultException>(async () => await reopened.StoreAsync(key, "other"));

        Assert.Contains("installation identity file is damaged", error.Message, StringComparison.Ordinal);
        Assert.Equal("not-a-guid", await File.ReadAllTextAsync(paths.InstallationIdFile));
        Assert.Single(keys.InstallationIds);
    }

    [Fact]
    public async Task AFreshVaultStillCreatesAnIdentity()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var vault = new EncryptedFileSecretVault(paths, new KeyStorePerInstallation());
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());

        await vault.StoreAsync(key, "synthetic-secret");

        Assert.True(Guid.TryParseExact((await File.ReadAllTextAsync(paths.InstallationIdFile)).Trim(), "N", out _));
        Assert.Equal("synthetic-secret", await vault.RetrieveAsync(key));
    }

    // Like the keyring stores: one master key per installation id, created on first lookup.
    private sealed class KeyStorePerInstallation : IPlatformMasterKeyStore
    {
        private readonly Dictionary<string, byte[]> _keys = new(StringComparer.Ordinal);

        public IReadOnlyCollection<string> InstallationIds => _keys.Keys;

        public string BackendName => "Test keyring";

        public ValueTask<byte[]> GetOrCreateAsync(string installationId, CancellationToken cancellationToken = default)
        {
            if (!_keys.TryGetValue(installationId, out var key))
            {
                _keys[installationId] = key = System.Security.Cryptography.RandomNumberGenerator.GetBytes(32);
            }

            return ValueTask.FromResult(key.ToArray());
        }
    }
}
