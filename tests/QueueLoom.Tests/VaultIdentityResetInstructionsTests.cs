using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The lost-identity errors told users to "remove the secrets file and enter the passwords again". On Windows that left
// vault-key.dpapi protected with the old identity's entropy, so the next save minted a new id and then failed with the
// copied-device error. The messages now name every file the active key store needs removed, and following them works.
public sealed class VaultIdentityResetInstructionsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FollowingTheWindowsResetInstructionsLetsPasswordsBeSavedAgain(bool damaged)
    {
        if (!OperatingSystem.IsWindows()) return; // DPAPI
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        using (var vault = new EncryptedFileSecretVault(paths, new WindowsDpapiMasterKeyStore(paths)))
        {
            await vault.StoreAsync(key, "synthetic-secret");
        }
        if (damaged) await File.WriteAllTextAsync(paths.InstallationIdFile, "not-a-guid");
        else File.Delete(paths.InstallationIdFile);

        using (var vault = new EncryptedFileSecretVault(paths, new WindowsDpapiMasterKeyStore(paths)))
        {
            var error = await Assert.ThrowsAsync<SecretVaultException>(async () => await vault.RetrieveAsync(key));
            await Assert.ThrowsAsync<SecretVaultException>(async () =>
                await vault.StoreAsync(ProfileSecretKey.ConnectionString(Guid.NewGuid()), "other"));
            Assert.Contains("installation.id", error.Message, StringComparison.Ordinal);
            Assert.Contains(paths.SecretsFile, error.Message, StringComparison.Ordinal);
            Assert.Contains(paths.ProtectedMasterKeyFile, error.Message, StringComparison.Ordinal);
            Assert.Contains("will be lost", error.Message, StringComparison.Ordinal);
        }

        File.Delete(paths.SecretsFile);
        File.Delete(paths.ProtectedMasterKeyFile);
        using (var vault = new EncryptedFileSecretVault(paths, new WindowsDpapiMasterKeyStore(paths)))
        {
            await vault.StoreAsync(key, "again");
            Assert.Equal("again", await vault.RetrieveAsync(key));
        }
    }

    [Fact]
    public void KeyringStoresNameTheSecretsFileAndTheOrphanedKeyringItem()
    {
        var paths = QueueLoomPaths.ForRoot(Path.Combine(Path.GetTempPath(), "ql-reset-hint"));

        IPlatformMasterKeyStore linux = new LinuxSecretServiceMasterKeyStore();
        IPlatformMasterKeyStore mac = new MacOsKeychainMasterKeyStore();
        var linuxHint = linux.DescribeReset(paths);
        var macHint = mac.DescribeReset(paths);

        Assert.Contains(paths.SecretsFile, linuxHint, StringComparison.Ordinal);
        Assert.DoesNotContain("vault-key.dpapi", linuxHint, StringComparison.Ordinal);
        Assert.Contains("secret-tool clear application QueueLoom vault", linuxHint, StringComparison.Ordinal);
        Assert.Contains(paths.SecretsFile, macHint, StringComparison.Ordinal);
        Assert.DoesNotContain("vault-key.dpapi", macHint, StringComparison.Ordinal);
        Assert.Contains("io.queueloom.master-key", macHint, StringComparison.Ordinal);
    }
}
