using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// When the DPAPI-protected vault key vanished (an antivirus quarantine, a partial restore) while encrypted secrets
// remained, QueueLoom silently made a new key: every saved password stopped decrypting, and new ones were written with
// a key the restored old one would not open. It now refuses and says what is missing, as the Linux store does.
public sealed class WindowsVaultKeyLossTests
{
    [Fact]
    public async Task AMissingKeyWithStoredSecretsIsRefusedNotReplaced()
    {
        if (!OperatingSystem.IsWindows()) return; // DPAPI
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var vault = new EncryptedFileSecretVault(paths, new WindowsDpapiMasterKeyStore(paths));
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        await vault.StoreAsync(key, "synthetic-secret");
        File.Delete(paths.ProtectedMasterKeyFile);

        var error = await Assert.ThrowsAsync<SecretVaultException>(async () => await vault.RetrieveAsync(key));
        await Assert.ThrowsAsync<SecretVaultException>(async () => await vault.StoreAsync(ProfileSecretKey.ConnectionString(Guid.NewGuid()), "other"));

        Assert.Contains("key file is missing", error.Message, StringComparison.Ordinal);
        Assert.False(File.Exists(paths.ProtectedMasterKeyFile), "A new vault key replaced the missing one.");
    }

    [Fact]
    public async Task AFreshVaultOrOneWithNoSecretsLeftStillGetsAKey()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var vault = new EncryptedFileSecretVault(paths, new WindowsDpapiMasterKeyStore(paths));
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        await vault.StoreAsync(key, "synthetic-secret");
        await vault.RemoveAsync(key);
        File.Delete(paths.ProtectedMasterKeyFile);

        await vault.StoreAsync(key, "again");

        Assert.Equal("again", await vault.RetrieveAsync(key));
    }
}
