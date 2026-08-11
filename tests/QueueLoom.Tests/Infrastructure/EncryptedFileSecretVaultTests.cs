using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;

namespace QueueLoom.Tests.Infrastructure;

public sealed class EncryptedFileSecretVaultTests
{
    [Fact]
    public async Task StoreRetrieveAndRemove_KeepPlaintextOutOfEncryptedFile()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        var key = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        var wrongProfileKey = ProfileSecretKey.ConnectionString(Guid.NewGuid());
        const string secret =
            "Endpoint=sb://orders.servicebus.windows.net/;" +
            "SharedAccessKeyName=RootManageSharedAccessKey;" +
            "SharedAccessKey=queue-loom-plaintext-must-never-leak";

        using (var vault = CreateVault(paths))
        {
            Assert.False(await vault.ExistsAsync(key));

            await vault.StoreAsync(key, secret);

            Assert.True(await vault.ExistsAsync(key));
            Assert.Equal(secret, await vault.RetrieveAsync(key));
            Assert.False(await vault.ExistsAsync(wrongProfileKey));
            Assert.Null(await vault.RetrieveAsync(wrongProfileKey));
        }

        var encryptedFile = await File.ReadAllTextAsync(paths.SecretsFile);
        Assert.Contains("ciphertext", encryptedFile, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, encryptedFile, StringComparison.Ordinal);

        using (var reopenedVault = CreateVault(paths))
        {
            Assert.Equal(secret, await reopenedVault.RetrieveAsync(key));
            Assert.True(await reopenedVault.RemoveAsync(key));
            Assert.False(await reopenedVault.RemoveAsync(key));
            Assert.False(await reopenedVault.ExistsAsync(key));
            Assert.Null(await reopenedVault.RetrieveAsync(key));
        }
    }

    private static EncryptedFileSecretVault CreateVault(QueueLoomPaths paths) =>
        OperatingSystem.IsWindows()
            ? new EncryptedFileSecretVault(paths)
            : new EncryptedFileSecretVault(paths, new TestMasterKeyStore());

    private sealed class TestMasterKeyStore : IPlatformMasterKeyStore
    {
        private static readonly byte[] MasterKey =
            Enumerable.Range(1, 32).Select(value => (byte)value).ToArray();

        public string BackendName => "Test master key";

        public ValueTask<byte[]> GetOrCreateAsync(
            string installationId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(MasterKey.ToArray());
    }

    [Fact]
    public async Task NullEntriesDocument_ProducesControlledVaultError()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        paths.EnsureCreated();
        await File.WriteAllTextAsync(paths.SecretsFile, "{\"schemaVersion\":1,\"entries\":null}");
        using var vault = new EncryptedFileSecretVault(paths);

        var exception = await Assert.ThrowsAsync<SecretVaultException>(async () =>
            await vault.RetrieveAsync(ProfileSecretKey.ConnectionString(Guid.NewGuid())));

        Assert.Contains("damaged", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
