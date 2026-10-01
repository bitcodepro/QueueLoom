using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("https://dummy:dummy@registry.invalid", "before")]
    [InlineData("https://registry.invalid?token=dummy", "before")]
    [InlineData("https://registry.invalid#token=dummy", "before")]
    [InlineData("https://dummy:dummy@registry.invalid", "after")]
    [InlineData("https://registry.invalid?token=dummy", "after")]
    [InlineData("https://registry.invalid#token=dummy", "after")]
    [InlineData("https://dummy:dummy@registry.invalid", "none")]
    [InlineData("https://registry.invalid?token=dummy", "none")]
    [InlineData("https://registry.invalid#token=dummy", "none")]
    [InlineData("https://dummy:dummy@registry.invalid", "remove-before")]
    [InlineData("https://registry.invalid?token=dummy", "remove-before")]
    [InlineData("https://registry.invalid#token=dummy", "remove-before")]
    [InlineData("https://dummy:dummy@registry.invalid", "remove-after")]
    [InlineData("https://registry.invalid?token=dummy", "remove-after")]
    [InlineData("https://registry.invalid#token=dummy", "remove-after")]
    [InlineData("https://dummy:dummy@registry.invalid", "cancel")]
    [InlineData("https://registry.invalid?token=dummy", "cancel")]
    [InlineData("https://registry.invalid#token=dummy", "cancel")]
    [InlineData("https://registry.invalid?token=dummy", "no-change")]
    [InlineData("https://dummy:dummy@registry.invalid", "metadata-failure")]
    [InlineData("https://registry.invalid?token=dummy", "metadata-failure")]
    [InlineData("https://registry.invalid#token=dummy", "metadata-failure")]
    public async Task LegacyRegistryEdit_VaultFailurePreservesPersistedProfileSelectionAndPassword(
        string url, string failure)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var legacy = ServiceBusProfile.CreateNew("Legacy", EnvironmentKind.Development,
            new(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker.invalid:9092", SchemaRegistryUrl: url,
                SchemaRegistryUserName: "dummy-user")
        };
        var unrelated = CreateProfile("Other", EnvironmentKind.Test);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            Converters = { new JsonStringEnumConverter() }
        };
        await File.WriteAllTextAsync(paths.ProfilesFile, JsonSerializer.Serialize(
            new { SchemaVersion = 1, SelectedProfileId = unrelated.Id,
                Profiles = new[] { unrelated, legacy } }, options));
        var originalBytes = await File.ReadAllBytesAsync(paths.ProfilesFile);
        using var repository = new JsonProfileRepository(paths);
        using var encryptedVault = new EncryptedFileSecretVault(paths, new RegistryEditMasterKeyStore());
        var faultVault = new FailingRegistryEditVault(failure);
        ISecretVault vault = failure is "none" or "no-change" ? encryptedVault : faultVault;
        var key = ProfileSecretKey.SchemaRegistryPassword(legacy.Id);
        await vault.StoreAsync(key, "old-dummy-password");
        var repaired = legacy with
        {
            Name = "Repaired",
            Kafka = legacy.Kafka! with { SchemaRegistryUrl = "https://registry.invalid" }
        };
        var dialogs = new FakeDialogService
        {
            EditResult = new ProfileEditorResult(repaired, null, false)
            {
                SchemaRegistryPassword = failure.StartsWith("remove-", StringComparison.Ordinal) ||
                    failure == "no-change" ? null : "new-dummy-password",
                RemovesSchemaRegistryPassword = failure.StartsWith("remove-", StringComparison.Ordinal)
            }
        };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs, secretVault: vault);
        await vm.InitializeAsync();
        vm.SelectedProfile = Assert.Single(vm.Profiles, p => p.Id == legacy.Id);
        if (failure == "cancel") faultVault.AfterReplacement = vm.EditEnvironmentCommand.Cancel;
        FileStream? metadataBlock = null;
        if (failure == "metadata-failure")
            faultVault.AfterReplacement = () =>
                metadataBlock = new FileStream(paths.ProfilesFile, FileMode.Open, FileAccess.Read, FileShare.None);
        try
        {
            await vm.EditEnvironmentCommand.ExecuteAsync();
        }
        finally
        {
            metadataBlock?.Dispose();
        }

        using var reopened = new JsonProfileRepository(paths);
        var stored = await reopened.GetAsync(legacy.Id);
        if (failure is "none" or "no-change")
        {
            Assert.Equal("Repaired", stored!.Name);
            Assert.Equal("https://registry.invalid", stored.Kafka!.SchemaRegistryUrl);
            Assert.Equal(legacy.Id, await reopened.GetSelectedProfileIdAsync());
            Assert.Equal(failure == "no-change" ? "old-dummy-password" : "new-dummy-password",
                await vault.RetrieveAsync(key));
            Assert.True(string.IsNullOrEmpty(vm.ErrorText));
        }
        else
        {
            Assert.Equal(legacy, stored);
            Assert.Equal(originalBytes, await File.ReadAllBytesAsync(paths.ProfilesFile));
            Assert.Equal(unrelated.Id, await reopened.GetSelectedProfileIdAsync());
            Assert.Equal("old-dummy-password", await vault.RetrieveAsync(key));
            if (failure == "metadata-failure")
                Assert.False(string.IsNullOrEmpty(vm.ErrorText));
            else if (failure != "cancel")
                Assert.Contains("registry vault failure", vm.ErrorText, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("could not be fully restored", vm.ErrorText, StringComparison.OrdinalIgnoreCase);
            await Assert.ThrowsAsync<ArgumentException>(() => reopened.UpsertAsync(legacy));
            Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Export([stored!]));
        }
        Assert.Equal(unrelated, await reopened.GetAsync(unrelated.Id));
    }

    private sealed class FailingRegistryEditVault(string failure) : ISecretVault
    {
        private readonly FakeSecretVault _inner = new();
        private bool _failed;
        public Action? AfterReplacement { get; set; }
        public async ValueTask StoreAsync(ProfileSecretKey key, string secret,
            CancellationToken cancellationToken = default)
        {
            if (!_failed && failure is "before" or "after" && secret == "new-dummy-password")
            {
                _failed = true;
                if (failure == "after") await _inner.StoreAsync(key, secret, cancellationToken);
                throw new IOException("Injected registry vault failure.");
            }
            await _inner.StoreAsync(key, secret, cancellationToken);
            if (secret == "new-dummy-password") AfterReplacement?.Invoke();
        }
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key,
            CancellationToken cancellationToken = default) => _inner.RetrieveAsync(key, cancellationToken);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key,
            CancellationToken cancellationToken = default) => _inner.ExistsAsync(key, cancellationToken);
        public async ValueTask<bool> RemoveAsync(ProfileSecretKey key,
            CancellationToken cancellationToken = default)
        {
            if (!_failed && failure.StartsWith("remove-", StringComparison.Ordinal))
            {
                _failed = true;
                if (failure == "remove-after") await _inner.RemoveAsync(key, cancellationToken);
                throw new IOException("Injected registry vault failure.");
            }
            return await _inner.RemoveAsync(key, cancellationToken);
        }
    }
    private sealed class RegistryEditMasterKeyStore : IPlatformMasterKeyStore
    {
        public string BackendName => "Isolated registry edit fixture";
        public ValueTask<byte[]> GetOrCreateAsync(string installationId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Enumerable.Range(1, 32).Select(value => (byte)value).ToArray());
    }
}
