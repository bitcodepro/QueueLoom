using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class LegacyProfileRepositoryAuditTests
{
    [Theory]
    [InlineData("https://dummy:dummy@registry.invalid")]
    [InlineData("https://registry.invalid?token=dummy")]
    [InlineData("https://registry.invalid#token=dummy")]
    public async Task LegacyRegistryUrl_MixedProfilesRemainAccessibleAndRepairableWithoutDataLoss(string url)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var legacy = KafkaProfile(url);
        var valid = KafkaProfile("https://valid-registry.invalid") with { Name = "Unrelated valid environment" };
        await WriteLegacyDocument(paths, valid, legacy);
        using var vault = new EncryptedFileSecretVault(paths, new FixtureMasterKeyStore());
        var key = ProfileSecretKey.SchemaRegistryPassword(legacy.Id);
        const string password = "dummy-registry-password";
        await vault.StoreAsync(key, password);
        var originalVaultBytes = await File.ReadAllBytesAsync(paths.SecretsFile);
        using var repository = new JsonProfileRepository(paths);
        IReadOnlyList<ServiceBusProfile>? profiles = null;
        var error = await Record.ExceptionAsync(async () => profiles = await repository.ListAsync());
        Assert.Null(error);
        Assert.Equal(2, profiles!.Count);
        Assert.Equal(legacy, await repository.GetAsync(legacy.Id));
        Assert.Equal(valid, await repository.GetAsync(valid.Id));
        Assert.Equal(legacy.Id, await repository.GetSelectedProfileIdAsync());

        var renamed = valid with { Name = "Edited unrelated environment" };
        await repository.UpsertAsync(renamed);
        Assert.Equal(legacy, await repository.GetAsync(legacy.Id));
        Assert.True(await repository.DeleteAsync(valid.Id));
        Assert.Equal(legacy, Assert.Single(await repository.ListAsync()));
        await repository.UpsertAsync(renamed);
        await repository.SetSelectedProfileIdAsync(valid.Id);
        using (var reopened = new JsonProfileRepository(paths))
        {
            Assert.Equal(2, (await reopened.ListAsync()).Count);
            Assert.Equal(legacy, await reopened.GetAsync(legacy.Id));
            Assert.Equal(valid.Id, await reopened.GetSelectedProfileIdAsync());
        }

        // Legacy loading is not permission to create/edit or share a credential-bearing URL.
        await Assert.ThrowsAsync<ArgumentException>(() => repository.UpsertAsync(legacy));
        var stored = await repository.ListAsync();
        Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Export(stored));
        var repaired = legacy with { Kafka = legacy.Kafka! with { SchemaRegistryUrl = "https://registry.invalid" } };
        await repository.UpsertAsync(repaired);
        using (var reopened = new JsonProfileRepository(paths))
            Assert.Equal(repaired, await reopened.GetAsync(legacy.Id));
        var export = EnvironmentTransfer.Export([renamed, repaired]);
        Assert.DoesNotContain(password, export, StringComparison.Ordinal);
        Assert.DoesNotContain(url, export, StringComparison.Ordinal);
        Assert.Equal(originalVaultBytes, await File.ReadAllBytesAsync(paths.SecretsFile));
        Assert.Equal(password, await vault.RetrieveAsync(key));
    }

    [Fact]
    public async Task LegacyRegistryUrl_DoesNotBypassOtherStoredMetadataValidation()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var invalid = KafkaProfile("https://dummy:dummy@registry.invalid") with { Id = Guid.Empty };
        await WriteLegacyDocument(paths, KafkaProfile("https://valid-registry.invalid"), invalid);
        using var repository = new JsonProfileRepository(paths);
        await Assert.ThrowsAsync<InvalidDataException>(() => repository.ListAsync());
    }

    private static ServiceBusProfile KafkaProfile(string url) =>
        ServiceBusProfile.CreateNew("Legacy", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker.invalid:9092", SchemaRegistryUrl: url, SchemaRegistryUserName: "fixture-registry-user")
        };

    private static async Task WriteLegacyDocument(QueueLoomPaths paths, ServiceBusProfile valid, ServiceBusProfile legacy)
    {
        paths.EnsureCreated();
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            Converters = { new JsonStringEnumConverter() }
        };
        await File.WriteAllTextAsync(paths.ProfilesFile, JsonSerializer.Serialize(
            new { SchemaVersion = 1, SelectedProfileId = legacy.Id, Profiles = new[] { valid, legacy } }, options));
    }

    private sealed class FixtureMasterKeyStore : IPlatformMasterKeyStore
    {
        public string BackendName => "Isolated audit fixture";
        public ValueTask<byte[]> GetOrCreateAsync(string installationId, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Enumerable.Range(1, 32).Select(value => (byte)value).ToArray());
    }
}