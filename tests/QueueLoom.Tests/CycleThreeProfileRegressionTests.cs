using System.Collections.Concurrent;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CycleThreeProfiles_ConcurrentWindowsCannotCommitMixedMetadataAndCredentials(bool registry)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var repositoryA = new JsonProfileRepository(paths);
        using var repositoryB = new JsonProfileRepository(paths);
        var original = registry
            ? ServiceBusProfile.CreateNew("Original", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone)) with
                { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092", SchemaRegistryUrl: "https://registry.invalid", SchemaRegistryUserName: "original") }
            : CreateConnectionStringProfile("Original");
        await repositoryA.UpsertAndSelectAsync(original);
        var key = registry ? ProfileSecretKey.SchemaRegistryPassword(original.Id) : ProfileSecretKey.ConnectionString(original.Id);
        var vault = new PausingSyntheticVault();
        await vault.StoreAsync(key, "synthetic-original");
        var a = original with { Name = "Writer A", FullyQualifiedNamespace = registry ? null : "a.servicebus.windows.net",
            Kafka = registry ? original.Kafka! with { SchemaRegistryUserName = "writer-a" } : null };
        var b = original with { Name = "Writer B", FullyQualifiedNamespace = registry ? null : "b.servicebus.windows.net",
            Kafka = registry ? original.Kafka! with { SchemaRegistryUserName = "writer-b" } : null };
        ProfileEditorResult Result(ServiceBusProfile profile, string value) => registry
            ? new(profile, null, false) { SchemaRegistryPassword = value }
            : new(profile, value, true);
        var workspaceA = new FakeWorkspace(); var workspaceB = new FakeWorkspace();
        await using var vmA = CreateViewModel(repositoryA, workspaceA, new FakeDialogService { EditResult = Result(a, "synthetic-a") }, secretVault: vault);
        await using var vmB = CreateViewModel(repositoryB, workspaceB, new FakeDialogService { EditResult = Result(b, "synthetic-b") }, secretVault: vault);
        await vmA.InitializeAsync(); await vmB.InitializeAsync();
        vault.PauseValue = "synthetic-a";
        var saveA = vmA.EditEnvironmentCommand.ExecuteAsync();
        await vault.Paused.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var saveB = vmB.EditEnvironmentCommand.ExecuteAsync();
        try
        {
            // Baseline B completes while A is paused after writing its secret. A then commits last.
            // With transaction ownership B waits; after A commits B's stale edit must be rejected.
            await Task.WhenAny(saveB, Task.Delay(300));
        }
        finally { vault.Release.TrySetResult(); }
        await Task.WhenAll(saveA, saveB).WaitAsync(TimeSpan.FromSeconds(10));
        var persisted = (await repositoryB.GetAsync(original.Id))!;
        Assert.Equal("Writer A", persisted.Name);
        Assert.Equal("synthetic-a", await vault.RetrieveAsync(key));
        Assert.False(vmA.HasError, vmA.ErrorText);
        Assert.True(vmB.HasError, "The stale second editor must not overwrite the committed environment.");
        Assert.Empty(workspaceA.SentMessages); Assert.Empty(workspaceB.SentMessages);
    }

    [Fact]
    public async Task CycleThreeProfiles_SequentialFreshEditorsCanReplaceSyntheticCredentials()
    {
        using var directory = new TemporaryDirectory();
        using var repository = new JsonProfileRepository(QueueLoomPaths.ForRoot(directory.Path));
        var original = CreateConnectionStringProfile("Original");
        await repository.UpsertAndSelectAsync(original);
        var vault = new PausingSyntheticVault();
        for (var i = 1; i <= 2; i++)
        {
            var current = (await repository.GetAsync(original.Id))!;
            var updated = current with { Name = "Sequential " + i };
            await using var vm = CreateViewModel(repository, new FakeWorkspace(), new FakeDialogService
                { EditResult = new(updated, "synthetic-" + i, true) }, secretVault: vault);
            await vm.InitializeAsync(); await vm.EditEnvironmentCommand.ExecuteAsync();
            Assert.False(vm.HasError, vm.ErrorText);
            Assert.Equal(updated.Name, (await repository.GetAsync(original.Id))!.Name);
            Assert.Equal("synthetic-" + i, await vault.RetrieveAsync(ProfileSecretKey.ConnectionString(original.Id)));
        }
    }

    private sealed class PausingSyntheticVault : ISecretVault
    {
        private readonly ConcurrentDictionary<ProfileSecretKey, string> _secrets = new();
        public string? PauseValue { get; set; }
        public TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ISecretVault? Inner { get; init; }
        public async ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default)
        {
            _secrets[key] = secret;
            if (Inner is not null) await Inner.StoreAsync(key, secret, cancellationToken);
            if (secret == PauseValue) { Paused.TrySetResult(); await Release.Task.WaitAsync(cancellationToken); }
        }
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(_secrets.GetValueOrDefault(key));
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(_secrets.ContainsKey(key));
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(_secrets.TryRemove(key, out _));
    }

    [Fact]
    public async Task CycleThreeProfiles_FailedSaveCannotRollbackAnotherWindowsCommittedSecret()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var repositoryA = new JsonProfileRepository(paths); using var repositoryB = new JsonProfileRepository(paths);
        var original = CreateConnectionStringProfile("Original");
        await repositoryA.UpsertAndSelectAsync(original);
        var vault = new PausingSyntheticVault();
        var key = ProfileSecretKey.ConnectionString(original.Id);
        await vault.StoreAsync(key, "synthetic-original");
        await using var vmA = CreateViewModel(repositoryA, new FakeWorkspace(), new FakeDialogService
            { EditResult = new(original with { Name = "" }, "synthetic-a", true) }, secretVault: vault);
        await using var vmB = CreateViewModel(repositoryB, new FakeWorkspace(), new FakeDialogService
            { EditResult = new(original with { Name = "Writer B" }, "synthetic-b", true) }, secretVault: vault);
        await vmA.InitializeAsync(); await vmB.InitializeAsync();
        vault.PauseValue = "synthetic-a";
        var saveA = vmA.EditEnvironmentCommand.ExecuteAsync(); await vault.Paused.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var saveB = vmB.EditEnvironmentCommand.ExecuteAsync();
        try { await Task.WhenAny(saveB, Task.Delay(300)); }
        finally { vault.Release.TrySetResult(); }
        await Task.WhenAll(saveA, saveB).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(vmA.HasError); Assert.False(vmB.HasError, vmB.ErrorText);
        Assert.Equal("Writer B", (await repositoryA.GetAsync(original.Id))!.Name);
        Assert.Equal("synthetic-b", await vault.RetrieveAsync(key));
    }

    [Fact]
    public async Task CycleThreeProfiles_ConcurrentConnectionRejectsStaleMetadataBeforeParsingNewCredential()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var repository = new JsonProfileRepository(paths);
        var original = CreateConnectionStringProfile("Original");
        await repository.UpsertAndSelectAsync(original);
        using var realVault = new EncryptedFileSecretVault(paths, new SyntheticMasterKeyStore());
        var writerVault = new PausingSyntheticVault { Inner = realVault };
        var key = ProfileSecretKey.ConnectionString(original.Id);
        await writerVault.StoreAsync(key, "synthetic-original");
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), new FakeDialogService
            { EditResult = new(original with { FullyQualifiedNamespace = "new.servicebus.windows.net" }, "synthetic-a", true) }, secretVault: writerVault);
        await vm.InitializeAsync(); writerVault.PauseValue = "synthetic-a";
        var save = vm.EditEnvironmentCommand.ExecuteAsync(); await writerVault.Paused.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await using var reader = new AzureServiceBusWorkspace(realVault, backupStore: new DeadLetterJsonBackupStore(paths));
        var connect = Record.ExceptionAsync(() => reader.ConnectAsync(original)).AsTask();
        try { await Task.WhenAny(connect, Task.Delay(300)); }
        finally { writerVault.Release.TrySetResult(); }
        await save;
        // Synthetic strings are deliberately unparsable, so baseline cannot make any remote request.
        var error = await connect.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.IsType<InvalidOperationException>(error);
        Assert.Contains("configuration changed", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(reader.ConnectedProfileId);
    }

    private sealed class SyntheticMasterKeyStore : IPlatformMasterKeyStore
    {
        public string BackendName => "Isolated synthetic master key";
        public ValueTask<byte[]> GetOrCreateAsync(string installationId, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Enumerable.Range(1, 32).Select(i => (byte)i).ToArray());
    }
}
