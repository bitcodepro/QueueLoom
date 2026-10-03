using System.Text.Json.Nodes;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class CycleThreeImportLifecycleRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CycleThreeImport_ReportDialogDoesNotOwnProfileTransaction(bool includeValid)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var importingRepository = new JsonProfileRepository(paths);
        using var otherRepository = new JsonProfileRepository(paths);
        var source = ViewModelStateTests.CreateProfile("Imported", EnvironmentKind.Test);
        var document = JsonNode.Parse(EnvironmentTransfer.Export(includeValid ? [source] : []))!.AsObject();
        document["environments"]!.AsArray().Add(JsonNode.Parse("""{"name":"Broken","environment":"Test","authentication":{"kind":"EntraId"}}"""));
        var file = Path.Combine(directory.Path, "isolated-import.json");
        await File.WriteAllTextAsync(file, document.ToJsonString());
        var dialogs = new PausedImportReport(file);
        var workspace = new ViewModelStateTests.FakeWorkspace();
        await using var vm = new MainWindowViewModel(importingRepository, new UnusedVault(), workspace, dialogs);
        await vm.InitializeAsync();
        var import = vm.ImportEnvironmentsCommand.ExecuteAsync();
        try
        {
            await dialogs.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("Some environments were not imported", dialogs.Title);
            Assert.Contains("Broken", dialogs.Message, StringComparison.Ordinal);
            Assert.False(import.IsCompleted);
            Assert.False(dialogs.Dismissed.Task.IsCompleted);
            var imported = await otherRepository.ListAsync();
            Assert.Equal(includeValid ? 1 : 0, imported.Count);
            if (includeValid)
            {
                Assert.NotEqual(source.Id, Assert.Single(imported).Id);
                Assert.Equal(ProfileAccessMode.ReadOnly, Assert.Single(imported).AccessMode);
            }

            // A second actual repository instance must own a profile operation while the
            // report remains open. Cancellation bounds the witness without dismissing it.
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var error = await Record.ExceptionAsync(async () =>
            {
                await using var ownership = await otherRepository.AcquireProfileMutationAsync(timeout.Token);
                await otherRepository.UpsertAsync(ViewModelStateTests.CreateProfile("Other", EnvironmentKind.Test), timeout.Token);
            });
            Assert.Null(error);
            Assert.False(dialogs.Dismissed.Task.IsCompleted);
            Assert.False(import.IsCompleted);
            Assert.Equal(includeValid ? 2 : 1, (await otherRepository.ListAsync()).Count);
            Assert.Empty(workspace.SentMessages);
        }
        finally
        {
            dialogs.Dismissed.TrySetResult();
            await import.WaitAsync(TimeSpan.FromSeconds(5));
        }
        Assert.False(vm.HasError, vm.ErrorText);
    }

    [Fact]
    public async Task CycleThreeImport_ValidOnlyImportCommitsAndReleasesTransactionWithoutReport()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var repository = new JsonProfileRepository(paths);
        using var other = new JsonProfileRepository(paths);
        var source = ViewModelStateTests.CreateProfile("Valid", EnvironmentKind.Test);
        var file = Path.Combine(directory.Path, "isolated-import.json");
        await File.WriteAllTextAsync(file, EnvironmentTransfer.Export([source]));
        var dialogs = new PausedImportReport(file);
        await using var vm = new MainWindowViewModel(repository, new UnusedVault(), new ViewModelStateTests.FakeWorkspace(), dialogs);
        await vm.InitializeAsync(); await vm.ImportEnvironmentsCommand.ExecuteAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(vm.HasError, vm.ErrorText);
        Assert.False(dialogs.Entered.Task.IsCompleted);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var ownership = await other.AcquireProfileMutationAsync(timeout.Token);
        var imported = Assert.Single(await other.ListAsync());
        Assert.NotEqual(source.Id, imported.Id);
        Assert.Equal(ProfileAccessMode.ReadOnly, imported.AccessMode);
    }

    private sealed class PausedImportReport(string file) : IUserDialogService
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Dismissed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public string? Title { get; private set; }
        public string? Message { get; private set; }
        public Task<ProfileEditorResult?> EditProfileAsync(ServiceBusProfile? profile, CancellationToken token = default) => Task.FromResult<ProfileEditorResult?>(null);
        public Task<bool> ConfirmAsync(string title, string message, bool isDangerous = false, string? requiredText = null, CancellationToken cancellationToken = default) => Task.FromResult(false);
        public Task<string?> ChooseOpenFileAsync(string title, IReadOnlyList<(string Name, string Pattern)> fileTypes, CancellationToken token = default) => Task.FromResult<string?>(file);
        public async Task ShowMessageAsync(string title, string message, bool isError = false, CancellationToken cancellationToken = default)
        {
            Title = title; Message = message;
            Entered.TrySetResult();
            await Dismissed.Task.WaitAsync(cancellationToken);
        }
    }

    private sealed class UnusedVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken token = default) => throw new InvalidOperationException("Import must not store credentials.");
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => throw new InvalidOperationException("Import must not remove credentials.");
    }
}
