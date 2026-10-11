using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BackupRefresh_PreservesTheSelectionAtCompletion(bool changeSelectionWhileLoading)
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var first = SearchMessage(ServiceBusEntityReference.Queue("first"), 1, "2026-10-11T10:00:00Z");
        var second = SearchMessage(ServiceBusEntityReference.Queue("second"), 2, "2026-10-11T10:01:00Z");
        var summaries = new[] { CreateBackupSummary(profile, first.Source, first), CreateBackupSummary(profile, second.Source, second) };
        var repository = new PausedBackupRefreshRepository(summaries);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), backupRepository: repository);
        await vm.InitializeAsync();
        await vm.RefreshBackupsCommand.ExecuteAsync();
        vm.SelectedBackup = vm.BackupMessages[0];
        repository.PauseNextList = true;

        var refreshing = vm.RefreshBackupsCommand.ExecuteAsync();
        string expectedPath;
        try
        {
            await repository.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(vm.IsBusy);
            // Backed up messages remain selectable in the actual page while metadata is loading.
            if (changeSelectionWhileLoading) vm.SelectedBackup = vm.BackupMessages[1];
            expectedPath = vm.SelectedBackup!.FilePath;
        }
        finally
        {
            repository.Release.TrySetResult();
            await refreshing.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.False(vm.HasError, vm.ErrorText);
        Assert.Equal(expectedPath, vm.SelectedBackup?.FilePath);
        Assert.Equal(2, vm.VisibleBackupCount);
        Assert.Null(vm.SelectedBackupMessage);
        Assert.Equal(0, repository.LoadCalls);
    }

    private sealed class PausedBackupRefreshRepository(DeadLetterBackupSummary[] summaries) : IDeadLetterBackupRepository
    {
        public string RootDirectory => "Unused synthetic backup root";
        public bool PauseNextList { get; set; }
        public int LoadCalls { get; private set; }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default)
        {
            if (PauseNextList)
            {
                PauseNextList = false;
                Entered.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }
            return summaries;
        }

        public Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            LoadCalls++;
            throw new InvalidOperationException("Refreshing metadata must not load a message body.");
        }

        public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("This selection regression must not delete backups.");
    }
}
