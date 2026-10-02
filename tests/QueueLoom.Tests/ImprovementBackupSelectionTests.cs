using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeletingBackupDoesNotOpenRemainingBodyOrLoseSuccessfulActivity(bool visibleGroup)
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var first = SearchMessage(ServiceBusEntityReference.Queue("delete-me"), 1, "2026-10-02T01:00:00Z");
        var second = SearchMessage(ServiceBusEntityReference.Queue("keep-me"), 2, "2026-10-02T02:00:00Z");
        var summaries = new[] { CreateBackupSummary(profile, first.Source, first), CreateBackupSummary(profile, second.Source, second) };
        var repository = new UnviewableBackupRepository(summaries);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), dialogs, repository);
        await vm.InitializeAsync();
        await vm.RefreshBackupsCommand.ExecuteAsync();
        if (visibleGroup) vm.SelectedBackupGroup = vm.BackupGroups.Single(g => g.Kind == QueueLoom.App.ViewModels.BackupGroupKind.Queue && g.Title == "delete-me");
        else vm.SelectedBackup = vm.BackupMessages.Single(b => b.Summary == summaries[0]);

        if (visibleGroup) await vm.DeleteVisibleBackupsCommand.ExecuteAsync();
        else await vm.DeleteSelectedBackupCommand.ExecuteAsync();

        Assert.Equal(summaries[0], Assert.Single(repository.Deleted));
        Assert.Equal(summaries[1], Assert.Single(vm.BackupMessages).Summary);
        Assert.Equal(summaries[1], vm.SelectedBackup?.Summary);
        Assert.Equal(0, repository.LoadCalls);
        Assert.Null(vm.SelectedBackupMessage);
        Assert.StartsWith("Deleted", vm.BackupStatus, StringComparison.Ordinal);
        Assert.Contains(vm.Activity, entry => entry.Action == (visibleGroup ? "Local backups deleted" : "Local backup deleted"));
    }

    private sealed class UnviewableBackupRepository(DeadLetterBackupSummary[] summaries) : IDeadLetterBackupRepository
    {
        public string RootDirectory => "Unused fake backup root";
        public List<DeadLetterBackupSummary> Deleted { get; } = [];
        public int LoadCalls { get; private set; }
        public Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<DeadLetterBackupSummary>>(summaries.Except(Deleted).ToArray());
        public Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            LoadCalls++;
            throw new InvalidDataException("This remaining backup exceeds the 64 MiB body viewer limit.");
        }
        public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            Deleted.Add(summary);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task ChangingBackupSelectionCancelsAndDiscardsStaleBodyEvenIfReaderIgnoresCancellation()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var first = SearchMessage(ServiceBusEntityReference.Queue("first"), 1, "2026-10-02T01:00:00Z");
        var second = SearchMessage(ServiceBusEntityReference.Queue("second"), 2, "2026-10-02T02:00:00Z");
        var summaries = new[] { CreateBackupSummary(profile, first.Source, first), CreateBackupSummary(profile, second.Source, second) };
        var repository = new DelayedBackupRepository(summaries, [first, second]);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), backupRepository: repository);
        await vm.InitializeAsync();
        await vm.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(0, repository.LoadCalls);
        var loading = vm.LoadSelectedBackupCommand.ExecuteAsync();
        await repository.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        vm.SelectedBackup = vm.BackupMessages[1];
        Assert.True(repository.Token.IsCancellationRequested);
        repository.Release.SetResult(); await loading;
        Assert.Null(vm.SelectedBackupMessage);
        await vm.LoadSelectedBackupCommand.ExecuteAsync();
        Assert.Equal(2, vm.SelectedBackupMessage?.SequenceNumber);
    }

    private sealed class DelayedBackupRepository(DeadLetterBackupSummary[] summaries, BrowsedMessage[] messages) : IDeadLetterBackupRepository
    {
        public string RootDirectory => "Unused fake backup root";
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationToken Token { get; private set; }
        public int LoadCalls { get; private set; }
        public Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<DeadLetterBackupSummary>>(summaries);
        public async Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            Token = cancellationToken;
            if (++LoadCalls == 1) { Started.SetResult(); await Release.Task; }
            return messages[Array.IndexOf(summaries, summary)];
        }
        public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
