using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
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
