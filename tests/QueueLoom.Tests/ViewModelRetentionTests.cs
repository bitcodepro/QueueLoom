using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Settings;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Retention_DeletesOnlyReadableBackupsOlderThanTheLimitAtStartUp()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterBackupSummary Backup(long number, int daysAgo, string? error = null) => new(
            Path.Combine(Path.GetTempPath(), "backups", $"{number}.json"), dev.Id, dev.Name, "Development", null,
            ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, number, $"m-{number}", null, null, null,
            DateTimeOffset.UtcNow.AddDays(-daysAgo), 10, error);
        var backups = new ListBackupRepository(Backup(1, 40), Backup(2, 5), Backup(3, 90, "Unreadable backup"));
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), new FakeWorkspace(), dialogs, backups);
        viewModel.ApplyPreferences(new AppSettings { BackupRetentionDays = 30 });

        await viewModel.InitializeAsync();

        Assert.Equal([1L], backups.Deleted.Select(item => item.SequenceNumber));
        Assert.Empty(dialogs.Confirmations);
        Assert.Contains(viewModel.Activity, item => item.Action == "Old backups cleaned up");

        viewModel.BackupRetentionDays = 3;
        await viewModel.DeleteOldBackupsCommand.ExecuteAsync();

        Assert.Equal([1L, 2], backups.Deleted.Select(item => item.SequenceNumber));
        Assert.Contains("more than 3 day(s) ago", Assert.Single(dialogs.Confirmations).Message, StringComparison.Ordinal);

        viewModel.BackupRetentionDays = 0;
        Assert.False(viewModel.DeleteOldBackupsCommand.CanExecute(null));
    }
}
