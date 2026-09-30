using QueueLoom.App.Commands;
using QueueLoom.Core.Settings;

namespace QueueLoom.App.ViewModels;

/// <summary>Deleting backups that are older than the chosen number of days, by hand or at start-up.</summary>
public sealed partial class MainWindowViewModel
{
    private int _backupRetentionDays;

    /// <summary>Backups older than this are deleted when QueueLoom starts; 0 keeps them forever.</summary>
    public int BackupRetentionDays
    {
        get => _backupRetentionDays;
        set
        {
            if (SetProperty(ref _backupRetentionDays, Math.Clamp(value, 0, AppSettings.MaximumBackupRetentionDays)))
            {
                OnPropertyChanged(nameof(BackupRetentionSummary));
                DeleteOldBackupsCommand?.NotifyCanExecuteChanged();
            }
        }
    }

    public string BackupRetentionSummary => BackupRetentionDays == 0
        ? "Backups are kept until you delete them."
        : $"Backups older than {BackupRetentionDays:N0} day(s) are deleted when QueueLoom starts.";

    public AsyncRelayCommand DeleteOldBackupsCommand { get; private set; } = null!;

    private void InitializeRetention()
    {
        DeleteOldBackupsCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Deleting old backups", token2 => DeleteOldBackupsAsync(automatic: false, token2), token,
                allowCancellation: false),
            () => !IsBusy && BackupRetentionDays > 0 && _backupRepository is not null);
    }

    /// <summary>Deletes readable backups saved before the retention cutoff. Unreadable files are left for the operator.</summary>
    private async Task DeleteOldBackupsAsync(bool automatic, CancellationToken cancellationToken)
    {
        var repository = _backupRepository;
        if (repository is null || BackupRetentionDays <= 0)
        {
            return;
        }

        var cutoff = DateTimeOffset.UtcNow.AddDays(-BackupRetentionDays);
        var old = (await repository.ListAsync(cancellationToken).ConfigureAwait(true))
            .Where(summary => summary.IsReadable && summary.BackedUpAt < cutoff)
            .ToArray();
        if (old.Length == 0)
        {
            if (!automatic)
            {
                BackupStatus = $"No backups are older than {BackupRetentionDays:N0} day(s).";
            }
            return;
        }

        if (!automatic)
        {
            var confirmed = await _dialogs.ConfirmAsync(
                "Delete old backups",
                $"Delete {old.Length:N0} local backup file(s) saved more than {BackupRetentionDays:N0} day(s) ago?\n\n" +
                "The queues themselves are not changed. Deleted files cannot be restored by QueueLoom.",
                isDangerous: true,
                cancellationToken: cancellationToken).ConfigureAwait(true);
            if (!confirmed)
            {
                BackupStatus = "Backup clean-up cancelled";
                return;
            }
        }

        var deleted = 0;
        foreach (var summary in old)
        {
            try
            {
                await repository.DeleteAsync(summary, CancellationToken.None).ConfigureAwait(true);
                deleted++;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
            {
                // Left in place; the next clean-up tries again.
            }
        }

        _backupsLoaded = false;
        BackupStatus = $"Deleted {deleted:N0} backup(s) older than {BackupRetentionDays:N0} day(s).";
        AddActivity("Info", automatic ? "Old backups cleaned up" : "Old backups deleted",
            $"{deleted:N0} of {old.Length:N0} backup(s) older than {BackupRetentionDays:N0} day(s)");
        if (!automatic && CurrentPage == Models.NavigationPage.Backups)
        {
            await RefreshBackupsAsync(cancellationToken).ConfigureAwait(true);
        }
    }
}
