namespace QueueLoom.App.ViewModels;

/// <summary>Local purge backups.</summary>
public sealed partial class MainWindowViewModel
{
    public BackupMessageItemViewModel? SelectedBackup
    {
        get => _selectedBackup;
        set
        {
            if (SetProperty(ref _selectedBackup, value))
            {
                SelectedBackupMessage = null;
                OnPropertyChanged(nameof(HasSelectedBackup));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedBackup => SelectedBackup is not null;

    public MessageItemViewModel? SelectedBackupMessage
    {
        get => _selectedBackupMessage;
        private set
        {
            if (SetProperty(ref _selectedBackupMessage, value))
            {
                OnPropertyChanged(nameof(HasLoadedBackupMessage));
                OnPropertyChanged(nameof(CanOpenBackupAsDraft));
                OnPropertyChanged(nameof(BackupDraftHint));
                OnPropertyChanged(nameof(HasBackupDraftHint));
                NotifyCommandStates();
            }
        }
    }

    public bool HasLoadedBackupMessage => SelectedBackupMessage is not null;

    public bool CanOpenBackupAsDraft =>
        IsConnected &&
        SelectedBackupMessage is { CanOpenAsDraft: true } message &&
        message.ProfileId == ConnectedProfileId;

    public string BackupDraftHint => SelectedBackupMessage switch
    {
        null => string.Empty,
        { CanOpenAsDraft: false } message => message.EditLimitText,
        _ when !IsConnected => "Connect the message's environment to open this backup as a draft.",
        { ProfileId: { } profileId } when profileId != ConnectedProfileId =>
            "This backup belongs to another environment. Connect that environment to open it as a draft.",
        _ => string.Empty
    };

    public bool HasBackupDraftHint => !string.IsNullOrWhiteSpace(BackupDraftHint);

    public string BackupFilterText
    {
        get => _backupFilterText;
        set
        {
            if (SetProperty(ref _backupFilterText, value))
            {
                ApplyBackupFilter();
            }
        }
    }

    public string BackupStatus
    {
        get => _backupStatus;
        private set => SetProperty(ref _backupStatus, value);
    }

    public string BackupRootDirectory => _backupRepository?.RootDirectory ?? "Backups are unavailable";

    public int VisibleBackupCount => FilteredBackupMessages.Count;

    private async Task RefreshBackupsAsync(CancellationToken cancellationToken)
    {
        var repository = _backupRepository ?? throw new InvalidOperationException("Backup storage is unavailable.");
        var selectedPath = SelectedBackup?.FilePath;
        var summaries = await repository.ListAsync(cancellationToken).ConfigureAwait(true);
        _backupsLoaded = true;

        BackupMessages.Clear();
        foreach (var summary in summaries)
        {
            BackupMessages.Add(new BackupMessageItemViewModel(summary));
        }
        ApplyBackupFilter(selectedPath);
        BackupStatus = BackupMessages.Count == 0
            ? $"No backup messages found in {repository.RootDirectory}"
            : $"{BackupMessages.Count:N0} local backup message(s) · newest first";

        if (SelectedBackup?.IsReadable == true)
        {
            await LoadSelectedBackupAsync(cancellationToken).ConfigureAwait(true);
        }
    }

    private void ApplyBackupFilter(string? preferredPath = null)
    {
        preferredPath ??= SelectedBackup?.FilePath;
        var query = BackupFilterText.Trim();
        var filtered = string.IsNullOrWhiteSpace(query)
            ? BackupMessages
            : BackupMessages.Where(item =>
                item.ProfileName.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.EnvironmentLabel.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.Summary.Environment.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.SourceDisplay.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.MessageId.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.CorrelationId.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                item.Subject.Contains(query, StringComparison.OrdinalIgnoreCase));

        FilteredBackupMessages.Clear();
        foreach (var item in filtered)
        {
            FilteredBackupMessages.Add(item);
        }

        SelectedBackup = FilteredBackupMessages.FirstOrDefault(item =>
                             string.Equals(item.FilePath, preferredPath, StringComparison.OrdinalIgnoreCase))
                         ?? FilteredBackupMessages.FirstOrDefault();
        OnPropertyChanged(nameof(VisibleBackupCount));
    }

    private async Task LoadSelectedBackupAsync(CancellationToken cancellationToken)
    {
        var repository = _backupRepository ?? throw new InvalidOperationException("Backup storage is unavailable.");
        var selected = SelectedBackup ?? throw new InvalidOperationException("Select a backup message first.");
        var message = await repository.LoadAsync(selected.Summary, cancellationToken).ConfigureAwait(true);
        SelectedBackupMessage = new MessageItemViewModel(
            message,
            selected.Summary.ProfileId,
            selected.ProfileName,
            selected.EnvironmentLabel,
            selected.EnvironmentTone);
        BackupStatus = $"Loaded {selected.MessageId} · {selected.SourceDisplay} · {selected.BodySize}";
    }

    private async Task DeleteSelectedBackupAsync(CancellationToken cancellationToken)
    {
        var repository = _backupRepository ?? throw new InvalidOperationException("Backup storage is unavailable.");
        var selected = SelectedBackup ?? throw new InvalidOperationException("Select a backup message first.");
        var confirmed = await _dialogs.ConfirmAsync(
            "Delete local backup",
            $"Delete the local JSON backup for message '{selected.MessageId}' from " +
            $"'{selected.SourceDisplay}'?\n\nThe queue itself is not changed. This local file cannot be restored by QueueLoom.",
            isDangerous: true,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            BackupStatus = "Backup deletion cancelled";
            return;
        }

        await repository.DeleteAsync(selected.Summary, cancellationToken).ConfigureAwait(true);
        BackupMessages.Remove(selected);
        FilteredBackupMessages.Remove(selected);
        SelectedBackup = FilteredBackupMessages.FirstOrDefault();
        SelectedBackupMessage = null;
        if (SelectedBackup?.IsReadable == true)
        {
            await LoadSelectedBackupAsync(cancellationToken).ConfigureAwait(true);
        }
        BackupStatus = $"Deleted local backup {selected.FileName}. Azure was not changed.";
        OnPropertyChanged(nameof(VisibleBackupCount));
        AddActivity(
            "Warning",
            "Local backup deleted",
            $"{selected.ProfileName} · {selected.SourceDisplay} · {selected.MessageId}",
            selected.Summary.Source);
    }
}
