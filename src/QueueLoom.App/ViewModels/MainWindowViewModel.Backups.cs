namespace QueueLoom.App.ViewModels;

/// <summary>Local purge backups.</summary>
public sealed partial class MainWindowViewModel
{
    private CancellationTokenSource? _backupSelectionCancellation;
    public BackupMessageItemViewModel? SelectedBackup
    {
        get => _selectedBackup;
        set
        {
            if (SetProperty(ref _selectedBackup, value))
            {
                _backupSelectionCancellation?.Cancel();
                SelectedBackupMessage = null;
                OnPropertyChanged(nameof(HasSelectedBackup));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedBackup => SelectedBackup is not null;

    public BackupGroupItemViewModel? SelectedBackupGroup
    {
        get => _selectedBackupGroup;
        set
        {
            if (SetProperty(ref _selectedBackupGroup, value))
            {
                ApplyBackupFilter();
            }
        }
    }

    /// <summary>"Delete 12 backups…": everything the chosen group and the filter text show.</summary>
    public string DeleteVisibleBackupsLabel => FilteredBackupMessages.Count == 1
        ? "Delete 1 backup…"
        : $"Delete {FilteredBackupMessages.Count:N0} backups…";

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
        RebuildBackupGroups();
        ApplyBackupFilter(selectedPath);
        BackupStatus = BackupMessages.Count == 0
            ? $"No backup messages found in {repository.RootDirectory}"
            : $"{BackupMessages.Count:N0} local backup message(s) · newest first";

        // Browsing metadata never loads a body. The operator opens one explicitly.
    }

    private void ApplyBackupFilter(string? preferredPath = null)
    {
        preferredPath ??= SelectedBackup?.FilePath;
        var query = BackupFilterText.Trim();
        var group = SelectedBackupGroup;
        var inGroup = group is null ? BackupMessages : BackupMessages.Where(group.Contains);
        var filtered = string.IsNullOrWhiteSpace(query)
            ? inGroup
            : inGroup.Where(item =>
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

    private void RebuildBackupGroups()
    {
        var selectedKey = SelectedBackupGroup?.Key;
        var groups = BackupGroupItemViewModel.Build(BackupMessages);
        BackupGroups.Clear();
        foreach (var group in groups)
        {
            BackupGroups.Add(group);
        }

        // Keep the chosen group while it still has backups; otherwise fall back to all of them.
        _selectedBackupGroup = BackupGroups.FirstOrDefault(group => group.Key == selectedKey) ?? BackupGroups[0];
        OnPropertyChanged(nameof(SelectedBackupGroup));
    }

    private async Task DeleteVisibleBackupsAsync(CancellationToken cancellationToken)
    {
        var repository = _backupRepository ?? throw new InvalidOperationException("Backup storage is unavailable.");
        var targets = FilteredBackupMessages.ToArray();
        if (targets.Length == 0)
        {
            throw new InvalidOperationException("There are no backups to delete.");
        }

        var group = SelectedBackupGroup;
        var scope = group is null or { Kind: BackupGroupKind.All } ? "all environments" : $"{group.KindLabel.ToLowerInvariant()} '{group.Title}'";
        var filterNote = string.IsNullOrWhiteSpace(BackupFilterText) ? string.Empty : $" matching '{BackupFilterText.Trim()}'";
        var sources = targets
            .GroupBy(item => $"{item.ProfileName} · {item.SourceDisplay}")
            .OrderByDescending(item => item.Count())
            .Take(8)
            .Select(item => $"• {item.Key}: {item.Count():N0}");
        var confirmed = await _dialogs.ConfirmAsync(
            "Delete local backups",
            $"Delete {targets.Length:N0} local backup file(s) from {scope}{filterNote}?\n\n" +
            string.Join("\n", sources) +
            (targets.Select(item => item.SourceDisplay).Distinct().Count() > 8 ? "\n• …" : string.Empty) +
            "\n\nThe queues themselves are not changed. Deleted files cannot be restored by QueueLoom.",
            isDangerous: true,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            BackupStatus = "Backup deletion cancelled";
            return;
        }

        var deleted = 0;
        var failed = 0;
        foreach (var item in targets)
        {
            try
            {
                await repository.DeleteAsync(item.Summary, CancellationToken.None).ConfigureAwait(true);
                BackupMessages.Remove(item);
                deleted++;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
            {
                failed++;
            }
        }

        SelectedBackupMessage = null;
        RebuildBackupGroups();
        ApplyBackupFilter();
        BackupStatus = failed == 0
            ? $"Deleted {deleted:N0} local backup(s). The queues were not changed."
            : $"Deleted {deleted:N0} local backup(s); {failed:N0} could not be deleted. Refresh to see what is left.";
        AddActivity(
            failed == 0 ? "Warning" : "Error",
            "Local backups deleted",
            $"{deleted:N0} deleted, {failed:N0} failed · {scope}{filterNote}");
    }

    private async Task LoadSelectedBackupAsync(CancellationToken cancellationToken)
    {
        var repository = _backupRepository ?? throw new InvalidOperationException("Backup storage is unavailable.");
        var selected = SelectedBackup ?? throw new InvalidOperationException("Select a backup message first.");
        using var selection = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _backupSelectionCancellation?.Cancel();
        _backupSelectionCancellation = selection;
        QueueLoom.Core.ServiceBus.BrowsedMessage message;
        try { message = await repository.LoadAsync(selected.Summary, selection.Token).ConfigureAwait(true); }
        catch (OperationCanceledException) when (selection.IsCancellationRequested && !cancellationToken.IsCancellationRequested) { return; }
        finally { if (ReferenceEquals(_backupSelectionCancellation, selection)) _backupSelectionCancellation = null; }
        if (selection.IsCancellationRequested || SelectedBackup != selected) return;
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
        SelectedBackupMessage = null;
        RebuildBackupGroups();
        ApplyBackupFilter();
        BackupStatus = $"Deleted local backup {selected.FileName}. The queue was not changed.";
        OnPropertyChanged(nameof(VisibleBackupCount));
        AddActivity(
            "Warning",
            "Local backup deleted",
            $"{selected.ProfileName} · {selected.SourceDisplay} · {selected.MessageId}",
            selected.Summary.Source);
    }
}
