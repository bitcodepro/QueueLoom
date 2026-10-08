using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class OperationItemViewModel(OperationItem item) : ObservableObject
{
    private bool _isMarked;
    public OperationItem Item { get; } = item;
    public string Description => $"{Item.Index + 1}: {Item.Origin} → {Item.Destination} · {Item.MessageId}";
    public string Outcome => Item.State switch
    {
        "OriginalKept" => $"Send confirmed; the original was not removed and is still in the source (or already gone). Never resend. {Item.Detail}",
        "SentOriginalKept" => $"Send confirmed; source removal not confirmed. Never resend. {Item.Detail}",
        "DeleteUncertain" or "Deleting" => $"Send confirmed; source deletion unknown. Manual inspection required. {Item.Detail}",
        "AwaitingScheduleClaim" => "Blocked scheduled snapshot: claim/activation was not completed. Review the scheduled job before any new operation.",
        _ => $"{Item.State} {Item.Detail}"
    };
    public bool IsMarked { get => _isMarked; set => SetProperty(ref _isMarked, value); }
    public bool CanSelect => Item.CanContinue || Item.CanRetry;
}

public sealed class OperationHistoryViewModel(ReplayPlan plan, string? profileName = null)
{
    public ReplayPlan Plan { get; } = plan;
    public string Title => $"{Plan.CreatedAt.ToLocalTime():yyyy-MM-dd HH:mm} · {profileName ?? Plan.ProfileId.ToString()} · {Plan.Kind} / {Plan.Mode} · {Plan.Count} items · {Plan.Id:N}";
}

public sealed partial class MainWindowViewModel
{
    private static string OperationWarnings(IReadOnlyList<string> warnings) => warnings.Count == 0
        ? string.Empty
        : " Warning: " + string.Join(" ", warnings.Select(QueueLoom.Core.Diagnostics.SensitiveDataRedactor.Redact));

    private OperationHistoryViewModel? _selectedOperation;
    private OperationItemViewModel? _selectedOperationItem;
    public ObservableCollection<OperationHistoryViewModel> OperationHistory { get; } = [];
    public bool HasOperationHistory => OperationHistory.Count > 0;
    public ObservableCollection<OperationItemViewModel> OperationItems { get; } = [];
    public OperationHistoryViewModel? SelectedOperation
    {
        get => _selectedOperation;
        set
        {
            if (!SetProperty(ref _selectedOperation, value)) return;
            // Cleared at once: ticked items are indexes into the shown operation, so items of the previous one must
            // never stay listed under the new selection while its own are read.
            SelectedOperationItem = null;
            OperationItems.Clear();
            OperationItemsLoad = LoadOperationItemsAsync(value, ++_operationItemsGeneration, restoreItem: null);
        }
    }

    private int _operationItemsGeneration;
    private int _operationHistoryGeneration;

    /// <summary>The latest read of the selected operation's items; tests await it.</summary>
    internal Task OperationItemsLoad { get; private set; } = Task.CompletedTask;

    /// <summary>The latest read of the operation list (and then of the selected operation's items); tests await it.</summary>
    internal Task OperationHistoryRefresh { get; private set; } = Task.CompletedTask;

    /// <summary>
    /// Reads an operation's items off the UI thread: up to 1,000 items with several small files each. Only the latest
    /// selection's read is shown; one that finishes after another selection is dropped.
    /// </summary>
    private async Task LoadOperationItemsAsync(OperationHistoryViewModel? operation, int generation, int? restoreItem)
    {
        if (operation is null || _replayStore is not { } store) return;
        OperationHistory history;
        try
        {
            history = await Task.Run(() => store.ReadHistory(operation.Plan)).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (generation == _operationItemsGeneration && !_isDisposed) ErrorText = SanitizeException(exception);
            return;
        }
        if (generation != _operationItemsGeneration || _isDisposed) return;
        foreach (var item in history.Items) OperationItems.Add(new(item));
        if (restoreItem is { } index) SelectedOperationItem = OperationItems.FirstOrDefault(item => item.Item.Index == index);
    }
    public OperationItemViewModel? SelectedOperationItem
    {
        get => _selectedOperationItem;
        set
        {
            if (SetProperty(ref _selectedOperationItem, value))
                OnPropertyChanged(nameof(SelectedOperationItemDetails));
        }
    }
    public string SelectedOperationItemDetails => SelectedOperationItem is { } item
        ? $"{item.Description}\n\n{item.Outcome}" : string.Empty;
    public AsyncRelayCommand RefreshOperationHistoryCommand { get; private set; } = null!;
    public AsyncRelayCommand ContinueOperationCommand { get; private set; } = null!;
    public AsyncRelayCommand RetryRejectedOperationCommand { get; private set; } = null!;

    private void InitializeOperationHistory()
    {
        OperationHistory.CollectionChanged += (_, _) => OnPropertyChanged(nameof(HasOperationHistory));
        RefreshOperationHistoryCommand = _commands.Create(_ => { RefreshOperationHistory(); return OperationHistoryRefresh; }, () => !IsBusy && _replayStore is not null);
        ContinueOperationCommand = _commands.Create(token => RunWorkspaceOperationAsync("Continuing unattempted items", ct => RecoverOperationAsync(false, ct), token),
            () => !IsBusy && CanWrite && SelectedOperation is not null);
        RetryRejectedOperationCommand = _commands.Create(token => RunWorkspaceOperationAsync("Retrying proven rejections", ct => RecoverOperationAsync(true, ct), token),
            () => !IsBusy && CanWrite && SelectedOperation is not null);
        PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(IsBusy) or nameof(CanWrite) or nameof(SelectedOperation))
            {
                ContinueOperationCommand.NotifyCanExecuteChanged(); RetryRejectedOperationCommand.NotifyCanExecuteChanged();
                RefreshOperationHistoryCommand.NotifyCanExecuteChanged();
            }
        };
        RefreshOperationHistory();
    }

    private void RefreshOperationHistory() => OperationHistoryRefresh = RefreshOperationHistoryAsync();

    /// <summary>
    /// Lists the saved operations off the UI thread (one folder per operation of the retention period), then selects the
    /// same operation and item again. Only the latest refresh is applied.
    /// </summary>
    private async Task RefreshOperationHistoryAsync()
    {
        if (_replayStore is not { } store) return;
        var generation = ++_operationHistoryGeneration;
        IReadOnlyList<ReplayPlan> plans;
        try
        {
            plans = await Task.Run(store.List).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (generation == _operationHistoryGeneration && !_isDisposed) ErrorText = SanitizeException(exception);
            return;
        }
        if (generation != _operationHistoryGeneration || _isDisposed) return;
        // Read now, not before the list: the operator may have chosen another operation or item meanwhile.
        var selected = SelectedOperation?.Plan.Id;
        var selectedItem = SelectedOperationItem?.Item.Index;
        OperationHistory.Clear();
        foreach (var plan in plans) OperationHistory.Add(new(plan, Profiles.FirstOrDefault(p => p.Id == plan.ProfileId)?.Name));
        var operation = OperationHistory.FirstOrDefault(p => p.Plan.Id == selected) ?? OperationHistory.FirstOrDefault();
        // Set through the field so the item to select again is passed to the one read of the new list's items.
        if (SetProperty(ref _selectedOperation, operation, nameof(SelectedOperation)))
        {
            SelectedOperationItem = null;
            OperationItems.Clear();
            OperationItemsLoad = LoadOperationItemsAsync(operation, ++_operationItemsGeneration,
                operation?.Plan.Id == selected ? selectedItem : null);
        }
        await OperationItemsLoad.ConfigureAwait(true);
    }

    private async Task RecoverOperationAsync(bool retry, CancellationToken token)
    {
        var plan = SelectedOperation?.Plan ?? throw new InvalidOperationException("Choose an operation.");
        var indexes = OperationItems.Where(i => i.IsMarked).Select(i => i.Item.Index).ToArray();
        if (indexes.Length == 0) throw new InvalidOperationException("Tick the items to continue or retry.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect the operation environment.");
        if (plan.ProfileId != profile.Id) throw new InvalidOperationException("Connect the operation's original environment.");
        var store = _replayStore ?? throw new InvalidOperationException("Operation history is not available.");
        // The plan and the ticked items were taken above; a new selection while this reads does not change them.
        var states = (await Task.Run(() => store.ReadHistory(plan), token).ConfigureAwait(true)).Items;
        if (indexes.Any(i => retry ? !states[i].CanRetry : !states[i].CanContinue))
            throw new InvalidOperationException("Continue accepts only Pending items. Retry accepts only provider-proven Rejected items. Unknown and confirmed sends cannot be retried.");
        if (!await _dialogs.ConfirmAsync(retry ? "Retry proven rejections" : "Continue unattempted items",
            $"Environment: {profile.Name}\n{profile.EndpointDisplay}\nOperation: {plan.Id:N}\n{indexes.Length} selected items\n" +
            string.Join("\n", indexes.Select(i => $"{states[i].Origin} → {states[i].Destination}")) +
            "\n\nSaved bodies, destinations and stable IDs will be used. Topics can fan out. " +
            (plan.Mode == ResendMode.Move ? "Confirmed sends are backed up before source removal; source identity is checked by the provider. " : "Originals are retained. ") +
            "Unknown delivery or deletion outcomes require manual inspection; this action never retries them.", true,
            requiredText: profile.Environment == EnvironmentKind.Production ? profile.Name : null, cancellationToken: token)) return;
        RecordOperationIntent("Operation recovery started", $"{plan.Id:N} · {indexes.Length} selected items", null);
        try
        {
            var result = await store.RunItemsAsync(plan, indexes, retry, _workspace, () => CanWrite, null, token);
            RemoveResentOriginals(result);
            var summary = $"{result.SentCount:N0} of {indexes.Length:N0} acknowledged" +
                          (plan.Mode == ResendMode.Move ? $" · {result.MovedCount:N0} originals removed" : string.Empty) +
                          (result.OriginalsKeptCount > 0 ? $" · {result.OriginalsKeptCount:N0} originals kept" : string.Empty) +
                          (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed or uncertain" : string.Empty) +
                          (result.CancelledCount > 0 ? $" · {result.CancelledCount:N0} not sent (cancelled)" : string.Empty) + OperationWarnings(result.Warnings);
            StatusText = $"Recovery: {summary}. Review item outcomes.";
            var complete = result.FailedCount == 0 && result.OriginalsKeptCount == 0 && result.CancelledCount == 0;
            AddActivity(complete && result.Warnings.Count == 0 ? "Success" : "Warning",
                complete ? "Operation recovery completed" : result.CancelledCount > 0 ? "Operation recovery cancelled" : "Operation recovery incomplete",
                $"{plan.Id:N} · {summary}" + (result.BackupDirectory is null ? string.Empty : $" · backup {result.BackupDirectory}"));
        }
        finally { RefreshOperationHistory(); }
    }

    private async Task<ResendResult> RunDurableResendAsync(IReadOnlyList<ResendItem> items, ResendMode mode, int rate,
        string kind, IProgress<ResendProgress>? progress, CancellationToken token, ReplayPlan? preparedPlan = null)
    {
        // Tests and embedders without a configured durable store keep the existing executor.
        if (_replayStore is null) return await DeadLetterResender.ResendAsync(_workspace, items, mode, rate, progress, token);
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var plan = preparedPlan ?? await _replayStore.CreateResendAsync(profile.Id, items, mode, rate, profile.EndpointDisplay,
            ScheduledResend.IdentityFor(profile), kind, token, provider: profile.Provider);
        try { return await _replayStore.RunItemsAsync(plan, Enumerable.Range(0, plan.Count).ToArray(), false, _workspace, () => CanWrite, progress, token); }
        finally { RefreshOperationHistory(); }
    }
}
