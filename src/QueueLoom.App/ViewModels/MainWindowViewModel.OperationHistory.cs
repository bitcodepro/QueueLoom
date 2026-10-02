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
    private OperationHistoryViewModel? _selectedOperation;
    private OperationItemViewModel? _selectedOperationItem;
    public ObservableCollection<OperationHistoryViewModel> OperationHistory { get; } = [];
    public ObservableCollection<OperationItemViewModel> OperationItems { get; } = [];
    public OperationHistoryViewModel? SelectedOperation
    {
        get => _selectedOperation;
        set
        {
            if (!SetProperty(ref _selectedOperation, value)) return;
            SelectedOperationItem = null;
            OperationItems.Clear();
            try
            {
                if (value is not null && _replayStore is not null)
                    foreach (var item in _replayStore.ReadHistory(value.Plan).Items) OperationItems.Add(new(item));
            }
            catch (Exception exception) { ErrorText = SanitizeException(exception); }
        }
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
        RefreshOperationHistoryCommand = new AsyncRelayCommand(_ => { RefreshOperationHistory(); return Task.CompletedTask; }, () => !IsBusy && _replayStore is not null);
        ContinueOperationCommand = new AsyncRelayCommand(token => RunWorkspaceOperationAsync("Continuing unattempted items", ct => RecoverOperationAsync(false, ct), token),
            () => !IsBusy && CanWrite && SelectedOperation is not null);
        RetryRejectedOperationCommand = new AsyncRelayCommand(token => RunWorkspaceOperationAsync("Retrying proven rejections", ct => RecoverOperationAsync(true, ct), token),
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

    private void RefreshOperationHistory()
    {
        if (_replayStore is null) return;
        var selected = SelectedOperation?.Plan.Id;
        var selectedItem = SelectedOperationItem?.Item.Index;
        try
        {
            var plans = _replayStore.List();
            OperationHistory.Clear();
            foreach (var plan in plans) OperationHistory.Add(new(plan, Profiles.FirstOrDefault(p => p.Id == plan.ProfileId)?.Name));
            SelectedOperation = OperationHistory.FirstOrDefault(p => p.Plan.Id == selected) ?? OperationHistory.FirstOrDefault();
            if (SelectedOperation?.Plan.Id == selected)
                SelectedOperationItem = OperationItems.FirstOrDefault(item => item.Item.Index == selectedItem);
        }
        catch (Exception exception) { ErrorText = SanitizeException(exception); }
    }

    private async Task RecoverOperationAsync(bool retry, CancellationToken token)
    {
        var plan = SelectedOperation?.Plan ?? throw new InvalidOperationException("Choose an operation.");
        var indexes = OperationItems.Where(i => i.IsMarked).Select(i => i.Item.Index).ToArray();
        if (indexes.Length == 0) throw new InvalidOperationException("Tick the items to continue or retry.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect the operation environment.");
        if (plan.ProfileId != profile.Id) throw new InvalidOperationException("Connect the operation's original environment.");
        var states = _replayStore!.ReadHistory(plan).Items;
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
            var result = await _replayStore.RunItemsAsync(plan, indexes, retry, _workspace, () => CanWrite, null, token);
            RemoveResentOriginals(result);
            StatusText = $"Recovery: {result.SentCount} acknowledged; {result.FailedCount} failed or uncertain. Review item outcomes.";
            AddActivity("Info", "Operation recovery stopped", StatusText);
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
            ScheduledResend.IdentityFor(profile), kind, token);
        try { return await _replayStore.RunItemsAsync(plan, Enumerable.Range(0, plan.Count).ToArray(), false, _workspace, () => CanWrite, progress, token); }
        finally { RefreshOperationHistory(); }
    }
}
