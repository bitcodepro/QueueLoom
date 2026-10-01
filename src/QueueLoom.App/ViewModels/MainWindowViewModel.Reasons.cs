using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>The dead-letter reasons of the listed messages, and ticking all messages of one reason.</summary>
public sealed partial class MainWindowViewModel
{
    /// <summary>Reasons of the listed dead letters, most frequent first.</summary>
    public ObservableCollection<DeadLetterReasonItemViewModel> DeadLetterReasons { get; } = [];

    /// <summary>Worth showing once the list holds dead letters with at least one reason.</summary>
    public bool HasDeadLetterReasons => DeadLetterReasons.Count > 0;

    public string DeadLetterReasonsSummary => DeadLetterReasons.All(reason => reason.IsState)
        ? DeadLetterReasons.Count == 1 ? "1 kind" : $"{DeadLetterReasons.Count:N0} kinds"
        : DeadLetterReasons.Count == 1
            ? "1 reason"
            : $"{DeadLetterReasons.Count:N0} reasons";

    /// <summary>"REASONS" for dead letters; "WAITING" for scheduled and deferred messages, which were not delivered yet.</summary>
    public string DeadLetterReasonsTitle => DeadLetterReasons.Count > 0 && DeadLetterReasons.All(reason => reason.IsState)
        ? "WAITING"
        : "REASONS";

    public RelayCommand<DeadLetterReasonItemViewModel> SelectDeadLetterReasonCommand { get; private set; } = null!;

    /// <summary>
    /// The causes of the listed dead letters (reason plus the shape of the error description), most frequent first;
    /// only the first <see cref="MaximumCauseChips"/> are shown.
    /// </summary>
    public ObservableCollection<DeadLetterCauseItemViewModel> DeadLetterCauses { get; } = [];

    private const int MaximumCauseChips = 10;

    private int _causeCount;

    /// <summary>Worth showing when the descriptions say more than the reasons do.</summary>
    public bool HasDeadLetterCauses => DeadLetterCauses.Count > 0;

    public string DeadLetterCausesSummary => _causeCount == 1 ? "1 cause" : $"{_causeCount:N0} causes";

    public string DeadLetterCausesMore => _causeCount > MaximumCauseChips ? $"and {_causeCount - MaximumCauseChips:N0} more" : string.Empty;

    public bool HasMoreDeadLetterCauses => _causeCount > MaximumCauseChips;

    public RelayCommand<DeadLetterCauseItemViewModel> SelectDeadLetterCauseCommand { get; private set; } = null!;

    private void InitializeReasons()
    {
        SelectDeadLetterReasonCommand = new RelayCommand<DeadLetterReasonItemViewModel>(SelectDeadLetterReason);
        SelectDeadLetterCauseCommand = new RelayCommand<DeadLetterCauseItemViewModel>(SelectDeadLetterCause);
    }

    /// <summary>Ticks exactly the messages with this cause; when they are already ticked, unticks them.</summary>
    private void SelectDeadLetterCause(DeadLetterCauseItemViewModel? cause)
    {
        if (cause is null)
        {
            return;
        }

        var untick = cause.IsSelected;
        _updatingReasonSelection = true;
        try
        {
            using var batch = BatchMessageUpdates();
            foreach (var message in Messages.Where(message => message.CanDelete))
            {
                message.IsMarked = !untick && cause.Matches(message);
            }
        }
        finally
        {
            _updatingReasonSelection = false;
        }

        RebuildDeadLetterReasons();
        StatusText = untick ? $"Unticked the messages with cause {cause.Text}" : $"Ticked {cause.Count:N0} message(s) with cause {cause.Text}";
    }

    /// <summary>Ticks exactly the messages with this reason; when they are already ticked, unticks them.</summary>
    private void SelectDeadLetterReason(DeadLetterReasonItemViewModel? reason)
    {
        if (reason is null)
        {
            return;
        }

        var untick = reason.IsSelected;
        _updatingReasonSelection = true;
        try
        {
            using var batch = BatchMessageUpdates();
            foreach (var message in Messages.Where(message => message.CanDelete))
            {
                message.IsMarked = !untick &&
                                   DeadLetterReasonItemViewModel.ReasonOf(message) == reason.Reason;
            }
        }
        finally
        {
            _updatingReasonSelection = false;
        }

        RebuildDeadLetterReasons();
        StatusText = (untick, reason.IsState) switch
        {
            (true, true) => $"Unticked the {reason.Reason.ToLowerInvariant()} messages",
            (false, true) => $"Ticked {reason.Count:N0} {reason.Reason.ToLowerInvariant()} message(s)",
            (true, false) => $"Unticked the messages with reason {reason.Reason}",
            _ => $"Ticked {reason.Count:N0} message(s) with reason {reason.Reason}"
        };
    }

    private bool _updatingReasonSelection;

    private void RebuildDeadLetterReasons()
    {
        if (_updatingReasonSelection)
        {
            return;
        }

        var groups = Messages
            .Where(message => message.CanDelete)
            .GroupBy(DeadLetterReasonItemViewModel.ReasonOf)
            .Select(group => new DeadLetterReasonItemViewModel(
                group.Key, group.Count(), group.All(message => message.IsMarked), SelectDeadLetterReasonCommand))
            .OrderByDescending(group => group.Count)
            .ThenBy(group => group.Reason, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        DeadLetterReasons.Clear();
        foreach (var group in groups)
        {
            DeadLetterReasons.Add(group);
        }
        RebuildDeadLetterCauses();
        OnPropertyChanged(nameof(HasDeadLetterReasons));
        OnPropertyChanged(nameof(DeadLetterReasonsSummary));
        OnPropertyChanged(nameof(DeadLetterReasonsTitle));
    }

    private void RebuildDeadLetterCauses()
    {
        var deadLetters = Messages.Where(message => message.CanDelete && !message.IsScheduled && !message.IsDeferred).ToArray();
        var causes = deadLetters
            .GroupBy(message => message.CauseKey)
            .Select(group => (Cause: new DeadLetterCause(group.Key.Reason, group.Key.Pattern,
                group.Select(message => message.Message.DeadLetterErrorDescription).FirstOrDefault(text => !string.IsNullOrWhiteSpace(text))?.Trim().Split('\n')[0],
                group.Count()), Selected: group.All(message => message.IsMarked)))
            .OrderByDescending(item => item.Cause.Count)
            .ThenBy(item => item.Cause.Label, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        // Causes only add something when the error descriptions split at least one reason into several kinds of failure.
        var useful = causes.Any(item => item.Cause.Pattern is not null) &&
                     causes.Length > causes.Select(item => item.Cause.Reason).Distinct(StringComparer.Ordinal).Count();
        _causeCount = useful ? causes.Length : 0;
        DeadLetterCauses.Clear();
        foreach (var (cause, selected) in useful ? causes.Take(MaximumCauseChips) : [])
        {
            DeadLetterCauses.Add(new DeadLetterCauseItemViewModel(cause, selected, SelectDeadLetterCauseCommand));
        }
        OnPropertyChanged(nameof(HasDeadLetterCauses));
        OnPropertyChanged(nameof(DeadLetterCausesSummary));
        OnPropertyChanged(nameof(DeadLetterCausesMore));
        OnPropertyChanged(nameof(HasMoreDeadLetterCauses));
    }
}
