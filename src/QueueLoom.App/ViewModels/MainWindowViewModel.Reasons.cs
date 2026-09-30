using System.Collections.ObjectModel;
using QueueLoom.App.Commands;

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

    private void InitializeReasons()
    {
        SelectDeadLetterReasonCommand = new RelayCommand<DeadLetterReasonItemViewModel>(SelectDeadLetterReason);
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
        OnPropertyChanged(nameof(HasDeadLetterReasons));
        OnPropertyChanged(nameof(DeadLetterReasonsSummary));
        OnPropertyChanged(nameof(DeadLetterReasonsTitle));
    }
}
