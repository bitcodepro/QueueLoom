using QueueLoom.App.Commands;
using QueueLoom.Core.Abstractions;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private DateTimeOffset? _activityCutoff;
    private readonly List<ActivityItemViewModel> _hiddenActivity = [];
    public AsyncRelayCommand ClearActivityViewCommand { get; private set; } = null!;
    public AsyncRelayCommand RestoreActivityViewCommand { get; private set; } = null!;
    public string ActivityViewNotice => _activityCutoff is { } cutoff
        ? $"Activity through {cutoff.ToLocalTime():g} is hidden. Restore view shows retained journal entries."
        : "Clear view hides Activity entries only. Backups, schedules, operation history and messages are retained.";

    private void InitializeActivityView()
    {
        try { _activityCutoff = (_activityJournal as IActivityViewJournal)?.ClearViewCutoff; }
        catch (Exception exception) { ErrorText = SanitizeException(exception); }
        ClearActivityViewCommand = _commands.Create(async token =>
        {
            var cutoff = DateTimeOffset.UtcNow;
            if (!await _dialogs.ConfirmAsync("Clear Activity view", "Hide Activity entries up to now? Newer entries remain visible. This is a restorable display filter; journal files, operation/retry history, backups, schedules and message data are retained.",
                false, cancellationToken: token)) return;
            try
            {
                // The journal takes a lock other windows share and writes a file: off the window's thread.
                var journal = _activityJournal as IActivityViewJournal;
                _activityCutoff = await Task.Run(() =>
                {
                    journal?.SetClearViewCutoff(cutoff);
                    return journal?.ClearViewCutoff ?? cutoff;
                }, CancellationToken.None).ConfigureAwait(true);
                foreach (var item in Activity.Where(i => i.Timestamp <= _activityCutoff).ToArray())
                { _hiddenActivity.Add(item); Activity.Remove(item); }
                OnPropertyChanged(nameof(ActivityViewNotice));
                RestoreActivityViewCommand.NotifyCanExecuteChanged();
            }
            catch (Exception exception) { ErrorText = SanitizeException(exception); }
        });
        RestoreActivityViewCommand = _commands.Create(async _ =>
        {
            try
            {
                var journal = _activityJournal;
                var records = await Task.Run(() =>
                {
                    (journal as IActivityViewJournal)?.SetClearViewCutoff(null);
                    return journal?.ReadRecent() ?? [];
                }, CancellationToken.None).ConfigureAwait(true);
                _activityCutoff = null;
                var items = Activity.Concat(_hiddenActivity).ToList();
                items.AddRange(records.Select(item => new ActivityItemViewModel(item.Timestamp, item.Level, item.Action,
                    $"{item.ProfileName} · {item.Details} · operation {item.OperationId:N}", item.Source)));
                Activity.Clear();
                foreach (var item in items.DistinctBy(i => (i.Timestamp, i.Action, i.Source)).OrderByDescending(i => i.Timestamp).Take(500)) Activity.Add(item);
                _hiddenActivity.Clear();
                OnPropertyChanged(nameof(ActivityViewNotice));
                RestoreActivityViewCommand!.NotifyCanExecuteChanged();
            }
            catch (Exception exception) { ErrorText = SanitizeException(exception); }
        }, () => _activityCutoff is not null);
    }
}
