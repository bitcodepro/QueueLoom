using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.Settings;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>A choice in the "enqueued within" filter of the dead-letter search.</summary>
public sealed record SearchWindowOption(string Label, int? Minutes);

/// <summary>Saved dead-letter searches and the "enqueued within" filter.</summary>
public sealed partial class MainWindowViewModel
{
    private SearchWindowOption _searchWindow = SearchWindows[0];
    private SavedSearch? _selectedSavedSearch;

    public static IReadOnlyList<SearchWindowOption> SearchWindows { get; } =
    [
        new("Any time", null),
        new("Last 15 minutes", 15),
        new("Last hour", 60),
        new("Last 24 hours", 1_440),
        new("Last 7 days", 10_080)
    ];

    public IReadOnlyList<SearchWindowOption> SearchWindowOptions => SearchWindows;

    public SearchWindowOption SearchWindow
    {
        get => _searchWindow;
        set => SetProperty(ref _searchWindow, value ?? SearchWindows[0]);
    }

    /// <summary>Saved searches; the shell persists them when this collection changes.</summary>
    public ObservableCollection<SavedSearch> SavedSearches { get; } = [];

    public bool HasSavedSearches => SavedSearches.Count > 0;

    /// <summary>Choosing a saved search fills in its text, environment and time window and runs it.</summary>
    public SavedSearch? SelectedSavedSearch
    {
        get => _selectedSavedSearch;
        set
        {
            var selectionChanged = SetProperty(ref _selectedSavedSearch, value);
            if (value is null)
            {
                DeleteSavedSearchCommand?.NotifyCanExecuteChanged();
                return;
            }

            var filter = value.EnvironmentId is { } environmentId
                ? DeadLetterEnvironmentFilters.FirstOrDefault(item => item.ProfileId == environmentId)
                : null;
            if (!selectionChanged && (value.EnvironmentId is null || filter is not null))
            {
                return;
            }

            DeadLetterSearchQuery = value.Query;
            SearchWindow = SearchWindows.FirstOrDefault(option => option.Minutes == value.WithinMinutes) ?? SearchWindows[0];
            if (value.EnvironmentId is not null)
            {
                if (filter is null)
                {
                    SelectedDeadLetterEnvironmentFilter = null;
                    InvalidateDeadLetterSearchResults();
                    ErrorText = "The saved search environment is no longer available. Choose an environment before searching again.";
                    DeleteSavedSearchCommand?.NotifyCanExecuteChanged();
                    return;
                }
                SelectedDeadLetterEnvironmentFilter = filter;
            }
            DeleteSavedSearchCommand?.NotifyCanExecuteChanged();
            if (SearchDeadLettersCommand.CanExecute(null))
            {
                SearchDeadLettersCommand.Execute(null);
            }
            else if (IsBusy)
            {
                // A search still running belongs to the previous query: its matches must not appear under this one.
                _messageResultsGeneration++;
                ResetBrowsePaging();
                Messages.Clear();
                SelectedMessage = null;
                DeadLetterSearchStatus = "The earlier search was set aside. Search again to run the saved search.";
                ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
            }
        }
    }

    public RelayCommand SaveSearchCommand { get; private set; } = null!;

    public RelayCommand DeleteSavedSearchCommand { get; private set; } = null!;

    private void InitializeSavedSearches()
    {
        SaveSearchCommand = new RelayCommand(SaveCurrentSearch, () => !string.IsNullOrWhiteSpace(DeadLetterSearchQuery));
        DeleteSavedSearchCommand = new RelayCommand(DeleteSelectedSavedSearch, () => SelectedSavedSearch is not null);
        SavedSearches.CollectionChanged += (_, _) =>
        {
            OnPropertyChanged(nameof(HasSavedSearches));
            if (!_loadingSavedSearches)
            {
                // Tells the shell to persist the list.
                OnPropertyChanged(nameof(SavedSearches));
            }
        };
    }

    /// <summary>Loads saved searches from settings without treating them as a change to save.</summary>
    private void LoadSavedSearches(IEnumerable<SavedSearch> searches)
    {
        _loadingSavedSearches = true;
        try
        {
            SavedSearches.Clear();
            foreach (var search in searches)
            {
                SavedSearches.Add(search);
            }
            _savedSearchLastCaptured = [.. SavedSearches];
            _pendingSavedSearchChanges.Clear();
        }
        finally
        {
            _loadingSavedSearches = false;
        }
    }

    private bool _loadingSavedSearches;

    /// <summary>The saved searches as of the last load or capture by this window.</summary>
    private SavedSearch[] _savedSearchLastCaptured = [];
    private long _savedSearchCaptures;

    /// <summary>This window's changes, in order, that no save has confirmed written yet.</summary>
    private readonly List<(long Capture, SavedSearch[] Added, SavedSearch[] Removed)> _pendingSavedSearchChanges = [];

    /// <summary>
    /// Captures this window's saved-search change since the previous capture, and returns an update that applies
    /// every change not yet confirmed written, in order, to the stored list, plus the acknowledgement to call once that
    /// update was written. A failed write leaves its change pending for the next save; saves queued before an earlier
    /// one completes still replay that earlier change first, so add-then-remove or remove-then-reinsert end right.
    /// </summary>
    public SavedSearchSave CaptureSavedSearchChanges()
    {
        var previous = _savedSearchLastCaptured;
        var local = SavedSearches.ToArray();
        _savedSearchLastCaptured = local;
        var capture = ++_savedSearchCaptures;
        _pendingSavedSearchChanges.Add((capture,
            local.Where(search => !previous.Contains(search)).ToArray(),
            previous.Where(search => !local.Contains(search)).ToArray()));
        var changes = _pendingSavedSearchChanges.ToArray();
        return new SavedSearchSave(
            stored =>
            {
                foreach (var change in changes)
                {
                    stored = ApplySavedSearchChange(change.Added, change.Removed, stored);
                }
                return stored;
            },
            // Written: this change and every earlier one are on disk, and later saves no longer replay them.
            () => _pendingSavedSearchChanges.RemoveAll(change => change.Capture <= capture));
    }

    /// <summary>
    /// Applies this window's additions and removals (local against baseline) to the stored list. Everything else in
    /// the stored list, including searches another window added, edited or deleted, is kept as stored.
    /// </summary>
    internal static IReadOnlyList<SavedSearch> MergeSavedSearches(
        IReadOnlyList<SavedSearch> baseline, IReadOnlyList<SavedSearch> local, IReadOnlyList<SavedSearch> stored) =>
        ApplySavedSearchChange(local.Where(search => !baseline.Contains(search)).ToArray(),
            baseline.Where(search => !local.Contains(search)).ToArray(), stored);

    private static IReadOnlyList<SavedSearch> ApplySavedSearchChange(
        IReadOnlyList<SavedSearch> added, IReadOnlyList<SavedSearch> removed, IReadOnlyList<SavedSearch> stored)
    {
        var merged = stored.Where(search => !removed.Contains(search)
            // A search added here replaces a stored one with the same name (names are shown case-insensitively).
            && !added.Any(item => string.Equals(item.Name, search.Name, StringComparison.OrdinalIgnoreCase))).ToList();
        merged.InsertRange(0, added);
        return merged.Take(AppSettings.MaximumSavedSearches).ToArray();
    }

    /// <summary>True while settings are being applied; the shell skips saving then.</summary>
    public bool IsLoadingSavedSearches => _loadingSavedSearches;

    private void SaveCurrentSearch()
    {
        var query = DeadLetterSearchQuery.Trim();
        if (query.Length == 0)
        {
            return;
        }
        try
        {
            // A broken /regular expression/ or $.json.path is refused now, not when the saved search is picked later.
            MessageSearchQuery.Parse(query);
        }
        catch (MessageSearchQueryException exception)
        {
            ErrorText = exception.Message;
            return;
        }

        var environment = SelectedDeadLetterEnvironmentFilter;
        var name = query.Length > 40 ? query[..40] + "…" : query;
        if (environment?.ProfileId is not null)
        {
            name += $" · {environment.Name}";
        }
        if (SearchWindow.Minutes is not null)
        {
            name += $" · {SearchWindow.Label.ToLowerInvariant()}";
        }

        // The display label is shortened and case-insensitive; the actual query can distinguish both.
        var existing = SavedSearches.FirstOrDefault(item =>
            string.Equals(item.Query, query, StringComparison.Ordinal) &&
            item.EnvironmentId == environment?.ProfileId && item.WithinMinutes == SearchWindow.Minutes);
        if (existing is not null)
        {
            SavedSearches.Remove(existing);
        }
        var baseName = name;
        for (var suffix = 2; SavedSearches.Any(item => string.Equals(item.Name, name, StringComparison.OrdinalIgnoreCase)); suffix++)
        {
            name = $"{baseName} ({suffix})";
        }
        var search = new SavedSearch(name, query, environment?.ProfileId, SearchWindow.Minutes);
        if (SavedSearches.Count >= AppSettings.MaximumSavedSearches)
        {
            SavedSearches.RemoveAt(SavedSearches.Count - 1);
        }
        SavedSearches.Insert(0, search);
        _selectedSavedSearch = search;
        OnPropertyChanged(nameof(SelectedSavedSearch));
        DeleteSavedSearchCommand.NotifyCanExecuteChanged();
        StatusText = $"Saved search '{name}'";
    }

    private void DeleteSelectedSavedSearch()
    {
        if (SelectedSavedSearch is not { } search)
        {
            return;
        }

        _selectedSavedSearch = null;
        OnPropertyChanged(nameof(SelectedSavedSearch));
        SavedSearches.Remove(search);
        DeleteSavedSearchCommand.NotifyCanExecuteChanged();
        StatusText = $"Removed saved search '{search.Name}'";
    }

    /// <summary>
    /// Keeps only messages enqueued inside the chosen window; messages without a time are dropped too. The search
    /// already applies the window before its result cap; this also covers a workspace that returned more.
    /// </summary>
    private static IReadOnlyList<MessageItemViewModel> ApplySearchWindow(
        IReadOnlyList<MessageItemViewModel> results, DateTimeOffset? enqueuedSince, out int hidden)
    {
        hidden = 0;
        if (enqueuedSince is not { } since)
        {
            return results;
        }

        var kept = results.Where(result => result.Message.EnqueuedAt is { } enqueuedAt && enqueuedAt >= since).ToArray();
        hidden = results.Count - kept.Length;
        return kept;
    }
}

/// <summary>A captured saved-search change: the update to apply to the stored list, and the call to make once written.</summary>
public sealed record SavedSearchSave(
    Func<IReadOnlyList<SavedSearch>, IReadOnlyList<SavedSearch>> Merge,
    Action Acknowledge);
