using System.Globalization;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.App.ViewModels;

/// <summary>A row under the history chart: a dead-letter queue and how its count changed over the period.</summary>
public sealed record DeadLetterTrendItemViewModel(string Name, long Now, long Change)
{
    public string NowText => Now.ToString("N0", CultureInfo.CurrentCulture);

    public string ChangeText => Change switch
    {
        > 0 => $"+{Change:N0}",
        < 0 => $"−{-Change:N0}",
        _ => "no change"
    };

    public bool IsRising => Change > 0;

    public bool IsFalling => Change < 0;

    public bool IsUnchanged => Change == 0;
}

/// <summary>Dead-letter history: every complete monitor check and scan is kept for 30 days and drawn on Monitors.</summary>
public sealed partial class MainWindowViewModel
{
    private static readonly (string Label, TimeSpan Span)[] HistorySpans =
    [
        ("Last 6 hours", TimeSpan.FromHours(6)),
        ("Last 24 hours", TimeSpan.FromHours(24)),
        ("Last 7 days", TimeSpan.FromDays(7)),
        ("Last 30 days", TimeSpan.FromDays(30))
    ];

    private IDeadLetterHistoryStore? _history;
    private ProfileItemViewModel? _historyProfile;
    private string _historyRange = HistorySpans[1].Label;
    private DeadLetterHistorySummary? _historySummary;
    private DateTimeOffset _historyFrom;
    private DateTimeOffset _historyTo;

    public IReadOnlyList<string> HistoryRanges { get; } = HistorySpans.Select(span => span.Label).ToArray();

    public string HistoryRange
    {
        get => _historyRange;
        set
        {
            if (SetProperty(ref _historyRange, value ?? HistorySpans[1].Label))
            {
                RefreshHistory();
            }
        }
    }

    /// <summary>The environment whose history is drawn; the connected one until the user picks another.</summary>
    public ProfileItemViewModel? HistoryProfile
    {
        get => _historyProfile;
        set
        {
            if (SetProperty(ref _historyProfile, value))
            {
                RefreshHistory();
            }
        }
    }

    public bool IsHistoryAvailable => _history is not null;

    public bool HasHistory => _historySummary is not null;

    public IReadOnlyList<DeadLetterHistoryPoint> HistoryPoints => _historySummary?.Points ?? [];

    public DateTimeOffset HistoryFrom => _historyFrom;

    public DateTimeOffset HistoryTo => _historyTo;

    public string HistoryNowText => _historySummary is { } summary ? summary.Now.ToString("N0", CultureInfo.CurrentCulture) : "—";

    public string HistoryPeakText => _historySummary is { } summary
        ? summary.Peak.Count.ToString("N0", CultureInfo.CurrentCulture)
        : "—";

    public string HistoryPeakTime => _historySummary is { } summary
        ? summary.Peak.At.ToLocalTime().ToString("ddd d MMM, HH:mm", CultureInfo.CurrentCulture)
        : string.Empty;

    public string HistoryChangeText => _historySummary?.Change switch
    {
        null => "—",
        > 0 and var change => $"+{change:N0}",
        < 0 and var change => $"−{-change:N0}",
        _ => "0"
    };

    public bool IsHistoryRising => _historySummary?.Change > 0;

    public bool IsHistoryFalling => _historySummary?.Change < 0;

    public string HistoryFootnote => _historySummary is { } summary
        ? $"{summary.SampleCount:N0} check{(summary.SampleCount == 1 ? string.Empty : "s")} in this period. " +
          "Counts are recorded by the monitor and by dead-letter scans, at most once a minute, and kept for 30 days."
        : string.Empty;

    public string HistoryEmptyText => HistoryProfile is null
        ? "Choose an environment to see how its dead-letter count changed."
        : $"No checks of {HistoryProfile.Name} in this period. Start the monitor or scan for dead letters to record counts.";

    public IReadOnlyList<DeadLetterTrendItemViewModel> HistorySources =>
        _historySummary?.Sources.Select(source => new DeadLetterTrendItemViewModel(source.Name, source.Now, source.Change)).ToArray()
        ?? [];

    public bool HasHistorySources => _historySummary?.Sources.Count > 0;

    private void InitializeHistory(IDeadLetterHistoryStore? history) => _history = history;

    /// <summary>Records a complete snapshot of a whole environment; partial or single-queue checks would draw false dips.</summary>
    private async Task RecordDeadLetterHistoryAsync(ProfileItemViewModel profile, DeadLetterSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        if (_history is null || snapshot.HasFailures)
        {
            return;
        }

        try
        {
            await _history.AppendAsync(DeadLetterHistorySample.FromSnapshot(snapshot, profile.Name), cancellationToken)
                .ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(exception, "The dead-letter history could not be saved");
            return;
        }

        if (CurrentPage == Models.NavigationPage.Monitors && (HistoryProfile is null || HistoryProfile.Id == profile.Id))
        {
            RefreshHistory();
        }
    }

    private int _historyGeneration;

    /// <summary>The latest history read the window started; tests await it.</summary>
    internal Task HistoryRefresh { get; private set; } = Task.CompletedTask;

    private void RefreshHistory() => HistoryRefresh = RefreshHistoryAsync();

    /// <summary>
    /// Reads the history off the UI thread: the shared file's cross-process ownership can be held by another window or
    /// the MCP server. Only the latest read is shown; one that finishes after a newer request is dropped.
    /// </summary>
    private async Task RefreshHistoryAsync()
    {
        if (_historyProfile is not null && !Profiles.Contains(_historyProfile))
        {
            // The environment was edited or deleted; follow the current item with the same ID, if any.
            _historyProfile = Profiles.FirstOrDefault(profile => profile.Id == _historyProfile.Id);
            OnPropertyChanged(nameof(HistoryProfile));
        }
        if (_historyProfile is null)
        {
            _historyProfile = Profiles.FirstOrDefault(profile => profile.Id == _workspace.ConnectedProfileId) ?? Profiles.FirstOrDefault();
            OnPropertyChanged(nameof(HistoryProfile));
        }
        var generation = ++_historyGeneration;
        var span = HistorySpans.FirstOrDefault(item => item.Label == HistoryRange).Span;
        var to = DateTimeOffset.UtcNow;
        var from = to - (span == default ? TimeSpan.FromHours(24) : span);
        DeadLetterHistorySummary? summary = null;
        if (_history is not null && HistoryProfile is { } profile)
        {
            try
            {
                summary = DeadLetterHistory.Summarize(await _history.ReadAsync(profile.Id, from).ConfigureAwait(true), from, to);
            }
            // Nothing awaits this read: whatever it fails with is logged here, not lost unobserved.
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                _logger.LogWarning(exception, "The dead-letter history could not be read");
            }
        }
        if (generation != _historyGeneration || _isDisposed)
        {
            return;
        }

        _historyFrom = from;
        _historyTo = to;
        _historySummary = summary;

        OnPropertyChanged(nameof(HasHistory));
        OnPropertyChanged(nameof(HistoryPoints));
        OnPropertyChanged(nameof(HistoryFrom));
        OnPropertyChanged(nameof(HistoryTo));
        OnPropertyChanged(nameof(HistoryNowText));
        OnPropertyChanged(nameof(HistoryPeakText));
        OnPropertyChanged(nameof(HistoryPeakTime));
        OnPropertyChanged(nameof(HistoryChangeText));
        OnPropertyChanged(nameof(IsHistoryRising));
        OnPropertyChanged(nameof(IsHistoryFalling));
        OnPropertyChanged(nameof(HistoryFootnote));
        OnPropertyChanged(nameof(HistoryEmptyText));
        OnPropertyChanged(nameof(HistorySources));
        OnPropertyChanged(nameof(HasHistorySources));
    }
}
