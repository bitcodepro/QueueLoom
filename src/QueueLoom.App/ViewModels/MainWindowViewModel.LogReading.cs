using System.Globalization;
using System.Text.RegularExpressions;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>A way to start reading a Kafka topic, as offered above the message list.</summary>
public sealed record LogReadModeOption(BrowseStartKind Kind, string Label)
{
    public override string ToString() => Label;
}

/// <summary>Kafka keeps its messages, so reading can start at the oldest, the newest, a time or an offset.</summary>
public sealed partial class MainWindowViewModel
{
    private LogReadModeOption _selectedLogReadMode = LogReadModes[0];
    private string _logReadValue = string.Empty;
    private BrowseStart _browseStart = BrowseStart.Oldest;
    private Dictionary<int, long>? _logPositions;

    public static IReadOnlyList<LogReadModeOption> LogReadModes { get; } =
    [
        new(BrowseStartKind.Oldest, "Oldest first"),
        new(BrowseStartKind.Newest, "Newest first"),
        new(BrowseStartKind.FromTime, "From a time"),
        new(BrowseStartKind.FromOffset, "From an offset")
    ];

    public AsyncRelayCommand ReadLogCommand { get; private set; } = null!;

    public LogReadModeOption SelectedLogReadMode
    {
        get => _selectedLogReadMode;
        set
        {
            if (SetProperty(ref _selectedLogReadMode, value ?? LogReadModes[0]))
            {
                OnPropertyChanged(nameof(HasLogReadValue));
                OnPropertyChanged(nameof(LogReadPlaceholder));
            }
        }
    }

    /// <summary>A time ("2026-09-30 14:00", or "30m" / "2h" / "1d" ago) or an offset ("1500", or "2:1500" for partition 2).</summary>
    public string LogReadValue
    {
        get => _logReadValue;
        set => SetProperty(ref _logReadValue, value);
    }

    public bool HasLogReadValue => SelectedLogReadMode.Kind is BrowseStartKind.FromTime or BrowseStartKind.FromOffset;

    public string LogReadPlaceholder => SelectedLogReadMode.Kind == BrowseStartKind.FromTime
        ? "2026-09-30 14:00, or 30m, 2h, 1d ago"
        : "1500, or 2:1500 for partition 2";

    /// <summary>The bar shows while a Kafka topic is listed.</summary>
    public bool ShowLogReadBar => _browseSource is not null && _browseProfile?.Provider == MessagingProvider.Kafka;

    private void InitializeLogReading()
    {
        ReadLogCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Reading topic", ReadLogAgainAsync, token),
            () => !IsBusy && ShowLogReadBar);
    }

    private async Task ReadLogAgainAsync(CancellationToken cancellationToken)
    {
        var profile = _browseProfile ?? throw new InvalidOperationException("Open a topic first.");
        var source = _browseSource ?? throw new InvalidOperationException("Open a topic first.");
        await BrowseAsync(profile, source, _browseSubQueue, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>Called when a new browse begins: the mode and value in the bar apply from its first page.</summary>
    private void BeginLogRead(ProfileItemViewModel profile)
    {
        _logPositions = null;
        _browseStart = profile.Provider == MessagingProvider.Kafka
            ? ParseLogStart(SelectedLogReadMode.Kind, LogReadValue, DateTimeOffset.Now)
            : BrowseStart.Oldest;
    }

    private BrowseStart NextLogStart() => _browseStart with { Positions = _logPositions };

    /// <summary>Moves each partition's position past the page just shown (or before it, when reading newest first).</summary>
    private void AdvanceLogPositions(IEnumerable<BrowsedMessage> page)
    {
        var newest = _browseStart.Kind == BrowseStartKind.Newest;
        foreach (var message in page)
        {
            if (message.Position is not { } position)
            {
                continue;
            }
            _logPositions ??= [];
            var next = newest ? position.Offset : position.Offset + 1;
            _logPositions[position.Partition] = _logPositions.TryGetValue(position.Partition, out var current)
                ? newest ? Math.Min(current, next) : Math.Max(current, next)
                : next;
        }
    }

    public static BrowseStart ParseLogStart(BrowseStartKind kind, string? value, DateTimeOffset now)
    {
        var text = value?.Trim() ?? string.Empty;
        switch (kind)
        {
            case BrowseStartKind.FromTime:
                var relative = Regex.Match(text, @"^(\d+)\s*(m|min|h|d)$", RegexOptions.IgnoreCase);
                if (relative.Success)
                {
                    // A number that does not fit in an int or a TimeSpan is bad input, not an overflow crash.
                    if (!int.TryParse(relative.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var amount))
                    {
                        throw LogTimeError();
                    }
                    TimeSpan span;
                    try
                    {
                        span = relative.Groups[2].Value.ToLowerInvariant() switch
                        {
                            "h" => TimeSpan.FromHours(amount),
                            "d" => TimeSpan.FromDays(amount),
                            _ => TimeSpan.FromMinutes(amount)
                        };
                    }
                    catch (Exception exception) when (exception is OverflowException or ArgumentOutOfRangeException)
                    {
                        throw LogTimeError();
                    }
                    try
                    {
                        return new BrowseStart(kind, Time: now - span);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        throw LogTimeError();
                    }
                }
                if (DateTimeOffset.TryParse(text, CultureInfo.CurrentCulture, DateTimeStyles.AssumeLocal, out var time) ||
                    DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out time))
                {
                    return new BrowseStart(kind, Time: time);
                }
                throw LogTimeError();
            case BrowseStartKind.FromOffset:
                var parts = text.Split(':', StringSplitOptions.TrimEntries);
                if (parts.Length == 1 && long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var offset))
                {
                    return new BrowseStart(kind, Offset: offset);
                }
                if (parts.Length == 2 &&
                    int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var partition) &&
                    long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out offset))
                {
                    return new BrowseStart(kind, Offset: offset, Partition: partition);
                }
                throw new InvalidOperationException("Enter an offset such as 1500, or partition:offset such as 2:1500.");
            default:
                return new BrowseStart(kind);
        }

        static InvalidOperationException LogTimeError() =>
            new("Enter a time such as 2026-09-30 14:00, or how long ago: 30m, 2h or 1d.");
    }
}
