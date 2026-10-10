using System.Text.Json.Serialization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Monitoring;

/// <summary>Dead-letter counts of one environment at one moment, from a complete monitor check or scan.</summary>
/// <param name="Sources">Non-empty dead-letter queues by display name ("orders", "orders (transfer)"); the largest only.</param>
/// <param name="TotalQuality">
/// How far the total can be trusted. Always written by this version; a sample without it was recorded before count
/// quality was kept, and is read as <see cref="DeadLetterCountQuality.Unqualified"/>, never as exact.
/// </param>
/// <param name="SourceQualities">
/// Sources whose count is not exact, kept in Sources or not (a sampled queue that showed nothing is listed here, so it
/// is never read back as a proven 0); null when every counted source was exact.
/// </param>
public sealed record DeadLetterHistorySample(
    DateTimeOffset At,
    Guid ProfileId,
    string Environment,
    long Total,
    IReadOnlyDictionary<string, long> Sources,
    DeadLetterCountQuality? TotalQuality = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    IReadOnlyDictionary<string, DeadLetterCountQuality>? SourceQualities = null)
{
    public const int MaximumSources = 25;

    public static DeadLetterHistorySample FromSnapshot(DeadLetterSnapshot snapshot, string environment)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var sources = snapshot.Entities
            .Where(entity => entity.Count > 0)
            .GroupBy(SourceName, StringComparer.Ordinal)
            .Select(group => (Name: group.Key, Count: group.Sum(entity => entity.Count!.Value)))
            .OrderByDescending(source => source.Count)
            .ThenBy(source => source.Name, StringComparer.Ordinal)
            .Take(MaximumSources)
            .ToDictionary(source => source.Name, source => source.Count, StringComparer.Ordinal);
        // Kept sources that are not exact, and every lower bound even when it showed nothing or fell below the top.
        var qualities = snapshot.Entities
            .Where(entity => entity.Count.HasValue && entity.CountQuality != DeadLetterCountQuality.Exact)
            .GroupBy(SourceName, StringComparer.Ordinal)
            .Where(group => sources.ContainsKey(group.Key) || group.Any(entity => entity.CountIsLowerBound))
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => DeadLetterCountQualities.Combine(group.Select(entity => entity.CountQuality)),
                StringComparer.Ordinal);
        return new DeadLetterHistorySample(snapshot.CapturedAt, snapshot.ProfileId, environment, snapshot.TotalCount, sources,
            snapshot.TotalQuality, qualities.Count == 0 ? null : qualities);
    }

    /// <summary>The total's quality; an old sample without one is unqualified.</summary>
    [JsonIgnore]
    public DeadLetterCountQuality Quality => TotalQuality ?? DeadLetterCountQuality.Unqualified;

    [JsonIgnore]
    public bool TotalIsLowerBound => Quality == DeadLetterCountQuality.LowerBound;

    /// <summary>A source's quality: as recorded, else exact (or, in an old sample, unqualified).</summary>
    public DeadLetterCountQuality QualityOf(string source) =>
        SourceQualities?.TryGetValue(source, out var quality) == true ? quality
        : TotalQuality is null ? DeadLetterCountQuality.Unqualified
        : DeadLetterCountQuality.Exact;

    public bool IsLowerBound(string source) => QualityOf(source) == DeadLetterCountQuality.LowerBound;

    /// <summary>Sources recorded as lower bounds, including any that showed nothing.</summary>
    [JsonIgnore]
    public IEnumerable<string> LowerBoundSources =>
        SourceQualities?.Where(pair => pair.Value == DeadLetterCountQuality.LowerBound).Select(pair => pair.Key) ?? [];

    public static string SourceName(DeadLetterEntitySnapshot entity) =>
        entity.SubQueue == ServiceBusSubQueue.TransferDeadLetter
            ? $"{entity.Entity.DisplayName} (transfer)"
            : entity.Entity.DisplayName;
}

public sealed record DeadLetterHistoryPoint(DateTimeOffset At, long Count)
{
    public DeadLetterCountQuality Quality { get; init; }

    /// <summary>The count is only a lower bound.</summary>
    public bool IsLowerBound => Quality == DeadLetterCountQuality.LowerBound;
}

/// <param name="Start">Count at the first sample of the period; null when that sample kept only larger queues.</param>
/// <param name="Now">Count at the last sample; null when that sample kept only larger queues.</param>
public sealed record DeadLetterSourceTrend(string Name, long? Start, long? Now)
{
    public DeadLetterCountQuality StartQuality { get; init; }

    public DeadLetterCountQuality NowQuality { get; init; }

    public bool StartIsLowerBound => StartQuality == DeadLetterCountQuality.LowerBound;

    public bool NowIsLowerBound => NowQuality == DeadLetterCountQuality.LowerBound;

    /// <summary>
    /// Null when either end is unknown (a queue below a truncated sample's top is not known to be empty) or only a
    /// lower bound (a sampled queue's real growth or shrinkage cannot be told from two samples).
    /// </summary>
    public long? Change => Start is { } start && Now is { } now && !StartIsLowerBound && !NowIsLowerBound ? now - start : null;

    /// <summary>Exact between exact counts, estimated when either end is an estimate.</summary>
    public DeadLetterCountQuality ChangeQuality => DeadLetterCountQualities.Combine(StartQuality, NowQuality);
}

public sealed record DeadLetterHistorySummary(
    IReadOnlyList<DeadLetterHistoryPoint> Points,
    long Now,
    long Start,
    DeadLetterHistoryPoint Peak,
    IReadOnlyList<DeadLetterSourceTrend> Sources,
    int SampleCount)
{
    public DeadLetterCountQuality NowQuality { get; init; }

    public DeadLetterCountQuality StartQuality { get; init; }

    public bool NowIsLowerBound => NowQuality == DeadLetterCountQuality.LowerBound;

    public bool StartIsLowerBound => StartQuality == DeadLetterCountQuality.LowerBound;

    /// <summary>Null when either total is only a lower bound; see <see cref="ChangeQuality"/> for an estimate.</summary>
    public long? Change => NowIsLowerBound || StartIsLowerBound ? null : Now - Start;

    public DeadLetterCountQuality ChangeQuality => DeadLetterCountQualities.Combine(StartQuality, NowQuality);
}

public interface IDeadLetterHistoryStore
{
    /// <summary>Keeps the sample unless the same environment was recorded less than a minute ago with the same total.</summary>
    Task AppendAsync(DeadLetterHistorySample sample, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<DeadLetterHistorySample>> ReadAsync(Guid profileId, DateTimeOffset since, CancellationToken cancellationToken = default);
}

public static class DeadLetterHistory
{
    public static readonly TimeSpan Retention = TimeSpan.FromDays(30);

    public static readonly TimeSpan MinimumSpacing = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Reduces samples to at most <paramref name="maximumPoints"/> chart points. Each time bucket keeps its highest
    /// count, so a short spike of dead letters stays visible in a 30-day view.
    /// </summary>
    public static DeadLetterHistorySummary? Summarize(
        IReadOnlyList<DeadLetterHistorySample> samples,
        DateTimeOffset from,
        DateTimeOffset to,
        int maximumPoints = 360,
        int maximumSources = 6)
    {
        ArgumentNullException.ThrowIfNull(samples);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumPoints, 2);
        var inRange = samples.Where(sample => sample.At >= from && sample.At <= to).OrderBy(sample => sample.At).ToArray();
        if (inRange.Length == 0)
        {
            return null;
        }

        var points = inRange.Select(sample => new DeadLetterHistoryPoint(sample.At, sample.Total) { Quality = sample.Quality }).ToArray();
        if (points.Length > maximumPoints)
        {
            var bucket = (to - from).Ticks / maximumPoints + 1;
            // A bucket keeps its highest count, with the weakest quality of its samples.
            points = points
                .GroupBy(point => (point.At - from).Ticks / bucket)
                .Select(group => group.MaxBy(point => point.Count)! with { Quality = DeadLetterCountQualities.Combine(group.Select(point => point.Quality)) })
                .ToArray();
        }

        var first = inRange[0];
        var last = inRange[^1];
        // An uncertain point anywhere in the period may hide a higher real count: the peak is no more certain than any point.
        var peak = points.MaxBy(point => point.Count)! with { Quality = DeadLetterCountQualities.Combine(points.Select(point => point.Quality)) };
        // Sampled queues are listed even when nothing was seen in them: their count is unknown, not zero.
        var sources = last.Sources.Keys.Concat(first.Sources.Keys)
            .Concat(last.LowerBoundSources).Concat(first.LowerBoundSources)
            .Distinct(StringComparer.Ordinal)
            .Select(name => new DeadLetterSourceTrend(name, CountIn(first, name), CountIn(last, name))
                { StartQuality = first.QualityOf(name), NowQuality = last.QualityOf(name) })
            .OrderByDescending(source => source.Now ?? -1)
            .ThenByDescending(source => Math.Abs(source.Change ?? 0))
            .ThenBy(source => source.Name, StringComparer.Ordinal)
            .Take(maximumSources)
            .ToArray();
        return new DeadLetterHistorySummary(points, last.Total, first.Total, peak, sources, inRange.Length)
            { NowQuality = last.Quality, StartQuality = first.Quality };
    }

    /// <summary>
    /// A queue's count in a sample. A sample keeps at most <see cref="DeadLetterHistorySample.MaximumSources"/> queues, the
    /// largest: missing from a sample that kept fewer, the queue was empty; missing from a full one, its count is unknown.
    /// </summary>
    private static long? CountIn(DeadLetterHistorySample sample, string name) =>
        sample.Sources.TryGetValue(name, out var count) ? count
        : sample.Sources.Count < DeadLetterHistorySample.MaximumSources ? 0
        : null;
}
