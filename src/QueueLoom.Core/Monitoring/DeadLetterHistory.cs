using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Monitoring;

/// <summary>Dead-letter counts of one environment at one moment, from a complete monitor check or scan.</summary>
/// <param name="Sources">Non-empty dead-letter queues by display name ("orders", "orders (transfer)"); the largest only.</param>
/// <param name="TotalIsLowerBound">The total includes a count that is only a lower bound (a sampled Pub/Sub queue).</param>
/// <param name="LowerBoundSources">Sources whose count is only a lower bound, kept in Sources or not; null when there are none.</param>
public sealed record DeadLetterHistorySample(
    DateTimeOffset At,
    Guid ProfileId,
    string Environment,
    long Total,
    IReadOnlyDictionary<string, long> Sources,
    // Omitted while unset, so samples without lower bounds are written exactly as before.
    [property: System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault)]
    bool TotalIsLowerBound = false,
    [property: System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    IReadOnlyList<string>? LowerBoundSources = null)
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
        var lowerBound = snapshot.Entities
            .Where(entity => entity.CountIsLowerBound && entity.Count.HasValue)
            .Select(SourceName)
            // Kept or not: a sampled queue that showed nothing (or fell below the top) is not known to be empty.
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        return new DeadLetterHistorySample(snapshot.CapturedAt, snapshot.ProfileId, environment, snapshot.TotalCount, sources,
            snapshot.TotalIsLowerBound, lowerBound.Length == 0 ? null : lowerBound);
    }

    public bool IsLowerBound(string source) => LowerBoundSources?.Contains(source, StringComparer.Ordinal) == true;

    public static string SourceName(DeadLetterEntitySnapshot entity) =>
        entity.SubQueue == ServiceBusSubQueue.TransferDeadLetter
            ? $"{entity.Entity.DisplayName} (transfer)"
            : entity.Entity.DisplayName;
}

public sealed record DeadLetterHistoryPoint(DateTimeOffset At, long Count)
{
    /// <summary>The count is only a lower bound.</summary>
    public bool IsLowerBound { get; init; }
}

/// <param name="Start">Count at the first sample of the period; null when that sample kept only larger queues.</param>
/// <param name="Now">Count at the last sample; null when that sample kept only larger queues.</param>
public sealed record DeadLetterSourceTrend(string Name, long? Start, long? Now)
{
    public bool StartIsLowerBound { get; init; }

    public bool NowIsLowerBound { get; init; }

    /// <summary>
    /// Null when either end is unknown (a queue below a truncated sample's top is not known to be empty) or only a
    /// lower bound (a sampled queue's real growth or shrinkage cannot be told from two samples).
    /// </summary>
    public long? Change => Start is { } start && Now is { } now && !StartIsLowerBound && !NowIsLowerBound ? now - start : null;
}

public sealed record DeadLetterHistorySummary(
    IReadOnlyList<DeadLetterHistoryPoint> Points,
    long Now,
    long Start,
    DeadLetterHistoryPoint Peak,
    IReadOnlyList<DeadLetterSourceTrend> Sources,
    int SampleCount)
{
    public bool NowIsLowerBound { get; init; }

    public bool StartIsLowerBound { get; init; }

    /// <summary>Null when either total is only a lower bound.</summary>
    public long? Change => NowIsLowerBound || StartIsLowerBound ? null : Now - Start;
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

        var points = inRange.Select(sample => new DeadLetterHistoryPoint(sample.At, sample.Total) { IsLowerBound = sample.TotalIsLowerBound }).ToArray();
        if (points.Length > maximumPoints)
        {
            var bucket = (to - from).Ticks / maximumPoints + 1;
            // A bucket keeps its highest count, and stays a lower bound when any of its samples was one.
            points = points
                .GroupBy(point => (point.At - from).Ticks / bucket)
                .Select(group => group.MaxBy(point => point.Count)! with { IsLowerBound = group.Any(point => point.IsLowerBound) })
                .ToArray();
        }

        var first = inRange[0];
        var last = inRange[^1];
        // An uncertain point anywhere in the period may hide a higher real count, so the peak is then only a lower bound.
        var peak = points.MaxBy(point => point.Count)! with { IsLowerBound = points.Any(point => point.IsLowerBound) };
        // Sampled queues are listed even when nothing was seen in them: their count is unknown, not zero.
        var sources = last.Sources.Keys.Concat(first.Sources.Keys)
            .Concat(last.LowerBoundSources ?? []).Concat(first.LowerBoundSources ?? [])
            .Distinct(StringComparer.Ordinal)
            .Select(name => new DeadLetterSourceTrend(name, CountIn(first, name), CountIn(last, name))
                { StartIsLowerBound = first.IsLowerBound(name), NowIsLowerBound = last.IsLowerBound(name) })
            .OrderByDescending(source => source.Now ?? -1)
            .ThenByDescending(source => Math.Abs(source.Change ?? 0))
            .ThenBy(source => source.Name, StringComparer.Ordinal)
            .Take(maximumSources)
            .ToArray();
        return new DeadLetterHistorySummary(points, last.Total, first.Total, peak, sources, inRange.Length)
            { NowIsLowerBound = last.TotalIsLowerBound, StartIsLowerBound = first.TotalIsLowerBound };
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
