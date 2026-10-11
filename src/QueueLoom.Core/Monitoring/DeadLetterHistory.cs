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
    IReadOnlyDictionary<string, DeadLetterCountQuality>? SourceQualities = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    IReadOnlyDictionary<string, DeadLetterObservation>? SourceObservations = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    IReadOnlyDictionary<string, string>? SharedAliases = null,
    bool CountsSharedOnce = false)
{
    public const int MaximumSources = 25;

    public static DeadLetterHistorySample FromSnapshot(DeadLetterSnapshot snapshot, string environment)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        // A count taken from a known destination (an SQS dead-letter queue, a Pub/Sub reader subscription) is recorded
        // under that destination's name, whichever and however many sources use it: one source joining or leaving a
        // shared queue changes nothing in its history.
        // Its short name, unless that is ambiguous in this sample (two destinations with the same short name, or a
        // source of that name counted by itself): then the full destination.
        var unmeasured = snapshot.Entities.Where(entity => entity.MeasuredFrom is null)
            .Select(DeadLetterHistorySample.SourceName).ToHashSet(StringComparer.Ordinal);
        var physicalNames = snapshot.Entities.Select(entity => entity.MeasuredFrom).OfType<string>().Distinct(StringComparer.Ordinal)
            .GroupBy(SharedName, StringComparer.Ordinal)
            .SelectMany(group => group.Select(target =>
                (Target: target, Name: group.Count() > 1 || unmeasured.Contains(group.Key) ? target : group.Key)))
            .ToDictionary(pair => pair.Target, pair => pair.Name, StringComparer.Ordinal);
        string SourceName(DeadLetterEntitySnapshot entity) =>
            entity.MeasuredFrom is { } target ? physicalNames[target] : DeadLetterHistorySample.SourceName(entity);
        // Which source used which destination: a source moved to another destination is a target change.
        var aliases = snapshot.Entities.Where(entity => entity.MeasuredFrom is not null)
            .Select(entity => (Alias: DeadLetterHistorySample.SourceName(entity), Key: physicalNames[entity.MeasuredFrom!]))
            .GroupBy(pair => pair.Alias, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.First().Key, StringComparer.Ordinal);
        var sources = snapshot.CountedOnce
            .Where(entity => entity.Count > 0)
            .GroupBy(SourceName, StringComparer.Ordinal)
            .Select(group => (Name: group.Key, Count: checked(group.Sum(entity => entity.Count!.Value))))
            .OrderByDescending(source => source.Count)
            .ThenBy(source => source.Name, StringComparer.Ordinal)
            .Take(MaximumSources)
            .ToDictionary(source => source.Name, source => source.Count, StringComparer.Ordinal);
        // Every source that is not exact, kept in Sources or not: an estimated or sampled zero is never read back as a
        // proven empty queue.
        var qualities = snapshot.CountedOnce
            .Where(entity => entity.CountQuality != DeadLetterCountQuality.Exact)
            .GroupBy(SourceName, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => DeadLetterCountQualities.Combine(group.Select(entity => entity.CountQuality)),
                StringComparer.Ordinal);
        // When and through what a count was taken, where the service says (a Cloud Monitoring point's time, a Pub/Sub
        // reader, an SQS dead-letter queue): counts of different targets are never compared later.
        var observations = snapshot.CountedOnce
            .Where(entity => entity.MeasuredAt is not null || entity.MeasuredFrom is not null)
            .GroupBy(SourceName, StringComparer.Ordinal)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => new DeadLetterObservation(
                group.Select(entity => entity.MeasuredAt).Min(),
                group.Select(entity => entity.MeasuredFrom).Distinct(StringComparer.Ordinal).Count() == 1 ? group.First().MeasuredFrom : string.Join(",",
                    group.Select(entity => entity.MeasuredFrom).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))),
                StringComparer.Ordinal);
        return new DeadLetterHistorySample(snapshot.CapturedAt, snapshot.ProfileId, environment, snapshot.TotalCount, sources,
            snapshot.TotalQuality, qualities.Count == 0 ? null : qualities, observations.Count == 0 ? null : observations,
            aliases.Count == 0 ? null : aliases, CountsSharedOnce: true);
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

    /// <summary>Every source recorded as not exact, an estimated or sampled zero included.</summary>
    [JsonIgnore]
    public IEnumerable<string> QualifiedSources => SourceQualities?.Keys ?? [];

    /// <summary>What a source's count was taken from, when recorded.</summary>
    /// <summary>Every source this sample recorded a count, quality or observation of.</summary>
    [JsonIgnore]
    public IEnumerable<string> RecordedSources =>
        Sources.Keys.Concat(SourceQualities?.Keys ?? []).Concat(SourceObservations?.Keys ?? []).Distinct(StringComparer.Ordinal);

    /// <summary>Every source by its own name: those counted through a destination, and those counted by themselves.</summary>
    [JsonIgnore]
    public IEnumerable<string> CountedSources =>
        (SharedAliases?.Keys ?? []).Concat(RecordedSources.Where(name => TargetOf(name) is null)).Distinct(StringComparer.Ordinal);

    /// <summary>
    /// What a source's dead letters were counted through: the destination its alias names. Aliases are source names,
    /// apart from the recorded destinations, which can have the same name (a dead-letter queue that is a source too).
    /// </summary>
    public string? AliasTargetOf(string source) =>
        SharedAliases?.TryGetValue(source, out var destination) == true && SourceObservations?.TryGetValue(destination, out var observation) == true
            ? observation.MeasuredFrom
            : null;

    public string? TargetOf(string source) => SourceObservations?.TryGetValue(source, out var observation) == true ? observation.MeasuredFrom : null;

    /// <summary>A measured destination's name in history: its short name, whoever uses it.</summary>
    public static string SharedSourceName(string target) => SharedName(target);

    /// <summary>Same counts, qualities, sources and observations: a later sample adds nothing.</summary>
    public bool RecordsTheSameAs(DeadLetterHistorySample other) =>
        Total == other.Total && TotalQuality == other.TotalQuality &&
        Same(SourceQualities, other.SourceQualities) && Same(SourceObservations, other.SourceObservations) &&
        Same(SharedAliases, other.SharedAliases) && CountsSharedOnce == other.CountsSharedOnce;

    private static bool Same<T>(IReadOnlyDictionary<string, T>? first, IReadOnlyDictionary<string, T>? second) =>
        (first ?? new Dictionary<string, T>()).OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .SequenceEqual((second ?? new Dictionary<string, T>()).OrderBy(pair => pair.Key, StringComparer.Ordinal));

    /// <summary>The short name of a shared destination: the last part of its ARN or resource name.</summary>
    public static string SharedName(string target) =>
        target[(target.LastIndexOfAny([':', '/']) + 1)..] is { Length: > 0 } name ? name : target;

    public static string SourceName(DeadLetterEntitySnapshot entity) =>
        entity.SubQueue == ServiceBusSubQueue.TransferDeadLetter
            ? $"{entity.Entity.DisplayName} (transfer)"
            : entity.Entity.DisplayName;
}

/// <summary>When a count was true and what it was taken from, where the service says.</summary>
public sealed record DeadLetterObservation(DateTimeOffset? MeasuredAt, string? MeasuredFrom);

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

    /// <summary>What the first count was taken from (a Pub/Sub reader, an SQS dead-letter queue), when recorded.</summary>
    public string? StartMeasuredFrom { get; init; }

    public string? NowMeasuredFrom { get; init; }

    /// <summary>
    /// Null when either end is unknown (a queue below a truncated sample's top is not known to be empty), a lower bound,
    /// unqualified, or a count of another target: see <see cref="DeadLetterMeasurement.Difference"/>.
    /// </summary>
    public long? Change => Start is { } start && Now is { } now
        ? DeadLetterMeasurement.Difference(new DeadLetterMeasurement(start, StartQuality) { MeasuredFrom = StartMeasuredFrom },
            new DeadLetterMeasurement(now, NowQuality) { MeasuredFrom = NowMeasuredFrom })?.Count
        : null;

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

    /// <summary>A queue that was counted through another target at the end than at the start: the totals are not comparable.</summary>
    public bool TargetsChanged { get; init; }

    /// <summary>
    /// Null when either total is a lower bound or unqualified, or a queue changed target; see <see cref="ChangeQuality"/>
    /// for an estimate.
    /// </summary>
    public long? Change => TargetsChanged ? null :
        DeadLetterMeasurement.Difference(new DeadLetterMeasurement(Start, StartQuality), new DeadLetterMeasurement(Now, NowQuality))?.Count;

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
        // Freshness is judged over every sample given, before the range cuts away the context; and a count measured
        // before the range began is not one of its observations: without the newer one that superseded it (left
        // outside the range), it could otherwise come back as the latest.
        var inRange = Fresh(samples.OrderBy(sample => sample.At))
            .Where(sample => sample.At >= from && sample.At <= to)
            .Where(sample => sample.SourceObservations is not { } observations ||
                             observations.Values.All(observation => observation.MeasuredAt is not { } at || at >= from))
            .ToArray();
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
        var migrating = first.CountsSharedOnce != last.CountsSharedOnce;
        // Sampled queues are listed even when nothing was seen in them: their count is unknown, not zero.
        var sources = last.Sources.Keys.Concat(first.Sources.Keys)
            .Concat(last.LowerBoundSources).Concat(first.LowerBoundSources)
            .Distinct(StringComparer.Ordinal)
            .Select(name => (Name: name, Start: CountAcross(first, name, migrating), Now: CountAcross(last, name, migrating)))
            // A count a sample did not keep is unknown, never exact.
            .Select(source => new DeadLetterSourceTrend(source.Name, source.Start, source.Now)
            {
                StartQuality = source.Start is null ? DeadLetterCountQuality.Unknown : first.QualityOf(source.Name),
                NowQuality = source.Now is null ? DeadLetterCountQuality.Unknown : last.QualityOf(source.Name),
                // A name only an alias in a sample (a source counted through a destination) is of that destination.
                StartMeasuredFrom = first.TargetOf(source.Name) ?? first.AliasTargetOf(source.Name),
                NowMeasuredFrom = last.TargetOf(source.Name) ?? last.AliasTargetOf(source.Name)
            })
            .OrderByDescending(source => source.Now ?? -1)
            .ThenByDescending(source => Math.Abs(source.Change ?? 0))
            .ThenBy(source => source.Name, StringComparer.Ordinal)
            .Take(maximumSources)
            .ToArray();
        // As strict as a source's own change: a known reader against a missing one is another target too.
        // Every source either sample recorded anything of (a count, a quality or an observation), whether or not its
        // count was zero or below the kept largest.
        var targetsChanged = first.RecordedSources.Intersect(last.RecordedSources, StringComparer.Ordinal)
            .Any(name => !string.Equals(first.TargetOf(name), last.TargetOf(name), StringComparison.Ordinal)) ||
            // A source now counted through another destination, compared apart from the destinations' own trends.
            first.CountedSources.Intersect(last.CountedSources, StringComparer.Ordinal)
                .Any(source => !string.Equals(first.AliasTargetOf(source), last.AliasTargetOf(source), StringComparison.Ordinal)) ||
            // Recorded before shared destinations were counted once: its total may count one several times.
            (first.CountsSharedOnce != last.CountsSharedOnce && (first.SharedAliases ?? last.SharedAliases) is not null);
        return new DeadLetterHistorySummary(points, last.Total, first.Total, peak, sources, inRange.Length)
            { NowQuality = last.Quality, StartQuality = first.Quality, TargetsChanged = targetsChanged };
    }

    /// <summary>
    /// Leaves out a sample that holds an observation older than one already recorded for the same source and target:
    /// another process (or a restart) with its own cache can record a delayed point after a newer one.
    /// </summary>
    private static IEnumerable<DeadLetterHistorySample> Fresh(IEnumerable<DeadLetterHistorySample> ordered)
    {
        var latest = new Dictionary<(string Source, string? Target), DateTimeOffset>();
        foreach (var sample in ordered)
        {
            // By what was measured: a destination is the same one whatever name or sharing it was recorded under.
            static (string Source, string? Target) KeyOf(KeyValuePair<string, DeadLetterObservation> pair) =>
                pair.Value.MeasuredFrom is { } target ? (string.Empty, target) : (pair.Key, null);
            var observations = sample.SourceObservations ?? new Dictionary<string, DeadLetterObservation>();
            if (observations.Any(pair => pair.Value.MeasuredAt is { } at && latest.TryGetValue(KeyOf(pair), out var newer) && at < newer))
            {
                continue;
            }
            foreach (var pair in observations)
            {
                if (pair.Value.MeasuredAt is { } at && (!latest.TryGetValue(KeyOf(pair), out var known) || at > known))
                {
                    latest[KeyOf(pair)] = at;
                }
            }
            yield return sample;
        }
    }

    /// <summary>
    /// A source's count in one end of a summary. Across the change to recording shared destinations once (one sample
    /// keyed by source, the other by destination), a name the sample did not record itself (a source now an alias of
    /// its destination, or a destination the old sample knew only through its sources) is unknown: its absence there
    /// is a change of naming, not an empty queue.
    /// </summary>
    private static long? CountAcross(DeadLetterHistorySample sample, string name, bool migrating) =>
        migrating && !sample.RecordedSources.Contains(name, StringComparer.Ordinal) ? null : CountIn(sample, name);

    /// <summary>
    /// A queue's count in a sample. A sample keeps at most <see cref="DeadLetterHistorySample.MaximumSources"/> queues, the
    /// largest: missing from a sample that kept fewer, the queue was empty; missing from a full one, its count is unknown.
    /// </summary>
    private static long? CountIn(DeadLetterHistorySample sample, string name) =>
        sample.Sources.TryGetValue(name, out var count) ? count
        : sample.Sources.Count < DeadLetterHistorySample.MaximumSources ? 0
        : null;
}
