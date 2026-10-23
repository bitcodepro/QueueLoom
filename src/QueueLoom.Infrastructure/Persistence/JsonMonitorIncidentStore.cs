using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// The monitor's open incidents in one small JSON file of the data folder, replaced whole on every save. Only what
/// de-duplicates alerts is kept; a file saved longer ago than <see cref="LocalHistoryRetention.Period"/> is not used (an
/// incident still there after days away is worth one alert again), and a damaged one is read as no incidents.
/// </summary>
public sealed class JsonMonitorIncidentStore(string path, TimeProvider? clock = null) : IMonitorIncidentStore
{
    /// <summary>More are not kept: an alert repeated after a restart is better than an unbounded file.</summary>
    public const int MaximumIncidents = 5_000;

    /// <summary>A larger file is not read.</summary>
    public const int MaximumFileBytes = 16 * 1024 * 1024;

    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    public JsonMonitorIncidentStore(QueueLoomPaths paths) : this(Path.Combine(paths.RootDirectory, "monitor-incidents.json"))
    {
    }

    public async Task<IReadOnlyList<MonitorIncident>> LoadAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            if (stream.Length > MaximumFileBytes)
            {
                return [];
            }
            var file = await JsonSerializer.DeserializeAsync<IncidentFile>(stream, Options, cancellationToken).ConfigureAwait(false);
            if (file?.Incidents is null || _clock.GetUtcNow() - file.SavedAt > LocalHistoryRetention.Period)
            {
                return [];
            }
            return file.Incidents
                .Where(incident => incident is { Key: not null, Source: not null, SubQueueLabel: not null } &&
                                   incident.ProfileId is not null && incident.Count > 0 &&
                                   Enum.IsDefined(incident.CountQuality))
                .DistinctBy(incident => incident.Key, StringComparer.Ordinal)
                .Take(MaximumIncidents)
                .ToArray();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException
                                              or InvalidOperationException or NotSupportedException or ArgumentException)
        {
            return [];
        }
    }

    public async Task SaveAsync(IReadOnlyCollection<MonitorIncident> incidents, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(incidents);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(
            new IncidentFile(_clock.GetUtcNow(), incidents.Take(MaximumIncidents).ToArray()), Options);
        // Every window of this data folder shares the lock: two saving at once replace the file one after the other.
        await using var ownership = await CrossProcessFileLock.AcquireAsync(path + ".lock", cancellationToken).ConfigureAwait(false);
        await AtomicFile.WriteBytesAsync(path, bytes, cancellationToken).ConfigureAwait(false);
    }

    private sealed record IncidentFile(DateTimeOffset SavedAt, MonitorIncident[]? Incidents);
}
