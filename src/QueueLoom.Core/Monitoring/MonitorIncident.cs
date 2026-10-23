using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Monitoring;

/// <summary>
/// What the dead-letter monitor already alerted for one destination, kept across restarts: without it, the first check
/// after an update restart or a reboot alerts every non-empty dead-letter queue again as newly found.
/// </summary>
/// <param name="Key">The incident key, "{profile id:N}|{monitor key}".</param>
/// <param name="Count">The count last alerted or shown, with <paramref name="CountQuality"/>.</param>
public sealed record MonitorIncident(
    string Key,
    ServiceBusEntityReference Source,
    string SubQueueLabel,
    long Count,
    DeadLetterCountQuality CountQuality,
    DateTimeOffset FirstDetectedAt,
    DateTimeOffset LastDetectedAt)
{
    public DateTimeOffset? LastMeasuredAt { get; init; }
    public string? MeasuredFrom { get; init; }
    public string? SharedName { get; init; }

    /// <summary>What the queue held at the last check, so a replaced message after the restart is still "new".</summary>
    public IReadOnlyCollection<string>? ContentMarkers { get; init; }

    public DeadLetterOffsets? Offsets { get; init; }

    [System.Text.Json.Serialization.JsonIgnore]
    public Guid? ProfileId => Key.Length >= 32 && Guid.TryParseExact(Key[..32], "N", out var id) ? id : null;
}

public interface IMonitorIncidentStore
{
    /// <summary>The incidents last saved; empty when there are none, or the file is missing, damaged or too old.</summary>
    Task<IReadOnlyList<MonitorIncident>> LoadAsync(CancellationToken cancellationToken = default);

    /// <summary>Replaces the saved incidents with these.</summary>
    Task SaveAsync(IReadOnlyCollection<MonitorIncident> incidents, CancellationToken cancellationToken = default);
}
