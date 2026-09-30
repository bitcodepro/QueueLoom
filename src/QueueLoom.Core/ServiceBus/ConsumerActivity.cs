using System.Globalization;

namespace QueueLoom.Core.ServiceBus;

/// <summary>How far one Kafka consumer group is behind: messages written but not yet committed by the group.</summary>
public sealed record ConsumerGroupLag(string Group, long Lag, string? State = null);

/// <summary>
/// Who reads a queue: the number of connected consumers (RabbitMQ) or the lag of each consumer group (Kafka).
/// </summary>
public sealed record ConsumerActivity(int? Consumers, IReadOnlyList<ConsumerGroupLag> Groups)
{
    public static ConsumerActivity Connected(int consumers) => new(consumers, []);

    /// <summary>The lag of the group furthest behind; null when no group reads the topic.</summary>
    public long? MaximumLag => Groups.Count == 0 ? null : Groups.Max(group => group.Lag);

    /// <summary>"2 consumers", "no consumers", "lag 1,204 (billing)" or "3 groups, lag up to 1,204 (billing)".</summary>
    public string Summary
    {
        get
        {
            var parts = new List<string>();
            if (Consumers is { } consumers)
            {
                parts.Add(consumers switch
                {
                    0 => "no consumers",
                    1 => "1 consumer",
                    _ => $"{consumers.ToString("N0", CultureInfo.CurrentCulture)} consumers"
                });
            }
            if (Groups.Count > 0)
            {
                var furthest = Groups.MaxBy(group => group.Lag)!;
                var lag = $"{furthest.Lag.ToString("N0", CultureInfo.CurrentCulture)} ({furthest.Group})";
                parts.Add(Groups.Count == 1 ? $"lag {lag}" : $"{Groups.Count.ToString(CultureInfo.CurrentCulture)} groups, lag up to {lag}");
            }
            return string.Join(" · ", parts);
        }
    }

    /// <summary>Every group on its own line, for a tooltip.</summary>
    public string Details => Groups.Count == 0
        ? Summary
        : string.Join(Environment.NewLine, Groups.OrderByDescending(group => group.Lag).Select(group =>
            $"{group.Group}: lag {group.Lag.ToString("N0", CultureInfo.CurrentCulture)}" +
            (string.IsNullOrEmpty(group.State) ? string.Empty : $" ({group.State})")));
}
