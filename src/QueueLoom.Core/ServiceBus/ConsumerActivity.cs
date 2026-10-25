using System.Globalization;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// How far one Kafka consumer group is behind: messages written but not yet committed by the group. Null when the end
/// of a partition it reads could not be read, so the lag is unknown rather than understated.
/// </summary>
public sealed record ConsumerGroupLag(string Group, long? Lag, string? State = null);

/// <summary>
/// Who reads a queue: the number of connected consumers (RabbitMQ) or the lag of each consumer group (Kafka).
/// </summary>
public sealed record ConsumerActivity(int? Consumers, IReadOnlyList<ConsumerGroupLag> Groups)
{
    public static ConsumerActivity Connected(int consumers) => new(consumers, []);

    /// <summary>The lag of the group furthest behind; null when no group reads the topic or no group's lag is known.</summary>
    public long? MaximumLag => Groups.Max(group => group.Lag);

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
                var furthest = Groups.Where(group => group.Lag is not null).MaxBy(group => group.Lag);
                var lag = furthest is null ? "unknown" : $"{Format(furthest.Lag)} ({furthest.Group})";
                parts.Add(Groups.Count == 1 ? $"lag {lag}" : $"{Groups.Count.ToString(CultureInfo.CurrentCulture)} groups, lag up to {lag}");
                if (furthest is not null && Groups.Any(group => group.Lag is null))
                {
                    parts.Add("some lag unknown");
                }
            }
            return string.Join(" · ", parts);
        }
    }

    /// <summary>Every group on its own line, for a tooltip.</summary>
    public string Details => Groups.Count == 0
        ? Summary
        : string.Join(Environment.NewLine, Groups.OrderByDescending(group => group.Lag ?? long.MaxValue).Select(group =>
            $"{group.Group}: lag {Format(group.Lag)}" +
            (string.IsNullOrEmpty(group.State) ? string.Empty : $" ({group.State})")));

    private static string Format(long? lag) => lag is { } value
        ? value.ToString("N0", CultureInfo.CurrentCulture)
        : "unknown (a partition end could not be read)";
}
