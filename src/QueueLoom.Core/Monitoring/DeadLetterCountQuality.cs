using System.Text.Json.Serialization;

namespace QueueLoom.Core.Monitoring;

/// <summary>
/// How far a dead-letter count can be trusted. An unknown count has no value at all (null, with an error); these
/// describe a count that has one.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<DeadLetterCountQuality>))]
public enum DeadLetterCountQuality
{
    /// <summary>The service's own count (Azure Service Bus, RabbitMQ, Kafka offsets).</summary>
    Exact,

    /// <summary>
    /// The service says the number is approximate or delayed: SQS's ApproximateNumberOfMessages, Pub/Sub's Cloud
    /// Monitoring series. Close to the real count, but a change between two of them is only approximate.
    /// </summary>
    Estimated,

    /// <summary>Only what reading the queue showed (Pub/Sub without a count): more may be there, and 0 is not empty.</summary>
    LowerBound,

    /// <summary>Recorded before QueueLoom kept count quality (old history): not claimed as exact.</summary>
    Unqualified
}

public static class DeadLetterCountQualities
{
    /// <summary>The weakest of several counts: a total is a lower bound if any part is, else estimated, and so on.</summary>
    public static DeadLetterCountQuality Combine(IEnumerable<DeadLetterCountQuality> qualities)
    {
        var result = DeadLetterCountQuality.Exact;
        foreach (var quality in qualities)
        {
            if (Weakness(quality) > Weakness(result)) result = quality;
        }
        return result;
    }

    public static DeadLetterCountQuality Combine(DeadLetterCountQuality first, DeadLetterCountQuality second) =>
        Weakness(second) > Weakness(first) ? second : first;

    private static int Weakness(DeadLetterCountQuality quality) => quality switch
    {
        DeadLetterCountQuality.Exact => 0,
        DeadLetterCountQuality.Unqualified => 1,
        DeadLetterCountQuality.Estimated => 2,
        _ => 3
    };

    /// <summary>"exact", "estimated", "lowerBound" or "unqualified", as MCP clients see it.</summary>
    public static string Name(DeadLetterCountQuality quality) => quality switch
    {
        DeadLetterCountQuality.Exact => "exact",
        DeadLetterCountQuality.Estimated => "estimated",
        DeadLetterCountQuality.LowerBound => "lowerBound",
        _ => "unqualified"
    };
}
