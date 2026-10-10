using System.Text.Json.Serialization;

namespace QueueLoom.Core.Monitoring;

/// <summary>How far a dead-letter count can be trusted.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<DeadLetterCountQuality>))]
public enum DeadLetterCountQuality
{
    /// <summary>The service's own count (Azure Service Bus, RabbitMQ, Kafka offsets).</summary>
    Exact,

    /// <summary>
    /// The service says the number is approximate or delayed: SQS's ApproximateNumberOfMessages, Pub/Sub's Cloud
    /// Monitoring series. Close to the real count, but neither a floor nor a ceiling.
    /// </summary>
    Estimated,

    /// <summary>Only what reading the queue showed: more may be there, and 0 is not empty.</summary>
    LowerBound,

    /// <summary>
    /// No guarantee either way: recorded before QueueLoom kept count quality (old history), or a mix of an estimate with a
    /// lower bound or an unknown part, which bounds the real number in neither direction.
    /// </summary>
    Unqualified,

    /// <summary>No count at all (the service could not be read).</summary>
    Unknown
}

public static class DeadLetterCountQualities
{
    /// <summary>
    /// The quality of a sum (or of the largest) of several counts, keeping only what the parts justify:
    /// <list type="bullet">
    /// <item>exact parts give an exact total;</item>
    /// <item>exact with lower bounds or unknown parts gives a lower bound (counts are never negative, so the known part
    /// is a floor);</item>
    /// <item>exact with estimates gives an estimate;</item>
    /// <item>an estimate with a lower bound or an unknown part, or any unqualified part, gives no guarantee;</item>
    /// <item>only unknown parts give an unknown total.</item>
    /// </list>
    /// </summary>
    public static DeadLetterCountQuality Combine(IEnumerable<DeadLetterCountQuality> qualities)
    {
        bool any = false, known = false, estimated = false, floor = false, unqualified = false;
        foreach (var quality in qualities)
        {
            any = true;
            switch (quality)
            {
                case DeadLetterCountQuality.Unknown: floor = true; break;
                case DeadLetterCountQuality.LowerBound: known = true; floor = true; break;
                case DeadLetterCountQuality.Estimated: known = true; estimated = true; break;
                case DeadLetterCountQuality.Unqualified: known = true; unqualified = true; break;
                default: known = true; break;
            }
        }
        return !any ? DeadLetterCountQuality.Exact
            : !known ? DeadLetterCountQuality.Unknown
            : unqualified || (estimated && floor) ? DeadLetterCountQuality.Unqualified
            : floor ? DeadLetterCountQuality.LowerBound
            : estimated ? DeadLetterCountQuality.Estimated
            : DeadLetterCountQuality.Exact;
    }

    public static DeadLetterCountQuality Combine(DeadLetterCountQuality first, DeadLetterCountQuality second) =>
        Combine([first, second]);

    /// <summary>The quality of an entity's reported dead-letter count, as the topology describes it.</summary>
    public static DeadLetterCountQuality OfReported(QueueLoom.Core.ServiceBus.ServiceBusEntityRuntime runtime) =>
        runtime.CountsUnavailable || runtime.DeadLetterCountError is not null ? DeadLetterCountQuality.Unknown
        : runtime.CountsAreEstimates ? DeadLetterCountQuality.Estimated
        : DeadLetterCountQuality.Exact;

    /// <summary>Whether the count proves the queue empty when it is 0: only an exact 0 does.</summary>
    public static bool ProvesEmpty(DeadLetterCountQuality quality) => quality == DeadLetterCountQuality.Exact;

    /// <summary>"exact", "estimated", "lowerBound", "unqualified" or "unknown", as MCP clients see it.</summary>
    public static string Name(DeadLetterCountQuality quality) => quality switch
    {
        DeadLetterCountQuality.Exact => "exact",
        DeadLetterCountQuality.Estimated => "estimated",
        DeadLetterCountQuality.LowerBound => "lowerBound",
        DeadLetterCountQuality.Unqualified => "unqualified",
        _ => "unknown"
    };
}
