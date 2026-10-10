using System.Globalization;

namespace QueueLoom.Core.Monitoring;

/// <summary>How a dead-letter count is written when it may be only a lower bound.</summary>
public static class DeadLetterCountText
{
    /// <summary>How many messages a dead-letter queue without a reported count (Pub/Sub) is read to count it.</summary>
    public const int SampleLimit = 1_000;

    /// <summary>
    /// "1,234" for an exact count. A lower bound is "1,000+" (more may be there) and "none seen" for zero: an empty
    /// sample does not prove the queue is empty.
    /// </summary>
    public static string Format(long? count, bool isLowerBound, IFormatProvider? culture = null)
    {
        culture ??= CultureInfo.CurrentCulture;
        return count switch
        {
            null => "\u2014",
            0 when isLowerBound => "none seen",
            { } value when isLowerBound => value.ToString("N0", culture) + "+",
            { } value => value.ToString("N0", culture)
        };
    }

    public const string LowerBoundNote =
        "Counted by reading the queue (no exact count is available), so this is only a lower bound and further growth is unknown.";
}
