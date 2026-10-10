using System.Globalization;

namespace QueueLoom.Core.Monitoring;

/// <summary>How a dead-letter count is written, according to how far it can be trusted.</summary>
public static class DeadLetterCountText
{
    /// <summary>How many messages a dead-letter queue without a reported count (Pub/Sub) is read to count it.</summary>
    public const int SampleLimit = 1_000;

    /// <summary>
    /// "1,234" for an exact count, "≈1,234" for an estimate, "~1,234" with no guarantee (old history, or a mix of an
    /// estimate and a lower bound), "unknown" without a count. A lower bound is "1,000+" (more may be there) and
    /// "none seen" for zero: an empty sample does not prove the queue is empty.
    /// </summary>
    public static string Format(long? count, DeadLetterCountQuality quality, IFormatProvider? culture = null)
    {
        culture ??= CultureInfo.CurrentCulture;
        return count switch
        {
            null => "—",
            _ when quality == DeadLetterCountQuality.Unknown => "unknown",
            0 when quality == DeadLetterCountQuality.LowerBound => "none seen",
            { } value when quality == DeadLetterCountQuality.LowerBound => value.ToString("N0", culture) + "+",
            { } value when quality == DeadLetterCountQuality.Estimated => "≈" + value.ToString("N0", culture),
            { } value when quality == DeadLetterCountQuality.Unqualified => "~" + value.ToString("N0", culture),
            { } value => value.ToString("N0", culture)
        };
    }

    public static string Format(long? count, bool isLowerBound, IFormatProvider? culture = null) =>
        Format(count, isLowerBound ? DeadLetterCountQuality.LowerBound : DeadLetterCountQuality.Exact, culture);

    /// <summary>A signed change: "+5", "−3", "≈+5", "~+5", "+20+" (at least 20).</summary>
    public static string FormatChange(long change, DeadLetterCountQuality quality, IFormatProvider? culture = null)
    {
        culture ??= CultureInfo.CurrentCulture;
        var magnitude = Math.Abs(change).ToString("N0", culture);
        var signed = change > 0 ? "+" + magnitude : change < 0 ? "−" + magnitude : "0";
        return quality switch
        {
            DeadLetterCountQuality.LowerBound => signed + "+",
            DeadLetterCountQuality.Estimated => "≈" + signed,
            DeadLetterCountQuality.Unqualified => "~" + signed,
            _ => signed
        };
    }

    /// <summary>Why a count is not exact, or null for an exact one.</summary>
    public static string? Note(DeadLetterCountQuality quality) => quality switch
    {
        DeadLetterCountQuality.LowerBound => LowerBoundNote,
        DeadLetterCountQuality.Estimated => EstimatedNote,
        DeadLetterCountQuality.Unqualified =>
            "No guarantee either way: recorded before QueueLoom tracked count quality, or a mix of an approximate and a sampled count.",
        DeadLetterCountQuality.Unknown => "The count could not be read.",
        _ => null
    };

    public const string LowerBoundNote =
        "Counted by reading the queue (no exact count is available), so this is only a lower bound and further growth is unknown.";

    public const string EstimatedNote =
        "The service reports this count as approximate or delayed (SQS, Pub/Sub Cloud Monitoring).";
}
