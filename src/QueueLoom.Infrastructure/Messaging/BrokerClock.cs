namespace QueueLoom.Infrastructure.Messaging;

/// <summary>
/// Broker timestamps and durations that do not fit in <see cref="DateTimeOffset"/> or <see cref="TimeSpan"/>.
/// Callers already treat an unreadable value as missing; a number outside the calendar or a TimeSpan must do the same
/// instead of throwing and aborting the read.
/// </summary>
internal static class BrokerClock
{
    /// <summary>Null when <paramref name="seconds"/> is outside year 1 through 9999, including a millisecond count stored in a seconds field.</summary>
    public static DateTimeOffset? FromUnixSeconds(long seconds)
    {
        try
        {
            return DateTimeOffset.FromUnixTimeSeconds(seconds);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>Null when <paramref name="milliseconds"/> is outside year 1 through 9999.</summary>
    public static DateTimeOffset? FromUnixMilliseconds(long milliseconds)
    {
        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

    /// <summary>Null when <paramref name="milliseconds"/> does not fit in a TimeSpan. Negative spans that do fit are kept.</summary>
    public static TimeSpan? FromMilliseconds(long milliseconds)
    {
        try
        {
            return TimeSpan.FromMilliseconds(milliseconds);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}
