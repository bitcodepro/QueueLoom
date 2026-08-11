namespace QueueLoom.Core.Monitoring;

public sealed record DeadLetterMonitorSettings
{
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan MaximumInterval = TimeSpan.FromHours(24);

    public DeadLetterMonitorSettings(
        bool isEnabled,
        TimeSpan interval,
        DeadLetterMonitorScope scope,
        long alertThreshold = 1)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfLessThan(alertThreshold, 1);

        if (interval < MinimumInterval || interval > MaximumInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(interval),
                $"The monitor interval must be between {MinimumInterval} and {MaximumInterval}.");
        }

        IsEnabled = isEnabled;
        Interval = interval;
        Scope = scope;
        AlertThreshold = alertThreshold;
    }

    public bool IsEnabled { get; }

    public TimeSpan Interval { get; }

    public DeadLetterMonitorScope Scope { get; }

    public long AlertThreshold { get; }

    public static DeadLetterMonitorSettings Disabled { get; } =
        new(false, TimeSpan.FromMinutes(1), DeadLetterMonitorScope.All);
}
