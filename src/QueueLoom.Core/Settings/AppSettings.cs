namespace QueueLoom.Core.Settings;

public enum AppThemePreference
{
    Dark,
    Light,
    System
}

public sealed record AppSettings(
    int MonitorIntervalSeconds = AppSettings.DefaultMonitorIntervalSeconds,
    AppThemePreference Theme = AppThemePreference.Dark)
{
    public const int DefaultMonitorIntervalSeconds = 60;
    public const int MinimumMonitorIntervalSeconds = 15;
    public const int MaximumMonitorIntervalSeconds = 86_400;

    public static AppSettings Default { get; } = new();

    public AppSettings Normalize() => this with
    {
        MonitorIntervalSeconds = Math.Clamp(
            MonitorIntervalSeconds,
            MinimumMonitorIntervalSeconds,
            MaximumMonitorIntervalSeconds),
        Theme = Enum.IsDefined(Theme) ? Theme : AppThemePreference.Dark
    };
}
