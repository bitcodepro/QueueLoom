namespace QueueLoom.Core.Settings;

public enum AppThemePreference
{
    Dark,
    Light,
    System
}

/// <summary>A dead-letter search the operator saved to run again later.</summary>
/// <param name="EnvironmentId">The environment to search, or null for the one selected at the time.</param>
/// <param name="WithinMinutes">Only messages enqueued in this many minutes before the search, or null for any age.</param>
public sealed record SavedSearch(string Name, string Query, Guid? EnvironmentId = null, int? WithinMinutes = null);

public sealed record AppSettings(
    int MonitorIntervalSeconds = AppSettings.DefaultMonitorIntervalSeconds,
    AppThemePreference Theme = AppThemePreference.Dark)
{
    public const int DefaultMonitorIntervalSeconds = 60;
    public const int MinimumMonitorIntervalSeconds = 15;
    public const int MaximumMonitorIntervalSeconds = 86_400;
    public const int MaximumSavedSearches = 50;
    public const int MaximumBackupRetentionDays = 3_650;

    public static AppSettings Default { get; } = new();

    public IReadOnlyList<SavedSearch> SavedSearches { get; init; } = [];

    /// <summary>Show an operating-system notification when a monitor finds new dead letters and QueueLoom is not in front.</summary>
    public bool SystemNotifications { get; init; } = true;

    /// <summary>Slack or Microsoft Teams incoming-webhook address that receives monitor alerts, or null.</summary>
    public string? AlertWebhookUrl { get; init; }

    /// <summary>Backups older than this many days are deleted at start-up; 0 keeps them forever.</summary>
    public int BackupRetentionDays { get; init; }

    public AppSettings Normalize() => this with
    {
        MonitorIntervalSeconds = Math.Clamp(
            MonitorIntervalSeconds,
            MinimumMonitorIntervalSeconds,
            MaximumMonitorIntervalSeconds),
        Theme = Enum.IsDefined(Theme) ? Theme : AppThemePreference.Dark,
        SavedSearches = (SavedSearches ?? [])
            .Where(search => search is not null && !string.IsNullOrWhiteSpace(search.Name) && !string.IsNullOrWhiteSpace(search.Query))
            .Select(search => search with
            {
                Name = search.Name.Trim(),
                Query = search.Query.Trim(),
                WithinMinutes = search.WithinMinutes is > 0 ? search.WithinMinutes : null
            })
            .DistinctBy(search => search.Name, StringComparer.OrdinalIgnoreCase)
            .Take(MaximumSavedSearches)
            .ToArray(),
        AlertWebhookUrl = IsValidWebhookUrl(AlertWebhookUrl) ? AlertWebhookUrl!.Trim() : null,
        BackupRetentionDays = Math.Clamp(BackupRetentionDays, 0, MaximumBackupRetentionDays)
    };

    /// <summary>Only https addresses are accepted: alerts carry environment and queue names.</summary>
    public static bool IsValidWebhookUrl(string? value) =>
        !string.IsNullOrWhiteSpace(value) &&
        Uri.TryCreate(value.Trim(), UriKind.Absolute, out var uri) &&
        uri.Scheme == Uri.UriSchemeHttps &&
        string.IsNullOrEmpty(uri.UserInfo);
}
