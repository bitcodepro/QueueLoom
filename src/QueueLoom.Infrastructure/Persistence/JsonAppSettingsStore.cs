using System.Text.Json;
using QueueLoom.Core.Settings;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonAppSettingsStore(QueueLoomPaths paths) : IDisposable
{
    public const int DefaultMonitorIntervalSeconds = AppSettings.DefaultMonitorIntervalSeconds;

    private static readonly JsonSerializerOptions SerializerOptions = new() { WriteIndented = true };

    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public async Task<AppSettings> LoadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                return await ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                return AppSettings.Default;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Applies <paramref name="update"/> to the stored settings so unrelated preferences are preserved.
    /// If the file exists but cannot be read (for example it is locked), nothing is written and the
    /// I/O exception propagates, so a transient failure never resets the other preferences.
    /// </summary>
    public async Task<AppSettings> UpdateAsync(
        Func<AppSettings, AppSettings> update,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(update);
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var current = await ReadAsync(cancellationToken).ConfigureAwait(false);
            var updated = update(current).Normalize();
            var document = JsonSerializer.Serialize(
                new SettingsDocument
                {
                    MonitorIntervalSeconds = updated.MonitorIntervalSeconds,
                    Theme = updated.Theme.ToString(),
                    SavedSearches = updated.SavedSearches.ToList(),
                    SystemNotifications = updated.SystemNotifications,
                    KeepInTray = updated.KeepInTray,
                    AlertWebhookUrl = updated.AlertWebhookUrl,
                    BackupRetentionDays = updated.BackupRetentionDays
                },
                SerializerOptions);
            paths.EnsureCreated();
            await AtomicFile.WriteTextAsync(paths.SettingsFile, document, cancellationToken).ConfigureAwait(false);
            return updated;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<int> LoadMonitorIntervalSecondsAsync(CancellationToken cancellationToken = default) =>
        (await LoadAsync(cancellationToken).ConfigureAwait(false)).MonitorIntervalSeconds;

    public Task SaveMonitorIntervalSecondsAsync(
        int monitorIntervalSeconds,
        CancellationToken cancellationToken = default) =>
        UpdateAsync(settings => settings with { MonitorIntervalSeconds = monitorIntervalSeconds }, cancellationToken);

    public Task SaveThemeAsync(AppThemePreference theme, CancellationToken cancellationToken = default) =>
        UpdateAsync(settings => settings with { Theme = theme }, cancellationToken);

    public void Dispose()
    {
        _disposed = true;
        _gate.Dispose();
    }

    /// <summary>Reads the file; a missing or corrupt file yields defaults, an unreadable one throws.</summary>
    private async Task<AppSettings> ReadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(paths.SettingsFile))
        {
            return AppSettings.Default;
        }

        try
        {
            await using var stream = new FileStream(
                paths.SettingsFile,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                16 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            var document = await JsonSerializer.DeserializeAsync<SettingsDocument>(
                    stream,
                    SerializerOptions,
                    cancellationToken)
                .ConfigureAwait(false);
            if (document is not { SchemaVersion: 1 })
            {
                return AppSettings.Default;
            }

            // Unknown theme names (for example from a newer version) keep the default theme
            // instead of invalidating the whole file.
            var theme = Enum.TryParse<AppThemePreference>(document.Theme, ignoreCase: true, out var parsed) &&
                        Enum.IsDefined(parsed)
                ? parsed
                : AppSettings.Default.Theme;
            return new AppSettings(document.MonitorIntervalSeconds, theme)
            {
                SavedSearches = document.SavedSearches ?? [],
                SystemNotifications = document.SystemNotifications ?? AppSettings.Default.SystemNotifications,
                KeepInTray = document.KeepInTray,
                AlertWebhookUrl = document.AlertWebhookUrl,
                BackupRetentionDays = document.BackupRetentionDays
            }.Normalize();
        }
        catch (JsonException)
        {
            return AppSettings.Default;
        }
    }

    private sealed class SettingsDocument
    {
        public int SchemaVersion { get; set; } = 1;
        public int MonitorIntervalSeconds { get; set; } = DefaultMonitorIntervalSeconds;
        public string? Theme { get; set; }
        public List<SavedSearch>? SavedSearches { get; set; }
        public bool? SystemNotifications { get; set; }
        public bool KeepInTray { get; set; }
        public string? AlertWebhookUrl { get; set; }
        public int BackupRetentionDays { get; set; }
    }
}
