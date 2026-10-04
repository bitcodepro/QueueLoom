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
                return (await ReadAsync(cancellationToken).ConfigureAwait(false)).Settings;
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
            await using var transaction = await CrossProcessFileLock.AcquireAsync(paths.SettingsFile + ".lock", cancellationToken)
                .ConfigureAwait(false);
            var (current, state) = await ReadAsync(cancellationToken).ConfigureAwait(false);
            if (state == FileState.Newer)
            {
                // A file from a newer QueueLoom is left as it is; rewriting it here would drop what this version cannot read.
                throw new InvalidDataException("The settings file was written by a newer version of QueueLoom, so it is not changed.");
            }
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
                    ProtobufSchemaPath = updated.ProtobufSchemaPath,
                    BackupRetentionDays = updated.BackupRetentionDays
                },
                SerializerOptions);
            paths.EnsureCreated();
            if (state == FileState.Damaged)
            {
                // The damaged file is kept aside, so saved searches and the webhook can still be recovered from it.
                File.Move(paths.SettingsFile, paths.SettingsFile + $".damaged-{DateTime.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}");
            }
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

    private enum FileState { Read, Damaged, Newer }

    /// <summary>Reads the file; a missing, corrupt or newer file yields defaults, an unreadable one throws.</summary>
    private async Task<(AppSettings Settings, FileState State)> ReadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(paths.SettingsFile))
        {
            return (AppSettings.Default, FileState.Read);
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
            using var json = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            // The schema version is read before the v1 model is bound: a newer file may change the type of a known field.
            if (json.RootElement.ValueKind == JsonValueKind.Object &&
                json.RootElement.TryGetProperty(nameof(SettingsDocument.SchemaVersion), out var version) &&
                version.ValueKind == JsonValueKind.Number && version.TryGetInt32(out var number) && number > 1)
            {
                return (AppSettings.Default, FileState.Newer);
            }
            var document = json.RootElement.Deserialize<SettingsDocument>(SerializerOptions);
            if (document is not { SchemaVersion: 1 })
            {
                return (AppSettings.Default, FileState.Damaged);
            }

            // Unknown theme names (for example from a newer version) keep the default theme
            // instead of invalidating the whole file.
            var theme = Enum.TryParse<AppThemePreference>(document.Theme, ignoreCase: true, out var parsed) &&
                        Enum.IsDefined(parsed)
                ? parsed
                : AppSettings.Default.Theme;
            return (new AppSettings(document.MonitorIntervalSeconds, theme)
            {
                SavedSearches = document.SavedSearches ?? [],
                SystemNotifications = document.SystemNotifications ?? AppSettings.Default.SystemNotifications,
                KeepInTray = document.KeepInTray,
                AlertWebhookUrl = document.AlertWebhookUrl,
                ProtobufSchemaPath = document.ProtobufSchemaPath,
                BackupRetentionDays = document.BackupRetentionDays
            }.Normalize(), FileState.Read);
        }
        catch (JsonException)
        {
            return (AppSettings.Default, FileState.Damaged);
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
        public string? ProtobufSchemaPath { get; set; }
        public int BackupRetentionDays { get; set; }
    }
}
