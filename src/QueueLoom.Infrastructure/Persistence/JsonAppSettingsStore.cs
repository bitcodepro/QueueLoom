using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Settings;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonAppSettingsStore(QueueLoomPaths paths) : IDisposable
{
    public const int DefaultMonitorIntervalSeconds = AppSettings.DefaultMonitorIntervalSeconds;

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public async Task<AppSettings> LoadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Applies <paramref name="update"/> to the stored settings so unrelated preferences are preserved.</summary>
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
                    Theme = updated.Theme
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
            return document is { SchemaVersion: 1 }
                ? new AppSettings(document.MonitorIntervalSeconds, document.Theme).Normalize()
                : AppSettings.Default;
        }
        catch (Exception exception) when (exception is JsonException or IOException or UnauthorizedAccessException)
        {
            return AppSettings.Default;
        }
    }

    private sealed class SettingsDocument
    {
        public int SchemaVersion { get; set; } = 1;
        public int MonitorIntervalSeconds { get; set; } = DefaultMonitorIntervalSeconds;
        public AppThemePreference Theme { get; set; } = AppThemePreference.System;
    }
}
