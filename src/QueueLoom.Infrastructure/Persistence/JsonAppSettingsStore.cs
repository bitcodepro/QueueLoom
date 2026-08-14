using System.Text.Json;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonAppSettingsStore(QueueLoomPaths paths) : IDisposable
{
    public const int DefaultMonitorIntervalSeconds = 60;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public async Task<int> LoadMonitorIntervalSecondsAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!File.Exists(paths.SettingsFile))
            {
                return DefaultMonitorIntervalSeconds;
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
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return document is { SchemaVersion: 1 }
                    ? Math.Clamp(document.MonitorIntervalSeconds, 15, 86_400)
                    : DefaultMonitorIntervalSeconds;
            }
            catch (Exception exception) when (exception is JsonException or IOException or UnauthorizedAccessException)
            {
                return DefaultMonitorIntervalSeconds;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SaveMonitorIntervalSecondsAsync(
        int monitorIntervalSeconds,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var document = JsonSerializer.Serialize(
            new SettingsDocument
            {
                MonitorIntervalSeconds = Math.Clamp(monitorIntervalSeconds, 15, 86_400)
            },
            new JsonSerializerOptions { WriteIndented = true });

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            paths.EnsureCreated();
            await AtomicFile.WriteTextAsync(paths.SettingsFile, document, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _disposed = true;
        _gate.Dispose();
    }

    private sealed class SettingsDocument
    {
        public int SchemaVersion { get; set; } = 1;
        public int MonitorIntervalSeconds { get; set; } = DefaultMonitorIntervalSeconds;
    }
}
