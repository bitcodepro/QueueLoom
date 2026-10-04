using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Infrastructure.Logging;

/// <summary>
/// Writes redacted diagnostic lines to one file per day and keeps a bounded history.
/// Logging failures are swallowed: diagnostics must never interrupt Service Bus work.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private const long MaximumFileBytes = 10 * 1024 * 1024;
    private const string WriteLockName = ".queueloom-log.lock";

    private readonly ConcurrentDictionary<string, FileLogger> _loggers = new(StringComparer.Ordinal);
    private readonly object _sync = new();
    private readonly string _directory;
    private readonly int _retainedDays;
    private readonly Func<DateTimeOffset> _clock;
    private readonly LogLevel _minimumLevel;
    private bool _disposed;
    private DateTime _retentionDay;

    public FileLoggerProvider(
        string directory,
        LogLevel minimumLevel = LogLevel.Information,
        int retainedDays = 14,
        Func<DateTimeOffset>? clock = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = Path.GetFullPath(directory);
        _minimumLevel = minimumLevel;
        _retainedDays = Math.Max(1, retainedDays);
        _clock = clock ?? (() => DateTimeOffset.Now);
        TryDeleteExpiredFiles();
    }

    public string Directory => _directory;

    public string CurrentFilePath => Path.Combine(_directory, FileNameFor(_clock()));

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new FileLogger(this, name));

    public void Dispose()
    {
        lock (_sync)
        {
            _disposed = true;
        }
    }

    internal bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && logLevel >= _minimumLevel;

    internal void Write(string category, LogLevel level, EventId eventId, string message, Exception? exception)
    {
        var timestamp = _clock();
        var line = new StringBuilder(256)
            .Append(timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture))
            .Append(" [").Append(LevelLabel(level)).Append("] ")
            .Append(category);
        if (eventId.Id != 0)
        {
            line.Append('[').Append(eventId.Id.ToString(CultureInfo.InvariantCulture)).Append(']');
        }
        line.Append(": ").Append(SensitiveDataRedactor.Redact(message));
        if (exception is not null)
        {
            line.AppendLine().Append(SensitiveDataRedactor.Redact(exception.ToString()));
        }
        line.AppendLine();

        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                System.IO.Directory.CreateDirectory(_directory);
                // Long-running processes (the tray app, an MCP server) must prune too, not only at construction.
                if (timestamp.Date != _retentionDay)
                {
                    TryDeleteExpiredFiles(timestamp);
                }
                var path = Path.Combine(_directory, FileNameFor(timestamp));
                // The desktop app and every MCP server process append to the same daily file. Each append opens the
                // file, seeks to its end and writes, so two unsynchronized processes overwrite each other's lines
                // (or, on Windows, one fails with a sharing violation). A short cross-process lock serializes appends
                // without locking the log file itself, so readers are never blocked.
                using var writeLock = TryAcquireWriteLock();
                if (writeLock is null)
                {
                    return;
                }
                if (File.Exists(path) && new FileInfo(path).Length > MaximumFileBytes)
                {
                    return;
                }
                File.AppendAllText(path, line.ToString(), Encoding.UTF8);
            }
            catch (Exception writeException) when (writeException is IOException or UnauthorizedAccessException)
            {
                // A locked or read-only log directory must not break the application.
            }
        }
    }

    private FileStream? TryAcquireWriteLock()
    {
        var path = Path.Combine(_directory, WriteLockName);
        var started = Environment.TickCount64;
        while (true)
        {
            try
            {
                return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
            }
            catch (IOException) when (Environment.TickCount64 - started < 2_000)
            {
                Thread.Sleep(1);
            }
            catch (IOException)
            {
                // Another process holds the lock far longer than an append takes; drop this line rather than block work.
                return null;
            }
        }
    }

    private void TryDeleteExpiredFiles() => TryDeleteExpiredFiles(_clock());

    private void TryDeleteExpiredFiles(DateTimeOffset now)
    {
        _retentionDay = now.Date;
        try
        {
            if (!System.IO.Directory.Exists(_directory))
            {
                return;
            }

            var cutoff = now.AddDays(-_retainedDays).Date;
            foreach (var file in System.IO.Directory.EnumerateFiles(_directory, "queueloom-*.log"))
            {
                var stamp = Path.GetFileNameWithoutExtension(file)["queueloom-".Length..];
                if (DateTime.TryParseExact(stamp, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day) &&
                    day < cutoff)
                {
                    File.Delete(file);
                }
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // Retention is best effort.
        }
    }

    private static string FileNameFor(DateTimeOffset timestamp) =>
        $"queueloom-{timestamp.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.log";

    private static string LevelLabel(LogLevel level) => level switch
    {
        LogLevel.Trace => "TRC",
        LogLevel.Debug => "DBG",
        LogLevel.Information => "INF",
        LogLevel.Warning => "WRN",
        LogLevel.Error => "ERR",
        LogLevel.Critical => "CRT",
        _ => "???"
    };

    private sealed class FileLogger(FileLoggerProvider provider, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => provider.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            try { provider.Write(category, logLevel, eventId, formatter(state, exception), exception); }
            catch { /* Formatting and exception rendering are best effort; never log raw fallback data. */ }
        }
    }
}
