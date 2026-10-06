using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Infrastructure.Logging;

/// <summary>
/// Writes redacted diagnostic lines to one file per day and keeps a bounded history.
/// Callers enqueue formatted lines without waiting on the cross-process lock; a background
/// writer drains the queue and may wait on that lock as long as needed. When the queue is
/// full, the line is counted and a single notice is written once the lock is next held.
/// Logging failures are swallowed: diagnostics must never interrupt Service Bus work.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private const long MaximumFileBytes = 10 * 1024 * 1024;
    internal const string WriteLockName = ".queueloom-log.lock";
    private const int DefaultMaxQueuedLines = 8192;
    private static readonly TimeSpan DisposeFlushWait = TimeSpan.FromSeconds(15);

    private readonly ConcurrentDictionary<string, FileLogger> _loggers = new(StringComparer.Ordinal);
    private readonly object _lifetime = new();
    private readonly string _directory;
    private readonly int _retainedDays;
    private readonly Func<DateTimeOffset> _clock;
    private readonly LogLevel _minimumLevel;
    private readonly Channel<WorkItem> _queue;
    private readonly Task _writer;
    private int _overflowCount;
    private bool _disposed;
    private DateTime _retentionDay;

    public FileLoggerProvider(
        string directory,
        LogLevel minimumLevel = LogLevel.Information,
        int retainedDays = 14,
        Func<DateTimeOffset>? clock = null,
        int maxQueuedLines = DefaultMaxQueuedLines)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxQueuedLines, 1);
        _directory = Path.GetFullPath(directory);
        _minimumLevel = minimumLevel;
        _retainedDays = Math.Max(1, retainedDays);
        _clock = clock ?? (() => DateTimeOffset.Now);
        _queue = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(maxQueuedLines)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        _writer = Task.Factory.StartNew(
            WriteQueuedLines,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
        TryDeleteExpiredFiles();
    }

    public string Directory => _directory;

    public string CurrentFilePath => Path.Combine(_directory, FileNameFor(_clock()));

    /// <summary>Lines refused because the bounded queue was full (not yet written as a notice).</summary>
    internal int PendingOverflowCount => Volatile.Read(ref _overflowCount);

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new FileLogger(this, name));

    /// <summary>Waits until every enqueued line has been written (or counted as overflow).</summary>
    internal void Flush()
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lifetime)
        {
            if (_disposed)
            {
                done.TrySetResult();
            }
            else if (!_queue.Writer.TryWrite(new FlushItem(done)))
            {
                // Queue is saturated with log lines; wait briefly for a slot so tests and shutdown can drain.
                if (!_queue.Writer.WaitToWriteAsync().AsTask().Wait(DisposeFlushWait) ||
                    !_queue.Writer.TryWrite(new FlushItem(done)))
                {
                    done.TrySetResult();
                }
            }
        }

        try
        {
            done.Task.Wait(DisposeFlushWait);
        }
        catch (AggregateException)
        {
        }
    }

    public void Dispose()
    {
        lock (_lifetime)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _queue.Writer.TryComplete();
        }

        try
        {
            _writer.Wait(DisposeFlushWait);
        }
        catch (AggregateException)
        {
            // Writer failures are swallowed; callers must not see them.
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

        lock (_lifetime)
        {
            if (_disposed)
            {
                return;
            }

            // Bounded enqueue: never wait on the cross-process lock (or on a full queue) on the caller's thread.
            if (!_queue.Writer.TryWrite(new LineItem(line.ToString())))
            {
                Interlocked.Increment(ref _overflowCount);
            }
        }
    }

    private async Task WriteQueuedLines()
    {
        try
        {
            await foreach (var item in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                switch (item)
                {
                    case LineItem line:
                        AppendUnderWriteLock(line.Text);
                        break;
                    case FlushItem flush:
                        // Drain any overflow notice that piled up while the lock was held.
                        AppendUnderWriteLock(preparedLine: null);
                        flush.Completion.TrySetResult();
                        break;
                }
            }

            AppendUnderWriteLock(preparedLine: null);
        }
        catch (Exception)
        {
            // A failed writer must not take down the process; remaining lines are abandoned.
        }
    }

    private void AppendUnderWriteLock(string? preparedLine)
    {
        try
        {
            System.IO.Directory.CreateDirectory(_directory);
            var timestamp = _clock();
            // Long-running processes (the tray app, an MCP server) must prune too, not only at construction.
            if (timestamp.Date != _retentionDay)
            {
                TryDeleteExpiredFiles(timestamp);
            }

            using var writeLock = AcquireWriteLock();
            if (writeLock is null)
            {
                if (preparedLine is not null)
                {
                    Interlocked.Increment(ref _overflowCount);
                }
                return;
            }

            var path = Path.Combine(_directory, FileNameFor(timestamp));
            if (File.Exists(path) && new FileInfo(path).Length > MaximumFileBytes)
            {
                // Once per file: say why the log stops here, so a gap in it is not mistaken for silence. The marker
                // is created first: if it cannot be, nothing is appended, so the capped file never keeps growing.
                if (!File.Exists(path + ".full"))
                {
                    using (new FileStream(path + ".full", FileMode.CreateNew, FileAccess.Write)) { }
                    File.AppendAllText(path,
                        $"{timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture)} [WRN] QueueLoom: " +
                        $"this log reached {MaximumFileBytes / (1024 * 1024)} MB; nothing more is written to it today.{Environment.NewLine}",
                        Encoding.UTF8);
                }
                // Cap drops are intentional and already explained; do not also count them as queue overflow.
                return;
            }

            var dropped = Interlocked.Exchange(ref _overflowCount, 0);
            if (dropped > 0)
            {
                File.AppendAllText(path,
                    $"{timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture)} [WRN] QueueLoom: " +
                    $"{dropped.ToString(CultureInfo.InvariantCulture)} log lines dropped because the log queue was full.{Environment.NewLine}",
                    Encoding.UTF8);
            }

            if (preparedLine is not null)
            {
                File.AppendAllText(path, preparedLine, Encoding.UTF8);
            }
        }
        catch (Exception writeException) when (writeException is IOException or UnauthorizedAccessException)
        {
            // A locked or read-only log directory must not break the application. Count the line so accounting stays honest.
            if (preparedLine is not null)
            {
                Interlocked.Increment(ref _overflowCount);
            }
        }
    }

    /// <summary>
    /// Waits until the cross-process lock is free. Runs only on the background writer, never on the UI thread.
    /// Returns null only when Dispose has begun and the flush wait has already been abandoned.
    /// </summary>
    private FileStream? AcquireWriteLock()
    {
        var path = Path.Combine(_directory, WriteLockName);
        var overrideAcquire = AcquireWriteLockOverride.Value;
        if (overrideAcquire is not null)
        {
            return overrideAcquire(path);
        }

        var started = Environment.TickCount64;
        while (true)
        {
            try
            {
                return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
            }
            catch (IOException) when (Environment.TickCount64 - started < DisposeFlushWait.TotalMilliseconds || !_disposed)
            {
                // Background delivery may wait through contended appends; stop only after Dispose's flush budget.
                Thread.Sleep(1);
            }
            catch (IOException)
            {
                return null;
            }
        }
    }

    /// <summary>Test seam: replaces opening the cross-process write lock.</summary>
    internal static readonly AsyncLocal<Func<string, FileStream?>?> AcquireWriteLockOverride = new();

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
            foreach (var marker in System.IO.Directory.EnumerateFiles(_directory, "queueloom-*.log.full"))
            {
                var stamp = Path.GetFileName(marker)["queueloom-".Length..^".log.full".Length];
                if (DateTime.TryParseExact(stamp, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day) && day < cutoff)
                {
                    File.Delete(marker);
                }
            }
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

    private abstract record WorkItem;
    private sealed record LineItem(string Text) : WorkItem;
    private sealed record FlushItem(TaskCompletionSource Completion) : WorkItem;

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
