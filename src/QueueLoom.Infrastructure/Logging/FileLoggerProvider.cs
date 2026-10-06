using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Infrastructure.Logging;

/// <summary>
/// Writes redacted diagnostic lines to one file per day and keeps a bounded history.
/// Callers only format a line and put it in a bounded queue: they never wait on the disk or on the cross-process lock.
/// One background writer appends the lines under that lock. When the queue is full the line is counted, and the next
/// successful append says how many were dropped. Disposing drains the queue within one shutdown budget.
/// Logging failures are swallowed: diagnostics must never interrupt Service Bus work.
/// </summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private const long MaximumFileBytes = 10 * 1024 * 1024;
    internal const string WriteLockName = ".queueloom-log.lock";
    private const int DefaultMaxQueuedLines = 8192;
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(15);
    private const string DroppedNotice = "log lines dropped because the log queue was full";

    private readonly ConcurrentDictionary<string, FileLogger> _loggers = new(StringComparer.Ordinal);
    private readonly string _directory;
    private readonly int _retainedDays;
    private readonly Func<DateTimeOffset> _clock;
    private readonly LogLevel _minimumLevel;
    private readonly TimeSpan _shutdownBudget;
    private readonly Channel<WorkItem> _queue;
    private readonly Thread _writer;
    private int _overflowCount;
    private int _disposed;
    // Set once the shutdown budget is spent: the writer then appends nothing more and only empties the queue.
    private volatile bool _abandoned;
    private DateTime _retentionDay;

    public FileLoggerProvider(
        string directory,
        LogLevel minimumLevel = LogLevel.Information,
        int retainedDays = 14,
        Func<DateTimeOffset>? clock = null,
        int maxQueuedLines = DefaultMaxQueuedLines,
        TimeSpan? shutdownBudget = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxQueuedLines, 1);
        _directory = Path.GetFullPath(directory);
        _minimumLevel = minimumLevel;
        _retainedDays = Math.Max(1, retainedDays);
        _clock = clock ?? (() => DateTimeOffset.Now);
        _shutdownBudget = shutdownBudget ?? DefaultShutdownBudget;
        _queue = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(maxQueuedLines)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
        TryDeleteExpiredFiles();
        _writer = new Thread(WriteQueuedLines) { IsBackground = true, Name = "QueueLoom log writer" };
        _writer.Start();
    }

    public string Directory => _directory;

    public string CurrentFilePath => Path.Combine(_directory, FileNameFor(_clock()));

    /// <summary>Lines refused (queue full) or not written, not yet named in a notice in the log.</summary>
    internal int PendingOverflowCount => Volatile.Read(ref _overflowCount);

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new FileLogger(this, name));

    /// <summary>
    /// Waits until every line queued before the call has been written or counted as dropped, for at most the shutdown
    /// budget. No lock is held while waiting, so logging from other threads goes on meanwhile.
    /// </summary>
    internal void Flush()
    {
        var deadline = Environment.TickCount64 + (long)_shutdownBudget.TotalMilliseconds;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        while (!_queue.Writer.TryWrite(new FlushItem(done)))
        {
            // Full (the writer is waiting for the cross-process lock) or completed (disposed).
            if (Volatile.Read(ref _disposed) != 0 || Environment.TickCount64 >= deadline)
            {
                return;
            }
            Thread.Sleep(5);
        }
        var remaining = deadline - Environment.TickCount64;
        if (remaining > 0)
        {
            done.Task.Wait(TimeSpan.FromMilliseconds(remaining));
        }
    }

    /// <summary>
    /// Stops taking lines and lets the writer append what is queued, within one shutdown budget for the whole drain.
    /// When the budget is spent the writer stops appending (nothing is written after this returns) and exits.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        _queue.Writer.TryComplete();
        if (!_writer.Join(_shutdownBudget))
        {
            _abandoned = true;
            // The writer checks the flag between lines and while waiting for the lock, so it ends promptly.
            _writer.Join(TimeSpan.FromSeconds(1));
        }
    }

    internal bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && logLevel >= _minimumLevel;

    internal void Write(string category, LogLevel level, EventId eventId, string message, Exception? exception)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }
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

        // Never waits: a full queue counts the line instead.
        if (!_queue.Writer.TryWrite(new LineItem(timestamp, line.ToString())))
        {
            Interlocked.Increment(ref _overflowCount);
        }
    }

    // Kept as small, plain methods: code-coverage instrumentation rewrote a single loop with nested try blocks here
    // into invalid IL on CI.
    private void WriteQueuedLines()
    {
        var reader = _queue.Reader;
        while (WaitForWork(reader))
        {
            while (reader.TryRead(out var item))
            {
                ProcessSafely(item);
            }
        }
    }

    private static bool WaitForWork(ChannelReader<WorkItem> reader)
    {
        try
        {
            return reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult();
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>One failure (any exception) costs that item only; the writer goes on with the next one.</summary>
    private void ProcessSafely(WorkItem item)
    {
        try
        {
            Process(item);
        }
        catch (Exception)
        {
            Abandon(item);
        }
    }

    private void Abandon(WorkItem item)
    {
        if (item is LineItem)
        {
            Interlocked.Increment(ref _overflowCount);
        }
        else if (item is FlushItem flush)
        {
            flush.Completion.TrySetResult();
        }
    }

    private void Process(WorkItem item)
    {
        switch (item)
        {
            case LineItem when _abandoned:
                Interlocked.Increment(ref _overflowCount);
                return;
            case LineItem line:
                Append(line.Timestamp, line.Text);
                return;
            case FlushItem flush:
                if (!_abandoned && Volatile.Read(ref _overflowCount) > 0)
                {
                    Append(_clock(), null);
                }
                flush.Completion.TrySetResult();
                return;
        }
    }

    /// <summary>Appends one line (or, with null, only a pending dropped-lines notice) to the file of its own day.</summary>
    private void Append(DateTimeOffset timestamp, string? text)
    {
        try
        {
            System.IO.Directory.CreateDirectory(_directory);
            // Long-running processes (the tray app, an MCP server) must prune too, not only at construction.
            if (timestamp.Date != _retentionDay)
            {
                TryDeleteExpiredFiles(timestamp);
            }
            // The line's own time picks the file: a line logged just before midnight is not moved to the next day.
            var path = Path.Combine(_directory, FileNameFor(timestamp));
            // The desktop app and every MCP server process append to the same daily file; the cross-process lock
            // serializes appends without locking the log file itself, so readers are never blocked.
            using var writeLock = AcquireWriteLock();
            // Checked again once the lock is held: an acquisition that succeeds after the shutdown budget was spent
            // must not start an append after Dispose returned.
            if (writeLock is null || _abandoned)
            {
                if (text is not null) Interlocked.Increment(ref _overflowCount);
                return;
            }
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
                // Lines past the cap are dropped on purpose and already explained; they are not counted as overflow.
                return;
            }

            // The drop count is taken only to write it, and given back if that write fails, so it is never lost.
            var dropped = Interlocked.Exchange(ref _overflowCount, 0);
            if (dropped > 0)
            {
                try
                {
                    File.AppendAllText(path,
                        $"{timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture)} [WRN] QueueLoom: " +
                        $"{dropped.ToString(CultureInfo.InvariantCulture)} {DroppedNotice}.{Environment.NewLine}",
                        Encoding.UTF8);
                }
                catch
                {
                    Interlocked.Add(ref _overflowCount, dropped);
                    throw;
                }
            }
            if (text is not null)
            {
                File.AppendAllText(path, text, Encoding.UTF8);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // A locked or read-only log directory must not break the application; the line counts as dropped.
            if (text is not null) Interlocked.Increment(ref _overflowCount);
        }
    }

    /// <summary>
    /// Waits for the cross-process lock on the background writer (never on a caller's thread). Gives up only when the
    /// shutdown budget is spent.
    /// </summary>
    private FileStream? AcquireWriteLock()
    {
        var path = Path.Combine(_directory, WriteLockName);
        var overrideAcquire = AcquireWriteLockOverride.Value;
        if (overrideAcquire is not null)
        {
            return overrideAcquire(path);
        }
        while (true)
        {
            try
            {
                return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
            }
            // Only contention is waited out. A missing directory (renamed or deleted logs folder) is not: it goes to the
            // item's own recovery, and the next item recreates the directory before trying again.
            catch (IOException exception) when (exception is not DirectoryNotFoundException && !_abandoned)
            {
                LockBusyObserved.Value?.Invoke();
                Thread.Sleep(1);
            }
            catch (IOException)
            {
                return null;
            }
        }
    }

    /// <summary>Test seam: told each time the writer finds the cross-process lock busy and waits.</summary>
    internal static readonly AsyncLocal<Action?> LockBusyObserved = new();

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

    private sealed record LineItem(DateTimeOffset Timestamp, string Text) : WorkItem;

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
