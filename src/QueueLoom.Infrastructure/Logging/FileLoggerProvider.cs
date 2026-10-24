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
    // One lock hold appends at most this much, so a busy process never keeps the other one out for long.
    internal const int MaxBatchLines = 512;
    private const int MaxBatchChars = 256 * 1024;
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
            // The writer checks the flag between batches and while waiting for the lock, so it ends promptly.
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
        var batch = new List<LineItem>();
        while (WaitForWork(reader))
        {
            DrainQueued(reader, batch);
        }
    }

    /// <summary>
    /// Takes everything queued now and appends consecutive lines together, one lock hold per bounded batch. A flush
    /// request ends the batch before it, so it completes only after every earlier line was written or counted.
    /// </summary>
    private void DrainQueued(ChannelReader<WorkItem> reader, List<LineItem> batch)
    {
        var chars = 0;
        while (reader.TryRead(out var item))
        {
            if (item is LineItem line)
            {
                batch.Add(line);
                chars += line.Text.Length;
                if (batch.Count < MaxBatchLines && chars < MaxBatchChars)
                {
                    continue;
                }
            }
            WriteBatch(batch);
            chars = 0;
            if (item is FlushItem flush)
            {
                CompleteFlush(flush);
            }
        }
        WriteBatch(batch);
    }

    /// <summary>Appends the batch split at day boundaries, so each line goes to the file of its own day; then clears it.</summary>
    private void WriteBatch(List<LineItem> batch)
    {
        var start = 0;
        while (start < batch.Count)
        {
            var end = start + 1;
            while (end < batch.Count && batch[end].Timestamp.Date == batch[start].Timestamp.Date)
            {
                end++;
            }
            AppendLinesSafely(batch, start, end - start);
            start = end;
        }
        batch.Clear();
    }

    /// <summary>One failure (any exception) costs those lines only, counted as dropped; the writer goes on.</summary>
    private void AppendLinesSafely(List<LineItem> batch, int start, int count)
    {
        try
        {
            if (_abandoned)
            {
                Interlocked.Add(ref _overflowCount, count);
                return;
            }
            var text = new StringBuilder();
            for (var i = start; i < start + count; i++)
            {
                text.Append(batch[i].Text);
            }
            Append(batch[start].Timestamp, text.ToString(), count);
        }
        catch (Exception)
        {
            Interlocked.Add(ref _overflowCount, count);
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

    /// <summary>Writes a pending dropped-lines notice, then completes the flush even if that failed.</summary>
    private void CompleteFlush(FlushItem flush)
    {
        try
        {
            if (!_abandoned && Volatile.Read(ref _overflowCount) > 0)
            {
                Append(_clock(), null, 0);
            }
        }
        catch (Exception)
        {
            // The notice stays pending and goes out with the next append.
        }
        flush.Completion.TrySetResult();
    }

    /// <summary>
    /// Appends the lines of one day (or, with null, only a pending dropped-lines notice) to the file of that day, with
    /// one hold of the cross-process lock and one open of the file. If that fails the <paramref name="lineCount"/>
    /// lines count as dropped.
    /// </summary>
    private void Append(DateTimeOffset timestamp, string? text, int lineCount)
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
                Interlocked.Add(ref _overflowCount, lineCount);
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

            // The drop count is taken only to write it, and given back if that write fails, so it is never lost. The
            // notice goes ahead of the lines in the same append.
            var dropped = Interlocked.Exchange(ref _overflowCount, 0);
            var notice = dropped > 0
                ? $"{timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture)} [WRN] QueueLoom: " +
                  $"{dropped.ToString(CultureInfo.InvariantCulture)} {DroppedNotice}.{Environment.NewLine}"
                : string.Empty;
            try
            {
                if (notice.Length > 0 || text is not null)
                {
                    AppendRetryingSharing(path, notice + text);
                }
            }
            catch
            {
                Interlocked.Add(ref _overflowCount, dropped);
                throw;
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // A locked or read-only log directory must not break the application; the lines count as dropped.
            Interlocked.Add(ref _overflowCount, lineCount);
        }
    }

    /// <summary>
    /// Appends, waiting out a brief sharing violation: QueueLoom's own writers are serialized by the lock, but another
    /// program (an antivirus scan, a viewer opening the file exclusively) can hold the file for a moment. An I/O failure
    /// is retried on the background writer (never a caller's thread) for at most <see cref="SharingRetryBudget"/>, and
    /// not once shutdown has given up; a failure that stays drops the line as before (counted and announced).
    /// </summary>
    private void AppendRetryingSharing(string path, string text)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                File.AppendAllText(path, text, Encoding.UTF8);
                return;
            }
            // Windows reports the holder as a sharing violation; Unix (where .NET emulates file sharing with advisory
            // locks) as a plain IOException. A missing directory is not waited out: the item's own recovery handles it.
            catch (IOException exception) when (exception is not DirectoryNotFoundException && !_abandoned && waited.Elapsed < SharingRetryBudget)
            {
                Thread.Sleep(10);
            }
        }
    }

    private static readonly TimeSpan SharingRetryBudget = TimeSpan.FromSeconds(2);

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
