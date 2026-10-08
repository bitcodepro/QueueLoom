using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using QueueLoom.Core.Abstractions;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// One file per action; a crash cannot corrupt previous records. Ordinary entries are written by one background
/// writer, so the window's thread never waits on the disk for them; the record before a destructive operation is
/// written and forced to disk before <see cref="Append"/> returns. Closing writes what is queued, waiting at most
/// <see cref="CloseDeadline"/>.
/// </summary>
public sealed class FileActivityJournal(string directory) : IActivityViewJournal, IReportsActivityWriteFailures, IAsyncDisposable, IDisposable
{
    // Records to write, in order, and markers a caller waits on until everything before them is written.
    private readonly Channel<object> _queue = Channel.CreateUnbounded<object>(new UnboundedChannelOptions { SingleReader = true });
    // Queued but not yet on disk, so reading meanwhile still shows them.
    private readonly List<ActivityRecord> _pending = [];
    private readonly object _writerGate = new();
    private Task? _writer;
    private bool _closed;

    public event Action<Exception>? EntryWriteFailed;

    /// <summary>How long closing waits for queued entries to be written.</summary>
    public TimeSpan CloseDeadline { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Tests hold the writer just before it writes an entry.</summary>
    internal Func<Task>? BeforeEntryWrite { get; set; }

    public DateTimeOffset? ClearViewCutoff => File.Exists(Path.Combine(directory, ".view-cutoff")) &&
        DateTimeOffset.TryParse(File.ReadAllText(Path.Combine(directory, ".view-cutoff")), CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind, out var cutoff) ? cutoff : null;

    public void SetClearViewCutoff(DateTimeOffset? cutoff)
    {
        Directory.CreateDirectory(directory);
        using var ownership = CrossProcessFileLock.AcquireAsync(Path.Combine(directory, ".view-lock"), CancellationToken.None).GetAwaiter().GetResult();
        if (cutoff is null) { File.Delete(Path.Combine(directory, ".view-cutoff")); return; }
        var existing = ClearViewCutoff;
        if (existing > cutoff) cutoff = existing;
        AtomicFile.WriteTextAsync(Path.Combine(directory, ".view-cutoff"), cutoff.Value.ToString("O", CultureInfo.InvariantCulture), CancellationToken.None).GetAwaiter().GetResult();
    }
    public void Append(ActivityRecord record) => Write(record, durable: true);

    public void AppendEntry(ActivityRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        lock (_writerGate)
        {
            if (!_closed)
            {
                lock (_pending) _pending.Add(record);
                _queue.Writer.TryWrite(record);
                _writer ??= Task.Run(WriteQueuedEntriesAsync);
                return;
            }
        }
        // After closing nothing writes the queue any more: write it here, so the entry is not lost.
        Write(record, durable: false);
    }

    /// <summary>Completes once every entry queued before this call is written (or failed).</summary>
    internal Task WaitForPendingEntriesAsync()
    {
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_writerGate)
        {
            if (_closed || _writer is null) return Task.CompletedTask;
            _queue.Writer.TryWrite(written);
        }
        return written.Task;
    }

    private async Task WriteQueuedEntriesAsync()
    {
        await foreach (var item in _queue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            if (item is TaskCompletionSource marker)
            {
                marker.TrySetResult();
                continue;
            }
            var record = (ActivityRecord)item;
            try
            {
                if (BeforeEntryWrite is { } hold) await hold().ConfigureAwait(false);
                Write(record, durable: false);
            }
            catch (Exception exception)
            {
                // One entry that cannot be written does not stop the ones after it; the window is told.
                try { EntryWriteFailed?.Invoke(exception); } catch { }
            }
            finally
            {
                lock (_pending) _pending.Remove(record);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        Task? writer;
        lock (_writerGate)
        {
            if (_closed) return;
            _closed = true;
            _queue.Writer.TryComplete();
            writer = _writer;
        }
        if (writer is null) return;
        try { await writer.WaitAsync(CloseDeadline).ConfigureAwait(false); }
        catch (TimeoutException) { /* The disk does not answer: closing is not held by it. */ }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>Tests observe each record forced to disk.</summary>
    internal static readonly AsyncLocal<Action?> ForcedToDisk = new();

    private void Write(ActivityRecord record, bool durable)
    {
        var day = Path.Combine(directory, record.Timestamp.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        Directory.CreateDirectory(day);
        AtomicFile.RestrictDirectoryToCurrentUser(day);
        var target = Path.Combine(day, $"{record.Timestamp.UtcTicks}-{Guid.NewGuid():N}.json");
        var temporary = target + ".tmp";
        try
        {
            using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                AtomicFile.RestrictToCurrentUser(temporary);
                var bytes = JsonSerializer.SerializeToUtf8Bytes(record);
                stream.Write(bytes);
                // The rename below keeps a reader from ever seeing a half-written record either way; forcing the
                // bytes to disk is what costs a disk round trip, so only durable records pay it.
                stream.Flush(flushToDisk: durable);
                if (durable) ForcedToDisk.Value?.Invoke();
            }
            File.Move(temporary, target);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    public IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500)
    {
        // Queued entries are taken first: one the writer saves and dequeues while the disk is read is then still in
        // this snapshot, and a first entry still queued is listed even before the folder exists.
        ActivityRecord[] queued;
        lock (_pending) queued = [.. _pending];
        var cutoff = ClearViewCutoff;
        var records = queued.Where(record => cutoff is null || record.Timestamp > cutoff).ToList();
        if (!Directory.Exists(directory)) return records.OrderByDescending(r => r.Timestamp).Take(maximum).ToArray();
        foreach (var file in NewestRecordFiles(maximum))
        {
            try
            {
                var record = JsonSerializer.Deserialize<ActivityRecord>(File.ReadAllText(file));
                if (record is not null && (cutoff is null || record.Timestamp > cutoff)) records.Add(record);
            }
            // A damaged record must not hide the remaining history: invalid JSON, valid JSON the model refuses
            // (an entity without a name throws ArgumentException), or a file that cannot be read.
            catch (Exception exception) when (exception is JsonException or ArgumentException or NotSupportedException or
                                                  IOException or UnauthorizedAccessException) { }
        }
        // An entry written just now can be both on disk and in the queued snapshot: it is listed once.
        return records.Distinct().OrderByDescending(r => r.Timestamp).Take(maximum).ToArray();
    }

    /// <summary>
    /// The newest <paramref name="maximum"/> record files, newest first (by path, as records are named by day and time).
    /// Day folders are visited newest first and the walk stops once enough are found, instead of listing and sorting
    /// every record of the retention period (over a hundred thousand files with a busy monitor) at each start. A day
    /// folder that cannot be read is skipped, so it does not hide the rest of the history.
    /// </summary>
    /// <summary>Tests act between listing the day folders and reading them.</summary>
    internal static readonly AsyncLocal<Action?> AfterDaysListed = new();

    private IEnumerable<string> NewestRecordFiles(int maximum)
    {
        var options = new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = true };
        var candidates = Directory.EnumerateFiles(directory, "*.json", new EnumerationOptions { IgnoreInaccessible = true }).ToList();
        var fromDays = 0;
        var days = Directory.EnumerateDirectories(directory).OrderDescending(StringComparer.Ordinal).ToList();
        AfterDaysListed.Value?.Invoke();
        foreach (var day in days)
        {
            if (fromDays >= maximum)
            {
                break;
            }
            // A folder named after a later day sorts after every file of an earlier one, so older days cannot hold
            // newer records than those already found.
            List<string> files;
            try
            {
                files = Directory.EnumerateFiles(day, "*.json", options).ToList();
            }
            // Retention (this process or an MCP server) may have removed an expired day since it was listed, or the
            // folder itself cannot be opened: what was found in newer days still counts.
            catch (Exception exception) when (exception is DirectoryNotFoundException or UnauthorizedAccessException or IOException)
            {
                continue;
            }
            candidates.AddRange(files);
            fromDays += files.Count;
        }
        return candidates.OrderDescending(StringComparer.Ordinal).Take(maximum);
    }

    /// <summary>
    /// Deletes records older than <paramref name="cutoff"/> by their own timestamp (see <see cref="LocalHistoryRetention"/>).
    /// A damaged record has no trustworthy timestamp, so its file time decides. Only record files (and leftover
    /// temporary files) inside dated day folders of this journal are touched; the view cutoff and locks stay.
    /// Best effort: anything that cannot be read or deleted now is left for the next run.
    /// </summary>
    /// <returns>The number of files deleted.</returns>
    public int DeleteExpired(DateTimeOffset cutoff)
    {
        if (!Directory.Exists(directory)) return 0;
        // The desktop app and MCP servers share this journal; whoever holds the lock cleans up and the others skip.
        using var ownership = CrossProcessFileLock.TryAcquire(Path.Combine(directory, ".retention.lock"));
        if (ownership is null) return 0;
        var cutoffDay = cutoff.UtcDateTime.Date;
        var deleted = 0;
        foreach (var dayFolder in Directory.GetDirectories(directory))
        {
            // Append files a record under its UTC day, so a later day folder cannot hold an expired record.
            if (!DateTime.TryParseExact(Path.GetFileName(dayFolder), "yyyy-MM-dd", CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var day) || day > cutoffDay) continue;
            try
            {
                if (File.GetAttributes(dayFolder).HasFlag(FileAttributes.ReparsePoint)) continue; // never follow links out
                foreach (var file in Directory.GetFiles(dayFolder))
                {
                    try
                    {
                        if (RecordTime(file) is { } written && written < cutoff)
                        {
                            File.Delete(file);
                            deleted++;
                        }
                    }
                    catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
                }
                // Only a day wholly before the cutoff is finished; the folder Append is writing to is never removed.
                if (day < cutoffDay && !Directory.EnumerateFileSystemEntries(dayFolder).Any()) Directory.Delete(dayFolder);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
        }
        return deleted;
    }

    /// <summary>The record's own timestamp, or the file time for a damaged record or an interrupted write; null for
    /// files that are not journal records.</summary>
    private static DateTimeOffset? RecordTime(string file)
    {
        var name = Path.GetFileName(file);
        if (name.EndsWith(".json.tmp", StringComparison.Ordinal)) return File.GetLastWriteTimeUtc(file);
        if (!name.EndsWith(".json", StringComparison.Ordinal)) return null;
        try
        {
            var record = JsonSerializer.Deserialize<ActivityRecord>(File.ReadAllText(file));
            if (record is not null && record.Timestamp != default) return record.Timestamp;
        }
        // The same damage ReadRecent tolerates: invalid JSON or valid JSON the model refuses.
        catch (Exception exception) when (exception is JsonException or ArgumentException or NotSupportedException) { }
        return File.GetLastWriteTimeUtc(file);
    }
}
