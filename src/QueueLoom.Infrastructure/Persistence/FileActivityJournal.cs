using System.Globalization;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Abstractions;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>One durable file per action; a crash cannot corrupt previous records.</summary>
public sealed class FileActivityJournal(string directory) : IActivityViewJournal
{
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
    public void Append(ActivityRecord record)
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
                stream.Flush(true);
            }
            File.Move(temporary, target);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    public IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500)
    {
        if (!Directory.Exists(directory)) return [];
        var cutoff = ClearViewCutoff;
        var records = new List<ActivityRecord>();
        foreach (var file in Directory.EnumerateFiles(directory, "*.json", SearchOption.AllDirectories)
                     .OrderDescending(StringComparer.Ordinal).Take(maximum))
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
        return records.OrderByDescending(r => r.Timestamp).ToArray();
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
