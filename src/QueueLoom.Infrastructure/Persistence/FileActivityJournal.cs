using System.Globalization;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Abstractions;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>One durable file per action; a crash cannot corrupt previous records.</summary>
public sealed class FileActivityJournal(string directory) : IActivityViewJournal
{
    public DateTimeOffset? ClearViewCutoff => File.Exists(Path.Combine(directory, ".view-cutoff"))
        ? DateTimeOffset.Parse(File.ReadAllText(Path.Combine(directory, ".view-cutoff")), CultureInfo.InvariantCulture) : null;

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
            catch (JsonException) { /* A damaged record must not hide the remaining history. */ }
        }
        return records.OrderByDescending(r => r.Timestamp).ToArray();
    }
}
