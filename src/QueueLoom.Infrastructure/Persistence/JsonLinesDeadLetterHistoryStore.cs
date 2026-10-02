using System.Text;
using System.Text.Json;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// Dead-letter history in one JSON Lines file. Reads and updates share cross-process ownership. Samples older than
/// <see cref="DeadLetterHistory.Retention"/> are dropped when the file is rewritten, at most once a day.
/// </summary>
public sealed class JsonLinesDeadLetterHistoryStore(string file, TimeProvider? time = null) : IDeadLetterHistoryStore
{
    private readonly object _gate = new();
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private DateTimeOffset _lastCompaction;

    public void Append(DeadLetterHistorySample sample)
    {
        ArgumentNullException.ThrowIfNull(sample);
        lock (_gate)
        {
            using var ownership = OwnFile();
            var samples = Load();
            var previous = samples.LastOrDefault(item => item.ProfileId == sample.ProfileId);
            if (previous is not null && sample.At - previous.At < DeadLetterHistory.MinimumSpacing && previous.Total == sample.Total)
            {
                return;
            }

            samples.Add(sample);
            var now = _time.GetUtcNow();
            if (now - _lastCompaction > TimeSpan.FromDays(1) && samples[0].At < now - DeadLetterHistory.Retention)
            {
                samples.RemoveAll(item => item.At < now - DeadLetterHistory.Retention);
                Rewrite(samples);
                _lastCompaction = now;
                return;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            using var stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            AtomicFile.RestrictToCurrentUser(file);
            // Preserve a crash tail as evidence, but isolate the next complete record from it.
            if (stream.Length > 0)
            {
                stream.Seek(-1, SeekOrigin.End);
                if (stream.ReadByte() != '\n') stream.WriteByte((byte)'\n');
            }
            stream.Seek(0, SeekOrigin.End);
            stream.Write(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(sample) + "\n"));
            stream.Flush(flushToDisk: true);
        }
    }

    public IReadOnlyList<DeadLetterHistorySample> Read(Guid profileId, DateTimeOffset since)
    {
        lock (_gate)
        {
            using var ownership = OwnFile();
            return Load().Where(sample => sample.ProfileId == profileId && sample.At >= since).ToArray();
        }
    }

    private List<DeadLetterHistorySample> Load()
    {
        // Reload under ownership: another window may have appended or compacted the shared file.
        var samples = new List<DeadLetterHistorySample>();
        if (File.Exists(file))
        {
            foreach (var line in File.ReadLines(file))
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }
                try
                {
                    if (JsonSerializer.Deserialize<DeadLetterHistorySample>(line) is { Sources: not null } sample)
                    {
                        samples.Add(sample);
                    }
                }
                catch (JsonException)
                {
                    // A line cut short by a crash must not hide the rest of the history.
                }
            }
            samples.Sort((left, right) => left.At.CompareTo(right.At));
        }
        return samples;
    }

    private CrossProcessFileLock OwnFile() =>
        CrossProcessFileLock.AcquireAsync(file + ".lock", CancellationToken.None).GetAwaiter().GetResult();

    private void Rewrite(IReadOnlyList<DeadLetterHistorySample> samples)
    {
        AtomicFile.WriteTextAsync(file, string.Concat(samples.Select(sample => JsonSerializer.Serialize(sample) + "\n")),
            CancellationToken.None).GetAwaiter().GetResult();
    }
}
