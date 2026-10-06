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
            var samples = WithTail(Load());
            var previous = samples.LastOrDefault(item => item.ProfileId == sample.ProfileId);
            if (previous is not null && sample.At - previous.At < DeadLetterHistory.MinimumSpacing && previous.Total == sample.Total)
            {
                return;
            }

            var now = _time.GetUtcNow();
            var oldest = samples.Append(sample).Min(item => item.At);
            if (now - _lastCompaction > TimeSpan.FromDays(1) && oldest < now - DeadLetterHistory.Retention)
            {
                // The valid record of a crash tail (complete but without its newline) is kept too.
                var kept = samples.Append(sample).Where(item => item.At >= now - DeadLetterHistory.Retention)
                    .OrderBy(item => item.At).ToArray();
                // The cache is read again from the rewritten file, whether or not the rewrite succeeds.
                Reset();
                Rewrite(kept);
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
            // The cache is not changed here: the next Load reads this line from the file like any other appended line.
        }
    }

    public IReadOnlyList<DeadLetterHistorySample> Read(Guid profileId, DateTimeOffset since)
    {
        lock (_gate)
        {
            using var ownership = OwnFile();
            return WithTail(Load()).Where(sample => sample.ProfileId == profileId && sample.At >= since).ToArray();
        }
    }

    // The samples read so far, the file length they cover (always just after a newline), the file's first bytes and its
    // generation. The file only grows between compactions, so a later Load reads just the lines added since, by this or
    // another process. Every compaction first writes a new generation next to the file, and a changed generation (or
    // a shorter file, or other first bytes, for a file rewritten some other way) makes the next Load read everything
    // again. Before, every monitor check parsed the whole 30-day file.
    private readonly List<DeadLetterHistorySample> _samples = [];
    private long _readLength;
    private byte[] _head = [];
    private string? _generation;
    private const int HeadBytes = 4096;

    // A complete record after the last newline (a crash before its newline was written). It is not part of the
    // samples read so far: an append terminates it and the next Load reads it as an ordinary line.
    private DeadLetterHistorySample? _tail;

    private string GenerationFile => file + ".generation";

    private List<DeadLetterHistorySample> WithTail(List<DeadLetterHistorySample> samples) =>
        _tail is null ? samples : [.. samples, _tail];

    private List<DeadLetterHistorySample> Load()
    {
        // Reload under ownership: another window may have appended or compacted the shared file.
        _tail = null;
        var generation = ReadGeneration();
        if (generation is null || generation != _generation)
        {
            Reset();
            _generation = generation;
        }
        if (!File.Exists(file))
        {
            Reset();
            return _samples;
        }
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        if (stream.Length < _readLength || !HeadMatches(stream))
        {
            Reset();
        }
        stream.Position = _readLength;
        var added = new byte[stream.Length - _readLength];
        stream.ReadExactly(added);
        // Only complete lines; a crash tail without its newline is read once a later append terminates it.
        var end = Array.LastIndexOf(added, (byte)'\n') + 1;
        var appended = 0;
        foreach (var line in Encoding.UTF8.GetString(added, 0, end).Split('\n'))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            try
            {
                if (JsonSerializer.Deserialize<DeadLetterHistorySample>(line) is { Sources: not null } sample)
                {
                    _samples.Add(sample);
                    appended++;
                }
            }
            catch (JsonException)
            {
                // A line cut short by a crash must not hide the rest of the history.
            }
        }
        if (appended > 0)
        {
            _samples.Sort((left, right) => left.At.CompareTo(right.At));
        }
        _readLength += end;
        if (Encoding.UTF8.GetString(added, end, added.Length - end) is { } rest && !string.IsNullOrWhiteSpace(rest))
        {
            try
            {
                _tail = JsonSerializer.Deserialize<DeadLetterHistorySample>(rest) is { Sources: not null } complete ? complete : null;
            }
            catch (JsonException)
            {
                // Cut short: nothing to keep.
            }
        }
        if (_head.Length < HeadBytes && _readLength > _head.Length)
        {
            stream.Position = 0;
            _head = new byte[(int)Math.Min(HeadBytes, _readLength)];
            stream.ReadExactly(_head);
        }
        return _samples;
    }

    private bool HeadMatches(FileStream stream)
    {
        if (_head.Length == 0)
        {
            return true;
        }
        stream.Position = 0;
        var current = new byte[_head.Length];
        stream.ReadExactly(current);
        return current.AsSpan().SequenceEqual(_head);
    }

    private void Reset()
    {
        _samples.Clear();
        _readLength = 0;
        _head = [];
    }

    /// <summary>The current generation; "" before any compaction, null when it cannot be read (then nothing is reused).</summary>
    private string? ReadGeneration()
    {
        try
        {
            return File.Exists(GenerationFile) ? File.ReadAllText(GenerationFile) : string.Empty;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private CrossProcessFileLock OwnFile() =>
        CrossProcessFileLock.AcquireAsync(file + ".lock", CancellationToken.None).GetAwaiter().GetResult();

    private void Rewrite(IReadOnlyList<DeadLetterHistorySample> samples)
    {
        // The new generation first: if the rewrite fails after it, readers only read everything again.
        AtomicFile.WriteTextAsync(GenerationFile, Guid.NewGuid().ToString("N"), CancellationToken.None).GetAwaiter().GetResult();
        AtomicFile.WriteTextAsync(file, string.Concat(samples.Select(sample => JsonSerializer.Serialize(sample) + "\n")),
            CancellationToken.None).GetAwaiter().GetResult();
    }
}
