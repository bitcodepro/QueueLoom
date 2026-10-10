using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// Dead-letter history in one JSON Lines file. Reads and updates share cross-process ownership. Samples older than
/// <see cref="DeadLetterHistory.Retention"/> are dropped when the file is rewritten, at most once a day. The
/// cross-process ownership is awaited and the file work runs on the thread pool: the window records a sample after every
/// monitor check, and waiting for another window or the MCP server synchronously froze it for up to the lock's 30 s.
/// </summary>
public sealed class JsonLinesDeadLetterHistoryStore(string file, TimeProvider? time = null) : IDeadLetterHistoryStore
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private DateTimeOffset _lastCompaction;

    public async Task AppendAsync(DeadLetterHistorySample sample, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sample);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var ownership = await OwnFileAsync(cancellationToken).ConfigureAwait(false);
            // Once owned, the sample is written in full: a cancellation must not leave a half-done compaction.
            await Task.Run(() => AppendOwned(sample), CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private void AppendOwned(DeadLetterHistorySample sample)
    {
        var samples = WithTail(Load());
        var previous = samples.LastOrDefault(item => item.ProfileId == sample.ProfileId);
        // Within a minute an unchanged sample adds nothing; a quality change (exact 100, then an estimated or sampled 100)
        // is a change and is kept.
        if (previous is not null && sample.At - previous.At < DeadLetterHistory.MinimumSpacing && previous.RecordsTheSameAs(sample))
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

    public async Task<IReadOnlyList<DeadLetterHistorySample>> ReadAsync(Guid profileId, DateTimeOffset since,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var ownership = await OwnFileAsync(cancellationToken).ConfigureAwait(false);
            return await Task.Run<IReadOnlyList<DeadLetterHistorySample>>(
                () => WithTail(Load()).Where(sample => sample.ProfileId == profileId && sample.At >= since).ToArray(),
                CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    // The samples read so far, the file length they cover (always just after a newline) and a SHA-256 hash of exactly
    // those bytes. A later Load re-hashes that region and, when it is unchanged, parses only the lines added since, by
    // this or another process. Any other change to the region (a compaction by any version, with or without the
    // generation below, an edit, a replaced file) makes the next Load read and parse everything again. Hashing reads
    // the file but parses nothing: before, every monitor check deserialized the whole 30-day file, which is what
    // stalled the window. The generation, written by every compaction of this version, adds an explicit signal.
    /// <summary>Tests count the history lines parsed, which does not depend on how fast the machine is.</summary>
    internal long LinesParsed { get; private set; }
    private readonly List<DeadLetterHistorySample> _samples = [];
    private long _readLength;
    private byte[] _prefixHash = [];
    private string? _generation;

    // A complete record after the last newline (a crash before its newline was written). It is not part of the
    // samples read so far: an append terminates it and the next Load reads it as an ordinary line.
    private DeadLetterHistorySample? _tail;

    private string GenerationFile => file + ".generation";

    // In time order: the tail can be older than lines read before it, and Append takes the profile's latest sample.
    private List<DeadLetterHistorySample> WithTail(List<DeadLetterHistorySample> samples) =>
        _tail is null ? samples : [.. samples.Append(_tail).OrderBy(sample => sample.At)];

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
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        if (_readLength > 0 && (stream.Length < _readLength || !PrefixMatches(stream, hash)))
        {
            Reset();
            hash.GetHashAndReset();
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
                LinesParsed++;
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
        hash.AppendData(added, 0, end);
        _prefixHash = hash.GetCurrentHash();
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
        return _samples;
    }

    /// <summary>Hashes the region read before into <paramref name="hash"/> and says whether it is unchanged.</summary>
    private bool PrefixMatches(FileStream stream, IncrementalHash hash)
    {
        stream.Position = 0;
        var buffer = new byte[1024 * 1024];
        var remaining = _readLength;
        while (remaining > 0)
        {
            var read = stream.Read(buffer, 0, (int)Math.Min(buffer.Length, remaining));
            if (read == 0)
            {
                return false;
            }
            hash.AppendData(buffer, 0, read);
            remaining -= read;
        }
        return hash.GetCurrentHash().AsSpan().SequenceEqual(_prefixHash);
    }

    private void Reset()
    {
        _samples.Clear();
        _readLength = 0;
        _prefixHash = [];
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

    private ValueTask<CrossProcessFileLock> OwnFileAsync(CancellationToken cancellationToken) =>
        CrossProcessFileLock.AcquireAsync(file + ".lock", cancellationToken);

    private void Rewrite(IReadOnlyList<DeadLetterHistorySample> samples)
    {
        // The new generation first: if the rewrite fails after it, readers only read everything again.
        AtomicFile.WriteTextAsync(GenerationFile, Guid.NewGuid().ToString("N"), CancellationToken.None).GetAwaiter().GetResult();
        AtomicFile.WriteTextAsync(file, string.Concat(samples.Select(sample => JsonSerializer.Serialize(sample) + "\n")),
            CancellationToken.None).GetAwaiter().GetResult();
    }
}
