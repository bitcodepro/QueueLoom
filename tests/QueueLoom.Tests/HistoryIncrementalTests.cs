using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.Monitoring;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class HistoryIncrementalTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-02T12:00:00Z");
    private static readonly Guid Profile = Guid.NewGuid();

    private static DeadLetterHistorySample Sample(DateTimeOffset at, long total) =>
        new(at, Profile, "Test", total, Enumerable.Range(0, 25).ToDictionary(index => $"orders-{index:00} (DLQ)", _ => total));

    private sealed class Clock : TimeProvider { public override DateTimeOffset GetUtcNow() => Now; }

    // A month of history (one sample a minute, 25 queues each) is read once; each monitor check then appends without
    // parsing the whole file again. Before, every check parsed all of it on the window's thread.
    [Fact]
    public void AppendingToALongHistoryDoesNotReadItAllAgain()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        using (var writer = new StreamWriter(file))
        {
            for (var minute = 30 * 24 * 60; minute > 0; minute--)
            {
                writer.Write(JsonSerializer.Serialize(Sample(Now.AddMinutes(-minute), minute)) + "\n");
            }
        }
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        store.Read(Profile, Now.AddHours(-1));

        var watch = Stopwatch.StartNew();
        for (var check = 1; check <= 20; check++)
        {
            store.Append(Sample(Now.AddMinutes(check), check));
        }
        watch.Stop();

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), $"20 appends took {watch.Elapsed}.");
        Assert.Equal(Enumerable.Range(1, 20).Select(value => (long)value), store.Read(Profile, Now).Select(sample => sample.Total));
    }

    // Another process appends and later compacts the shared file: each read still shows exactly what the file holds.
    [Fact]
    public void ReadsFollowAnotherWritersAppendsAndCompaction()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        File.WriteAllText(file, JsonSerializer.Serialize(Sample(Now.AddDays(-40), 1)) + "\n" +
                                JsonSerializer.Serialize(Sample(Now.AddMinutes(-10), 2)) + "\n");
        var reader = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal([1L, 2L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        // Another window: its first append compacts away the 40-day-old sample and rewrites the file.
        new JsonLinesDeadLetterHistoryStore(file, new Clock()).Append(Sample(Now.AddMinutes(-5), 3));
        Assert.Equal([2L, 3L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        File.AppendAllText(file, JsonSerializer.Serialize(Sample(Now.AddMinutes(-4), 4)) + "\n");
        Assert.Equal([2L, 3L, 4L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        reader.Append(Sample(Now.AddMinutes(-3), 5));
        Assert.Equal([2L, 3L, 4L, 5L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
        Assert.Equal([2L, 3L, 4L, 5L], new JsonLinesDeadLetterHistoryStore(file, new Clock()).Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
    }

    // A compaction can leave the file's length and first bytes unchanged: an out-of-order file A (kept, over 4 KB),
    // B (expired), C becomes A, C, D when D is the size of B. A warm reader must still see D, and its next append must
    // not compact D away.
    [Fact]
    public void ACompactionThatKeepsLengthAndFirstBytesIsStillNoticed()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        var big = Enumerable.Range(0, 25).ToDictionary(index => new string('x', 200) + index.ToString("00"), _ => 1L);
        DeadLetterHistorySample Record(DateTimeOffset at, long total, IReadOnlyDictionary<string, long>? sources = null) =>
            new(at, Profile, "Test", total, sources ?? new Dictionary<string, long> { ["q"] = total });
        var a = Record(Now.AddDays(-20), 1, big);
        var b = Record(Now.AddDays(-40), 2);
        var c = Record(Now.AddDays(-10), 3);
        var d = Record(Now.AddDays(-5), 4);
        Assert.True(JsonSerializer.Serialize(a).Length > 4096);
        Assert.Equal(JsonSerializer.Serialize(b).Length, JsonSerializer.Serialize(d).Length);
        File.WriteAllText(file, string.Concat(new[] { a, b, c }.Select(sample => JsonSerializer.Serialize(sample) + "\n")));
        var lengthBefore = new FileInfo(file).Length;
        var reader = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal([2L, 1L, 3L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        new JsonLinesDeadLetterHistoryStore(file, new Clock()).Append(d);
        Assert.Equal(lengthBefore, new FileInfo(file).Length);

        Assert.Equal([1L, 3L, 4L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
        reader.Append(Record(Now.AddDays(-1), 5));
        Assert.Equal([1L, 3L, 4L, 5L], new JsonLinesDeadLetterHistoryStore(file, new Clock()).Read(Profile, DateTimeOffset.MinValue)
            .Select(sample => sample.Total));
    }

    // A complete record without its newline (a crash) after an expired one: the compaction run by the next append keeps
    // it, once.
    [Fact]
    public void ACompactionKeepsAValidRecordThatLostItsNewline()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        File.WriteAllText(file, JsonSerializer.Serialize(Sample(Now.AddDays(-40), 1)) + "\n" + JsonSerializer.Serialize(Sample(Now.AddMinutes(-5), 2)));
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal([1L, 2L], store.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        store.Append(Sample(Now.AddMinutes(-1), 3));

        Assert.Equal([2L, 3L], store.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
        Assert.Equal([2L, 3L], new JsonLinesDeadLetterHistoryStore(file, new Clock()).Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
    }

    // A version without generations (still running during an update) compacts the file in place of a newer one:
    // A, B, C becomes A, C, D at the same length and with the same first 4 KB, and no generation is written.
    [Fact]
    public void ACompactionByAnOlderVersionIsStillNoticed()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        var big = Enumerable.Range(0, 25).ToDictionary(index => new string('x', 200) + index.ToString("00"), _ => 1L);
        DeadLetterHistorySample Record(DateTimeOffset at, long total, IReadOnlyDictionary<string, long>? sources = null) =>
            new(at, Profile, "Test", total, sources ?? new Dictionary<string, long> { ["q"] = total });
        var a = Record(Now.AddDays(-20), 1, big);
        var b = Record(Now.AddDays(-40), 2);
        var c = Record(Now.AddDays(-10), 3);
        var d = Record(Now.AddDays(-5), 4);
        string Lines(params DeadLetterHistorySample[] samples) => string.Concat(samples.Select(sample => JsonSerializer.Serialize(sample) + "\n"));
        File.WriteAllText(file, Lines(a, b, c));
        var reader = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal([2L, 1L, 3L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        // The older version's compaction: write a temporary file and move it over the history, nothing else.
        File.WriteAllText(file + ".tmp", Lines(a, c, d));
        Assert.Equal(new FileInfo(file).Length, new FileInfo(file + ".tmp").Length);
        File.Move(file + ".tmp", file, overwrite: true);

        Assert.Equal([1L, 3L, 4L], reader.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
    }

    // A valid record that lost its newline can be older than the lines before it. The latest sample decides the
    // spacing: a changed count is recorded, and reads stay in time order.
    [Fact]
    public void AnOlderUnterminatedRecordDoesNotHideANewerCount()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        File.WriteAllText(file, JsonSerializer.Serialize(Sample(Now, 2)) + "\n" + JsonSerializer.Serialize(Sample(Now.AddSeconds(-10), 1)));
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal([1L, 2L], store.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));

        store.Append(Sample(Now.AddSeconds(10), 1));

        Assert.Equal([1L, 2L, 1L], new JsonLinesDeadLetterHistoryStore(file, new Clock()).Read(Profile, DateTimeOffset.MinValue)
            .Select(sample => sample.Total));
    }

    // The file is deleted (history cleared) and written anew: nothing from before is shown.
    [Fact]
    public void ADeletedFileStartsAFreshHistory()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        store.Append(Sample(Now.AddMinutes(-5), 1));
        Assert.Single(store.Read(Profile, DateTimeOffset.MinValue));

        File.Delete(file);
        Assert.Empty(store.Read(Profile, DateTimeOffset.MinValue));
        store.Append(Sample(Now.AddMinutes(-4), 2));
        Assert.Equal([2L], store.Read(Profile, DateTimeOffset.MinValue).Select(sample => sample.Total));
    }
}
