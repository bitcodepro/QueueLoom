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
