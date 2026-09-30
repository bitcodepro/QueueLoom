using QueueLoom.App.Controls;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed class DeadLetterHistoryTests : IDisposable
{
    private static readonly Guid Profile = Guid.NewGuid();
    private static readonly DateTimeOffset Start = DateTimeOffset.Parse("2026-09-01T10:00:00Z");
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "history", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Fact]
    public void Sample_NamesSourcesAndKeepsOnlyNonEmptyQueues()
    {
        var snapshot = new DeadLetterSnapshot(Profile, Start,
        [
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 5),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 2, subQueue: ServiceBusSubQueue.TransferDeadLetter),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Subscription("events", "audit"), 0)
        ]);

        var sample = DeadLetterHistorySample.FromSnapshot(snapshot, "Staging");

        Assert.Equal(7, sample.Total);
        Assert.Equal(new Dictionary<string, long> { ["orders"] = 5, ["orders (transfer)"] = 2 }, sample.Sources);
    }

    [Fact]
    public void Summary_KeepsShortSpikesWhenThereAreMoreSamplesThanPoints()
    {
        var samples = Enumerable.Range(0, 1_000)
            .Select(minute => Sample(Start.AddMinutes(minute), minute == 437 ? 900 : 10))
            .ToArray();

        var summary = DeadLetterHistory.Summarize(samples, Start, Start.AddMinutes(1_000), maximumPoints: 100)!;

        Assert.True(summary.Points.Count <= 100);
        Assert.Equal(900, summary.Peak.Count);
        Assert.Contains(summary.Points, point => point.Count == 900);
        Assert.Equal(1_000, summary.SampleCount);
    }

    [Fact]
    public void Summary_ShowsHowEachQueueChangedOverThePeriod()
    {
        var samples = new[]
        {
            Sample(Start, 12, ("orders", 10), ("billing", 2)),
            Sample(Start.AddHours(1), 20, ("orders", 4), ("payments", 16))
        };

        var summary = DeadLetterHistory.Summarize(samples, Start, Start.AddHours(2))!;

        Assert.Equal(8, summary.Change);
        Assert.Equal(
            [("payments", 16L, 16L), ("orders", 4L, -6L), ("billing", 0L, -2L)],
            summary.Sources.Select(source => (source.Name, source.Now, source.Change)));
        Assert.Null(DeadLetterHistory.Summarize(samples, Start.AddHours(3), Start.AddHours(4)));
    }

    [Fact]
    public void Store_KeepsSamplesAcrossRestartsAndSkipsRepeatsWithinAMinute()
    {
        var file = Path.Combine(_directory, "dlq-history.jsonl");
        var store = new JsonLinesDeadLetterHistoryStore(file);
        store.Append(Sample(Start, 3));
        store.Append(Sample(Start.AddSeconds(20), 3));
        store.Append(Sample(Start.AddSeconds(40), 4));
        store.Append(Sample(Start.AddMinutes(2), 4));
        File.AppendAllText(file, "{\"At\": broken\n");

        var reopened = new JsonLinesDeadLetterHistoryStore(file);

        Assert.Equal([3L, 4L, 4L], reopened.Read(Profile, Start).Select(sample => sample.Total));
        Assert.Empty(reopened.Read(Guid.NewGuid(), Start));
    }

    [Fact]
    public void Store_DropsSamplesOlderThanThirtyDays()
    {
        var file = Path.Combine(_directory, "dlq-history.jsonl");
        var now = new FixedTime(Start.AddDays(40));
        var store = new JsonLinesDeadLetterHistoryStore(file, now);
        store.Append(Sample(Start, 1));
        store.Append(Sample(Start.AddDays(35), 2));

        Assert.Equal([2L], new JsonLinesDeadLetterHistoryStore(file, now).Read(Profile, Start).Select(sample => sample.Total));
    }

    [Theory]
    [InlineData(0, 3)]
    [InlineData(4, 6)]
    [InlineData(7, 9)]
    [InlineData(95, 150)]
    [InlineData(1_234, 1_500)]
    public void ChartAxis_UsesRoundSteps(long peak, long expected) => Assert.Equal(expected, HistoryChart.AxisMaximum(peak));

    private static DeadLetterHistorySample Sample(DateTimeOffset at, long total, params (string Name, long Count)[] sources) =>
        new(at, Profile, "Staging", total, sources.ToDictionary(source => source.Name, source => source.Count));

    private sealed class FixedTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
