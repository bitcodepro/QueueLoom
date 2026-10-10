using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.Tests;

// A sample keeps only the 25 largest dead-letter queues. A queue missing from a full (truncated) sample was counted as 0,
// so a queue that grew from 40 (26th place at the start) to 500 showed "+500 from 0", and one that fell out of the top 25
// looked emptied. Missing from a truncated sample is now unknown; missing from a sample that kept every non-empty queue
// is still 0.
public sealed class DeadLetterHistoryTruncationTests
{
    private static readonly DateTimeOffset From = new(2026, 10, 9, 0, 0, 0, TimeSpan.Zero);
    private static readonly Guid Profile = Guid.NewGuid();

    [Fact]
    public void AQueueBelowTheTop25AtTheStartHasAnUnknownStartNotZero()
    {
        var first = Sample(From.AddMinutes(1), Enumerable.Range(1, 25).ToDictionary(i => $"big-{i}", i => 1_000L + i));
        var last = Sample(From.AddMinutes(30), new Dictionary<string, long> { ["grown"] = 500 });

        var trend = Trend(first, last, "grown");

        Assert.Null(trend.Start);
        Assert.Equal(500, trend.Now);
        Assert.Null(trend.Change);
    }

    [Fact]
    public void AQueueThatFellBelowTheTop25HasAnUnknownNowNotZero()
    {
        var first = Sample(From.AddMinutes(1), new Dictionary<string, long> { ["shrunk"] = 40 });
        var last = Sample(From.AddMinutes(30), Enumerable.Range(1, 25).ToDictionary(i => $"big-{i}", i => 1_000L + i));

        var trend = Trend(first, last, "shrunk");

        Assert.Equal(40, trend.Start);
        Assert.Null(trend.Now);
        Assert.Null(trend.Change);
    }

    [Fact]
    public void AQueueMissingFromASampleThatKeptEveryQueueIsZero()
    {
        var first = Sample(From.AddMinutes(1), new Dictionary<string, long> { ["other"] = 3 });
        var last = Sample(From.AddMinutes(30), new Dictionary<string, long> { ["new"] = 7, ["other"] = 3 });

        var trend = Trend(first, last, "new");

        Assert.Equal(0, trend.Start);
        Assert.Equal(7, trend.Now);
        Assert.Equal(7, trend.Change);
    }

    [Fact]
    public void TheMonitorsRowSaysAnUnknownChangeInsteadOfInventingOne()
    {
        var row = new DeadLetterTrendItemViewModel("grown", 500, null);

        Assert.Equal("500", row.NowText);
        Assert.False(row.IsRising);
        Assert.False(row.IsUnchanged);
        Assert.Contains("not tracked", row.ChangeText, StringComparison.Ordinal);
    }

    private static DeadLetterSourceTrend Trend(DeadLetterHistorySample first, DeadLetterHistorySample last, string name) =>
        DeadLetterHistory.Summarize([first, last], From, From.AddHours(1), maximumSources: 50)!.Sources.Single(source => source.Name == name);

    private static DeadLetterHistorySample Sample(DateTimeOffset at, Dictionary<string, long> sources) =>
        new(at, Profile, "Test", sources.Values.Sum(), sources, QueueLoom.Core.Monitoring.DeadLetterCountQuality.Exact);
}
