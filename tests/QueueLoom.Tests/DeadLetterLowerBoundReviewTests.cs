using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A sampled (lower-bound) count keeps its quality through scans, the monitor, history and the views built on them.
public sealed partial class ViewModelStateTests
{
    // A scan row keeps the quality through its rebuild, a sampled zero stays listed, and its change is unknown.
    [Fact]
    public async Task ScanRowsKeepALowerBoundAndListASampledQueueThatShowedNothing()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Sampled(params (string Name, long Count)[] queues) => Snapshot(dev.Id,
            queues.Select(queue => new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue(queue.Name), queue.Count) { CountIsLowerBound = true }).ToArray());
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Sampled(("orders", 1_000), ("payments", 0)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();

        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();

        var orders = vm.DeadLetterSources.Single(row => row.EntityName == "orders");
        Assert.Equal(DeadLetterCountText.Format(1_000, isLowerBound: true), orders.CountText);
        Assert.Equal("unknown", orders.Delta);
        Assert.Equal("none seen", vm.DeadLetterSources.Single(row => row.EntityName == "payments").CountText);
        Assert.EndsWith("+", vm.GlobalDlqDisplay, StringComparison.Ordinal);
        Assert.Contains("+ dead-letter messages", vm.StatusText, StringComparison.Ordinal);
    }

    // Samples 1,000, 300, 1,000 are not "increased by 700". Exact then a sampled zero keeps the incident but no longer shows
    // its old number as exact. A sampled count above an exact one is proven growth and alerts.
    [Fact]
    public async Task TheMonitorNeverInventsGrowthFromSamples()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterSnapshot Count(long count, bool sampled) => Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, count) { CountIsLowerBound = sampled });
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Count(1_000, true) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        await Check();
        workspace.Snapshots[dev.Id] = Count(300, true);
        await Check();
        workspace.Snapshots[dev.Id] = Count(1_000, true);
        await Check();
        Assert.DoesNotContain("increased", vm.MonitorAlert, StringComparison.Ordinal);

        workspace.Snapshots[dev.Id] = Count(50, false);
        await Check();
        Assert.DoesNotContain("increased", vm.MonitorAlert, StringComparison.Ordinal);
        var notification = Assert.Single(vm.MonitorNotifications);
        workspace.Snapshots[dev.Id] = Count(0, true);
        await Check();
        Assert.Same(notification, Assert.Single(vm.MonitorNotifications));
        Assert.True(notification.CountIsLowerBound);

        workspace.Snapshots[dev.Id] = Count(60, false);
        await Check();
        Assert.False(notification.CountIsLowerBound);
        workspace.Snapshots[dev.Id] = Count(80, true);
        await Check();
        Assert.Contains("increased by 20", vm.MonitorAlert, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, false, 5L, false, null)]
    [InlineData(10L, false, 15L, false, 5L)]
    [InlineData(10L, false, 15L, true, 5L)]
    [InlineData(10L, false, 5L, true, null)]
    [InlineData(1_000L, true, 5_000L, false, null)]
    [InlineData(1_000L, true, 1_000L, true, null)]
    public void OnlyProvenGrowthIsAnIncrease(long? before, bool beforeSampled, long now, bool nowSampled, long? expected) =>
        Assert.Equal(expected, DeadLetterMeasurement.ProvenIncrease(
            before is { } value ? new DeadLetterMeasurement(value, beforeSampled) : null, new DeadLetterMeasurement(now, nowSampled)));

    // An empty Pub/Sub sample written to the store, read back and summarised is a listed queue with an unknown count.
    [Fact]
    public async Task AnEmptySampleReadBackFromHistoryIsNotShownAsEmptyQueues()
    {
        using var directory = new TemporaryDirectory();
        var profile = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        var store = new JsonLinesDeadLetterHistoryStore(Path.Combine(directory.Path, "history.jsonl"));
        await store.AppendAsync(DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profile, at,
            [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 0) { CountIsLowerBound = true }]), "Test"));

        var summary = DeadLetterHistory.Summarize(await store.ReadAsync(profile, at.AddMinutes(-1)), at.AddMinutes(-1), at.AddMinutes(1))!;

        var row = Assert.Single(summary.Sources);
        Assert.Equal("orders", row.Name);
        Assert.True(row.NowIsLowerBound);
        Assert.Equal("none seen", new DeadLetterTrendItemViewModel(row.Name, row.Now, row.Change) { NowIsLowerBound = row.NowIsLowerBound }.NowText);
        Assert.True(summary.NowIsLowerBound);
    }

    // Within a minute, the same total of another quality is kept; downsampling keeps a bucket's lower bound.
    [Fact]
    public async Task HistoryKeepsAQualityChangeAndDownsamplingKeepsUncertainty()
    {
        using var directory = new TemporaryDirectory();
        var profile = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        var store = new JsonLinesDeadLetterHistoryStore(Path.Combine(directory.Path, "history.jsonl"));
        DeadLetterSnapshot Hundred(DateTimeOffset when, bool sampled) => new(profile, when,
            [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 100) { CountIsLowerBound = sampled }]);
        await store.AppendAsync(DeadLetterHistorySample.FromSnapshot(Hundred(at, false), "Test"));
        await store.AppendAsync(DeadLetterHistorySample.FromSnapshot(Hundred(at.AddSeconds(20), true), "Test"));
        var samples = await store.ReadAsync(profile, at.AddMinutes(-1));
        Assert.Equal([false, true], samples.Select(sample => sample.TotalIsLowerBound));

        var many = Enumerable.Range(0, 10).Select(minute => DeadLetterHistorySample.FromSnapshot(
            new DeadLetterSnapshot(profile, at.AddMinutes(minute),
                [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), minute == 3 ? 5 : 50) { CountIsLowerBound = minute == 3 }]), "Test")).ToArray();
        var summary = DeadLetterHistory.Summarize(many, at, at.AddMinutes(10), maximumPoints: 2)!;
        Assert.Contains(summary.Points, point => point.IsLowerBound);
    }
}
