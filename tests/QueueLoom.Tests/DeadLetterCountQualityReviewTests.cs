using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// What each count quality allows and forbids: arithmetic, unknown and partial counts, timing and readers, estimated
// zeros, history persistence and the emulator's capped peek.
public sealed partial class ViewModelStateTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    private static DeadLetterEntitySnapshot Counted(long? count, DeadLetterCountQuality quality, DateTimeOffset? at = null, string? from = null,
        string? error = null) =>
        new(Orders, count, null, error) { CountQuality = quality, MeasuredAt = at, MeasuredFrom = from };

    // Estimated 60, then at least 80: no growth is proven, the queue may have held 200 and fallen to 100.
    [Theory]
    [InlineData(60, DeadLetterCountQuality.Estimated, 80, DeadLetterCountQuality.LowerBound, null, null)]
    [InlineData(60, DeadLetterCountQuality.Exact, 80, DeadLetterCountQuality.LowerBound, 20L, DeadLetterCountQuality.LowerBound)]
    [InlineData(60, DeadLetterCountQuality.Estimated, 80, DeadLetterCountQuality.Exact, 20L, DeadLetterCountQuality.Estimated)]
    [InlineData(60, DeadLetterCountQuality.Unqualified, 80, DeadLetterCountQuality.Exact, null, null)]
    [InlineData(60, DeadLetterCountQuality.Exact, 80, DeadLetterCountQuality.Unknown, null, null)]
    public void GrowthIsOnlyWhatTheTwoCountsJustify(long before, DeadLetterCountQuality beforeQuality, long now, DeadLetterCountQuality nowQuality,
        long? expected, DeadLetterCountQuality? expectedQuality)
    {
        var increase = DeadLetterMeasurement.ProvenIncrease(new DeadLetterMeasurement(before, beforeQuality), new DeadLetterMeasurement(now, nowQuality));
        Assert.Equal(expected, increase?.Count);
        Assert.Equal(expectedQuality, increase?.Quality);
    }

    // Partial and failed scans: exact 5 plus a failed source is "at least 5", every source failing is unknown, and the
    // failed source itself is unknown, in the snapshot and in MCP.
    [Fact]
    public void UnreadableSourcesAreUnknownAndMakeTheTotalPartial()
    {
        var failed = new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("payments"), null, null, "timed out");
        var partial = new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(Orders, 5), failed]);
        var none = new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow, [failed]);

        Assert.Equal(DeadLetterCountQuality.Unknown, failed.CountQuality);
        Assert.Equal(DeadLetterCountQuality.LowerBound, partial.TotalQuality);
        Assert.Equal(DeadLetterCountQuality.Unknown, none.TotalQuality);
        Assert.Equal("unknown", McpMapping.ToInfo(failed).CountQuality);
        Assert.Equal("unknown", DeadLetterCountText.Format(0, none.TotalQuality));
    }

    // A queue a sample did not keep (no history, or truncated to the top 25) has an unknown count, not an exact one.
    [Fact]
    public void AnUnrecordedHistoryCountIsUnknownNotExact()
    {
        var profile = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        var first = new DeadLetterHistorySample(at, profile, "Test", 25_000,
            Enumerable.Range(1, 25).ToDictionary(i => $"big-{i}", i => 1_000L), DeadLetterCountQuality.Exact);
        var last = new DeadLetterHistorySample(at.AddMinutes(5), profile, "Test", 26_000,
            Enumerable.Range(1, 24).ToDictionary(i => $"big-{i}", i => 1_000L).Append(new("orders", 2_000L)).ToDictionary(pair => pair.Key, pair => pair.Value),
            DeadLetterCountQuality.Exact);

        var orders = DeadLetterHistory.Summarize([first, last], at, at.AddMinutes(10), maximumSources: 30)!.Sources.Single(source => source.Name == "orders");

        Assert.Null(orders.Start);
        Assert.Equal(DeadLetterCountQuality.Unknown, orders.StartQuality);
        Assert.Equal(DeadLetterCountQuality.Exact, orders.NowQuality);
        Assert.Null(orders.Change);
    }

    // History keeps a quality change with an unchanged total within a minute: exact 100, then estimated 100.
    [Fact]
    public async Task HistoryKeepsAnEstimateAfterAnExactCountOfTheSameSize()
    {
        using var directory = new TemporaryDirectory();
        var profile = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        var store = new JsonLinesDeadLetterHistoryStore(Path.Combine(directory.Path, "history.jsonl"));
        await store.AppendAsync(DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profile, at, [new DeadLetterEntitySnapshot(Orders, 100)]), "Test"));
        await store.AppendAsync(DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profile, at.AddSeconds(20),
            [new DeadLetterEntitySnapshot(Orders, 100) { CountQuality = DeadLetterCountQuality.Estimated }]), "Test"));

        Assert.Equal([DeadLetterCountQuality.Exact, DeadLetterCountQuality.Estimated],
            (await store.ReadAsync(profile, at.AddMinutes(-1))).Select(sample => sample.Quality));
    }

    // An estimated zero is never proof of empty: history keeps its quality, the empty state says so, the scan's overall
    // count stays approximate though no row is listed, and the monitor resolves only after two approximate zeros.
    [Fact]
    public async Task AnEstimatedZeroIsNeverTakenAsProvenEmpty()
    {
        var at = DateTimeOffset.UtcNow;
        var sample = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(Guid.NewGuid(), at,
            [new DeadLetterEntitySnapshot(Orders, 0) { CountQuality = DeadLetterCountQuality.Estimated }]), "Test");
        Assert.Equal(DeadLetterCountQuality.Estimated, sample.QualityOf("orders"));
        Assert.Equal(DeadLetterCountQuality.Estimated, DeadLetterHistory.Summarize([sample], at.AddMinutes(-1), at.AddMinutes(1))!.NowQuality);

        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, Counted(0, DeadLetterCountQuality.Estimated)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        Assert.Empty(vm.DeadLetterSources);
        Assert.StartsWith("≈", vm.GlobalDlqDisplay, StringComparison.Ordinal);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, Counted(12, DeadLetterCountQuality.Estimated));
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, Counted(0, DeadLetterCountQuality.Estimated));
        await Check();
        Assert.Single(vm.MonitorNotifications);
        await Check();
        Assert.Empty(vm.MonitorNotifications);
        Assert.Contains(vm.Activity, item => item.Action == "DLQ resolved" && item.Details.Contains("approximate", StringComparison.Ordinal));
    }

    // A fresh read that found messages is not overruled by an older Cloud Monitoring zero.
    [Fact]
    public async Task AnOlderMetricDoesNotResolveAFresherObservation()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var now = DateTimeOffset.UtcNow;
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, Counted(40, DeadLetterCountQuality.LowerBound, now, "dlq-reader")) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, Counted(0, DeadLetterCountQuality.Estimated, now.AddMinutes(-4), "dlq-reader"));
        await Check();
        await Check();

        var notification = Assert.Single(vm.MonitorNotifications);
        Assert.Equal(40, notification.Count);
    }

    // Counted through another reader subscription: the count is replaced without claiming growth, and a zero from the
    // new reader in that same check resolves nothing.
    [Fact]
    public async Task AChangedReaderIsNotComparedWithTheOldOne()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, Counted(10, DeadLetterCountQuality.Exact, null, "reader-a")) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, Counted(500, DeadLetterCountQuality.Exact, null, "reader-b"));
        await Check();

        Assert.DoesNotContain("increased", vm.MonitorAlert, StringComparison.Ordinal);
        var notification = Assert.Single(vm.MonitorNotifications);
        Assert.Equal(500, notification.Count);
        Assert.Equal("reader-b", notification.MeasuredFrom);
    }

    // The emulator is counted by peeking up to 1,000 messages: below the cap the peek saw everything, at it only that many.
    [Theory]
    [InlineData(0, DeadLetterCountQuality.Exact)]
    [InlineData(999, DeadLetterCountQuality.Exact)]
    [InlineData(1_000, DeadLetterCountQuality.LowerBound)]
    public void TheEmulatorsCappedPeekIsALowerBoundAtTheCap(long count, DeadLetterCountQuality expected) =>
        Assert.Equal(expected, AzureServiceBusWorkspace.EmulatorSampleQuality(count));
}
