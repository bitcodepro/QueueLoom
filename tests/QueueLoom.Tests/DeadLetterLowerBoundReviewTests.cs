using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Abstractions;
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
        // At least 20: growth measured against a sample is itself only a lower bound.
        Assert.Contains($"increased by {DeadLetterCountText.Format(20, isLowerBound: true)};", vm.MonitorAlert, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, false, 5L, false, null, false)]
    [InlineData(10L, false, 15L, false, 5L, false)]
    [InlineData(10L, false, 15L, true, 5L, true)]
    [InlineData(10L, false, 5L, true, null, false)]
    [InlineData(1_000L, true, 5_000L, false, null, false)]
    [InlineData(1_000L, true, 1_000L, true, null, false)]
    public void OnlyProvenGrowthIsAnIncreaseAndKeepsItsQuality(long? before, bool beforeSampled, long now, bool nowSampled,
        long? expected, bool expectedAtLeast)
    {
        var increase = DeadLetterMeasurement.ProvenIncrease(
            before is { } value ? new DeadLetterMeasurement(value, beforeSampled) : null, new DeadLetterMeasurement(now, nowSampled));
        Assert.Equal(expected, increase?.Count);
        Assert.Equal(expectedAtLeast, increase?.IsLowerBound == true);
    }

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

    // The monitor's aggregate: exact growth in one source plus at-least growth in another is at least their sum.
    [Fact]
    public async Task MixedExactAndLowerBoundGrowthIsReportedAsALowerBound()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Counts(long exact, long sampled, bool isSampled) => Snapshot(dev.Id,
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), exact),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("payments"), sampled) { CountIsLowerBound = isSampled });
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Counts(10, 10, false) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        workspace.Snapshots[dev.Id] = Counts(15, 30, true);
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        Assert.Contains($"2 DLQ source(s) increased by {DeadLetterCountText.Format(25, isLowerBound: true)};", vm.MonitorAlert, StringComparison.Ordinal);
    }

    // A purge stops after a few empty receives, which proves nothing for a sampled queue: its row stays as "none seen" and
    // the environment total is not an exact 0.
    [Fact]
    public async Task APurgedSampledSourceStaysListedAsUnknownNotZero()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, 40) { CountIsLowerBound = true }) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        var now = DateTimeOffset.UtcNow;

        typeof(MainWindowViewModel).GetMethod("ApplyCompletedPurgeToDeadLetterRows", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(vm, [new DeadLetterPurgeResult(dev.Id, now, now, [new DeadLetterPurgeSourceResult(source, ServiceBusSubQueue.DeadLetter, 40)], Path.GetTempPath())]);

        var row = Assert.Single(vm.DeadLetterSources);
        Assert.Equal("none seen", row.CountText);
        Assert.Equal("none seen", vm.GlobalDlqDisplay);
        vm.SelectedDlqSource = row;
        Assert.True(vm.BrowseDlqSourceCommand.CanExecute(null)); // An uncertain empty source can still be opened.
    }

    // An exact 100 at the start and a sampled 50 later: the period's peak is only "at least 100", with or without downsampling.
    [Theory]
    [InlineData(360)]
    [InlineData(2)]
    public void APeriodWithAnUncertainPointHasALowerBoundPeak(int maximumPoints)
    {
        var profile = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        DeadLetterHistorySample Sample(int seconds, long count, bool sampled) => DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profile,
            at.AddSeconds(seconds), [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), count) { CountIsLowerBound = sampled }]), "Test");

        var summary = DeadLetterHistory.Summarize([Sample(0, 100, false), Sample(30, 100, false), Sample(90, 50, true)],
            at, at.AddMinutes(2), maximumPoints)!;

        Assert.Equal(100, summary.Peak.Count);
        Assert.True(summary.Peak.IsLowerBound);
    }

    // The workspace's own snapshot: a reported count after a sample has no computable change, and the row says unknown.
    [Fact]
    public async Task AReportedCountAfterASampleHasAnUnknownChange()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new SwitchingDeadLetterWorkspace(QueueLoomPaths.ForRoot(directory.Path)) { Sampled = true, Seen = 1_000 };
        await workspace.ConnectAsync(ServiceBusProfile.CreateNew("Switching", EnvironmentKind.Test, new(AuthenticationKind.KafkaNone))
            with { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092") });
        await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        workspace.Sampled = false;
        workspace.Reported = 1_200;
        var entity = Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);

        Assert.Equal(1_200, entity.Count);
        Assert.False(entity.CountIsLowerBound);
        Assert.True(entity.PreviousIsLowerBound);
        Assert.Null(entity.Change);
        Assert.Equal("unknown", new DlqSourceItemViewModel(Guid.NewGuid(), "Test", "TEST", default, entity).Delta);
    }

    private sealed class SwitchingDeadLetterWorkspace(QueueLoomPaths paths) : QueueLoom.Infrastructure.Messaging.LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public bool Sampled { get; set; }
        public int Seen { get; set; }
        public long Reported { get; set; }
        public override MessagingProvider Provider => MessagingProvider.Kafka;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: Sampled ? 0 : Reported)) { CountsUnavailable = Sampled })]));
        protected override QueueLoom.Infrastructure.Messaging.ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) =>
            new CountingChannel(source, Seen);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message, CancellationToken token) =>
            throw new InvalidOperationException("This read-only fixture cannot send.");
    }

    private sealed class CountingChannel(ServiceBusEntityReference source, int count) : QueueLoom.Infrastructure.Messaging.ILeasedMessageChannel
    {
        private int _next;
        public string PhysicalName => source.Name + "-dlq";
        public int MaximumBatchSize => 100;
        public Task<IReadOnlyList<QueueLoom.Infrastructure.Messaging.LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            var batch = Enumerable.Range(_next, Math.Max(0, Math.Min(maxMessages, count - _next)))
                .Select(index => new QueueLoom.Infrastructure.Messaging.LeasedMessage(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, index, "x"u8.ToArray(),
                    new EditableMessageProperties(MessageId: $"m-{index}")), $"lease-{index}"))
                .ToArray();
            _next += batch.Length;
            return Task.FromResult<IReadOnlyList<QueueLoom.Infrastructure.Messaging.LeasedMessage>>(batch);
        }
        public Task ReleaseAsync(IReadOnlyCollection<QueueLoom.Infrastructure.Messaging.LeasedMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyCollection<QueueLoom.Infrastructure.Messaging.LeasedMessage>> SettleAsync(IReadOnlyCollection<QueueLoom.Infrastructure.Messaging.LeasedMessage> messages, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<QueueLoom.Infrastructure.Messaging.LeasedMessage>>([]);
        public void Dispose() { }
    }
}
