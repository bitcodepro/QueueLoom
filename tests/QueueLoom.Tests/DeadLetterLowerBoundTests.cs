using System.Reflection;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Pub/Sub without Cloud Monitoring reports no dead-letter count: it is counted by reading up to 1,000 messages. Such a
// count is only a lower bound: a full sample says nothing of further growth, and neither a smaller nor an empty sample
// proves the exact size (or an empty queue).
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(1_000)]
    [InlineData(12)]
    [InlineData(0)]
    public async Task ASampledDeadLetterCountIsALowerBoundWhateverItsSize(int seen)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new SampledDeadLetterWorkspace(QueueLoomPaths.ForRoot(directory.Path)) { Seen = seen };
        await workspace.ConnectAsync(SampledProfile());

        var snapshot = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        var entity = Assert.Single(snapshot.Entities);
        Assert.Equal(seen, entity.Count);
        Assert.True(entity.CountIsLowerBound);
        Assert.True(snapshot.TotalIsLowerBound);
        Assert.Null(entity.Change);
    }

    [Theory]
    [InlineData(1_000, true, "1,000+")]
    [InlineData(12, true, "12+")]
    [InlineData(0, true, "none seen")]
    [InlineData(1_000, false, "1,000")]
    [InlineData(0, false, "0")]
    public void ALowerBoundIsNeverWrittenAsAnExactCount(long count, bool lowerBound, string expected) =>
        Assert.Equal(expected, DeadLetterCountText.Format(count, lowerBound, System.Globalization.CultureInfo.InvariantCulture));

    // The monitor: an empty or smaller sample neither resolves nor lowers an open notification; growth still alerts.
    [Fact]
    public async Task TheMonitorDoesNotCloseOrLowerANotificationBecauseASampleShowedLess()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterSnapshot Sampled(long count) => Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, count) { CountIsLowerBound = true });
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Sampled(1_000) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        await Check();
        var notification = Assert.Single(vm.MonitorNotifications);
        Assert.Equal(DeadLetterCountText.Format(1_000, isLowerBound: true), notification.CountText);
        Assert.EndsWith("+", notification.CountText, StringComparison.Ordinal);

        workspace.Snapshots[dev.Id] = Sampled(300);
        await Check();
        Assert.Equal(1_000, Assert.Single(vm.MonitorNotifications).Count);

        workspace.Snapshots[dev.Id] = Sampled(0);
        await Check();
        Assert.Single(vm.MonitorNotifications);
        Assert.DoesNotContain(vm.Activity, item => item.Action.Contains("resolved", StringComparison.OrdinalIgnoreCase));

        // An exact zero (the metrics are back and say so) still resolves it.
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, 0));
        await Check();
        Assert.Empty(vm.MonitorNotifications);
    }

    [Fact]
    public void AnAlertWritesALowerBoundWithAPlus()
    {
        var alert = new MonitorAlert("Development", "orders (DLQ)", 1_000, null) { CountQuality = DeadLetterCountQuality.LowerBound };
        Assert.Contains("+ dead-lettered messages", alert.Text, StringComparison.Ordinal);
    }

    // History: the flags are kept on disk (and old samples without them still read), and a change involving a lower bound
    // is unknown rather than an invented number.
    [Fact]
    public void HistoryKeepsLowerBoundsAndReportsTheirChangeAsUnknown()
    {
        var profile = Guid.NewGuid();
        var start = DateTimeOffset.Parse("2026-10-10T08:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var exact = new DeadLetterSnapshot(profile, start, [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 40)]);
        var sampled = new DeadLetterSnapshot(profile, start.AddHours(1),
        [
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 1_000) { CountIsLowerBound = true },
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("payments"), 0) { CountIsLowerBound = true }
        ]);

        var first = DeadLetterHistorySample.FromSnapshot(exact, "Test");
        var last = JsonSerializer.Deserialize<DeadLetterHistorySample>(JsonSerializer.Serialize(DeadLetterHistorySample.FromSnapshot(sampled, "Test")))!;
        // Exact samples record their quality but no per-source list; old samples without a quality read as unqualified.
        Assert.Contains("\"TotalQuality\":\"Exact\"", JsonSerializer.Serialize(first), StringComparison.Ordinal);
        Assert.DoesNotContain("SourceQualities", JsonSerializer.Serialize(first), StringComparison.Ordinal);
        Assert.True(last.TotalIsLowerBound);
        Assert.Equal(["orders", "payments"], last.LowerBoundSources.ToArray());

        var summary = DeadLetterHistory.Summarize([first, last], start, start.AddHours(2))!;
        Assert.True(summary.NowIsLowerBound);
        Assert.Null(summary.Change);
        var orders = summary.Sources.Single(source => source.Name == "orders");
        Assert.True(orders.NowIsLowerBound);
        Assert.Null(orders.Change);
        Assert.True(summary.Points[^1].IsLowerBound);
    }

    [Fact]
    public void McpMarksLowerBoundsAndListsASampledQueueThatShowedNothing()
    {
        var info = McpMapping.ToInfo(new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 0) { CountIsLowerBound = true });
        Assert.Equal("lowerBound", info.CountQuality);
        Assert.Equal(0, info.Count);
    }

    private static ServiceBusProfile SampledProfile() =>
        ServiceBusProfile.CreateNew("Sampled", EnvironmentKind.Test, new(AuthenticationKind.KafkaNone)) with { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092") };

    private sealed class SampledDeadLetterWorkspace(QueueLoomPaths paths) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public int Seen { get; init; }
        public override MessagingProvider Provider => MessagingProvider.Kafka;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty) { CountsUnavailable = true })]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) =>
            new Channel(source, Seen);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message, CancellationToken token) =>
            throw new InvalidOperationException("This read-only fixture cannot send.");

        private sealed class Channel(ServiceBusEntityReference source, int count) : ILeasedMessageChannel
        {
            private int _next;
            public string PhysicalName => source.Name + "-dlq";
            public int MaximumBatchSize => 100;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
            {
                var batch = Enumerable.Range(_next, Math.Max(0, Math.Min(maxMessages, count - _next)))
                    .Select(index => new LeasedMessage(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, index, "x"u8.ToArray(),
                        new EditableMessageProperties(MessageId: $"m-{index}")), $"lease-{index}"))
                    .ToArray();
                _next += batch.Length;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(batch);
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) =>
                Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
            public void Dispose() { }
        }
    }
}
