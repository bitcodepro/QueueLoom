using QueueLoom.Core.Abstractions;
using QueueLoom.Tests.Infrastructure;
using QueueLoom.Infrastructure.Persistence;
using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// One dead letter replaced by another keeps the count, but the queue's contents changed: the monitor says so.
public sealed partial class ViewModelStateTests
{
    private static DeadLetterEntitySnapshot Holding(long count, params string[] markers) =>
        new(ServiceBusEntityReference.Queue("orders"), count) { ContentMarkers = markers };

    private async Task<(MainWindowViewModel Vm, FakeWorkspace Workspace, RecordingAlerts Alerts, Func<Task> Check)> MonitorAsync(
        DeadLetterEntitySnapshot first)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, first) } };
        var alerts = new RecordingAlerts();
        var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        return (vm, workspace, alerts, async () => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!);
    }

    [Fact]
    public async Task AReplacedDeadLetterIsAlertedAtTheSameCount()
    {
        var (vm, workspace, alerts, check) = await MonitorAsync(Holding(1, "1:a"));
        await using var _ = vm;
        var profile = workspace.Snapshots.Keys.Single();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[profile] = Snapshot(profile, Holding(1, "2:b"));
        await check();

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Contains(alerts.System, alert => alert.NewMessages == 1 && alert.Text.Contains("1 new dead-lettered message, 1 in total", StringComparison.Ordinal));
        Assert.Contains(vm.Activity, item => item.Action == "New dead letters");
        Assert.Single(vm.MonitorNotifications);
    }

    [Fact]
    public async Task TheSameDeadLettersAreNotAlertedAgain()
    {
        var (vm, workspace, alerts, check) = await MonitorAsync(Holding(2, "1:a", "2:b"));
        await using var _ = vm;
        var profile = workspace.Snapshots.Keys.Single();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[profile] = Snapshot(profile, Holding(2, "2:b", "1:a"));
        await check();
        workspace.Snapshots[profile] = Snapshot(profile, Holding(1, "2:b")); // One was removed: no new message either.
        await check();

        Assert.Single(alerts.System);
        Assert.DoesNotContain(vm.Activity, item => item.Action == "New dead letters");
    }

    // Growth is alerted once, as growth; the new message in it is not alerted a second time.
    [Fact]
    public async Task GrowthWithANewMessageIsOneAlert()
    {
        var (vm, workspace, alerts, check) = await MonitorAsync(Holding(1, "1:a"));
        await using var _ = vm;
        var profile = workspace.Snapshots.Keys.Single();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[profile] = Snapshot(profile, Holding(2, "1:a", "2:b"));
        await check();

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Equal(0, alerts.System[^1].NewMessages + alerts.System[0].NewMessages);
        Assert.DoesNotContain(vm.Activity, item => item.Action == "New dead letters");
    }

    // Without markers (a queue too large to peek, a provider that cannot look without side effects) nothing is claimed.
    [Fact]
    public async Task WithoutMarkersOnlyTheCountIsCompared()
    {
        var (vm, workspace, alerts, check) = await MonitorAsync(new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 1));
        await using var _ = vm;
        var profile = workspace.Snapshots.Keys.Single();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[profile] = Snapshot(profile, Holding(1, "2:b"));
        await check();

        Assert.Single(alerts.System);
    }

    private static DeadLetterOffsets? KafkaCheckpoint(string deadLetterTopic, long retained, Dictionary<int, long> ends) =>
        new KafkaTopologyIndex(
                [new KafkaTopicInfo("orders", [0], 1), new KafkaTopicInfo(deadLetterTopic, [.. ends.Keys], retained) { Ends = ends }],
                KafkaSettings.DefaultDeadLetterSuffixes)
            .ToTopology(DateTimeOffset.UtcNow).Queues.Single(queue => queue.Name == "orders").Runtime.DeadLetterOffsets;

    // Kafka's offsets are a checkpoint: what was written since counts, not which partition strings are new.
    [Theory]
    [InlineData("orders.DLT", 10, "0=100,1=0", 0L)]       // An empty partition added: nothing arrived.
    [InlineData("orders.DLT", 10, "0=110", 10L)]        // Ten expired, ten arrived: same count, ten new.
    [InlineData("orders.DLT", 5, "0=120", 20L)]         // More arrived than the count shows.
    [InlineData("orders.DLT", 10, "0=100", 0L)]         // Unchanged.
    [InlineData("orders.DLT", 10, "0=50", null)]        // Rolled back: the topic was recreated, nothing compares.
    [InlineData("orders-dlt", 10, "0=110", null)]       // Another dead-letter topic: unrelated offsets.
    public void KafkaCountsWhatWasWrittenSinceTheLastCheckpoint(string topic, long retained, string ends, long? arrived)
    {
        var before = KafkaCheckpoint("orders.DLT", 10, new() { [0] = 100 });
        var now = KafkaCheckpoint(topic, retained, ends.Split(',').Select(pair => pair.Split('='))
            .ToDictionary(pair => int.Parse(pair[0], System.Globalization.CultureInfo.InvariantCulture),
                pair => long.Parse(pair[1], System.Globalization.CultureInfo.InvariantCulture)));

        Assert.Equal(arrived, DeadLetterOffsets.Arrivals(before, now));
    }

    // Ten records expired and ten arrived: the count stayed, the monitor says up to ten are new.
    [Fact]
    public async Task KafkaArrivalsAtTheSameCountAreAlertedAsUpToThatMany()
    {
        DeadLetterEntitySnapshot At(long end) =>
            new(ServiceBusEntityReference.Queue("orders"), 10) { Offsets = KafkaCheckpoint("orders.DLT", 10, new() { [0] = end }) };
        var (vm, workspace, alerts, check) = await MonitorAsync(At(100));
        await using var _ = vm;
        var profile = workspace.Snapshots.Keys.Single();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[profile] = Snapshot(profile, At(110));
        await check();
        workspace.Snapshots[profile] = Snapshot(profile, At(110));
        await check();

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Contains(alerts.System, alert => alert.NewMessages == 10 && alert.Text.Contains("up to 10 new dead-lettered messages", StringComparison.Ordinal));
        Assert.Equal(2, alerts.System.Count);
    }

    // Replacements in many sources in one check: one Activity entry and one combined alert, as for count changes.
    [Fact]
    public async Task ManyReplacementsInOneCheckBecomeOneActivityEntry()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Holding(string marker) => Snapshot(dev.Id, Enumerable.Range(1, 12)
            .Select(index => new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue($"orders-{index:00}"), 1) { ContentMarkers = [$"{marker}-{index}"] }).ToArray());
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Holding("a") } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[dev.Id] = Holding("b");
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        await WaitUntilAsync(() => alerts.System.Count == 2);
        var entry = Assert.Single(vm.Activity, item => item.Action == "New dead letters");
        Assert.Contains("12 queues", entry.Details, StringComparison.Ordinal);
        Assert.Equal(12, vm.MonitorNotifications.Count);
        Assert.Equal(2, alerts.System.Count);
    }

    // A rejected older observation carries no checkpoint of its own read, even at the same count.
    [Fact]
    public async Task ARejectedOlderObservationCarriesNoCheckpoint()
    {
        using var directory = new TemporaryDirectory();
        var tenOClock = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        await using var workspace = new ReportedDeadLetterWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        {
            Count = 10, Reader = "reader", At = tenOClock, Offsets = new DeadLetterOffsets("orders.DLT", new Dictionary<int, long> { [0] = 100 })
        };
        await workspace.ConnectAsync(ReportedProfile());
        Assert.NotNull(Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities).Offsets);
        workspace.At = tenOClock.AddMinutes(-5);
        workspace.Offsets = new DeadLetterOffsets("orders.DLT", new Dictionary<int, long> { [0] = 90 });

        var entity = Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);

        Assert.Equal(10, entity.Count);
        Assert.Null(entity.Offsets);
    }

    // Azure peeks a small dead-letter queue page by page; a partial view is no view, so it can never invent a "new" message.
    [Theory]
    [InlineData(3, 3, true)]
    [InlineData(3, 2, false)]
    public async Task AzurePeeksEveryMessageOrReportsNoMarkers(long count, int available, bool complete)
    {
        var receiver = new PagingReceiver(available);

        var markers = await AzureServiceBusWorkspace.PeekContentMarkersAsync(receiver, count, CancellationToken.None);

        if (complete)
        {
            Assert.Equal(["1:m-1", "2:m-2", "3:m-3"], markers!.Order(StringComparer.Ordinal));
            Assert.True(receiver.Pages > 1);
        }
        else
        {
            Assert.Null(markers);
        }
    }

    private sealed class PagingReceiver(int available) : ServiceBusReceiver
    {
        public int Pages { get; private set; }

        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default)
        {
            Pages++;
            var from = fromSequenceNumber ?? 1;
            // Two messages a page at most, like a broker that returns fewer than asked.
            IReadOnlyList<ServiceBusReceivedMessage> page = Enumerable.Range((int)from, Math.Max(0, Math.Min(2, available - (int)from + 1)))
                .Select(number => ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: $"m-{number}", sequenceNumber: number))
                .ToArray();
            return Task.FromResult(page);
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
