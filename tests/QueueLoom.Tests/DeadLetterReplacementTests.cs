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

    // Kafka's dead-letter topic only grows at its end: its partition end offsets are the markers.
    [Fact]
    public void KafkaMarksADeadLetterTopicByItsEndOffsets()
    {
        var index = new KafkaTopologyIndex(
            [new KafkaTopicInfo("orders", [0], 1), new KafkaTopicInfo("orders.DLT", [0, 1], 3) { Ends = new Dictionary<int, long> { [1] = 9, [0] = 4 } }],
            KafkaSettings.DefaultDeadLetterSuffixes);

        var orders = index.ToTopology(DateTimeOffset.UtcNow).Queues.Single(queue => queue.Name == "orders");

        Assert.Equal(["0@4", "1@9"], orders.Runtime.DeadLetterContentMarkers);
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
