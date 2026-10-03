using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task CycleThreeKafka_LeaderFailureRetainsNotificationAndBaselineUntilARealZero()
    {
        using var directory = new TemporaryDirectory();
        var profile = ServiceBusProfile.CreateNew("Isolated Kafka", EnvironmentKind.Test, new(AuthenticationKind.KafkaNone)) with
            { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092") };
        await using var workspace = new SyntheticKafkaMonitorWorkspace(QueueLoomPaths.ForRoot(directory.Path));
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        // Exercise the real monitor check without its timer. All-environment scope needs no pinned entity.
        vm.MonitorScope = "All environments";
        async Task<bool> Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        workspace.DeadLetterCount = 7;
        Assert.True(await Check());
        Assert.Equal(7, Assert.Single(vm.MonitorNotifications).Count);
        workspace.CountError = "counts unavailable: a partition has no leader right now";
        Assert.False(await Check());
        Assert.Equal(7, Assert.Single(vm.MonitorNotifications).Count);
        Assert.DoesNotContain(vm.Activity, item => item.Action.Contains("resolved", StringComparison.OrdinalIgnoreCase));
        workspace.CountError = null; workspace.DeadLetterCount = 9;
        Assert.True(await Check());
        Assert.Equal(9, Assert.Single(vm.MonitorNotifications).Count);
        Assert.Contains("increased by 2", vm.MonitorAlert, StringComparison.Ordinal);
        workspace.DeadLetterCount = 0;
        Assert.True(await Check());
        Assert.Empty(vm.MonitorNotifications);
    }

    [Theory]
    [InlineData(null, 7, true)]
    [InlineData(null, 0, true)]
    [InlineData("isolated watermark failure", 0, false)]
    public async Task CycleThreeKafka_SnapshotDistinguishesKnownZeroFromUnavailableCount(string? error, long count, bool successful)
    {
        using var directory = new TemporaryDirectory();
        var profile = ServiceBusProfile.CreateNew("Isolated Kafka", EnvironmentKind.Test, new(AuthenticationKind.KafkaNone)) with
            { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092") };
        await using var workspace = new SyntheticKafkaMonitorWorkspace(QueueLoomPaths.ForRoot(directory.Path));
        await workspace.ConnectAsync(profile);
        workspace.DeadLetterCount = 7;
        Assert.Equal(7, Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities).Count);
        workspace.CountError = error; workspace.DeadLetterCount = count;
        var snapshot = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);
        var entity = Assert.Single(snapshot.Entities);
        Assert.Equal(successful, entity.IsSuccessful);
        Assert.Equal(successful ? count : null, entity.Count);
        Assert.Equal(!successful, snapshot.HasFailures);
        if (!successful)
        {
            Assert.Contains(error!, entity.Error!, StringComparison.Ordinal);
            workspace.CountError = null; workspace.DeadLetterCount = 9;
            Assert.Equal(7, Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities).PreviousCount);
        }
        Assert.Equal(0, workspace.OpenChannelCalls);
    }

    private sealed class SyntheticKafkaMonitorWorkspace(QueueLoomPaths paths) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public override MessagingProvider Provider => MessagingProvider.Kafka;
        public long DeadLetterCount { get; set; }
        public string? CountError { get; set; }
        public int OpenChannelCalls { get; private set; }
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(
            new KafkaTopologyIndex([new("orders", [0], 1), new("orders.DLT", [0], CountError is null ? DeadLetterCount : 0) { CountError = CountError }],
                KafkaSettings.DefaultDeadLetterSuffixes).ToTopology(DateTimeOffset.UtcNow));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
        { OpenChannelCalls++; throw new InvalidOperationException("Kafka must never sample unavailable offset counts as a lease."); }
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message, CancellationToken token) =>
            throw new InvalidOperationException("This read-only fixture cannot send.");
    }
}
