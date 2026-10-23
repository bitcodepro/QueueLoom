using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// Clearing notifications dismisses what is shown; the incidents behind them stay, so nothing unchanged alerts again.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ClearingNotificationsDoesNotRealertAnUnchangedDeadLetterQueue()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 5)) } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await WaitUntilAsync(() => alerts.System.Count == 1);

        vm.ClearMonitorNotificationsCommand.Execute(null);
        Assert.Empty(vm.MonitorNotifications);
        Assert.Equal(0, vm.MonitorNotificationCount);

        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Task.Delay(200);
        Assert.Single(alerts.System);
        Assert.Empty(vm.MonitorNotifications);
        Assert.Equal(0, vm.MonitorNotificationCount);
    }

    [Fact]
    public async Task GrowthAfterClearingNotificationsAlertsAndShowsTheNotificationAgain()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 5)) } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await WaitUntilAsync(() => alerts.System.Count == 1);
        vm.ClearMonitorNotificationsCommand.Execute(null);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 8));
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Equal(8, alerts.System[1].Count);
        Assert.Equal(5, alerts.System[1].PreviousCount);
        var shown = Assert.Single(vm.MonitorNotifications);
        Assert.Equal(8, shown.Count);
        Assert.Equal(1, vm.MonitorNotificationCount);
    }
}
