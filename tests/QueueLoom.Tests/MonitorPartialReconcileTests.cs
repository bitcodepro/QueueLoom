using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// One environment (or one source of it) failing used to stop every notification from being resolved: a deleted source
// of a fully read environment stayed listed for good.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ADeletedSourceIsResolvedWhileAnotherEnvironmentFails()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var orders = ServiceBusEntityReference.Queue("orders");
        var refunds = ServiceBusEntityReference.Queue("refunds");
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(orders, 5)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(refunds, 3))
            }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task<bool> Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        Assert.True(await Check());
        Assert.Equal(2, vm.MonitorNotifications.Count);

        // Development's queue is deleted; Test's read fails.
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id);
        workspace.Snapshots[test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(refunds, null, null, "timed out"));

        Assert.False(await Check());
        Assert.Equal("refunds", Assert.Single(vm.MonitorNotifications).SourceName);
    }
}
