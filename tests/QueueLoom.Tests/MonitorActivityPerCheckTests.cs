using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // An outage moves dead letters in many queues at every check: one Activity entry per check instead of one per
    // queue (each entry is written to disk on the window's thread). A few changes keep one entry each, with the queue.
    [Theory]
    [InlineData(12, 1, 0)]
    [InlineData(3, 0, 3)]
    public async Task ManyCountChangesInOneCheckBecomeOneActivityEntry(int queues, int combined, int separate)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Counts(long each) => Snapshot(dev.Id, Enumerable.Range(1, queues)
            .Select(index => new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue($"orders-{index:00}"), each)).ToArray());
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Counts(1) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        workspace.Snapshots[dev.Id] = Counts(2);
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        Assert.Equal(separate, vm.Activity.Count(item => item.Action == "DLQ count changed"));
        var entries = vm.Activity.Where(item => item.Action == "DLQ counts changed").ToArray();
        Assert.Equal(combined, entries.Length);
        if (combined == 1)
        {
            Assert.Contains($"{queues} queues", entries[0].Details, StringComparison.Ordinal);
            Assert.Contains("1 → 2", entries[0].Details, StringComparison.Ordinal);
            Assert.EndsWith($"and {queues - 5} more", entries[0].Details, StringComparison.Ordinal);
        }
        Assert.Equal(queues, vm.MonitorNotifications.Count);
    }
}
