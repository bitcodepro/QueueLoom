using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// A monitor check of another environment (B) temporarily connects the workspace there. What the window offers follows
// the operator's environment (A) throughout, as the draft state already does: purging A's dead letters stays available,
// purging B's is not offered, and an expired temporary write unlock shows read-only at once. A bound view reads these
// from change notifications, so the value last notified must match too.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task MonitorCheckOfAnotherEnvironment_KeepsTheOperatorsActionsAndTheirNotifications()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 1))
            }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        vm.SelectedDeadLetterEnvironmentFilter = vm.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        vm.SelectedDlqSource = vm.FilteredDeadLetterSources.Single();
        vm.MonitorScope = vm.MonitorScopes[1];
        vm.MonitorTargetChoice = vm.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;
        bool? renderedPurge = null;
        vm.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName == nameof(MainWindowViewModel.CanPurgeSelectedDeadLetters)) renderedPurge = vm.CanPurgeSelectedDeadLetters;
        };

        await vm.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(test.Id, workspace.ConnectedProfileId);
        Assert.Equal(dev.Id, vm.ConnectedProfileId);

        // B's own source, selected while B is held: not purgeable from A's window.
        Assert.False(vm.CanPurgeSelectedDeadLetters);
        // A's source: purgeable, while B is held.
        vm.SelectedDeadLetterEnvironmentFilter = vm.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == dev.Id);
        vm.SelectedDlqSource = vm.FilteredDeadLetterSources.Single();
        Assert.True(vm.CanPurgeSelectedDeadLetters);
        Assert.Equal(true, renderedPurge);

        await vm.ToggleMonitorCommand.ExecuteAsync();
        Assert.Equal(dev.Id, workspace.ConnectedProfileId);
        Assert.True(vm.CanPurgeSelectedDeadLetters);
        Assert.Equal(true, renderedPurge);
    }

    [Fact]
    public async Task MonitorCheckOfAnotherEnvironment_ShowsAnExpiredWriteUnlockAsReadOnlyAtOnce()
    {
        var production = CreateProfile("Production", EnvironmentKind.Production, ProfileAccessMode.ReadOnly);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [production.Id] = Snapshot(production.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 1))
            }
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([production, test], production.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        Assert.True(vm.CanWrite);
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        vm.SelectedDeadLetterEnvironmentFilter = vm.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        vm.SelectedDlqSource = vm.FilteredDeadLetterSources.Single();
        vm.MonitorScope = vm.MonitorScopes[1];
        vm.MonitorTargetChoice = vm.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;

        await vm.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(test.Id, workspace.ConnectedProfileId);
        // The unlock expires while B is held; the relock itself waits for the check to give the connection back.
        SetPrivate(vm, "_writeUnlockExpiresAt", DateTimeOffset.UtcNow.AddMinutes(-1));

        Assert.False(vm.CanWrite);
        Assert.Equal("READ ONLY", vm.WriteAccessLabel);
        await vm.ToggleMonitorCommand.ExecuteAsync();
        Assert.False(vm.CanWrite);
    }
}
