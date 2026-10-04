using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task NewMessage_DuringAMonitorCheckOfAnotherEnvironment_StaysOnTheConnectedOne()
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
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();
        viewModel.SelectedDeadLetterEnvironmentFilter = viewModel.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single();
        viewModel.MonitorScope = viewModel.MonitorScopes[1];
        viewModel.MonitorTargetChoice = viewModel.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(test.Id, workspace.ConnectedProfileId);
        viewModel.NewMessageCommand.Execute(null);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Equal(dev.Id, workspace.ConnectedProfileId);
        Assert.False(viewModel.HasDraftEnvironmentMismatch);
    }
}
