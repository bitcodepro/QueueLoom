using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PostReleaseCycle1_OpenAsDraftDuringAnotherEnvironmentMonitorUsesTheOperatorEnvironment(bool backup, bool operatorOrigin)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var origin = operatorOrigin ? dev : test;
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var message = new BrowsedMessage(queue.Reference, ServiceBusSubQueue.Active, 42,
            "origin body"u8.ToArray(), new EditableMessageProperties(MessageId: "origin"));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 1))
            }
        };
        var backups = new FakeBackupRepository(CreateBackupSummary(origin, queue.Reference, message), message);
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace, backupRepository: backups);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        if (backup)
        {
            await vm.RefreshBackupsCommand.ExecuteAsync();
            vm.SelectedBackup = Assert.Single(vm.BackupMessages);
            await vm.LoadSelectedBackupCommand.ExecuteAsync();
        }
        var command = backup ? vm.OpenBackupAsDraftCommand : vm.OpenMessageAsDraftCommand;
        vm.NewMessageCommand.Execute(null);
        vm.DraftBody = "operator draft";
        vm.SelectedDeadLetterEnvironmentFilter = vm.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        vm.SelectedDlqSource = vm.FilteredDeadLetterSources.Single();
        vm.MonitorScope = vm.MonitorScopes[1];
        vm.MonitorTargetChoice = vm.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;
        // Changing the DLQ filter clears live selection; choose the row after the monitor target is configured.
        if (!backup) vm.SelectedMessage = new MessageItemViewModel(message, origin.Id, origin.Name);
        Assert.Equal(operatorOrigin, command.CanExecute(null));

        await vm.ToggleMonitorCommand.ExecuteAsync();
        try
        {
            await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(test.Id, workspace.ConnectedProfileId);
            if (operatorOrigin)
            {
                // A button enabled before the monitor switched the broker must still open the A-origin draft.
                command.Execute(null);
                Assert.Equal("origin body", vm.DraftBody);
                Assert.True(command.CanExecute(null));
            }
            else
            {
                // Following the actual button state reproduced a B-origin draft silently bound to A on the baseline.
                if (command.CanExecute(null)) command.Execute(null);
                Assert.Equal("operator draft", vm.DraftBody);
                Assert.False(command.CanExecute(null));
                Assert.Throws<InvalidOperationException>(() => command.Execute(null));
                if (backup) Assert.Contains("another environment", vm.BackupDraftHint, StringComparison.Ordinal);
            }
        }
        finally
        {
            await vm.ToggleMonitorCommand.ExecuteAsync();
        }
        Assert.Equal(dev.Id, workspace.ConnectedProfileId);
        Assert.False(vm.HasDraftEnvironmentMismatch);
        Assert.Equal(operatorOrigin ? "origin body" : "operator draft", vm.DraftBody);
    }

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
