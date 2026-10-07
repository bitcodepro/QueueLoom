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

    // An A-origin draft opened while a monitor check temporarily holds the connection to B is bound to A, the operator's
    // environment. What the Composer shows (the mismatch warning and the Send button) follows that binding throughout,
    // as a bound view sees it: the value read when each change notification arrives. Those notifications must arrive,
    // and must never show a mismatch that the restored connection would then leave on screen. Either the check ends
    // without anything else happening (no send, no destination or page change), or the operator sends during the
    // check: the send waits for it and goes to A.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PostReleaseCycle1_AnOperatorDraftOpenedDuringAMonitorCheckStaysSendableAndSendsToTheOperatorEnvironment(bool backup, bool sendDuringCheck)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
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
        var sentFrom = new List<Guid?>();
        workspace.OnSend = () => sentFrom.Add(workspace.ConnectedProfileId);
        var backups = new FakeBackupRepository(CreateBackupSummary(dev, queue.Reference, message), message);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace, dialogs, backupRepository: backups);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        if (backup)
        {
            await vm.RefreshBackupsCommand.ExecuteAsync();
            vm.SelectedBackup = Assert.Single(vm.BackupMessages);
            await vm.LoadSelectedBackupCommand.ExecuteAsync();
        }
        var open = backup ? vm.OpenBackupAsDraftCommand : vm.OpenMessageAsDraftCommand;
        vm.SelectedDeadLetterEnvironmentFilter = vm.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        vm.SelectedDlqSource = vm.FilteredDeadLetterSources.Single();
        vm.MonitorScope = vm.MonitorScopes[1];
        vm.MonitorTargetChoice = vm.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;
        if (!backup) vm.SelectedMessage = new MessageItemViewModel(message, dev.Id, dev.Name);

        // What a bound view last rendered: the value read when each change notification arrived.
        bool? renderedMismatch = null;
        bool? renderedSend = null;
        vm.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName == nameof(MainWindowViewModel.HasDraftEnvironmentMismatch)) renderedMismatch = vm.HasDraftEnvironmentMismatch;
        };
        vm.SendDraftCommand.CanExecuteChanged += (_, _) => renderedSend = vm.SendDraftCommand.CanExecute(null);

        await vm.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(test.Id, workspace.ConnectedProfileId);
        open.Execute(null);
        vm.SelectedDestination = vm.Destinations.Single(item => item.Reference == queue.Reference);
        Assert.Equal("origin body", vm.DraftBody);
        var page = vm.CurrentPage;
        // Bound to A while B is held: opening the draft notified no mismatch, and Send was notified as available.
        Assert.False(vm.HasDraftEnvironmentMismatch);
        Assert.Equal(false, renderedMismatch);
        Assert.Equal(true, renderedSend);

        if (sendDuringCheck)
        {
            // Sending now cancels the check and waits for the connection to return to A before anything is sent.
            await vm.SendDraftCommand.ExecuteAsync();
            Assert.Equal([dev.Id], sentFrom);
            Assert.Equal(dev.Id, workspace.ConnectedProfileId);
            await vm.ToggleMonitorCommand.ExecuteAsync();
        }
        else
        {
            // The check ends with nothing else happening: the connection returns to A.
            await vm.ToggleMonitorCommand.ExecuteAsync();
            Assert.Equal(dev.Id, workspace.ConnectedProfileId);
            Assert.Empty(sentFrom);
            Assert.Equal(page, vm.CurrentPage);
            Assert.Equal(queue.Reference, vm.SelectedDestination?.Reference);
        }

        // What is on screen after the check: no mismatch warning and Send available, as last notified and as computed.
        Assert.Equal(false, renderedMismatch);
        Assert.Equal(true, renderedSend);
        Assert.False(vm.HasDraftEnvironmentMismatch);
        Assert.Equal(string.Empty, vm.DraftEnvironmentWarning);
        Assert.True(vm.SendDraftCommand.CanExecute(null));
    }

    // The Composer follows the operator's environment, so the send itself checks the actual connection: if it is not
    // on the draft's environment at that moment (whatever switched it), nothing is sent and the operator is told why.
    [Fact]
    public async Task PostReleaseCycle1_ASendNeverGoesThroughAConnectionToAnotherEnvironment()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]) };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.NewMessageCommand.Execute(null);
        vm.DraftBody = "for development";
        vm.SelectedDestination = vm.Destinations.Single(item => item.Reference == queue.Reference);
        Assert.True(vm.SendDraftCommand.CanExecute(null));

        // The connection moves to another environment behind the view model's back.
        await workspace.ConnectAsync(test);
        await vm.SendDraftCommand.ExecuteAsync();

        Assert.Empty(workspace.SentMessages);
        Assert.Contains("not on this draft's environment", vm.ErrorText, StringComparison.Ordinal);
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
