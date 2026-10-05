using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using System.Text;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task EmptyProfileList_UsesAddEnvironmentAsHeaderAction()
    {
        var repository = new FakeProfileRepository([], null);
        await using var viewModel = CreateViewModel(repository, new FakeWorkspace());

        await viewModel.InitializeAsync();

        Assert.False(viewModel.HasProfiles);
        Assert.Equal("Add environment", viewModel.EnvironmentActionLabel);
        Assert.Same(viewModel.AddEnvironmentCommand, viewModel.EnvironmentActionCommand);
    }

    [Fact]
    public async Task ConnectionIndicator_FollowsWorkspaceProfile_NotUiSelectionOrReload()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace();
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        var connectedDev = Assert.Single(viewModel.Profiles, item => item.Id == dev.Id);
        var disconnectedTest = Assert.Single(viewModel.Profiles, item => item.Id == test.Id);
        Assert.True(connectedDev.IsConnected);
        Assert.False(disconnectedTest.IsConnected);

        viewModel.SelectedProfile = disconnectedTest;

        Assert.True(connectedDev.IsConnected);
        Assert.False(viewModel.IsSelectedProfileConnected);
        Assert.Equal("Development", viewModel.ConnectedProfileName);
        Assert.Contains("Development", viewModel.ConnectionLabel, StringComparison.Ordinal);

        await repository.SetSelectedProfileIdAsync(test.Id);
        await viewModel.InitializeAsync();

        Assert.Equal(test.Id, viewModel.SelectedProfile?.Id);
        Assert.True(Assert.Single(viewModel.Profiles, item => item.Id == dev.Id).IsConnected);
        Assert.False(Assert.Single(viewModel.Profiles, item => item.Id == test.Id).IsConnected);
    }

    [Fact]
    public async Task EnvironmentHeaderAction_ConnectsAndDisconnectsOnlyTheSelectedEnvironment()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace();
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        Assert.Equal("Connect", viewModel.EnvironmentActionLabel);
        Assert.Same(viewModel.ConnectCommand, viewModel.EnvironmentActionCommand);

        await viewModel.ConnectCommand.ExecuteAsync();
        Assert.Equal("Disconnect", viewModel.EnvironmentActionLabel);
        Assert.Same(viewModel.DisconnectCommand, viewModel.EnvironmentActionCommand);

        viewModel.SelectedProfile = Assert.Single(viewModel.Profiles, profile => profile.Id == test.Id);
        Assert.Equal("Connect", viewModel.EnvironmentActionLabel);
        Assert.Same(viewModel.ConnectCommand, viewModel.EnvironmentActionCommand);

        viewModel.SelectedProfile = Assert.Single(viewModel.Profiles, profile => profile.Id == dev.Id);
        await viewModel.DisconnectCommand.ExecuteAsync();

        Assert.False(viewModel.IsConnected);
        Assert.Equal("Connect", viewModel.EnvironmentActionLabel);
        Assert.Equal(1, workspace.DisconnectCalls);
    }

    [Fact]
    public async Task FailedProfileSwitch_ClearsEveryConnectionIndicator()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace();
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedProfile = Assert.Single(viewModel.Profiles, item => item.Id == test.Id);
        workspace.FailNextConnection = true;

        await viewModel.ConnectCommand.ExecuteAsync();

        Assert.All(viewModel.Profiles, item => Assert.False(item.IsConnected));
        Assert.Null(viewModel.ConnectedProfileId);
        Assert.Equal("No environment connected", viewModel.ConnectedProfileName);
    }

    [Fact]
    public async Task CancelCurrentOperation_CancelsAWaitingAzureCallAndDisablesItself()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { WaitForConnectionCancellation = true };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([dev], dev.Id),
            workspace);

        await viewModel.InitializeAsync();
        var connection = viewModel.ConnectCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.IsBusy && viewModel.CancelCurrentOperationCommand.CanExecute(null));
        Assert.True(viewModel.ShowCancelCurrentOperation);

        // The operation may end inside Cancel() on some systems, so what the operator saw is recorded as it changes.
        var statuses = new List<string>();
        viewModel.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName == nameof(MainWindowViewModel.StatusText))
            {
                statuses.Add(viewModel.StatusText);
            }
        };
        viewModel.CancelCurrentOperationCommand.Execute(null);
        Assert.Equal("Cancelling…", statuses.First());
        Assert.True(viewModel.CancelOperationLabel == "Cancelling…" || !viewModel.IsBusy);
        Assert.False(viewModel.ShowCancelCurrentOperation);
        Assert.False(viewModel.CancelCurrentOperationCommand.CanExecute(null));
        await connection;

        Assert.False(viewModel.IsBusy);
        Assert.Equal("Operation cancelled", viewModel.StatusText);
        Assert.Equal("Cancel", viewModel.CancelOperationLabel);
    }

    [Fact]
    public async Task ProfileEdit_DoesNotExposeUnsafeCancellation()
    {
        var editCompletion = new TaskCompletionSource<ProfileEditorResult?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var dialogs = new FakeDialogService { PendingEdit = editCompletion };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([], null),
            new FakeWorkspace(),
            dialogs);

        await viewModel.InitializeAsync();
        var edit = viewModel.AddEnvironmentCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.IsBusy);

        Assert.False(viewModel.ShowCancelCurrentOperation);
        Assert.False(viewModel.CancelCurrentOperationCommand.CanExecute(null));

        editCompletion.SetResult(null);
        await edit;
        Assert.False(viewModel.IsBusy);
    }

    [Fact]
    public async Task BackupsSelectedWhileBusy_RefreshesOnceAfterOperationCompletes()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var message = SearchMessage(source, 42, "2026-08-12T10:00:00Z");
        var summary = CreateBackupSummary(dev, source, message);
        var backups = new FakeBackupRepository(summary, message);
        var connectionRelease = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var workspace = new FakeWorkspace { ConnectionRelease = connectionRelease };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([dev], dev.Id),
            workspace,
            backupRepository: backups);

        await viewModel.InitializeAsync();
        var connection = viewModel.ConnectCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.IsBusy);

        viewModel.SelectedNavigation = Assert.Single(
            viewModel.Navigation,
            item => item.Key == nameof(NavigationPage.Backups));
        Assert.Equal(0, backups.ListCalls);

        connectionRelease.SetResult(true);
        await connection;

        Assert.Equal(1, backups.ListCalls);
        Assert.Single(viewModel.BackupMessages);
    }

    [Fact]
    public async Task ProfileSave_SetSelectedFailure_RestoresProfileSelectionAndSecret()
    {
        var original = CreateConnectionStringProfile("Original");
        var other = CreateProfile("Other", EnvironmentKind.Test);
        var updated = new ServiceBusProfile(
            original.Id,
            "Updated",
            original.Environment,
            original.CustomEnvironmentName,
            original.FullyQualifiedNamespace,
            AuthenticationSettings.ConnectionString(),
            original.AccessMode);
        var repository = new FakeProfileRepository([original, other], other.Id)
        {
            FailNextSetSelected = true
        };
        var vault = new FakeSecretVault();
        var secretKey = ProfileSecretKey.ConnectionString(original.Id);
        await vault.StoreAsync(secretKey, "old-secret");
        var dialogs = new FakeDialogService
        {
            EditResult = new ProfileEditorResult(updated, "new-secret", ReplacesConnectionString: true)
        };
        await using var viewModel = CreateViewModel(
            repository,
            new FakeWorkspace(),
            dialogs,
            secretVault: vault);

        await viewModel.InitializeAsync();
        viewModel.SelectedProfile = Assert.Single(viewModel.Profiles, item => item.Id == original.Id);
        await viewModel.EditEnvironmentCommand.ExecuteAsync();

        Assert.Equal(original, await repository.GetAsync(original.Id));
        Assert.Equal(other.Id, await repository.GetSelectedProfileIdAsync());
        Assert.Equal("old-secret", await vault.RetrieveAsync(secretKey));
        Assert.Contains("selected profile failure", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UnlockWrites_ChangesLocalModeWithoutReconnectOrTopologyReload()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([dev], dev.Id);
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(repository, workspace, dialogs);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        Assert.Equal(1, workspace.ConnectCalls);
        Assert.Equal(1, workspace.TopologyCalls);

        await viewModel.UnlockWritesCommand.ExecuteAsync();

        Assert.True(viewModel.CanWrite);
        Assert.Equal(1, workspace.ConnectCalls);
        Assert.Equal(1, workspace.TopologyCalls);
        Assert.Equal([ProfileAccessMode.ReadWrite], workspace.AccessModeChanges);
    }

    [Fact]
    public async Task GlobalScanFailure_RestoresTemporaryWriteGrantWithItsRelockTimer()
    {
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([test, dev], dev.Id);
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(repository, workspace, dialogs);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.UnlockWritesCommand.ExecuteAsync();
        Assert.True(viewModel.CanWrite);

        workspace.FailNextConnection = true;
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();

        Assert.Equal(dev.Id, viewModel.ConnectedProfileId);
        Assert.True(viewModel.CanWrite);
        Assert.Equal("WRITE ENABLED", viewModel.WriteAccessLabel);
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode);
    }

    [Fact]
    public async Task MonitorDoesNotChangeSelectedEnvironmentAndReconcilesNotifications()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(
                    dev.Id,
                    new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3)),
                [test.Id] = Snapshot(test.Id)
            }
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedProfile = Assert.Single(viewModel.Profiles, item => item.Id == test.Id);

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.MonitorNotificationCount == 1);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Equal(test.Id, viewModel.SelectedProfile?.Id);
        var notification = Assert.Single(viewModel.MonitorNotifications);
        Assert.Equal("orders", notification.SourceName);
        Assert.Equal(3, notification.Count);
        var lastDetectedAt = notification.LastDetectedAt;

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.MonitorStatus.Contains("last check", StringComparison.OrdinalIgnoreCase));
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Single(viewModel.MonitorNotifications);
        Assert.Equal(lastDetectedAt, notification.LastDetectedAt);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.MonitorStatus.Contains("last check", StringComparison.OrdinalIgnoreCase));
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Empty(viewModel.MonitorNotifications);
    }

    [Fact]
    public async Task MonitorRestoresOriginalWriteModeWhenConnectedEnvironmentIsScannedLast()
    {
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var dev = CreateProfile(
            "Development",
            EnvironmentKind.Development,
            ProfileAccessMode.ReadWrite);
        var repository = new FakeProfileRepository([test, dev], dev.Id);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [test.Id] = Snapshot(test.Id),
                [dev.Id] = Snapshot(dev.Id)
            }
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode);

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.MonitorStatus.Contains("last check", StringComparison.OrdinalIgnoreCase));
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Equal(dev.Id, workspace.ConnectedProfileId);
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode);
        Assert.True(viewModel.CanWrite);
    }

    [Fact]
    public async Task InteractiveWorkspaceOperation_InterruptsOnlyTheCurrentMonitorCheck()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { WaitForSnapshotCancellation = true };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([dev], dev.Id),
            workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await viewModel.RefreshTopologyCommand.ExecuteAsync();

        Assert.Equal(1, workspace.CancelledSnapshotCalls);
        Assert.True(viewModel.IsMonitoring);
        Assert.False(viewModel.IsBusy);
        Assert.Contains("paused", viewModel.MonitorStatus, StringComparison.OrdinalIgnoreCase);

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        Assert.False(viewModel.IsMonitoring);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (!condition())
        {
            await Task.Delay(20, timeout.Token);
        }
    }

    [Fact]
    public async Task DeadLetterEnvironmentFilter_UpdatesRowsCountsAndRestoresSelection()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var prod = CreateProfile("Production", EnvironmentKind.Production);
        var repository = new FakeProfileRepository([dev, prod], dev.Id);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(
                    dev.Id,
                    new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("jobs"), 5),
                    new DeadLetterEntitySnapshot(ServiceBusEntityReference.Subscription("orders", "billing"), 3)),
                [prod.Id] = Snapshot(
                    prod.Id,
                    new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("alerts"), 7))
            }
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();

        Assert.Equal(2, viewModel.DeadLetterEnvironmentFilters.Count);
        Assert.Equal(dev.Id, viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId);
        Assert.Equal(2, viewModel.VisibleDlqSourceRowCount);
        Assert.Equal(8, viewModel.VisibleDlqSourceCount);
        Assert.Equal(15, viewModel.GlobalDlqSourceCount);
        Assert.Equal(["jobs", "billing"], viewModel.FilteredDeadLetterSources.Select(item => item.EntityName));

        var originalSelection = viewModel.FilteredDeadLetterSources[0];
        viewModel.SelectedDlqSource = originalSelection;
        viewModel.SelectedDeadLetterEnvironmentFilter = Assert.Single(
            viewModel.DeadLetterEnvironmentFilters,
            item => item.ProfileId == prod.Id);

        Assert.Single(viewModel.FilteredDeadLetterSources);
        Assert.Equal(7, viewModel.VisibleDlqSourceCount);
        Assert.Equal(15, viewModel.GlobalDlqSourceCount);
        Assert.Null(viewModel.SelectedDlqSource);

        viewModel.SelectedDeadLetterEnvironmentFilter = Assert.Single(
            viewModel.DeadLetterEnvironmentFilters,
            item => item.ProfileId == dev.Id);

        Assert.Same(originalSelection, viewModel.SelectedDlqSource);
        Assert.Equal(8, viewModel.VisibleDlqSourceCount);
    }

    [Fact]
    public async Task GlobalDeadLetterScan_RestoresOfflineState()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id),
                [test.Id] = Snapshot(test.Id)
            }
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();

        Assert.False(viewModel.IsConnected);
        Assert.Null(viewModel.ConnectedProfileId);
        Assert.All(viewModel.Profiles, profile => Assert.False(profile.IsConnected));
        Assert.Equal(1, workspace.DisconnectCalls);
    }

    [Fact]
    public void DeadLetterSource_ExposesQueueAndSubscriptionDisplayParts()
    {
        var queue = CreateSource(ServiceBusEntityReference.Queue("jobs"));
        var subscription = CreateSource(ServiceBusEntityReference.Subscription("orders", "billing"));

        Assert.True(queue.IsQueue);
        Assert.False(queue.IsSubscription);
        Assert.Equal("jobs", queue.EntityName);
        Assert.Empty(queue.ParentTopicName);
        Assert.True(subscription.IsSubscription);
        Assert.False(subscription.IsQueue);
        Assert.Equal("orders", subscription.ParentTopicName);
        Assert.Equal("billing", subscription.EntityName);
    }

    [Fact]
    public async Task BrowseDeadLetters_RequestsBoundedPageInSequenceOrder()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var repository = new FakeProfileRepository([dev], dev.Id);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, 3))
            },
            BrowseMessages =
            [
                SearchMessage(source, 3, "2026-08-12T12:00:00Z"),
                SearchMessage(source, 1, "2026-08-12T10:00:00Z"),
                SearchMessage(source, 2, "2026-08-12T11:00:00Z")
            ]
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.SelectedDlqSource = Assert.Single(viewModel.FilteredDeadLetterSources);
        await viewModel.BrowseDlqSourceCommand.ExecuteAsync();

        var request = Assert.Single(workspace.BrowseRequests);
        Assert.False(request.LoadAll);
        Assert.Equal(100, request.MaxMessages);
        Assert.Equal([1L, 2L, 3L], viewModel.Messages.Select(message => message.SequenceNumber));
    }

    [Fact]
    public async Task PurgeCommands_ResolveEnvironmentTopicAndSelectedEntityScopes()
    {
        var dev = CreateProfile(
            "Development",
            EnvironmentKind.Development,
            ProfileAccessMode.ReadWrite);
        var queue = new ServiceBusQueue("jobs", ServiceBusEntityRuntime.Empty);
        var billing = new ServiceBusSubscription("orders", "billing", ServiceBusEntityRuntime.Empty);
        var shipping = new ServiceBusSubscription("orders", "shipping", ServiceBusEntityRuntime.Empty);
        var topic = new ServiceBusTopic(
            "orders",
            ServiceBusEntityRuntime.Empty,
            [billing, shipping]);
        var repository = new FakeProfileRepository([dev], dev.Id);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue], [topic]),
            Snapshots =
            {
                [dev.Id] = Snapshot(
                    dev.Id,
                    new DeadLetterEntitySnapshot(queue.Reference, 2),
                    new DeadLetterEntitySnapshot(billing.Reference, 3),
                    new DeadLetterEntitySnapshot(shipping.Reference, 4))
            }
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(repository, workspace, dialogs);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.SelectedDeadLetterEnvironmentFilter = Assert.Single(
            viewModel.DeadLetterEnvironmentFilters,
            filter => filter.ProfileId == dev.Id);

        await viewModel.PurgeEnvironmentDeadLettersCommand.ExecuteAsync();
        Assert.Equal(3, workspace.PurgeRequests[0].Sources.Count);
        Assert.Equal(20, workspace.PurgeRequests[0].BatchSize);
        Assert.All(workspace.PurgeRequests[0].Targets, target =>
            Assert.Equal(ServiceBusSubQueue.DeadLetter, target.SubQueue));
        Assert.Single(dialogs.Confirmations);
        Assert.Equal(1000, workspace.PurgeRequests[0].MaximumMessagesPerSubQueue);

        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.SelectedDlqSource = Assert.Single(
            viewModel.FilteredDeadLetterSources,
            source => source.Entity == billing.Reference);
        await viewModel.PurgeTopicDeadLettersCommand.ExecuteAsync();
        Assert.Equal(
            [billing.Reference, shipping.Reference],
            workspace.PurgeRequests[1].Sources);

        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.SelectedDlqSource = Assert.Single(
            viewModel.FilteredDeadLetterSources,
            source => source.Entity == queue.Reference);
        await viewModel.PurgeSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Equal([queue.Reference], workspace.PurgeRequests[2].Sources);
        Assert.Equal(3, dialogs.Confirmations.Count);
    }

    [Fact]
    public async Task DeadLetterSearch_UsesEnvironmentFilterBuildsTimelineAndRestoresConnection()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue(
            "orders",
            new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 2)));
        var repository = new FakeProfileRepository([dev, test], dev.Id);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            SearchMatches =
            {
                [dev.Id] = [SearchMessage(queue.Reference, 2, "2026-08-12T11:00:00Z")],
                [test.Id] = [SearchMessage(queue.Reference, 1, "2026-08-12T10:00:00Z")]
            }
        };
        await using var viewModel = CreateViewModel(repository, workspace);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "correlation-42";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        Assert.Equal(dev.Id, viewModel.ConnectedProfileId);
        Assert.Equal([dev.Id], viewModel.Messages.Select(message => message.ProfileId));
        Assert.Equal([2L], viewModel.Messages.Select(message => message.SequenceNumber));
        Assert.All(workspace.SearchRequests, request => Assert.Equal("correlation-42", request.Query));
        Assert.Contains("oldest first", viewModel.DeadLetterSearchStatus, StringComparison.OrdinalIgnoreCase);

        viewModel.SelectedMessage = viewModel.Messages[0];
        Assert.True(viewModel.CanOpenSelectedMessageAsDraft);
        viewModel.SelectedDeadLetterEnvironmentFilter = Assert.Single(viewModel.DeadLetterEnvironmentFilters, item => item.ProfileId == test.Id);
        Assert.Empty(viewModel.Messages);
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();
        Assert.Equal([test.Id], viewModel.Messages.Select(message => message.ProfileId));
        Assert.Equal(dev.Id, viewModel.ConnectedProfileId);
        viewModel.SelectedMessage = viewModel.Messages[0];
        Assert.False(viewModel.CanOpenSelectedMessageAsDraft);
    }

    [Fact]
    public async Task BackupViewer_LoadsFiltersOpensDraftAndDeletesOnlyAfterConfirmation()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var message = SearchMessage(source, 42, "2026-08-12T10:00:00Z");
        var summary = new DeadLetterBackupSummary(
            Path.Combine(Path.GetTempPath(), "backups", "message-42.json"),
            dev.Id,
            dev.Name,
            dev.Environment.ToString(),
            dev.FullyQualifiedNamespace,
            source,
            ServiceBusSubQueue.DeadLetter,
            42,
            "message-42",
            "correlation-42",
            "order.failed",
            DateTimeOffset.Parse("2026-08-12T10:00:00Z"),
            DateTimeOffset.Parse("2026-08-12T10:05:00Z"),
            message.BodySize);
        var backups = new FakeBackupRepository(summary, message);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([dev], dev.Id),
            new FakeWorkspace(),
            dialogs,
            backups);

        await viewModel.InitializeAsync();
        Assert.Equal(0, backups.ListCalls);
        viewModel.SelectedNavigation = Assert.Single(
            viewModel.Navigation,
            item => item.Key == nameof(NavigationPage.Backups));
        await viewModel.RefreshBackupsCommand.Completion;
        Assert.Equal(1, backups.ListCalls);

        Assert.Single(viewModel.BackupMessages);
        Assert.Equal("message-42", viewModel.SelectedBackup?.MessageId);
        Assert.Null(viewModel.SelectedBackupMessage);
        await viewModel.LoadSelectedBackupCommand.ExecuteAsync();
        Assert.Equal(42, viewModel.SelectedBackupMessage?.SequenceNumber);

        viewModel.BackupFilterText = "correlation-42";
        Assert.Single(viewModel.FilteredBackupMessages);
        viewModel.BackupFilterText = "does-not-exist";
        Assert.Empty(viewModel.FilteredBackupMessages);
        viewModel.BackupFilterText = string.Empty;
        await viewModel.LoadSelectedBackupCommand.ExecuteAsync();

        await viewModel.ConnectCommand.ExecuteAsync();
        Assert.True(viewModel.CanOpenBackupAsDraft);
        viewModel.OpenBackupAsDraftCommand.Execute(null);
        Assert.Equal(NavigationPage.Composer, viewModel.CurrentPage);
        Assert.Contains("Local backup draft", viewModel.DraftOriginNotice, StringComparison.Ordinal);

        await viewModel.DeleteSelectedBackupCommand.ExecuteAsync();
        Assert.Equal(summary, backups.Deleted);
        Assert.Empty(viewModel.BackupMessages);
        Assert.Single(dialogs.Confirmations);
        Assert.Contains("The queue itself is not changed", dialogs.Confirmations[0].Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Backups_are_grouped_by_environment_topic_and_queue_and_a_whole_topic_can_be_deleted()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var prod = CreateProfile("Production", EnvironmentKind.Production);
        DeadLetterBackupSummary Backup(ServiceBusProfile profile, ServiceBusEntityReference source, long number) =>
            new(Path.Combine(Path.GetTempPath(), "backups", $"{profile.Name}-{source.Path.Replace('/', '_')}-{number}.json"),
                profile.Id, profile.Name, profile.Environment.ToString(), null, source, ServiceBusSubQueue.DeadLetter,
                number, $"m-{number}", null, null, null, DateTimeOffset.UnixEpoch.AddMinutes(number), 10);
        var billing = ServiceBusEntityReference.Subscription("events", "billing");
        var shipping = ServiceBusEntityReference.Subscription("events", "shipping");
        var orders = ServiceBusEntityReference.Queue("orders");
        var backups = new ListBackupRepository(
            Backup(dev, billing, 1), Backup(dev, billing, 2), Backup(dev, shipping, 3),
            Backup(dev, orders, 4), Backup(prod, billing, 5));
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev, prod], dev.Id), new FakeWorkspace(), dialogs, backups);
        await viewModel.InitializeAsync();
        await viewModel.RefreshBackupsCommand.ExecuteAsync();

        Assert.Equal(
            ["All backups:5", "Development · DEV:4", "events:3", "billing:2", "shipping:1", "orders:1", "Production · PROD:1", "events:1", "billing:1"],
            viewModel.BackupGroups.Select(group => $"{group.Title}:{group.Count}"));

        viewModel.SelectedBackupGroup = viewModel.BackupGroups.First(group =>
            group.Kind == BackupGroupKind.Topic && group.Key.Contains(dev.Id.ToString(), StringComparison.Ordinal));
        Assert.Equal(3, viewModel.FilteredBackupMessages.Count);
        Assert.Equal("Delete 3 backups…", viewModel.DeleteVisibleBackupsLabel);

        await viewModel.DeleteVisibleBackupsCommand.ExecuteAsync();

        Assert.Contains("topic 'events'", Assert.Single(dialogs.Confirmations).Message, StringComparison.Ordinal);
        Assert.Equal([1L, 2, 3], backups.Deleted.Select(item => item.SequenceNumber).Order());
        Assert.Equal([4L, 5], viewModel.BackupMessages.Select(item => item.Summary.SequenceNumber).Order());
        Assert.Equal(BackupGroupKind.All, viewModel.SelectedBackupGroup?.Kind);
        Assert.DoesNotContain(viewModel.BackupGroups, group => group.Key.StartsWith($"topic:{dev.Id}", StringComparison.Ordinal));

        dialogs.ConfirmResult = false;
        await viewModel.DeleteVisibleBackupsCommand.ExecuteAsync();
        Assert.Equal(3, backups.Deleted.Count);
    }

    private sealed class ListBackupRepository(params DeadLetterBackupSummary[] summaries) : IDeadLetterBackupRepository
    {
        public string RootDirectory => Path.Combine(Path.GetTempPath(), "backups");

        public List<DeadLetterBackupSummary> Deleted { get; } = [];

        public Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<DeadLetterBackupSummary>>(summaries.Except(Deleted).ToArray());

        public Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) =>
            Task.FromResult(SearchMessage(summary.Source, summary.SequenceNumber, "2026-08-12T10:00:00Z"));

        public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default)
        {
            Deleted.Add(summary);
            return Task.CompletedTask;
        }
    }

    private static MainWindowViewModel CreateViewModel(
        IProfileRepository repository,
        IServiceBusWorkspace workspace,
        IUserDialogService? dialogs = null,
        IDeadLetterBackupRepository? backupRepository = null,
        ISecretVault? secretVault = null,
        QueueLoom.App.Services.IMonitorAlertService? alerts = null,
        IDeadLetterHistoryStore? history = null,
        IBatchReplayStore? replayStore = null,
        IActivityJournal? activityJournal = null,
        IScheduledResendStore? scheduledResends = null) =>
        new(
            repository,
            secretVault ?? new FakeSecretVault(),
            workspace,
            dialogs ?? new FakeDialogService(),
            backupRepository,
            activityJournal: activityJournal,
            replayStore: replayStore,
            alerts: alerts,
            history: history,
            scheduledResends: scheduledResends);

    internal static ServiceBusProfile CreateProfile(
        string name,
        EnvironmentKind environment,
        ProfileAccessMode accessMode = ProfileAccessMode.ReadOnly) =>
        new(
            Guid.NewGuid(),
            name,
            environment,
            null,
            $"{name.ToLowerInvariant()}.servicebus.windows.net",
            AuthenticationSettings.Entra(),
            accessMode);

    private static ServiceBusProfile CreateConnectionStringProfile(string name) =>
        new(
            Guid.NewGuid(),
            name,
            EnvironmentKind.Development,
            null,
            $"{name.ToLowerInvariant()}.servicebus.windows.net",
            AuthenticationSettings.ConnectionString(),
            ProfileAccessMode.ReadOnly);

    private static DeadLetterSnapshot Snapshot(Guid profileId, params DeadLetterEntitySnapshot[] entities) =>
        new(profileId, DateTimeOffset.UtcNow, entities);

    private static DlqSourceItemViewModel CreateSource(ServiceBusEntityReference entity) =>
        new(
            Guid.NewGuid(),
            "Development",
            "DEV",
            Tone.Accent,
            new DeadLetterEntitySnapshot(entity, 1));

    internal static BrowsedMessage SearchMessage(
        ServiceBusEntityReference source,
        long sequenceNumber,
        string enqueuedAt) =>
        new(
            source,
            ServiceBusSubQueue.DeadLetter,
            sequenceNumber,
            Encoding.UTF8.GetBytes("correlation-42"),
            new EditableMessageProperties(CorrelationId: "correlation-42"),
            enqueuedAt: DateTimeOffset.Parse(enqueuedAt));

    private static DeadLetterBackupSummary CreateBackupSummary(
        ServiceBusProfile profile,
        ServiceBusEntityReference source,
        BrowsedMessage message) =>
        new(
            Path.Combine(Path.GetTempPath(), "backups", $"message-{message.SequenceNumber}.json"),
            profile.Id,
            profile.Name,
            profile.Environment.ToString(),
            profile.FullyQualifiedNamespace,
            source,
            message.SubQueue,
            message.SequenceNumber,
            $"message-{message.SequenceNumber}",
            message.Properties.CorrelationId,
            message.Properties.Subject,
            message.EnqueuedAt,
            DateTimeOffset.UtcNow,
            message.BodySize);

    internal sealed class FakeProfileRepository(
        IReadOnlyList<ServiceBusProfile> profiles,
        Guid? selectedProfileId) : IProfileRepository
    {
        private readonly List<ServiceBusProfile> _profiles = [.. profiles];
        private Guid? _selectedProfileId = selectedProfileId;

        public bool FailNextSetSelected { get; set; }
        /// <summary>Upserts after this many succeed fail (null: never).</summary>
        public int? FailUpsertsAfter { get; set; }
        public Func<CancellationToken, Task>? ListGate { get; set; }

        public async Task<IReadOnlyList<ServiceBusProfile>> ListAsync(CancellationToken cancellationToken = default)
        {
            if (ListGate is not null) await ListGate(cancellationToken);
            return _profiles.ToArray();
        }

        public Task<ServiceBusProfile?> GetAsync(Guid profileId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_profiles.FirstOrDefault(profile => profile.Id == profileId));

        public Task UpsertAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
            if (FailUpsertsAfter is { } remaining)
            {
                if (remaining <= 0) return Task.FromException(new IOException("The profiles file could not be written."));
                FailUpsertsAfter = remaining - 1;
            }
            _profiles.RemoveAll(item => item.Id == profile.Id);
            _profiles.Add(profile);
            return Task.CompletedTask;
        }

        public Task<bool> DeleteAsync(Guid profileId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_profiles.RemoveAll(profile => profile.Id == profileId) > 0);

        public Task<Guid?> GetSelectedProfileIdAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(_selectedProfileId);

        public Task SetSelectedProfileIdAsync(Guid? profileId, CancellationToken cancellationToken = default)
        {
            if (FailNextSetSelected)
            {
                FailNextSetSelected = false;
                throw new InvalidOperationException("Selected profile failure.");
            }

            _selectedProfileId = profileId;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSecretVault : ISecretVault
    {
        private readonly Dictionary<ProfileSecretKey, string> _secrets = [];

        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default)
        {
            _secrets[key] = secret;
            return ValueTask.CompletedTask;
        }

        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_secrets.GetValueOrDefault(key));

        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_secrets.ContainsKey(key));

        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_secrets.Remove(key));
    }

    private sealed class FakeBackupRepository(
        DeadLetterBackupSummary summary,
        BrowsedMessage message) : IDeadLetterBackupRepository
    {
        public string RootDirectory => Path.Combine(Path.GetTempPath(), "backups");

        public DeadLetterBackupSummary? Deleted { get; private set; }

        public int ListCalls { get; private set; }

        public Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(
            CancellationToken cancellationToken = default)
        {
            ListCalls++;
            return Task.FromResult<IReadOnlyList<DeadLetterBackupSummary>>(
                Deleted is null ? [summary] : []);
        }

        public Task<BrowsedMessage> LoadAsync(
            DeadLetterBackupSummary selected,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(message);

        public Task DeleteAsync(
            DeadLetterBackupSummary selected,
            CancellationToken cancellationToken = default)
        {
            Deleted = selected;
            return Task.CompletedTask;
        }
    }

    internal sealed class FakeWorkspace : IServiceBusWorkspace, ICleanupWarningSource
    {
        public Dictionary<Guid, DeadLetterSnapshot> Snapshots { get; } = [];

        public List<DeadLetterPurgeRequest> PurgeRequests { get; } = [];

        public List<DeadLetterSearchRequest> SearchRequests { get; } = [];

        public Func<CancellationToken, Task>? SearchGate { get; set; }

        public List<BrowseMessagesRequest> BrowseRequests { get; } = [];

        public IReadOnlyList<BrowsedMessage> BrowseMessages { get; set; } = [];
        public List<SendMessageRequest> SentMessages { get; } = [];
        public Action? OnSend { get; set; }
        public string? BrowseCleanupWarning { get; set; }
        public event EventHandler<string>? CleanupWarning;
        public Func<Task>? SendGate { get; set; }
        public Action? OnDelete { get; set; }
        public Func<CancellationToken, Task>? CleanupOperationGate { get; set; }
        public int DisposeCalls { get; private set; }

        public Dictionary<Guid, IReadOnlyList<BrowsedMessage>> SearchMatches { get; } = [];

        public ServiceBusTopology Topology { get; set; } = new(DateTimeOffset.UtcNow);

        public bool FailNextConnection { get; set; }

        public bool WaitForConnectionCancellation { get; set; }

        public bool WaitForSnapshotCancellation { get; set; }

        public TaskCompletionSource<bool> SnapshotStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int CancelledSnapshotCalls { get; private set; }

        public TaskCompletionSource<bool>? ConnectionRelease { get; set; }

        public int ConnectCalls { get; private set; }

        public Action? OnConnect { get; set; }

        public int TopologyCalls { get; private set; }

        public int DisconnectCalls { get; private set; }

        public List<ProfileAccessMode> AccessModeChanges { get; } = [];

        public WorkspaceConnectionState ConnectionState { get; private set; }

        public Guid? ConnectedProfileId { get; private set; }

        public string? ConnectedNamespace { get; private set; }
        public string? ConnectedConfigurationIdentity { get; private set; }
        public MessagingProvider? ConnectedProvider { get; private set; }

        public ProfileAccessMode? ConnectedAccessMode { get; private set; }

        public async Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
            ConnectCalls++;
            OnConnect?.Invoke();
            if (ConnectionRelease is not null)
            {
                await ConnectionRelease.Task.WaitAsync(cancellationToken);
            }
            else if (WaitForConnectionCancellation)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            if (FailNextConnection)
            {
                FailNextConnection = false;
                ConnectionState = WorkspaceConnectionState.Faulted;
                ConnectedProfileId = null;
                throw new InvalidOperationException("Connection failed.");
            }

            ConnectionState = WorkspaceConnectionState.Connected;
            ConnectedProfileId = profile.Id;
            ConnectedNamespace = profile.EndpointDisplay;
            ConnectedConfigurationIdentity = ScheduledResend.IdentityFor(profile);
            ConnectedProvider = profile.Provider;
            ConnectedAccessMode = profile.AccessMode;
        }

        public bool FailReadOnlyRevert { get; set; }
        public bool FailDisconnect { get; set; }

        public Task DisconnectAsync(CancellationToken cancellationToken = default)
        {
            DisconnectCalls++;
            if (FailDisconnect) return Task.FromException(new IOException("disconnect failed"));
            ConnectionState = WorkspaceConnectionState.Disconnected;
            ConnectedProfileId = null;
            ConnectedAccessMode = null;
            return Task.CompletedTask;
        }

        public Task SetAccessModeAsync(
            ProfileAccessMode accessMode,
            CancellationToken cancellationToken = default)
        {
            AccessModeChanges.Add(accessMode);
            if (FailReadOnlyRevert && accessMode == ProfileAccessMode.ReadOnly)
                return Task.FromException(new IOException("revert failed"));
            ConnectedAccessMode = accessMode;
            return Task.CompletedTask;
        }

        public Task<ServiceBusTopology> GetTopologyAsync(
            bool forceRefresh = false,
            CancellationToken cancellationToken = default)
        {
            TopologyCalls++;
            return Task.FromResult(Topology);
        }

        public async Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
            BrowseMessagesRequest request,
            CancellationToken cancellationToken = default)
        {
            BrowseRequests.Add(request);
            if (BrowseCleanupWarning is { } warning) CleanupWarning?.Invoke(this, warning);
            if (CleanupOperationGate is not null) await CleanupOperationGate(cancellationToken);
            return BrowseMessages
                .Where(m => m.SequenceNumber >= (request.FromSequenceNumber ?? 0))
                .Take(request.MaxMessages).ToArray();
        }

        public async Task<DeadLetterSearchResult> SearchDeadLettersAsync(
            DeadLetterSearchRequest request,
            CancellationToken cancellationToken = default)
        {
            SearchRequests.Add(request);
            if (SearchGate is not null) await SearchGate(cancellationToken);
            var now = DateTimeOffset.UtcNow;
            var profileId = ConnectedProfileId ?? throw new InvalidOperationException("Not connected.");
            var matches = SearchMatches.TryGetValue(profileId, out var configured) ? configured : [];
            var target = request.Targets[0];
            return new DeadLetterSearchResult(
                profileId,
                now,
                now,
                [new DeadLetterSearchSourceResult(
                    target.Source,
                    target.SubQueue,
                    matches.Count,
                    matches)]);
        }

        public async Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            SentMessages.Add(request);
            OnSend?.Invoke();
            if (CleanupOperationGate is not null) await CleanupOperationGate(cancellationToken);
            if (SendGate is not null) await SendGate();
        }

        public Task ResubmitDeadLetterAsync(
            ResubmitDeadLetterRequest request,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
            DeadLetterPurgeRequest request,
            CancellationToken cancellationToken = default,
            IProgress<DeadLetterPurgeProgress>? progress = null)
        {
            PurgeRequests.Add(request);
            var now = DateTimeOffset.UtcNow;
            var results = request.Targets.Select(target =>
                new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, 0));
            return Task.FromResult(new DeadLetterPurgeResult(
                ConnectedProfileId ?? throw new InvalidOperationException("Not connected."),
                now,
                now,
                results,
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup")));
        }

        public QueueManagementCapabilities? QueueManagement { get; set; }

        public bool SupportsSubscriptionRules { get; set; }

        public QueueLoom.Core.Routing.RoutingService RoutingService { get; set; } = QueueLoom.Core.Routing.RoutingService.ServiceBus;

        public Dictionary<string, List<QueueLoom.Core.Routing.SubscriptionRules>> TopicRules { get; } = [];

        public List<string> RuleChanges { get; } = [];

        public Task<IReadOnlyList<QueueLoom.Core.Routing.SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<QueueLoom.Core.Routing.SubscriptionRules>>(TopicRules.TryGetValue(topic, out var rules) ? rules.ToArray() : []);

        public Task SaveSubscriptionRuleAsync(string topic, string subscription, QueueLoom.Core.Routing.SubscriptionRule rule, bool replace,
            CancellationToken cancellationToken = default)
        {
            RuleChanges.Add($"{(replace ? "replace" : "add")} {topic}/{subscription}/{rule.Name}");
            var list = TopicRules[topic];
            var index = list.FindIndex(item => item.Subscription == subscription);
            list[index] = list[index] with { Rules = list[index].Rules.Where(item => item.Name != rule.Name).Append(rule).ToArray() };
            return Task.CompletedTask;
        }

        public Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default)
        {
            RuleChanges.Add($"delete {topic}/{subscription}/{rule}");
            var list = TopicRules[topic];
            var index = list.FindIndex(item => item.Subscription == subscription);
            list[index] = list[index] with { Rules = list[index].Rules.Where(item => item.Name != rule).ToArray() };
            return Task.CompletedTask;
        }

        public List<QueueDefinition> CreatedQueues { get; } = [];

        public List<(string Queue, QueueSettings Settings)> UpdatedQueues { get; } = [];

        public List<string> DeletedQueues { get; } = [];

        public QueueSettings CurrentQueueSettings { get; set; } = new();

        public Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
            Task.FromResult(CurrentQueueSettings);

        public async Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default)
        {
            if (CleanupOperationGate is not null) await CleanupOperationGate(cancellationToken);
            CreatedQueues.Add(definition);
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                Topology.Queues.Append(new ServiceBusQueue(definition.Name, ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active)),
                Topology.Topics);
        }

        public Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default)
        {
            UpdatedQueues.Add((queue, settings));
            return Task.CompletedTask;
        }

        public Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default)
        {
            DeletedQueues.Add(queue);
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, Topology.Queues.Where(item => item.Name != queue), Topology.Topics);
            return Task.CompletedTask;
        }

        public List<IReadOnlyList<BrowsedMessage>> PendingRemovals { get; } = [];

        /// <summary>The outcome the fake reports for each pending message; removed when not set (e.g. Cancelled for a stopped run).</summary>
        public Func<BrowsedMessage, DeadLetterMessageDeletionOutcome>? PendingOutcome { get; set; }

        public Task<RemovePendingMessagesResult> RemovePendingMessagesAsync(
            IReadOnlyList<BrowsedMessage> messages,
            CancellationToken cancellationToken = default)
        {
            PendingRemovals.Add(messages);
            return Task.FromResult(new RemovePendingMessagesResult(
                messages.Select(message => new PendingMessageRemovalResult(message, PendingOutcome?.Invoke(message) ?? DeadLetterMessageDeletionOutcome.Deleted)).ToArray(),
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup")));
        }

        public List<DeleteDeadLetterMessagesRequest> DeleteRequests { get; } = [];

        /// <summary>Sequence numbers reported as not found by the fake deletion.</summary>
        public HashSet<long> MissingSequenceNumbers { get; } = [];
        /// <summary>Settlement fails for these (it may have been accepted before the response was lost).</summary>
        public HashSet<long> FailedSequenceNumbers { get; } = [];

        public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
            DeleteDeadLetterMessagesRequest request,
            CancellationToken cancellationToken = default,
            IProgress<DeadLetterMessageDeletionProgress>? progress = null)
        {
            DeleteRequests.Add(request);
            OnDelete?.Invoke();
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(new DeleteDeadLetterMessagesResult(
                ConnectedProfileId ?? throw new InvalidOperationException("Not connected."),
                now,
                now,
                request.Messages.Select(key => new DeadLetterMessageDeletionResult(
                    key,
                    FailedSequenceNumbers.Contains(key.SequenceNumber)
                        ? DeadLetterMessageDeletionOutcome.Failed
                        : MissingSequenceNumbers.Contains(key.SequenceNumber)
                            ? DeadLetterMessageDeletionOutcome.NotFound
                            : DeadLetterMessageDeletionOutcome.Deleted,
                    FailedSequenceNumbers.Contains(key.SequenceNumber) ? "Settlement response lost."
                        : MissingSequenceNumbers.Contains(key.SequenceNumber) ? "Already gone." : null)),
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup")));
        }

        public async Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
            DeadLetterMonitorScope scope,
            CancellationToken cancellationToken = default)
        {
            if (WaitForSnapshotCancellation)
            {
                SnapshotStarted.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    CancelledSnapshotCalls++;
                    throw;
                }
            }

            var profileId = ConnectedProfileId ?? throw new InvalidOperationException("Not connected.");
            return Snapshots.TryGetValue(profileId, out var snapshot)
                ? snapshot
                : Snapshot(profileId);
        }

        public Func<ValueTask>? DisposeGate { get; set; }
        public ValueTask DisposeAsync() { DisposeCalls++; return DisposeGate?.Invoke() ?? ValueTask.CompletedTask; }
    }

    private sealed class FakeDialogService : IUserDialogService
    {
        public Action? BeforeConfirm { get; set; }
        public bool ConfirmResult { get; set; }

        public ProfileEditorResult? EditResult { get; set; }

        public TaskCompletionSource<ProfileEditorResult?>? PendingEdit { get; set; }

        public List<(string Title, string Message, bool IsDangerous, string? RequiredText)> Confirmations { get; } = [];

        public async Task<ProfileEditorResult?> EditProfileAsync(
            ServiceBusProfile? profile,
            CancellationToken cancellationToken = default)
        {
            if (PendingEdit is not null)
            {
                return await PendingEdit.Task.WaitAsync(cancellationToken);
            }

            return EditResult;
        }

        public Task<bool> ConfirmAsync(
            string title,
            string message,
            bool isDangerous = false,
            string? requiredText = null,
            CancellationToken cancellationToken = default)
        {
            Confirmations.Add((title, message, isDangerous, requiredText));
            BeforeConfirm?.Invoke();
            return Task.FromResult(ConfirmResult);
        }

        public Task ShowMessageAsync(
            string title,
            string message,
            bool isError = false,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        /// <summary>The file the operator picks in an open dialog; null cancels.</summary>
        public string? OpenFilePath { get; set; }

        public Task<string?> ChooseOpenFileAsync(
            string title,
            IReadOnlyList<(string Name, string Pattern)> fileTypes,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(OpenFilePath);

        /// <summary>The file the operator picks in a save dialog; null cancels.</summary>
        public string? SaveFilePath { get; set; }

        public Task<string?> ChooseSaveFileAsync(
            string title,
            string suggestedFileName,
            IReadOnlyList<(string Name, string Pattern)> fileTypes,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(SaveFilePath);

        /// <summary>Fills in the queue dialog like an operator would; null cancels it.</summary>
        public Action<QueueDialogViewModel>? FillQueueDialog { get; set; }

        public List<QueueDialogViewModel> QueueDialogs { get; } = [];

        public Task<object?> EditQueueAsync(QueueDialogViewModel viewModel, CancellationToken cancellationToken = default)
        {
            QueueDialogs.Add(viewModel);
            if (FillQueueDialog is null)
            {
                return Task.FromResult<object?>(null);
            }
            FillQueueDialog(viewModel);
            return Task.FromResult<object?>(viewModel.IsNew ? viewModel.TryBuildDefinition() : viewModel.TryBuildSettings());
        }

        public List<ResendDialogViewModel> ResendDialogs { get; } = [];

        public List<CompareDialogViewModel> Comparisons { get; } = [];

        public List<TopicRoutingViewModel> RoutingDialogs { get; } = [];

        public Func<TopicRoutingViewModel, Task>? OnRouting { get; set; }
        public Func<CancellationToken, Task>? RoutingGate { get; set; }

        public Func<RuleEditorViewModel, QueueLoom.Core.Routing.SubscriptionRule?>? RuleEdit { get; set; }

        public async Task ShowTopicRoutingAsync(TopicRoutingViewModel viewModel, CancellationToken cancellationToken = default)
        {
            RoutingDialogs.Add(viewModel);
            await viewModel.LoadAsync(cancellationToken);
            if (RoutingGate is not null) await RoutingGate(cancellationToken);
            if (OnRouting is not null)
            {
                await OnRouting(viewModel);
            }
        }

        public Task<QueueLoom.Core.Routing.SubscriptionRule?> EditRuleAsync(RuleEditorViewModel viewModel, CancellationToken cancellationToken = default) =>
            Task.FromResult(RuleEdit?.Invoke(viewModel));

        public Task ShowComparisonAsync(CompareDialogViewModel viewModel, CancellationToken cancellationToken = default)
        {
            Comparisons.Add(viewModel);
            return Task.CompletedTask;
        }

        /// <summary>What the operator picks in the resend dialog; null cancels. Defaults to the dialog's defaults.</summary>
        public Func<ResendDialogViewModel, ResendOptions?>? ResendChoice { get; set; }

        public Task<ResendOptions?> ChooseResendOptionsAsync(
            ResendDialogViewModel viewModel,
            CancellationToken cancellationToken = default)
        {
            ResendDialogs.Add(viewModel);
            return Task.FromResult(ResendChoice is null ? viewModel.ToOptions() : ResendChoice(viewModel));
        }
    }
}
