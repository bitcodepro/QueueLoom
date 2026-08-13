using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using System.Text;

namespace QueueLoom.Tests;

public sealed class ViewModelStateTests
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
    public async Task MonitorDoesNotChangeSelectedEnvironmentAndRetainsNotificationsUntilCleared()
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

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.MonitorStatus.Contains("last check", StringComparison.OrdinalIgnoreCase));
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.Single(viewModel.MonitorNotifications);
        viewModel.ClearMonitorNotificationsCommand.Execute(null);
        Assert.Empty(viewModel.MonitorNotifications);
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

        Assert.Equal(3, viewModel.DeadLetterEnvironmentFilters.Count);
        Assert.Equal(3, viewModel.VisibleDlqSourceRowCount);
        Assert.Equal(15, viewModel.VisibleDlqSourceCount);
        Assert.Equal(15, viewModel.GlobalDlqSourceCount);
        Assert.Equal(["jobs", "billing", "alerts"], viewModel.FilteredDeadLetterSources.Select(item => item.EntityName));

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
            item => item.IsAllEnvironments);

        Assert.Same(originalSelection, viewModel.SelectedDlqSource);
        Assert.Equal(15, viewModel.VisibleDlqSourceCount);
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
        Assert.Empty(dialogs.Confirmations);

        viewModel.SelectedDlqSource = Assert.Single(
            viewModel.FilteredDeadLetterSources,
            source => source.Entity == billing.Reference);
        await viewModel.PurgeTopicDeadLettersCommand.ExecuteAsync();
        Assert.Equal(
            [billing.Reference, shipping.Reference],
            workspace.PurgeRequests[1].Sources);

        viewModel.SelectedDlqSource = Assert.Single(
            viewModel.FilteredDeadLetterSources,
            source => source.Entity == queue.Reference);
        await viewModel.PurgeSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Equal([queue.Reference], workspace.PurgeRequests[2].Sources);
        Assert.Empty(dialogs.Confirmations);
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
        Assert.Equal([test.Id, dev.Id], viewModel.Messages.Select(message => message.ProfileId));
        Assert.Equal([1L, 2L], viewModel.Messages.Select(message => message.SequenceNumber));
        Assert.All(workspace.SearchRequests, request => Assert.Equal("correlation-42", request.Query));
        Assert.Contains("oldest first", viewModel.DeadLetterSearchStatus, StringComparison.OrdinalIgnoreCase);

        viewModel.SelectedMessage = viewModel.Messages[0];
        Assert.False(viewModel.CanOpenSelectedMessageAsDraft);
        viewModel.SelectedMessage = viewModel.Messages[1];
        Assert.True(viewModel.CanOpenSelectedMessageAsDraft);
    }

    private static MainWindowViewModel CreateViewModel(
        IProfileRepository repository,
        IServiceBusWorkspace workspace,
        IUserDialogService? dialogs = null) =>
        new(repository, new FakeSecretVault(), workspace, dialogs ?? new FakeDialogService());

    private static ServiceBusProfile CreateProfile(
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

    private static DeadLetterSnapshot Snapshot(Guid profileId, params DeadLetterEntitySnapshot[] entities) =>
        new(profileId, DateTimeOffset.UtcNow, entities);

    private static DlqSourceItemViewModel CreateSource(ServiceBusEntityReference entity) =>
        new(
            Guid.NewGuid(),
            "Development",
            "DEV",
            "#2DD4BF",
            new DeadLetterEntitySnapshot(entity, 1));

    private static BrowsedMessage SearchMessage(
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

    private sealed class FakeProfileRepository(
        IReadOnlyList<ServiceBusProfile> profiles,
        Guid? selectedProfileId) : IProfileRepository
    {
        private readonly List<ServiceBusProfile> _profiles = [.. profiles];
        private Guid? _selectedProfileId = selectedProfileId;

        public Task<IReadOnlyList<ServiceBusProfile>> ListAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<ServiceBusProfile>>(_profiles.ToArray());

        public Task<ServiceBusProfile?> GetAsync(Guid profileId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_profiles.FirstOrDefault(profile => profile.Id == profileId));

        public Task UpsertAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
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
            _selectedProfileId = profileId;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSecretVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default) =>
            ValueTask.CompletedTask;

        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<string?>(null);

        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(false);
    }

    private sealed class FakeWorkspace : IServiceBusWorkspace
    {
        public Dictionary<Guid, DeadLetterSnapshot> Snapshots { get; } = [];

        public List<DeadLetterPurgeRequest> PurgeRequests { get; } = [];

        public List<DeadLetterSearchRequest> SearchRequests { get; } = [];

        public Dictionary<Guid, IReadOnlyList<BrowsedMessage>> SearchMatches { get; } = [];

        public ServiceBusTopology Topology { get; set; } = new(DateTimeOffset.UtcNow);

        public bool FailNextConnection { get; set; }

        public WorkspaceConnectionState ConnectionState { get; private set; }

        public Guid? ConnectedProfileId { get; private set; }

        public Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
            if (FailNextConnection)
            {
                FailNextConnection = false;
                ConnectionState = WorkspaceConnectionState.Faulted;
                ConnectedProfileId = null;
                throw new InvalidOperationException("Connection failed.");
            }

            ConnectionState = WorkspaceConnectionState.Connected;
            ConnectedProfileId = profile.Id;
            return Task.CompletedTask;
        }

        public Task DisconnectAsync(CancellationToken cancellationToken = default)
        {
            ConnectionState = WorkspaceConnectionState.Disconnected;
            ConnectedProfileId = null;
            return Task.CompletedTask;
        }

        public Task<ServiceBusTopology> GetTopologyAsync(
            bool forceRefresh = false,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Topology);

        public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
            BrowseMessagesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<BrowsedMessage>>([]);

        public Task<DeadLetterSearchResult> SearchDeadLettersAsync(
            DeadLetterSearchRequest request,
            CancellationToken cancellationToken = default)
        {
            SearchRequests.Add(request);
            var now = DateTimeOffset.UtcNow;
            var profileId = ConnectedProfileId ?? throw new InvalidOperationException("Not connected.");
            var matches = SearchMatches.TryGetValue(profileId, out var configured) ? configured : [];
            var target = request.Targets[0];
            return Task.FromResult(new DeadLetterSearchResult(
                profileId,
                now,
                now,
                [new DeadLetterSearchSourceResult(
                    target.Source,
                    target.SubQueue,
                    matches.Count,
                    matches)]));
        }

        public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task ResubmitDeadLetterAsync(
            ResubmitDeadLetterRequest request,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
            DeadLetterPurgeRequest request,
            CancellationToken cancellationToken = default)
        {
            PurgeRequests.Add(request);
            var now = DateTimeOffset.UtcNow;
            var results = request.Sources.SelectMany(source =>
                request.SubQueues.Select(subQueue =>
                    new DeadLetterPurgeSourceResult(source, subQueue, 0)));
            return Task.FromResult(new DeadLetterPurgeResult(
                ConnectedProfileId ?? throw new InvalidOperationException("Not connected."),
                now,
                now,
                results,
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup")));
        }

        public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
            DeadLetterMonitorScope scope,
            CancellationToken cancellationToken = default)
        {
            var profileId = ConnectedProfileId ?? throw new InvalidOperationException("Not connected.");
            return Task.FromResult(Snapshots.TryGetValue(profileId, out var snapshot)
                ? snapshot
                : Snapshot(profileId));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeDialogService : IUserDialogService
    {
        public bool ConfirmResult { get; set; }

        public List<(string Title, string Message, bool IsDangerous, string? RequiredText)> Confirmations { get; } = [];

        public Task<ProfileEditorResult?> EditProfileAsync(
            ServiceBusProfile? profile,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<ProfileEditorResult?>(null);

        public Task<bool> ConfirmAsync(
            string title,
            string message,
            bool isDangerous = false,
            string? requiredText = null,
            CancellationToken cancellationToken = default)
        {
            Confirmations.Add((title, message, isDangerous, requiredText));
            return Task.FromResult(ConfirmResult);
        }

        public Task ShowMessageAsync(
            string title,
            string message,
            bool isError = false,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
