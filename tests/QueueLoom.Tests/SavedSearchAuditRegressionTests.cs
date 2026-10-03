using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("next-page", false)]
    [InlineData("next-page", true)]
    [InlineData("first-page", false)]
    [InlineData("first-page", true)]
    [InlineData("connection", false)]
    [InlineData("connection", true)]
    public async Task SavedSearchAudit_MissingBookmarkDiscardsDelayedBrowseAndPreservesFreshPaging(string phase, bool restoreScope)
    {
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var other = CreateProfile("Other", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 200)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            BrowseMessages = Enumerable.Range(1, 200)
                .Select(sequence => SearchMessage(queue.Reference, sequence, "2026-08-12T10:00:00Z")).ToArray()
        };
        foreach (var profile in new[] { development, other })
        {
            workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow,
                [new DeadLetterEntitySnapshot(queue.Reference, 200)]);
        }
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, other], development.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();
        var profileId = phase == "connection" ? other.Id : development.Id;
        var scope = viewModel.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == profileId);
        viewModel.SelectedDeadLetterEnvironmentFilter = scope;
        var source = Assert.Single(viewModel.FilteredDeadLetterSources);
        viewModel.SelectedDlqSource = source;
        if (phase == "next-page")
        {
            await viewModel.BrowseDlqSourceCommand.ExecuteAsync();
            Assert.Equal(100, viewModel.Messages.Count);
            Assert.True(viewModel.CanLoadMoreMessages);
        }

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (phase == "connection")
        {
            workspace.OnConnect = () => started.TrySetResult();
            workspace.ConnectionRelease = release;
        }
        else
        {
            workspace.CleanupOperationGate = _ =>
            {
                started.TrySetResult();
                // A provider may return late without observing cancellation.
                return release.Task;
            };
        }
        var pending = phase == "next-page"
            ? viewModel.LoadMoreMessagesCommand.ExecuteAsync()
            : viewModel.BrowseDlqSourceCommand.ExecuteAsync();
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(viewModel.IsBusy);
            var browseCalls = workspace.BrowseRequests.Count;
            var connectCalls = workspace.ConnectCalls;
            viewModel.SelectedSavedSearch = new SavedSearch("Deleted A", "missing-a", Guid.NewGuid());
            viewModel.SelectedSavedSearch = new SavedSearch("Deleted B", "missing-b", Guid.NewGuid());
            Assert.Empty(viewModel.Messages);
            Assert.Null(viewModel.SelectedMessage);
            Assert.Null(viewModel.SelectedDeadLetterEnvironmentFilter);
            Assert.Equal(browseCalls, workspace.BrowseRequests.Count);
            Assert.Equal(connectCalls, workspace.ConnectCalls);
            if (restoreScope) viewModel.SelectedDeadLetterEnvironmentFilter = scope;
        }
        finally
        {
            release.TrySetResult(true);
            await pending.WaitAsync(TimeSpan.FromSeconds(10));
            workspace.OnConnect = null;
            workspace.ConnectionRelease = null;
            workspace.CleanupOperationGate = null;
        }

        Assert.False(viewModel.IsBusy);
        Assert.Empty(viewModel.Messages);
        Assert.Null(viewModel.SelectedMessage);
        Assert.False(viewModel.CanLoadMoreMessages);
        Assert.Contains("no longer available", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("Saved search unavailable", viewModel.MessageListTitle);

        // The workspace gate must be released and a new browse must retain ordinary continuation.
        viewModel.SelectedDeadLetterEnvironmentFilter = scope;
        viewModel.SelectedDlqSource = source;
        await viewModel.BrowseDlqSourceCommand.ExecuteAsync();
        Assert.Equal(100, viewModel.Messages.Count);
        Assert.True(viewModel.CanLoadMoreMessages);
        await viewModel.LoadMoreMessagesCommand.ExecuteAsync();
        Assert.Equal(200, viewModel.Messages.Count);
        Assert.Equal(101, workspace.BrowseRequests[^1].FromSequenceNumber);
        Assert.Equal(200, viewModel.Messages.Select(message => message.Message.SequenceNumber).Distinct().Count());
        Assert.NotNull(viewModel.SelectedMessage);
    }

    [Theory]
    [InlineData("$.order.customer.account.identifier == 'customer-1001'", "$.order.customer.account.identifier == 'customer-1002'")]
    [InlineData("$.region == 'EU'", "$.region == 'eu'")]
    public async Task SavedSearchAudit_DifferentQueriesWithCollidingLabelsBothSurviveRestart(string first, string second)
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace());
        await viewModel.InitializeAsync();

        viewModel.DeadLetterSearchQuery = first;
        viewModel.SaveSearchCommand.Execute(null);
        viewModel.DeadLetterSearchQuery = second;
        viewModel.SaveSearchCommand.Execute(null);

        Assert.Equal(2, viewModel.SavedSearches.Count);
        Assert.Equal([second, first], viewModel.SavedSearches.Select(search => search.Query));
        Assert.Equal(2, viewModel.SavedSearches.Select(search => search.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());

        using var directory = new TemporaryDirectory();
        using var settings = new JsonAppSettingsStore(QueueLoomPaths.ForRoot(directory.Path));
        await settings.UpdateAsync(current => current with { SavedSearches = viewModel.SavedSearches.ToArray() });
        var reloaded = await settings.LoadAsync();
        Assert.Equal([second, first], reloaded.SavedSearches.Select(search => search.Query));

        // Re-saving the exact query updates that one bookmark, without duplicating it or discarding its neighbour.
        viewModel.DeadLetterSearchQuery = first;
        viewModel.SaveSearchCommand.Execute(null);
        Assert.Equal([first, second], viewModel.SavedSearches.Select(search => search.Query));
        Assert.Equal(2, viewModel.SavedSearches.Select(search => search.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public async Task SavedSearchAudit_DeletedEnvironmentDoesNotSearchTheCurrentlySelectedEnvironment()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var missing = new SavedSearch("Deleted production", "correlation-42", Guid.NewGuid());
        viewModel.SavedSearches.Add(missing);
        var searchesBefore = workspace.SearchRequests.Count;
        var connectionsBefore = workspace.ConnectCalls;

        viewModel.SelectedSavedSearch = missing;
        await viewModel.SearchDeadLettersCommand.Completion;

        Assert.Equal(searchesBefore, workspace.SearchRequests.Count);
        Assert.Equal(connectionsBefore, workspace.ConnectCalls);
        Assert.Null(viewModel.SelectedDeadLetterEnvironmentFilter);
        Assert.Contains("environment", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("no longer available", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(viewModel.Messages);
    }

    [Fact]
    public async Task SavedSearchAudit_LegacyUnscopedSearchUsesTheSelectedEnvironment()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var selected = viewModel.SelectedDeadLetterEnvironmentFilter;
        var searchesBefore = workspace.SearchRequests.Count;

        viewModel.SelectedSavedSearch = new SavedSearch("Legacy", "correlation-42");
        await viewModel.SearchDeadLettersCommand.Completion;

        Assert.Same(selected, viewModel.SelectedDeadLetterEnvironmentFilter);
        Assert.Equal(searchesBefore + 1, workspace.SearchRequests.Count);
        Assert.Equal(3, viewModel.Messages.Count);
    }

    [Fact]
    public async Task SavedSearchAudit_ExactQueryInDifferentWindowsRemainsDistinct()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace());
        await viewModel.InitializeAsync();
        viewModel.DeadLetterSearchQuery = "$.region == 'EU'";
        viewModel.SaveSearchCommand.Execute(null);
        viewModel.SearchWindow = MainWindowViewModel.SearchWindows.Single(option => option.Minutes == 60);
        viewModel.SaveSearchCommand.Execute(null);
        viewModel.SaveSearchCommand.Execute(null);

        Assert.Equal(2, viewModel.SavedSearches.Count);
        Assert.Equal(new int?[] { 60, null }, viewModel.SavedSearches.Select(search => search.WithinMinutes));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavedSearchAudit_MissingBookmarkDiscardsLateSearchEvenAfterScopeIsRestored(bool restoreScope)
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = viewModel;
        var originalScope = viewModel.SelectedDeadLetterEnvironmentFilter;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        workspace.SearchGate = _ =>
        {
            started.SetResult();
            // Simulate a provider returning late, even if the caller tries to cancel.
            return release.Task;
        };
        var pending = viewModel.SearchDeadLettersCommand.ExecuteAsync();
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(viewModel.IsBusy);
            var searches = workspace.SearchRequests.Count;
            var connections = workspace.ConnectCalls;
            viewModel.SelectedSavedSearch = new SavedSearch("Deleted A", "missing-a", Guid.NewGuid());
            viewModel.SelectedSavedSearch = new SavedSearch("Deleted B", "missing-b", Guid.NewGuid());
            Assert.Empty(viewModel.Messages);
            Assert.Null(viewModel.SelectedMessage);
            Assert.Null(viewModel.SelectedDeadLetterEnvironmentFilter);
            Assert.Equal(searches, workspace.SearchRequests.Count);
            Assert.Equal(connections, workspace.ConnectCalls);
            if (restoreScope) viewModel.SelectedDeadLetterEnvironmentFilter = originalScope;
        }
        finally
        {
            release.TrySetResult();
            await pending.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.False(viewModel.IsBusy);
        Assert.Empty(viewModel.Messages);
        Assert.Null(viewModel.SelectedMessage);
        Assert.Contains("no longer available", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Found", viewModel.StatusText, StringComparison.Ordinal);

        // A fresh valid legacy selection must still search successfully after the stale call finishes.
        workspace.SearchGate = null;
        viewModel.SelectedDeadLetterEnvironmentFilter = originalScope;
        viewModel.SelectedSavedSearch = new SavedSearch("Legacy", "correlation-42");
        await viewModel.SearchDeadLettersCommand.Completion;
        Assert.Equal(3, viewModel.Messages.Count);
        Assert.NotNull(viewModel.SelectedMessage);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavedSearchAudit_MissingBookmarkClearsExplorerResultsWhenScopeAlreadyNull(bool sameBookmark)
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = viewModel;
        workspace.BrowseMessages = viewModel.Messages.Select(item => item.Message).ToArray();
        var missing = new SavedSearch("Deleted", "missing", Guid.NewGuid());
        viewModel.SelectedSavedSearch = missing;
        Assert.Null(viewModel.SelectedDeadLetterEnvironmentFilter);
        Assert.True(viewModel.BrowseSelectedActiveCommand.CanExecute(null));
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();
        Assert.NotEmpty(viewModel.Messages);
        Assert.NotNull(viewModel.SelectedMessage);
        Assert.Null(viewModel.SelectedDeadLetterEnvironmentFilter);
        var searches = workspace.SearchRequests.Count;
        var connections = workspace.ConnectCalls;

        viewModel.SelectedSavedSearch = sameBookmark ? missing : new SavedSearch("Deleted again", "missing-again", Guid.NewGuid());

        Assert.Empty(viewModel.Messages);
        Assert.Null(viewModel.SelectedMessage);
        Assert.Contains("no longer available", viewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(searches, workspace.SearchRequests.Count);
        Assert.Equal(connections, workspace.ConnectCalls);
    }
}
