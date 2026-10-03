using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
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
}
