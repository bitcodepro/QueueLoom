using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Search_HidesMatchesOutsideTheChosenTimeWindow()
    {
        var (viewModel, _, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;

        viewModel.SearchWindow = MainWindowViewModel.SearchWindows.Single(option => option.Minutes == 60);
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        Assert.Empty(viewModel.Messages);
        Assert.Contains("3 older matches hidden (last hour)", viewModel.DeadLetterSearchStatus, StringComparison.Ordinal);

        viewModel.SearchWindow = MainWindowViewModel.SearchWindows[0];
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();
        Assert.Equal(3, viewModel.Messages.Count);
    }

    [Fact]
    public async Task SavedSearch_StoresTextEnvironmentAndWindowAndRunsWhenChosen()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var changes = new List<string?>();
        viewModel.PropertyChanged += (_, args) => changes.Add(args.PropertyName);
        viewModel.SearchWindow = MainWindowViewModel.SearchWindows.Single(option => option.Minutes == 1_440);

        viewModel.SaveSearchCommand.Execute(null);

        var saved = Assert.Single(viewModel.SavedSearches);
        Assert.Equal("correlation-42", saved.Query);
        Assert.Equal(1_440, saved.WithinMinutes);
        Assert.Equal(viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId, saved.EnvironmentId);
        Assert.Contains(nameof(MainWindowViewModel.SavedSearches), changes);

        viewModel.DeadLetterSearchQuery = "something else";
        viewModel.SearchWindow = MainWindowViewModel.SearchWindows[0];
        var searchesBefore = workspace.SearchRequests.Count;
        viewModel.SelectedSavedSearch = null;
        viewModel.SelectedSavedSearch = saved;
        await viewModel.SearchDeadLettersCommand.Completion;

        Assert.Equal("correlation-42", viewModel.DeadLetterSearchQuery);
        Assert.Equal(1_440, viewModel.SearchWindow.Minutes);
        Assert.True(workspace.SearchRequests.Count > searchesBefore);

        viewModel.DeleteSavedSearchCommand.Execute(null);
        Assert.Empty(viewModel.SavedSearches);
    }

    [Fact]
    public async Task Settings_RoundTripSavedSearchesAlertsAndRetention()
    {
        using var directory = new TemporaryDirectory();
        using var store = new JsonAppSettingsStore(QueueLoomPaths.ForRoot(directory.Path));
        var search = new SavedSearch("orders", "order-1042", Guid.NewGuid(), 60);

        await store.UpdateAsync(settings => settings with
        {
            SavedSearches = [search, new SavedSearch(" ", "ignored")],
            SystemNotifications = false,
            AlertWebhookUrl = "https://hooks.slack.com/services/T/B/X",
            BackupRetentionDays = 30
        });
        var loaded = await store.LoadAsync();

        Assert.Equal([search], loaded.SavedSearches);
        Assert.False(loaded.SystemNotifications);
        Assert.Equal("https://hooks.slack.com/services/T/B/X", loaded.AlertWebhookUrl);
        Assert.Equal(30, loaded.BackupRetentionDays);
        Assert.Null((loaded with { AlertWebhookUrl = "http://insecure.example.com" }).Normalize().AlertWebhookUrl);
    }
}
