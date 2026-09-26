using QueueLoom.App.Models;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Settings;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task CopyText_UsesTheClipboardAndConfirmsWithAToast()
    {
        var clipboard = new RecordingClipboard();
        var notifications = new RecordingNotifications();
        await using var viewModel = new MainWindowViewModel(
            new FakeProfileRepository([], null),
            new FakeSecretVault(),
            new FakeWorkspace(),
            new FakeDialogService(),
            clipboard: clipboard,
            notifications: notifications);

        await viewModel.CopyTextAsync("orders");

        Assert.Equal(["orders"], clipboard.Copied);
        Assert.Equal("Copied: orders", viewModel.StatusText);
        Assert.Equal(NotificationTone.Success, Assert.Single(notifications.Shown).Tone);
    }

    [Fact]
    public async Task CopyText_ReportsAnUnavailableClipboard()
    {
        var notifications = new RecordingNotifications();
        await using var viewModel = new MainWindowViewModel(
            new FakeProfileRepository([], null),
            new FakeSecretVault(),
            new FakeWorkspace(),
            new FakeDialogService(),
            clipboard: new RecordingClipboard { Available = false },
            notifications: notifications);

        await viewModel.CopyTextAsync("orders");

        Assert.Equal(NotificationTone.Warning, Assert.Single(notifications.Shown).Tone);
        Assert.Contains("unavailable", viewModel.StatusText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CycleTheme_GoesDarkLightSystemAndAppliesEachChoice()
    {
        var theme = new RecordingTheme();
        await using var viewModel = new MainWindowViewModel(
            new FakeProfileRepository([], null),
            new FakeSecretVault(),
            new FakeWorkspace(),
            new FakeDialogService(),
            theme: theme);

        viewModel.CycleThemeCommand.Execute(null);
        viewModel.CycleThemeCommand.Execute(null);
        viewModel.CycleThemeCommand.Execute(null);

        Assert.Equal([AppThemePreference.Light, AppThemePreference.System, AppThemePreference.Dark], theme.Applied);
        Assert.Equal("Dark theme", viewModel.ThemeLabel);
    }

    [Fact]
    public async Task ApplyPreferences_RestoresIntervalAndTheme()
    {
        var theme = new RecordingTheme();
        await using var viewModel = new MainWindowViewModel(
            new FakeProfileRepository([], null),
            new FakeSecretVault(),
            new FakeWorkspace(),
            new FakeDialogService(),
            theme: theme);

        viewModel.ApplyPreferences(new AppSettings(300, AppThemePreference.Light));

        Assert.Equal(300, viewModel.MonitorIntervalSeconds);
        Assert.Equal(AppThemePreference.Light, viewModel.ThemePreference);
        Assert.Equal([AppThemePreference.Light], theme.Applied);
    }

    [Fact]
    public async Task NavigateCommand_SelectsThePageByKey()
    {
        await using var viewModel = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());

        viewModel.NavigateCommand.Execute("Monitors");
        Assert.Equal(NavigationPage.Monitors, viewModel.CurrentPage);

        viewModel.NavigateCommand.Execute("NotAPage");
        Assert.Equal(NavigationPage.Monitors, viewModel.CurrentPage);
    }

    [Fact]
    public async Task SortEntities_OrdersCountersLargestFirstAndTogglesDirection()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(
                DateTimeOffset.UtcNow,
                [
                    new ServiceBusQueue("alpha", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1))),
                    new ServiceBusQueue("beta", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 30))),
                    new ServiceBusQueue("gamma", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 7)))
                ])
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        viewModel.SortEntitiesCommand.Execute("DeadLetters");
        Assert.Equal(["beta", "gamma", "alpha"], viewModel.Entities.Select(entity => entity.Name));
        Assert.True(viewModel.EntitySortDescending);

        viewModel.SortEntitiesCommand.Execute("DeadLetters");
        Assert.Equal(["alpha", "gamma", "beta"], viewModel.Entities.Select(entity => entity.Name));

        viewModel.SortEntitiesCommand.Execute("Hierarchy");
        Assert.Equal(["alpha", "beta", "gamma"], viewModel.Entities.Select(entity => entity.Name));
        Assert.Equal("Grouped by topic", viewModel.EntitySortDescription);
    }

    [Fact]
    public async Task SuccessfulOperation_DoesNotLeaveAnInProgressStatus()
    {
        var backups = new FakeBackupRepository(
            CreateBackupSummary(
                CreateProfile("Development", EnvironmentKind.Development),
                ServiceBusEntityReference.Queue("orders"),
                SearchMessage(ServiceBusEntityReference.Queue("orders"), 1, "2026-01-01T00:00:00Z")),
            SearchMessage(ServiceBusEntityReference.Queue("orders"), 1, "2026-01-01T00:00:00Z"));
        await using var viewModel = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), backupRepository: backups);

        await viewModel.RefreshBackupsCommand.ExecuteAsync();

        Assert.NotEqual("Loading backups", viewModel.StatusText);
    }

    [Fact]
    public async Task StartingASourceMonitorWhileOffline_ReportsAnErrorInsteadOfThrowing()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(
                DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)])
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        var entity = viewModel.SelectedEntity;
        await workspace.DisconnectAsync();
        viewModel.SelectedEntity = entity;
        viewModel.MonitorScope = "Selected queue / subscription";
        viewModel.MonitorTargetChoice = "Explorer selection";

        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        Assert.False(viewModel.IsMonitoring);
        Assert.Contains("Connect to an environment first", viewModel.ErrorText, StringComparison.Ordinal);
    }

    private sealed class RecordingClipboard : IClipboardService
    {
        public bool Available { get; init; } = true;

        public List<string> Copied { get; } = [];

        public Task<bool> SetTextAsync(string text)
        {
            if (Available)
            {
                Copied.Add(text);
            }
            return Task.FromResult(Available);
        }
    }

    private sealed class RecordingNotifications : INotificationService
    {
        public List<(string Title, string Message, NotificationTone Tone)> Shown { get; } = [];

        public void Show(string title, string message, NotificationTone tone = NotificationTone.Information) =>
            Shown.Add((title, message, tone));
    }

    private sealed class RecordingTheme : IThemeService
    {
        public List<AppThemePreference> Applied { get; } = [];

        public AppThemePreference Preference => Applied.LastOrDefault();

        public void Apply(AppThemePreference preference) => Applied.Add(preference);
    }
}
