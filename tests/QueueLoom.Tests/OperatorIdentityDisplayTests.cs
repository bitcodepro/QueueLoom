using System.Reflection;
using QueueLoom.App.Models;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

// A monitor check of another environment switches the workspace to it for a moment. Two displays read the workspace's
// environment instead of the operator's: the Environments list (refreshed, for example, when temporary write access
// expires during a check) marked the monitored environment as connected and the operator's as offline, and the Monitors
// history, opened for the first time during a check, chose the monitored environment. Both follow the operator now.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task OperatorIdentity_TheEnvironmentsListShowsTheOperatorsEnvironmentDuringAMonitorCheck()
    {
        var (viewModel, workspace, operatorProfile, monitored) = await ConnectedWhileAnotherIsCheckedAsync();
        await using var _ = viewModel;

        typeof(MainWindowViewModel).GetMethod("NotifyConnectionState", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(viewModel, null);

        Assert.Equal(monitored.Id, workspace.ConnectedProfileId);
        Assert.True(viewModel.Profiles.Single(profile => profile.Id == operatorProfile.Id).IsConnected);
        Assert.False(viewModel.Profiles.Single(profile => profile.Id == monitored.Id).IsConnected);
    }

    [Fact]
    public async Task OperatorIdentity_HistoryFirstOpenedDuringAMonitorCheckShowsTheOperatorsEnvironment()
    {
        var (viewModel, _, operatorProfile, _) = await ConnectedWhileAnotherIsCheckedAsync();
        await using var __ = viewModel;

        viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
        await viewModel.HistoryRefresh;

        Assert.Equal(operatorProfile.Id, viewModel.HistoryProfile?.Id);
    }

    /// <summary>The operator is connected to "Development"; the workspace is on "Test", as during a monitor check of it.</summary>
    private async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace, ServiceBusProfile Operator, ServiceBusProfile Monitored)>
        ConnectedWhileAnotherIsCheckedAsync()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var workspace = new FakeWorkspace();
        var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await workspace.ConnectAsync(test);
        return (viewModel, workspace, development, test);
    }
}
