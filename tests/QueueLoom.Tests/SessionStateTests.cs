using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class SessionStateTests
{
    [Theory]
    [InlineData(null, "This session has no state.")]
    [InlineData(new byte[0], "This session has no state.")]
    [InlineData(new byte[] { 0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D }, "{\"a\":1}")]
    [InlineData(new byte[] { 0xFF, 0x00, 0x10 }, "3 bytes, not text: FF 00 10")]
    public void SessionStateIsShownAsTextOrAsBytes(byte[]? state, string expected)
    {
        Assert.Equal(expected, SessionStateDisplay.Describe(state));
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task SessionStateIsReadAndClearedOnlyWithWriteAccessAndAConfirmation()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var sessions = new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: true);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [sessions]),
            SessionStates = { [(sessions.Reference, "customer-7")] = "step=3"u8.ToArray() }
        };
        var dialogs = new FakeDialogService { ConfirmResult = false };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single();
        Assert.False(viewModel.ShowSessionStateCommand.CanExecute(null));

        viewModel.SessionIdToOpen = "customer-7";
        await viewModel.ShowSessionStateCommand.ExecuteAsync();
        Assert.Equal("step=3", viewModel.SessionStateText);

        await viewModel.ClearSessionStateCommand.ExecuteAsync();
        Assert.Contains("customer-7", Assert.Single(dialogs.Confirmations).Message, StringComparison.Ordinal);
        Assert.True(workspace.SessionStates.ContainsKey((sessions.Reference, "customer-7")));

        dialogs.ConfirmResult = true;
        await viewModel.ClearSessionStateCommand.ExecuteAsync();
        Assert.False(workspace.SessionStates.ContainsKey((sessions.Reference, "customer-7")));
        Assert.Equal("This session has no state.", viewModel.SessionStateText);

        // Another session id forgets what was shown for the last one.
        viewModel.SessionIdToOpen = "customer-8";
        Assert.Equal(string.Empty, viewModel.SessionStateText);
    }

    [Fact]
    public async Task AReadOnlyEnvironmentCanShowButNotClearSessionState()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var sessions = new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: true);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [sessions]) };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single();
        viewModel.SessionIdToOpen = "customer-7";

        Assert.True(viewModel.ShowSessionStateCommand.CanExecute(null));
        Assert.False(viewModel.ClearSessionStateCommand.CanExecute(null));
    }
}
