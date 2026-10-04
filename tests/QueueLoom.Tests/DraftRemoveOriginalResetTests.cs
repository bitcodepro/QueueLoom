using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task OpeningAnotherDlqDraftResetsTheRemoveOriginalChoice()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = true;

        viewModel.SelectedMessage = viewModel.Messages.Single(message => message.SequenceNumber == 2);
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        viewModel.DraftMovesOriginal = true; // considered for message 2, then abandoned

        viewModel.SelectedMessage = viewModel.Messages.Single(message => message.SequenceNumber == 4);
        viewModel.OpenMessageAsDraftCommand.Execute(null);

        Assert.False(viewModel.DraftMovesOriginal);
        await viewModel.SendDraftCommand.ExecuteAsync();
        Assert.Empty(workspace.DeleteRequests);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task BugHunt_SwitchingEnvironmentsForgetsTheOldBrowsePosition()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var source = QueueLoom.Core.ServiceBus.ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            BrowseMessages = Enumerable.Range(1, 250).Select(i => new QueueLoom.Core.ServiceBus.BrowsedMessage(source,
                QueueLoom.Core.ServiceBus.ServiceBusSubQueue.Active, i, "body"u8.ToArray(),
                new QueueLoom.Core.ServiceBus.EditableMessageProperties(MessageId: "id-" + i))).ToArray()
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev, test], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedEntity = new EntityItemViewModel(source,
            new QueueLoom.Core.ServiceBus.ServiceBusEntityRuntime(new QueueLoom.Core.ServiceBus.ServiceBusMessageCounts()),
            QueueLoom.Core.ServiceBus.ServiceBusEntityStatus.Active, false, 0);
        await vm.BrowseSelectedActiveCommand.ExecuteAsync();
        Assert.NotEmpty(vm.Messages);
        Assert.True(vm.CanLoadMoreMessages);

        vm.SelectedProfile = Assert.Single(vm.Profiles, item => item.Id == test.Id);
        await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedProfile = Assert.Single(vm.Profiles, item => item.Id == dev.Id);
        await vm.ConnectCommand.ExecuteAsync();

        Assert.Empty(vm.Messages);
        // The list was cleared by the switch, so "Load more" must not continue the earlier page into an empty list.
        Assert.False(vm.CanLoadMoreMessages);
    }
}
