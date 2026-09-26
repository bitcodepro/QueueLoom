using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task DeleteMarked_DeletesExactlyTheTickedSearchResults()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;

        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.Messages.Single(message => message.SequenceNumber == 4).IsMarked = true;
        Assert.Equal(2, viewModel.MarkedMessageCount);
        Assert.Equal("Delete 2 messages…", viewModel.DeleteMarkedMessagesLabel);
        Assert.Null(viewModel.AreAllMessagesMarked);

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        var request = Assert.Single(workspace.DeleteRequests);
        Assert.Equal([2L, 4L], request.Messages.Select(message => message.SequenceNumber).Order());
        Assert.Equal([3L], viewModel.Messages.Select(message => message.SequenceNumber));
        Assert.Contains("2 of 2 deleted", viewModel.StatusText, StringComparison.Ordinal);
        Assert.Contains("exactly the ticked ones", dialogs.Confirmations.Last().Message, StringComparison.Ordinal);
        Assert.Equal(0, viewModel.MarkedMessageCount);

        var ordersRow = Assert.Single(viewModel.DeadLetterSources);
        Assert.Equal(1, ordersRow.Count);
    }

    [Fact]
    public async Task DeleteMarked_KeepsMessagesThatWereNotFoundAndExplains()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var __ = viewModel;
        workspace.MissingSequenceNumbers.Add(3);

        viewModel.AreAllMessagesMarked = true;
        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal([3L], viewModel.Messages.Select(message => message.SequenceNumber));
        Assert.Contains("1 not found", viewModel.ErrorText, StringComparison.Ordinal);
        Assert.Contains("Sequence 3", viewModel.ErrorText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteMarked_DoesNothingWhenTheConfirmationIsDeclined()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = false;

        viewModel.AreAllMessagesMarked = true;
        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Empty(workspace.DeleteRequests);
        Assert.Equal(3, viewModel.Messages.Count);
    }

    [Fact]
    public async Task DeleteMarked_RequiresWriteAccess()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadOnly);
        await using var __ = viewModel;

        viewModel.AreAllMessagesMarked = true;

        Assert.False(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();
        Assert.Empty(workspace.DeleteRequests);
    }

    [Fact]
    public async Task DeleteMarked_ProductionRequiresTheTypedEnvironmentName()
    {
        var (viewModel, _, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite, EnvironmentKind.Production);
        await using var __ = viewModel;

        viewModel.Messages[0].IsMarked = true;
        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        var confirmation = dialogs.Confirmations.Last();
        Assert.True(confirmation.IsDangerous);
        Assert.Equal("Orders", confirmation.RequiredText);
    }

    [Fact]
    public void ActiveMessages_CannotBeMarked()
    {
        var active = new MessageItemViewModel(new BrowsedMessage(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.Active,
            1,
            ReadOnlyMemory<byte>.Empty,
            EditableMessageProperties.Empty));

        active.IsMarked = true;

        Assert.False(active.CanDelete);
        Assert.False(active.IsMarked);
    }

    private static async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace, FakeDialogService Dialogs)>
        CreateSearchedViewModelAsync(ProfileAccessMode accessMode, EnvironmentKind environment = EnvironmentKind.Development)
    {
        var profile = CreateProfile("Orders", environment, accessMode);
        var queue = new ServiceBusQueue(
            "orders",
            new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 3)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            SearchMatches =
            {
                [profile.Id] =
                [
                    SearchMessage(queue.Reference, 2, "2026-08-12T10:00:00Z"),
                    SearchMessage(queue.Reference, 3, "2026-08-12T10:01:00Z"),
                    SearchMessage(queue.Reference, 4, "2026-08-12T10:02:00Z")
                ]
            }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(
            profile.Id,
            DateTimeOffset.UtcNow,
            [new DeadLetterEntitySnapshot(queue.Reference, 3)]);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);

        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "correlation-42";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();
        Assert.Equal(3, viewModel.Messages.Count);
        return (viewModel, workspace, dialogs);
    }
}
