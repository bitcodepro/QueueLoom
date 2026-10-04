using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static MessageItemViewModel ActiveRow(long sequence, Guid? profileId) =>
        new(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, sequence,
            Encoding.UTF8.GetBytes($$"""{"n":{{sequence}}}"""), new EditableMessageProperties(MessageId: $"active-{sequence}"),
            state: ServiceBusMessageState.Active), profileId);

    [Fact]
    public async Task CompareSelection_TwoActiveMessagesAreComparedButNeverDeletedOrResent()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var profileId = viewModel.ConnectedProfileId;
        viewModel.ReplaceMessages([ActiveRow(10, profileId), ActiveRow(11, profileId)]);
        Assert.True(viewModel.CanWrite);

        viewModel.Messages[0].IsMarked = true;
        viewModel.Messages[1].IsMarked = true;

        Assert.True(viewModel.CanCompareMarkedMessages);
        Assert.False(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.False(viewModel.ResendMarkedMessagesCommand.CanExecute(null));
        await viewModel.CompareMarkedMessagesCommand.ExecuteAsync();
        Assert.Single(dialogs.Comparisons);
        Assert.Empty(workspace.DeleteRequests);
    }

    [Fact]
    public async Task CompareSelection_AnActiveTickKeepsDeleteAndResendOffAndIsRefusedIfRunAnyway()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = true;
        var active = ActiveRow(10, viewModel.ConnectedProfileId);
        viewModel.Messages.Add(active);
        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        active.IsMarked = true;

        Assert.True(viewModel.CanCompareMarkedMessages);
        Assert.False(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.False(viewModel.ResendMarkedMessagesCommand.CanExecute(null));

        // Even when invoked directly, nothing is deleted while an active message is ticked.
        var delete = typeof(MainWindowViewModel).GetMethod("DeleteMarkedMessagesAsync",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => (Task)delete.Invoke(viewModel, [CancellationToken.None])!);
        Assert.Empty(workspace.DeleteRequests);

        active.IsMarked = false;
        Assert.True(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.True(viewModel.ResendMarkedMessagesCommand.CanExecute(null));
    }

    [Fact]
    public async Task CompareSelection_SelectAllTicksOnlyDeletableMessages()
    {
        var (viewModel, _, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var active = ActiveRow(10, viewModel.ConnectedProfileId);
        viewModel.Messages.Add(active);

        viewModel.AreAllMessagesMarked = true;

        Assert.False(active.IsMarked);
        Assert.Equal(viewModel.Messages.Count(message => message.CanDelete), viewModel.MarkedMessageCount);
        Assert.True(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
    }
}
