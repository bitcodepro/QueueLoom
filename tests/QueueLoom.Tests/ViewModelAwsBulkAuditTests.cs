using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AwsBulkMoveCannotPreserveDuplicateDetectionIds()
    {
        var (vm, broker, dialogs) = await CreateAzureComposerAsync(MessagingProvider.AmazonSqsSns);
        await using var owner = vm;
        var original = vm.SelectedMessage!;
        vm.Messages.Add(original); original.IsMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { Mode = ResendMode.Move, PreserveMessageIds = true };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        Assert.Empty(broker.SentMessages);
        Assert.Empty(broker.DeleteRequests);
        var dialog = Assert.Single(dialogs.ResendDialogs);
        Assert.True(dialog.RequiresNewIdsForMove);
        Assert.Contains("distinct new Message IDs", vm.ErrorText, StringComparison.Ordinal);
        dialog.Moves = true; dialog.PreserveMessageIds = true;
        Assert.False(dialog.CanConfirm);
    }
}
