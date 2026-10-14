using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(ServiceBusSubQueue.Active, false, true)]
    [InlineData(ServiceBusSubQueue.DeadLetter, false, true)]
    [InlineData(ServiceBusSubQueue.Active, true, true)]
    [InlineData(ServiceBusSubQueue.DeadLetter, true, true)]
    [InlineData(ServiceBusSubQueue.Active, false, false)]
    [InlineData(ServiceBusSubQueue.DeadLetter, false, false)]
    [InlineData(ServiceBusSubQueue.Active, true, false)]
    [InlineData(ServiceBusSubQueue.DeadLetter, true, false)]
    public async Task LateBrowseCompletionPreservesCancellationAndPaging(
        ServiceBusSubQueue subQueue, bool nextPage, bool cancel)
    {
        var profile = CreateProfile("SyntheticBrowse", EnvironmentKind.Development);
        var queue = new ServiceBusQueue("orders",
            new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 200, deadLetter: 200)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            BrowseMessages = Enumerable.Range(1, 200).Select(sequence => new BrowsedMessage(
                queue.Reference, subQueue, sequence, Encoding.UTF8.GetBytes($"synthetic-{sequence}"),
                new EditableMessageProperties(MessageId: $"synthetic-{sequence}"))).ToArray()
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var browse = subQueue == ServiceBusSubQueue.Active ? vm.BrowseSelectedActiveCommand : vm.BrowseSelectedDeadLettersCommand;
        if (nextPage)
        {
            await browse.ExecuteAsync();
            Assert.Equal(100, vm.Messages.Count);
            vm.SelectedMessage = vm.Messages[41];
            vm.Messages[41].IsMarked = true;
        }
        var originalRows = vm.Messages.ToArray();
        var originalSelection = vm.SelectedMessage;
        var originalTitle = vm.MessageListTitle;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken providerToken = default;
        workspace.CleanupOperationGate = token =>
        {
            providerToken = token;
            entered.TrySetResult();
            // Deliberately return a page after cancellation instead of throwing, as a late provider may do.
            return release.Task;
        };
        var pending = nextPage ? vm.LoadMoreMessagesCommand.ExecuteAsync() : browse.ExecuteAsync();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(vm.IsBusy);
            Assert.True(vm.CancelCurrentOperationCommand.CanExecute(null));
            if (cancel)
            {
                vm.CancelCurrentOperationCommand.Execute(null);
                Assert.True(providerToken.IsCancellationRequested);
                Assert.False(vm.ShowCancelCurrentOperation);
                Assert.False(vm.CancelCurrentOperationCommand.CanExecute(null));
            }
        }
        finally
        {
            release.TrySetResult();
            await pending.WaitAsync(TimeSpan.FromSeconds(10));
            workspace.CleanupOperationGate = null;
        }

        Assert.False(vm.IsBusy);
        Assert.False(vm.ShowCancelCurrentOperation);
        Assert.Empty(vm.ErrorText);
        if (cancel)
        {
            Assert.Equal(originalRows, vm.Messages.ToArray());
            Assert.Same(originalSelection, vm.SelectedMessage);
            Assert.Equal(originalTitle, vm.MessageListTitle);
            Assert.Equal("Operation cancelled", vm.StatusText);
            if (nextPage)
            {
                Assert.True(vm.Messages[41].IsMarked);
                Assert.True(vm.CanLoadMoreMessages);
                await vm.LoadMoreMessagesCommand.ExecuteAsync();
                Assert.Equal(101, workspace.BrowseRequests[^1].FromSequenceNumber);
            }
            else
            {
                await browse.ExecuteAsync();
                Assert.Equal(100, vm.Messages.Count);
                await vm.LoadMoreMessagesCommand.ExecuteAsync();
                Assert.Equal(101, workspace.BrowseRequests[^1].FromSequenceNumber);
            }
            Assert.Equal(200, vm.Messages.Count);
            Assert.Equal(200, vm.Messages.Select(row => row.Message.SequenceNumber).Distinct().Count());
            Assert.NotNull(vm.SelectedMessage);
        }
        else
        {
            Assert.Equal(nextPage ? 200 : 100, vm.Messages.Count);
            Assert.NotEqual("Operation cancelled", vm.StatusText);
            Assert.NotNull(vm.SelectedMessage);
            if (nextPage)
            {
                Assert.Same(originalSelection, vm.SelectedMessage);
                Assert.True(vm.Messages[41].IsMarked);
                Assert.Equal(101, workspace.BrowseRequests[^1].FromSequenceNumber);
            }
        }
        Assert.Empty(workspace.SentMessages);
        Assert.Empty(workspace.DeleteRequests);
    }
}
