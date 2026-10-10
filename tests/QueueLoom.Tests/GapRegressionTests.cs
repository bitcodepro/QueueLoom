using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

/// <summary>Known gaps carried over from earlier cycles.</summary>
public sealed partial class ViewModelStateTests
{
    // Gap 1: a search of another environment connected it read-only and then reconnected the original one, which
    // cleared the Composer destination of the draft the operator had open.
    [Fact]
    public async Task SearchOfAnotherEnvironmentKeepsTheDraftDestinationOfTheEnvironmentItReturnsTo()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 2)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            BrowseMessages = [SearchMessage(queue.Reference, 1, "2026-10-01T10:00:00Z")],
            SearchMatches = { [test.Id] = [SearchMessage(queue.Reference, 5, "2026-10-01T09:00:00Z")] }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == queue.Reference);
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        viewModel.SelectedMessage = viewModel.Messages[0];
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        Assert.Equal(queue.Reference, viewModel.SelectedDestination?.Reference);

        viewModel.SelectedDeadLetterEnvironmentFilter =
            Assert.Single(viewModel.DeadLetterEnvironmentFilters, item => item.ProfileId == test.Id);
        viewModel.DeadLetterSearchQuery = "correlation-42";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        Assert.False(viewModel.HasError, viewModel.ErrorText);
        Assert.Equal(development.Id, workspace.ConnectedProfileId);
        Assert.Equal([test.Id], viewModel.Messages.Select(message => message.ProfileId));
        Assert.Equal(queue.Reference, viewModel.SelectedDestination?.Reference);
        Assert.False(viewModel.HasDraftEnvironmentMismatch);
        // Search reads whole bodies (on Azure too, beyond the 1 MiB the list keeps); the title must not claim a boundary.
        Assert.DoesNotContain("reads up to the first 1 MiB", viewModel.MessageListTitle, StringComparison.Ordinal);
        Assert.Contains("bodies are searched in full", viewModel.MessageListTitle, StringComparison.Ordinal);
    }

    // Gap 2 (view model): an SQS FIFO read sees at most one receive batch per message group, yet the browse said
    // "End of available messages" while more of the group were still in the queue.
    [Fact]
    public async Task SqsFifoBrowseThatRanDryWhileHoldingAGroupSaysMoreMayExist()
    {
        var (viewModel, _) = await BrowseSqsDeadLettersAsync(fifo: true, count: 10, group: "g");
        await using var _ = viewModel;

        Assert.False(viewModel.HasError, viewModel.ErrorText);
        Assert.Equal(10, viewModel.Messages.Count);
        Assert.DoesNotContain("End of available messages", viewModel.BrowsePageStatus, StringComparison.Ordinal);
        Assert.Contains("more may exist", viewModel.BrowsePageStatus, StringComparison.Ordinal);
        Assert.False(viewModel.CanLoadMoreMessages);
    }

    // Gap 2: a standard queue that ran dry really is at its end.
    [Fact]
    public async Task SqsStandardBrowseThatRanDryStillSaysEndOfAvailableMessages()
    {
        var (viewModel, _) = await BrowseSqsDeadLettersAsync(fifo: false, count: 3, group: null);
        await using var _ = viewModel;

        Assert.Equal(3, viewModel.Messages.Count);
        Assert.Contains("End of available messages", viewModel.BrowsePageStatus, StringComparison.Ordinal);
    }

    private static async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace)> BrowseSqsDeadLettersAsync(
        bool fifo, int count, string? group)
    {
        var suffix = fifo ? ".fifo" : string.Empty;
        var profile = CreateProfile("Aws", EnvironmentKind.Test) with { Provider = MessagingProvider.AmazonSqsSns };
        var queue = new AwsQueueInfo($"orders{suffix}", $"http://localhost/orders{suffix}", $"arn:aws:sqs:us-east-1:123:orders{suffix}",
            fifo, 0, 0, 0, $"arn:aws:sqs:us-east-1:123:orders-dlq{suffix}", null, null);
        var deadLetterQueue = new AwsQueueInfo($"orders-dlq{suffix}", $"http://localhost/orders-dlq{suffix}",
            $"arn:aws:sqs:us-east-1:123:orders-dlq{suffix}", fifo, 25, 0, 0, null, null, null);
        var source = ServiceBusEntityReference.Queue(queue.Name);
        var workspace = new FakeWorkspace
        {
            Topology = new AwsTopologyIndex([queue, deadLetterQueue], []).ToTopology(DateTimeOffset.UtcNow),
            // What SQS hands out while QueueLoom holds the first messages of a FIFO group: nothing more of that group.
            BrowseMessages = Enumerable.Range(1, count).Select(index => new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter,
                    LeasedMessageIdentity.SequenceNumberFor($"m-{index}"), Encoding.UTF8.GetBytes($"m-{index}"),
                    new EditableMessageProperties(MessageId: $"m-{index}", SessionId: group),
                    enqueuedAt: DateTimeOffset.Parse("2026-10-01T10:00:00Z").AddSeconds(index))
                { HasSequenceNumber = false })
                .ToArray()
        };
        var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == source);
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        return (viewModel, workspace);
    }
}
