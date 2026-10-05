using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Bug 7. The broker accepts the message, then the operation is cancelled before the answer arrives. That is not
    // "not sent": the outcome is uncertain, the original is kept (a move removes nothing), and it is not counted as
    // cancelled, so nobody is told it is safe to send again.
    [Theory]
    [InlineData(ResendMode.Copy)]
    [InlineData(ResendMode.Move)]
    public async Task ACancelDuringAnAcceptedSendIsUncertainNotNotSent(ResendMode mode)
    {
        using var cancellation = new CancellationTokenSource();
        var workspace = new FakeWorkspace();
        workspace.OnSend = cancellation.Cancel; // the broker has the message at this point
        workspace.SendGate = () => Task.FromCanceled(cancellation.Token);
        var original = SearchMessage(ServiceBusEntityReference.Queue("orders"), 5, "2026-08-12T10:00:00Z");
        var item = new ResendItem(original, ServiceBusEntityReference.Queue("orders"), original.CreateDraft()).WithNewMessageId();

        var result = await DeadLetterResender.ResendAsync(workspace, [item], mode, cancellationToken: cancellation.Token);

        var outcome = Assert.Single(result.Items);
        Assert.Equal(ResendOutcome.Failed, outcome.Outcome);
        Assert.Contains("unknown", outcome.Detail, StringComparison.Ordinal);
        Assert.Equal(0, result.CancelledCount);
        Assert.Equal(1, result.FailedCount);
        Assert.Single(workspace.SentMessages);
        Assert.Empty(workspace.DeleteRequests);
    }

    // Control: cancelled while waiting for the rate limit, before the next send is attempted: "not sent" is right.
    [Fact]
    public async Task ACancelWhileWaitingForTheRateLimitIsNotSent()
    {
        using var cancellation = new CancellationTokenSource();
        var workspace = new FakeWorkspace();
        workspace.OnSend = () => cancellation.CancelAfter(TimeSpan.FromMilliseconds(50));
        var original = SearchMessage(ServiceBusEntityReference.Queue("orders"), 5, "2026-08-12T10:00:00Z");
        ResendItem Item() => new ResendItem(original, ServiceBusEntityReference.Queue("orders"), original.CreateDraft()).WithNewMessageId();

        var result = await DeadLetterResender.ResendAsync(workspace, [Item(), Item()], ResendMode.Copy, messagesPerSecond: 1,
            cancellationToken: cancellation.Token);

        Assert.Equal([ResendOutcome.Sent, ResendOutcome.Cancelled], result.Items.Select(item => item.Outcome));
        Assert.Single(workspace.SentMessages);
    }
}
