using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class ServiceBusModelTests
{
    [Fact]
    public void SubscriptionReference_BuildsAzurePathsAndCapabilities()
    {
        var reference = ServiceBusEntityReference.Subscription("orders", "accounting");

        Assert.Equal("orders/Subscriptions/accounting", reference.Path);
        Assert.Equal("orders/Subscriptions/accounting/$DeadLetterQueue", reference.DeadLetterPath);
        Assert.True(reference.CanBrowse);
        Assert.False(reference.CanSend);
    }

    [Fact]
    public void Topic_CanSendButCannotBrowseOrHaveDeadLetterPath()
    {
        var reference = ServiceBusEntityReference.Topic("events");

        Assert.True(reference.CanSend);
        Assert.False(reference.CanBrowse);
        Assert.Null(reference.DeadLetterPath);
        Assert.Throws<ArgumentException>(() => new BrowseMessagesRequest(reference));
    }

    [Fact]
    public void BinaryBody_RoundTripsThroughDraftCreation()
    {
        byte[] bytes = [0xff, 0x00, 0x80];
        var browsed = new BrowsedMessage(
            ServiceBusEntityReference.Queue("jobs"),
            ServiceBusSubQueue.DeadLetter,
            7,
            bytes,
            new EditableMessageProperties(MessageId: "job-7"));

        var draft = browsed.CreateDraft();

        Assert.Equal(MessageBodyFormat.Base64, draft.Body.Format);
        Assert.Equal(bytes, draft.Body.GetBytes());
        Assert.True(browsed.IsDeadLetter);
    }

    [Fact]
    public void MessageCounts_SumAllRuntimeBuckets()
    {
        var counts = ServiceBusMessageCounts.Sum(
            [
                new ServiceBusMessageCounts(active: 2, deadLetter: 3),
                new ServiceBusMessageCounts(scheduled: 5, transferDeadLetter: 7)
            ]);

        Assert.Equal(17, counts.Total);
        Assert.Equal(3, counts.DeadLetter);
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceBusMessageCounts(active: -1));
    }

    [Fact]
    public void TruncatedBrowseBody_CannotBecomeASilentPartialDraft()
    {
        var browsed = new BrowsedMessage(
            ServiceBusEntityReference.Queue("large"),
            ServiceBusSubQueue.DeadLetter,
            1,
            new byte[1024],
            EditableMessageProperties.Empty,
            originalBodySize: 2 * 1024 * 1024);

        Assert.True(browsed.IsBodyTruncated);
        Assert.Equal(2 * 1024 * 1024, browsed.BodySize);
        Assert.Throws<InvalidOperationException>(() => browsed.CreateDraft());
    }
}
