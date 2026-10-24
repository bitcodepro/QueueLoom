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
    public void BrowseRequest_LoadAll_IsAnExplicitPagingMode()
    {
        var request = new BrowseMessagesRequest(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            loadAll: true);

        Assert.True(request.LoadAll);
    }

    [Fact]
    public void BrowseRequest_SessionId_NamesOneSessionOfTheActiveQueue()
    {
        var source = ServiceBusEntityReference.Queue("orders");

        Assert.Null(new BrowseMessagesRequest(source).SessionId);
        Assert.Equal("customer-7", new BrowseMessagesRequest(source) { SessionId = "  customer-7 " }.SessionId);
        Assert.Throws<ArgumentException>(() => new BrowseMessagesRequest(source) { SessionId = " " });
        Assert.Throws<ArgumentException>(() => new BrowseMessagesRequest(source) { SessionId = new string('s', 129) });
        // Dead-letter queues never use sessions.
        Assert.Throws<ArgumentException>(() => new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter) { SessionId = "customer-7" });
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

    [Fact]
    public void PendingPurgeCounterRefresh_IsAdvisoryRatherThanADeletionFailure()
    {
        var source = new DeadLetterPurgeSourceResult(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            DeletedCount: 5,
            VerificationPending: true);
        var now = DateTimeOffset.UtcNow;
        var result = new DeadLetterPurgeResult(
            Guid.NewGuid(),
            now,
            now,
            [source],
            Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "purge"));

        Assert.True(source.IsSuccessful);
        Assert.False(result.HasFailures);
        Assert.True(result.HasPendingVerification);
        Assert.Equal(5, result.DeletedCount);
    }
}
