using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// Service Bus hands out only sessions that have an active message. A session whose messages were all deferred is never
// accepted by a browse, so its deferred messages were missing without a word, and the read deferred messages of other
// sessions were counted against the active count, which can hide held sessions. Now active and deferred are compared
// separately with the entity's counts.
public sealed class SessionBrowseDeferredNoticeTests
{
    [Fact]
    public void DeferredMessagesOfIdleSessionsAreReportedWithTheirCount()
    {
        var notice = Assert.Single(AzureServiceBusWorkspace.UnreadSessionMessageNotices("orders", active: 1, deferred: 3, readActive: 1, readDeferred: 1));

        Assert.Equal("Deferred session messages not listed", notice.Title);
        Assert.Contains("reports 3 deferred messages, but this browse read 1", notice.Details);
        Assert.Contains("the 2 deferred message(s)", notice.Details);
    }

    [Fact]
    public void ReadDeferredMessagesDoNotHideMissingActiveOnes()
    {
        var notice = Assert.Single(AzureServiceBusWorkspace.UnreadSessionMessageNotices("orders", active: 3, deferred: 2, readActive: 1, readDeferred: 2));

        Assert.Equal("Session browse may be incomplete", notice.Title);
    }

    [Fact]
    public void ACompleteBrowseSaysNothing() =>
        Assert.Empty(AzureServiceBusWorkspace.UnreadSessionMessageNotices("orders", active: 2, deferred: 1, readActive: 2, readDeferred: 1));

    [Theory]
    [InlineData(10, 2, 1, 3, 0, 0, 4)]
    [InlineData(6, 3, 1, 1, 0, 1, 0)]
    [InlineData(5, 6, 0, 0, 0, 0, 0)]
    public void TheDeferredCountIsWhatTheTotalHoldsBeyondTheOtherCounts(
        long total, long active, long scheduled, long deadLetter, long transfer, long transferDeadLetter, long deferred) =>
        Assert.Equal(deferred, AzureServiceBusWorkspace.DeferredCount(total, active, scheduled, deadLetter, transfer, transferDeadLetter));
}
