using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// Service Bus counts deferred messages as active and has no deferred count of its own, and it hands out only sessions
// that have an active message, so a session whose messages are all deferred is never browsed. The browse compares the
// active count with every non-scheduled message it read and reports one cautious gap, without guessing a cause.
public sealed class SessionBrowseDeferredNoticeTests
{
    private static readonly ServiceBusMessageState[] OneReadyTwoDeferred =
        [ServiceBusMessageState.Active, ServiceBusMessageState.Deferred, ServiceBusMessageState.Deferred];

    [Fact]
    public void ReadDeferredMessagesCountAgainstTheActiveCount() =>
        // 1 ready + 2 deferred: Active=3, and the browse read all three.
        Assert.Null(AzureServiceBusWorkspace.UnreadSessionMessageNotice("orders", active: 3, OneReadyTwoDeferred));

    [Fact]
    public void AnUnreadDeferredOnlySessionIsReportedAsAnUnclassifiedGap()
    {
        var notice = AzureServiceBusWorkspace.UnreadSessionMessageNotice("orders", active: 3, [ServiceBusMessageState.Active]);

        Assert.NotNull(notice);
        Assert.Equal("Session browse may be incomplete", notice.Value.Title);
        Assert.Contains("so 2 message(s) the counters report were not listed", notice.Value.Details);
        Assert.Contains("all deferred", notice.Value.Details);
        Assert.DoesNotContain("deferred messages, but", notice.Value.Details);
    }

    [Fact]
    public void ScheduledMessagesDoNotCount()
    {
        var notice = AzureServiceBusWorkspace.UnreadSessionMessageNotice("orders", active: 2,
            [ServiceBusMessageState.Active, ServiceBusMessageState.Scheduled, ServiceBusMessageState.Scheduled]);

        Assert.NotNull(notice);
        Assert.Contains("this browse read 1, so 1 message(s)", notice.Value.Details);
    }

    [Fact]
    public void ACompleteBrowseSaysNothing() =>
        Assert.Null(AzureServiceBusWorkspace.UnreadSessionMessageNotice("orders", active: 3,
            [ServiceBusMessageState.Active, ServiceBusMessageState.Unknown, ServiceBusMessageState.Deferred, ServiceBusMessageState.Scheduled]));
}
