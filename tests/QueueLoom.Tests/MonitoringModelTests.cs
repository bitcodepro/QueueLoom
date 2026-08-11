using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class MonitoringModelTests
{
    [Fact]
    public void SingleEntityScope_RejectsTopic()
    {
        Assert.Throws<ArgumentException>(() =>
            DeadLetterMonitorScope.ForEntity(ServiceBusEntityReference.Topic("events")));
    }

    [Fact]
    public void Settings_EnforceSafePollingRange()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DeadLetterMonitorSettings(
                true,
                TimeSpan.FromSeconds(1),
                DeadLetterMonitorScope.All));
    }

    [Fact]
    public void Snapshot_ReportsTotalsChangesAndPartialFailures()
    {
        var healthy = new DeadLetterEntitySnapshot(
            ServiceBusEntityReference.Queue("orders"),
            count: 5,
            previousCount: 2);
        var failed = new DeadLetterEntitySnapshot(
            ServiceBusEntityReference.Subscription("events", "billing"),
            count: null,
            error: "Unauthorized");

        var snapshot = new DeadLetterSnapshot(
            Guid.NewGuid(),
            DateTimeOffset.UtcNow,
            [healthy, failed]);

        Assert.Equal(5, snapshot.TotalCount);
        Assert.Equal(3, healthy.Change);
        Assert.True(snapshot.HasDeadLetters);
        Assert.True(snapshot.HasFailures);
    }
}
