using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DeadLetterPurgeModelTests
{
    [Fact]
    public void Request_DefaultsToBothDeadLetterSubQueues()
    {
        var request = new DeadLetterPurgeRequest(
            [ServiceBusEntityReference.Subscription("orders", "billing")]);

        Assert.Equal(DeadLetterPurgeRequest.DefaultBatchSize, request.BatchSize);
        Assert.Equal(
            [ServiceBusSubQueue.DeadLetter, ServiceBusSubQueue.TransferDeadLetter],
            request.SubQueues);
    }

    [Fact]
    public void Request_RejectsTopicsAndActiveMessages()
    {
        Assert.Throws<ArgumentException>(() =>
            new DeadLetterPurgeRequest([ServiceBusEntityReference.Topic("orders")]));
        Assert.Throws<ArgumentException>(() =>
            new DeadLetterPurgeRequest(
                [ServiceBusEntityReference.Queue("jobs")],
                [ServiceBusSubQueue.Active]));
    }

    [Fact]
    public void Request_PreservesExactTargetsWithoutAddingEmptySubQueues()
    {
        var source = ServiceBusEntityReference.Queue("jobs");
        var request = new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(source, ServiceBusSubQueue.DeadLetter)]);

        var target = Assert.Single(request.Targets);
        Assert.Equal(source, target.Source);
        Assert.Equal(ServiceBusSubQueue.DeadLetter, target.SubQueue);
        Assert.Equal([ServiceBusSubQueue.DeadLetter], request.SubQueues);
    }

    [Fact]
    public void Result_AggregatesDeletedMessagesAndFailures()
    {
        var source = ServiceBusEntityReference.Queue("jobs");
        var now = DateTimeOffset.UtcNow;
        var result = new DeadLetterPurgeResult(
            Guid.NewGuid(),
            now,
            now.AddSeconds(1),
            [
                new DeadLetterPurgeSourceResult(source, ServiceBusSubQueue.DeadLetter, 12),
                new DeadLetterPurgeSourceResult(
                    source,
                    ServiceBusSubQueue.TransferDeadLetter,
                    3,
                    "Permission denied")
            ],
            Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup"));

        Assert.Equal(15, result.DeletedCount);
        Assert.True(result.HasFailures);
        Assert.True(Path.IsPathFullyQualified(result.BackupDirectory));
    }
}
