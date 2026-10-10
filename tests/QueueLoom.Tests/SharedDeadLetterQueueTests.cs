using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;

namespace QueueLoom.Tests;

// Several sources can dead-letter into one destination: each reports that destination's whole count, which is one count.
public sealed partial class ViewModelStateTests
{
    private const string SharedArn = "arn:aws:sqs:us-east-1:123:orders-dlq";

    private static DeadLetterEntitySnapshot SharingOrdersDlq(string queue, long count) =>
        new(ServiceBusEntityReference.Queue(queue), count) { CountQuality = DeadLetterCountQuality.Estimated, MeasuredFrom = SharedArn };

    // SQS: two queues redrive to orders-dlq; the topology reports it through both, by the same ARN.
    [Fact]
    public void QueuesRedrivingToOneDeadLetterQueueAreCountedOnce()
    {
        var topology = new AwsTopologyIndex(
        [
            new AwsQueueInfo("orders", "http://localhost/orders", "arn:aws:sqs:us-east-1:123:orders", false, 0, 0, 0, SharedArn, null, null),
            new AwsQueueInfo("refunds", "http://localhost/refunds", "arn:aws:sqs:us-east-1:123:refunds", false, 0, 0, 0, SharedArn, null, null),
            new AwsQueueInfo("orders-dlq", "http://localhost/orders-dlq", SharedArn, false, 25, 0, 0, null, null, null)
        ], []).ToTopology(DateTimeOffset.UtcNow);
        var users = topology.Queues.Where(queue => queue.Name is "orders" or "refunds").ToArray();
        Assert.All(users, queue => Assert.Equal(SharedArn, queue.Runtime.DeadLetterCountSource));

        var snapshot = new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow,
        [
            SharingOrdersDlq("orders", 25), SharingOrdersDlq("refunds", 25),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("payments"), 5)
        ]);

        Assert.Equal(30, snapshot.TotalCount);
        Assert.Equal(DeadLetterCountQuality.Estimated, snapshot.TotalQuality);
    }

    // Pub/Sub: two subscriptions with one dead-letter topic are counted through its one reader subscription.
    [Fact]
    public void PubSubSubscriptionsSharingADeadLetterTopicShareOneReader()
    {
        var policy = new global::Google.Cloud.PubSub.V1.DeadLetterPolicy { DeadLetterTopic = "projects/p/topics/events-dlq", MaxDeliveryAttempts = 5 };
        var index = GooglePubSubTopology.Build("p", ["events", "events-dlq"],
        [
            new global::Google.Cloud.PubSub.V1.Subscription { Name = "projects/p/subscriptions/billing", Topic = "projects/p/topics/events", DeadLetterPolicy = policy },
            new global::Google.Cloud.PubSub.V1.Subscription { Name = "projects/p/subscriptions/shipping", Topic = "projects/p/topics/events", DeadLetterPolicy = policy },
            new global::Google.Cloud.PubSub.V1.Subscription { Name = "projects/p/subscriptions/dlq-reader", Topic = "projects/p/topics/events-dlq" }
        ], DateTimeOffset.UtcNow, undelivered: new Dictionary<string, long> { ["billing"] = 0, ["shipping"] = 0, ["dlq-reader"] = 40 });

        var sources = index.Topology.Topics.Single(topic => topic.Name == "events").Subscriptions;

        Assert.All(sources, subscription => Assert.Equal("projects/p/subscriptions/dlq-reader", subscription.Runtime.DeadLetterCountSource));
        var snapshot = new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow, sources.Select(subscription =>
            new DeadLetterEntitySnapshot(subscription.Reference, subscription.Runtime.MessageCounts.DeadLetter)
                { CountQuality = DeadLetterCountQuality.Estimated, MeasuredFrom = subscription.Runtime.DeadLetterCountSource }));
        Assert.Equal(40, snapshot.TotalCount);
    }

    // History records the shared queue once, under its own name, not as each source's own count.
    [Fact]
    public void HistoryRecordsASharedDeadLetterQueueOnce()
    {
        var sample = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow,
            [SharingOrdersDlq("orders", 25), SharingOrdersDlq("refunds", 25)]), "Test");

        Assert.Equal(25, sample.Total);
        Assert.Equal(25, Assert.Single(sample.Sources, pair => pair.Key == "orders-dlq (shared by 2)").Value);
        Assert.DoesNotContain("orders", sample.Sources.Keys);
    }

    // The monitor alerts the shared queue once and counts it once in the total and in its growth.
    [Fact]
    public async Task TheMonitorAlertsASharedDeadLetterQueueOnce()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Holding(long count) => Snapshot(dev.Id, SharingOrdersDlq("orders", count), SharingOrdersDlq("refunds", count));
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Holding(25) } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await WaitUntilAsync(() => alerts.System.Count == 1);
        Assert.Null(alerts.System[0].Combined);
        Assert.Contains("orders-dlq (dead-letter queue shared by orders, refunds)", alerts.System[0].Text, StringComparison.Ordinal);

        workspace.Snapshots[dev.Id] = Holding(30);
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Null(alerts.System[1].Combined);
        Assert.Equal(30, alerts.System[1].Count);
        Assert.Contains("1 DLQ source(s) increased by ≈5; total is ≈30", vm.MonitorAlert, StringComparison.Ordinal);
    }
}
