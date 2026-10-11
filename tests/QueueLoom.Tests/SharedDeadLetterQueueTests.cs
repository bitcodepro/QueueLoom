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
        Assert.Equal(25, Assert.Single(sample.Sources, pair => pair.Key == "orders-dlq").Value);
        Assert.DoesNotContain("orders", sample.Sources.Keys);
        Assert.Equal("orders-dlq", sample.SharedAliases!["refunds"]);
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

    private static DeadLetterEntitySnapshot Through(string queue, long count, string target, DateTimeOffset? measuredAt = null) =>
        new(ServiceBusEntityReference.Queue(queue), count) { CountQuality = DeadLetterCountQuality.Estimated, MeasuredFrom = target, MeasuredAt = measuredAt };

    // The listed totals count a shared queue once too, and each row says its count is the shared queue's.
    [Fact]
    public async Task ListedTotalsCountASharedDeadLetterQueueOnce()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("orders", 25), SharingOrdersDlq("refunds", 25)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();

        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();

        Assert.Equal(2, vm.DeadLetterSources.Count);
        Assert.Equal(25, vm.GlobalDlqSourceCount);
        Assert.Equal(25, vm.VisibleDlqSourceCount);
        Assert.All(vm.DeadLetterSources, row =>
        {
            Assert.True(row.IsShared);
            Assert.Contains("dead-letter queue orders-dlq, shared with", row.CountNote, StringComparison.Ordinal);
        });
    }

    // Both sources move from one shared queue to another: a target change, not a drop of 15.
    [Fact]
    public void SharedSourcesMovedToAnotherQueueHaveNoTotalChange()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        DeadLetterHistorySample Sample(DateTimeOffset when, long count, string target) => DeadLetterHistorySample.FromSnapshot(
            new DeadLetterSnapshot(profileId, when, [Through("orders", count, target), Through("refunds", count, target)]), "Test");

        var summary = DeadLetterHistory.Summarize(
            [Sample(at, 25, "arn:aws:sqs:us-east-1:123:dlq-a"), Sample(at.AddMinutes(1), 10, "arn:aws:sqs:us-east-1:123:dlq-b")],
            at.AddMinutes(-1), at.AddMinutes(5))!;

        Assert.True(summary.TargetsChanged);
        Assert.Null(summary.Change);
    }

    // A queue that gains a second user is still the same queue: an older point of it does not escape its newer one.
    [Fact]
    public void AnOlderPointOfAQueueThatGainedAUserIsLeftOut()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var alone = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profileId, at, [Through("orders", 100, SharedArn, at)]), "Test");
        var shared = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profileId, at.AddMinutes(5),
            [Through("orders", 0, SharedArn, at.AddMinutes(-5)), Through("refunds", 0, SharedArn, at.AddMinutes(-5))]), "Test");

        var summary = DeadLetterHistory.Summarize([alone, shared], at.AddMinutes(-10), at.AddMinutes(10))!;

        Assert.Equal(100, summary.Now);
        Assert.Equal(1, summary.SampleCount);
    }

    // History written before shared queues were counted once may count one twice: no "drop" against it.
    [Fact]
    public void HistoryBeforeCountingSharedQueuesOnceIsNotComparedWithIt()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var legacy = new DeadLetterHistorySample(at, profileId, "Test", 50, new Dictionary<string, long> { ["orders"] = 25, ["refunds"] = 25 },
            DeadLetterCountQuality.Estimated);
        var current = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profileId, at.AddMinutes(1),
            [SharingOrdersDlq("orders", 25), SharingOrdersDlq("refunds", 25)]), "Test");

        var summary = DeadLetterHistory.Summarize([legacy, current], at.AddMinutes(-1), at.AddMinutes(5))!;

        Assert.True(summary.TargetsChanged);
        Assert.Null(summary.Change);
    }

    // The incident is the shared queue's: a source joining or leaving it is no new incident and loses no growth.
    [Theory]
    [InlineData(30, 2)]
    [InlineData(25, 1)]
    public async Task ASourceJoiningASharedQueueKeepsItsIncident(long countAfterJoin, int expectedAlerts)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("refunds", 25)) } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();
        await WaitUntilAsync(() => alerts.System.Count == 1);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("orders", countAfterJoin), SharingOrdersDlq("refunds", countAfterJoin));
        await Check();
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("refunds", countAfterJoin));
        await Check();

        if (expectedAlerts == 2)
        {
            await WaitUntilAsync(() => alerts.System.Count == 2);
            Assert.Equal(25, alerts.System[1].PreviousCount);
            Assert.Contains("increased by ≈5", vm.Activity.First(item => item.Action == "DLQ alert").Details, StringComparison.Ordinal);
        }
        await Task.Delay(100);
        Assert.Equal(expectedAlerts, alerts.System.Count);
        // Back to one user: the incident shows that source, no longer as shared.
        Assert.Equal("refunds", Assert.Single(vm.MonitorNotifications).SourceName);
    }

    // While two sources use it, the incident says so.
    [Fact]
    public async Task ASharedIncidentNamesTheSourcesUsingItNow()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("refunds", 25)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        Assert.Equal("refunds", Assert.Single(vm.MonitorNotifications).SourceName);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, SharingOrdersDlq("orders", 25), SharingOrdersDlq("refunds", 25));
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        Assert.Equal("orders-dlq (dead-letter queue shared by orders, refunds)", Assert.Single(vm.MonitorNotifications).SourceName);
    }

    // Two sources sharing a queue are one observation: one approximate zero in a check is one, not two in a row.
    [Fact]
    public async Task SharedSourcesConfirmAnApproximateZeroOnceAPerCheck()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        DeadLetterSnapshot Holding(long count) => Snapshot(dev.Id, SharingOrdersDlq("orders", count), SharingOrdersDlq("refunds", count));
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Holding(25) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();

        workspace.Snapshots[dev.Id] = Holding(0);
        await Check();
        Assert.Single(vm.MonitorNotifications);

        // An interrupted check (the shared queue unreadable) starts the confirmation over.
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), null, null, "timed out") { MeasuredFrom = SharedArn });
        await Check();
        workspace.Snapshots[dev.Id] = Holding(0);
        await Check();
        Assert.Single(vm.MonitorNotifications);

        await Check();
        Assert.Empty(vm.MonitorNotifications);
    }

    // One shared backlog across one, two and again one user is one continuous trend, without invented changes.
    [Fact]
    public void HistoryOfASharedQueueIsOneTrendWhoeverUsesIt()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        DeadLetterHistorySample Sample(int minute, params string[] users) => DeadLetterHistorySample.FromSnapshot(
            new DeadLetterSnapshot(profileId, at.AddMinutes(minute), users.Select(user => SharingOrdersDlq(user, 25))), "Test");

        foreach (var (first, last) in new[] { (Sample(0, "orders"), Sample(1, "orders", "refunds")), (Sample(1, "orders", "refunds"), Sample(2, "orders")) })
        {
            var summary = DeadLetterHistory.Summarize([first, last], at.AddMinutes(-1), at.AddMinutes(5))!;

            var trend = Assert.Single(summary.Sources);
            Assert.Equal("orders-dlq", trend.Name);
            Assert.Equal(0, trend.Change);
            Assert.Equal(DeadLetterCountQuality.Estimated, trend.NowQuality);
            Assert.Equal(0, summary.Change);
            Assert.False(summary.TargetsChanged);
        }
    }

    // A partial scan: the first alias of the shared queue failed, the other read it. The known count stays.
    [Fact]
    public async Task ListedTotalsKeepASharedCountWhenOneAliasFailed()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id,
                    new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), null, null, "timed out") { MeasuredFrom = SharedArn },
                    SharingOrdersDlq("refunds", 25))
            }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();

        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();

        Assert.Equal(25, vm.GlobalDlqSourceCount);
        Assert.Equal(25, vm.VisibleDlqSourceCount);
    }

    // A dead-letter queue that is a source too (orders → orders-dlq → parking): its own destination changing is a
    // target change, although a recorded destination has its name; the orders-dlq backlog itself is unchanged.
    [Fact]
    public void ADeadLetterQueueThatIsASourceToo()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        DeadLetterHistorySample Sample(int minute, long parked, string parking) => DeadLetterHistorySample.FromSnapshot(
            new DeadLetterSnapshot(profileId, at.AddMinutes(minute),
                [Through("orders", 25, SharedArn), Through("orders-dlq", parked, $"arn:aws:sqs:us-east-1:123:{parking}")]), "Test");

        var summary = DeadLetterHistory.Summarize([Sample(0, 10, "parking-a"), Sample(1, 5, "parking-b")], at.AddMinutes(-1), at.AddMinutes(5))!;

        Assert.True(summary.TargetsChanged);
        Assert.Null(summary.Change);
        Assert.Equal(0, Assert.Single(summary.Sources, source => source.Name == "orders-dlq").Change);
    }

    // Two destinations with the same short name (another region) are recorded apart, under their full names.
    [Fact]
    public void DestinationsWithTheSameShortNameAreRecordedApart()
    {
        var sample = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(Guid.NewGuid(), DateTimeOffset.UtcNow,
            [Through("orders", 25, "arn:aws:sqs:us-east-1:123:dlq"), Through("refunds", 7, "arn:aws:sqs:eu-west-1:123:dlq")]), "Test");

        Assert.Equal(32, sample.Total);
        Assert.Equal(25, sample.Sources["arn:aws:sqs:us-east-1:123:dlq"]);
        Assert.Equal(7, sample.Sources["arn:aws:sqs:eu-west-1:123:dlq"]);
    }

    // History written before shared destinations were recorded by name: upgrading invents no per-source drain.
    [Theory]
    [InlineData("orders")]
    [InlineData("orders,refunds")]
    public void UpgradedHistoryInventsNoPerSourceDrain(string sources)
    {
        var users = sources.Split(',');
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var legacy = new DeadLetterHistorySample(at, profileId, "Test", 25L * users.Length,
            users.ToDictionary(user => user, _ => 25L), DeadLetterCountQuality.Estimated,
            users.ToDictionary(user => user, _ => DeadLetterCountQuality.Estimated),
            users.ToDictionary(user => user, _ => new DeadLetterObservation(null, SharedArn)));
        var current = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profileId, at.AddMinutes(1),
            users.Select(user => SharingOrdersDlq(user, 25))), "Test");

        var summary = DeadLetterHistory.Summarize([legacy, current], at.AddMinutes(-1), at.AddMinutes(5))!;

        Assert.Null(summary.Change);
        Assert.All(summary.Sources, trend =>
        {
            Assert.Null(trend.Change);
            Assert.NotEqual((0L, DeadLetterCountQuality.Exact), (trend.Now ?? -1, trend.NowQuality));
            Assert.NotEqual((0L, DeadLetterCountQuality.Exact), (trend.Start ?? -1, trend.StartQuality));
        });
    }

    // Growth of a shared destination between two current samples is still its growth.
    [Fact]
    public void SharedDestinationGrowthIsKept()
    {
        var profileId = Guid.NewGuid();
        var at = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        DeadLetterHistorySample Sample(int minute, long count) => DeadLetterHistorySample.FromSnapshot(
            new DeadLetterSnapshot(profileId, at.AddMinutes(minute), [SharingOrdersDlq("orders", count), SharingOrdersDlq("refunds", count)]), "Test");

        var summary = DeadLetterHistory.Summarize([Sample(0, 25), Sample(1, 30)], at.AddMinutes(-1), at.AddMinutes(5))!;

        Assert.Equal(5, Assert.Single(summary.Sources).Change);
        Assert.Equal(5, summary.Change);
    }
}
