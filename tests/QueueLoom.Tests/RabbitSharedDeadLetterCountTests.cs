using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class RabbitSharedDeadLetterCountTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SharedPhysicalQueueIsCountedOnce(bool defaultExchange)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TopologyWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        { Index = Routes(defaultExchange, ("orders", "dead", 100), ("invoices", "dead", 100)) };
        await workspace.ConnectAsync(Profile());

        var snapshot = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        Assert.Equal(100, snapshot.TotalCount);
        Assert.Equal(2, snapshot.Entities.Count); // Keep both source rows, not two physical counts.
        Assert.All(snapshot.Entities, entity =>
        {
            Assert.Equal(100, entity.Count);
            Assert.Equal("dead", entity.MeasuredFrom);
            Assert.False(entity.CountsOnlyThisSource);
        });
        Assert.Single(snapshot.CountedOnce);
        Assert.Equal(2, Assert.Single(snapshot.SharedTargets).Value.Count);
        Assert.Equal(2, Assert.Single(snapshot.MonitorUnits).Sources.Count);
        var history = DeadLetterHistorySample.FromSnapshot(snapshot, "Synthetic Rabbit DLQ");
        Assert.Equal(100, history.Total);
        Assert.Equal(100, Assert.Single(history.Sources).Value);
        Assert.Equal(0, workspace.ReceiveAttempts);
    }

    [Theory]
    [InlineData("dead", "DEAD")]
    [InlineData("dead", " dead ")]
    [InlineData("dead", "dead/letters")]
    public async Task DifferentQueuesBehindOneExchangeKeepExactPhysicalIdentities(string first, string second)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TopologyWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        { Index = Routes(false, ("orders", first, 100), ("invoices", second, 25)) };
        await workspace.ConnectAsync(Profile());

        var snapshot = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        Assert.Equal(125, snapshot.TotalCount);
        Assert.Equal(first, snapshot.Entities.Single(item => item.Entity.Name == "orders").MeasuredFrom);
        Assert.Equal(second, snapshot.Entities.Single(item => item.Entity.Name == "invoices").MeasuredFrom);
        Assert.Equal(2, snapshot.CountedOnce.Count());
        Assert.Equal(2, snapshot.MonitorUnits.Count);
        Assert.Empty(snapshot.SharedTargets);
    }

    [Fact]
    public async Task SwitchingQueueDoesNotInventGrowthBetweenDifferentDestinations()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TopologyWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        { Index = Routes(false, ("orders", "old-dead", 100)) };
        await workspace.ConnectAsync(Profile());
        await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        workspace.Index = Routes(false, ("orders", "new-dead", 150));
        var switched = Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);
        Assert.Null(switched.Change);
        Assert.Equal("old-dead", switched.PreviousMeasuredFrom);
        Assert.Equal("new-dead", switched.MeasuredFrom);

        workspace.Index = Routes(false, ("orders", "new-dead", 160));
        var sameTarget = Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);
        Assert.Equal(10, sameTarget.Change);
    }

    [Fact]
    public async Task SplittingSharedDestinationKeepsOnlyTheUnchangedSourcesDelta()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TopologyWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        { Index = Routes(false, ("orders", "dead", 100), ("invoices", "dead", 100)) };
        await workspace.ConnectAsync(Profile());
        await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        workspace.Index = Routes(false, ("orders", "dead", 110), ("invoices", "other-dead", 30));
        var split = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);
        Assert.Equal(140, split.TotalCount);
        Assert.Equal(10, split.Entities.Single(item => item.Entity.Name == "orders").Change);
        Assert.Null(split.Entities.Single(item => item.Entity.Name == "invoices").Change);
        Assert.Empty(split.SharedTargets);
        Assert.Equal(2, split.MonitorUnits.Count);
    }

    [Fact]
    public async Task UnresolvedQueueHasNoInventedCountIdentity()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TopologyWorkspace(QueueLoomPaths.ForRoot(directory.Path))
        { Index = new RabbitMqTopologyIndex([Queue("orders", 0, "", "missing")], [], []) };
        await workspace.ConnectAsync(Profile());
        var queue = Assert.Single((await workspace.GetTopologyAsync()).Queues);
        Assert.False(queue.HasDeadLetterQueue);
        Assert.Null(queue.Runtime.DeadLetterCountSource);
        Assert.Empty((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);
    }

    private static ServiceBusProfile Profile() =>
        ServiceBusProfile.CreateNew("Synthetic Rabbit DLQ", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword)) with
        { Provider = MessagingProvider.RabbitMq, RabbitMq = new("broker.invalid", "test-user") };

    private static RabbitQueueInfo Queue(string name, long ready, string? exchange = null, string? key = null) =>
        new(name, "classic", ready, 0, exchange, key, null, "running");

    private static RabbitMqTopologyIndex Routes(bool defaultExchange, params (string Source, string Target, long Count)[] routes) =>
        new(routes.Select(route => Queue(route.Source, 0, defaultExchange ? "" : "errors", route.Target))
                .Concat(routes.GroupBy(route => route.Target, StringComparer.Ordinal).Select(group => Queue(group.Key, group.First().Count))),
            defaultExchange ? [] : [new RabbitExchangeInfo("errors", "direct")],
            defaultExchange ? [] : routes.Select(route => new RabbitBindingInfo("errors", route.Target, route.Target)).Distinct());

    // Real Rabbit topology conversion and leased measurement/aggregation, with no network or broker side effects.
    private sealed class TopologyWorkspace(QueueLoomPaths paths) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public RabbitMqTopologyIndex Index { get; set; } = RabbitMqTopologyIndex.Empty;
        public int ReceiveAttempts { get; private set; }
        public override MessagingProvider Provider => MessagingProvider.RabbitMq;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(Index.ToTopology(DateTimeOffset.UtcNow));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
        { ReceiveAttempts++; throw new InvalidOperationException("Management counts must not receive messages."); }
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message, CancellationToken token) =>
            throw new InvalidOperationException("Read-only synthetic topology test.");
    }
}
