using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class ForwardingTests
{
    private static ServiceBusQueue Queue(string name, string? forwardTo = null, string? deadLettersTo = null) =>
        new(name, ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active) { ForwardTo = forwardTo, ForwardDeadLettersTo = deadLettersTo };

    private static ServiceBusTopic Topic(string name, params ServiceBusSubscription[] subscriptions) =>
        new(name, ServiceBusEntityRuntime.Empty, subscriptions, ServiceBusEntityStatus.Active);

    private static ServiceBusSubscription Subscription(string topic, string name, string? forwardTo = null) =>
        new(topic, name, ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active) { ForwardTo = forwardTo };

    [Theory]
    [InlineData("archive", "archive")]
    [InlineData("sb://contoso.servicebus.windows.net/archive", "archive")]
    [InlineData("https://contoso.servicebus.windows.net/Archive/", "Archive")]
    [InlineData(" ", null)]
    public void Forward_targets_are_entity_names(string forwardTo, string? expected) =>
        Assert.Equal(expected, Forwarding.TargetName(forwardTo));

    [Fact]
    public void Chains_are_followed_through_topics_and_described_on_each_entity()
    {
        var topology = Forwarding.Annotate(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [Queue("inbox", "orders"), Queue("parked"), Queue("billing", deadLettersTo: "parked")],
            [Topic("orders", Subscription("orders", "eu", "eu-orders"), Subscription("orders", "audit")), Topic("eu-orders")]));

        Assert.Equal("Forwards to orders; messages end up in eu-orders, orders/audit", topology.Queues[0].Note);
        Assert.Equal("Gets forwarded messages from inbox", topology.Topics[0].Note);
        Assert.Equal("Gets forwarded messages from billing (dead letters)", topology.Queues[1].Note);
        Assert.Equal("Dead letters are forwarded to parked", topology.Queues[2].Note);
        Assert.Equal("Forwards to eu-orders", topology.Topics[0].Subscriptions[0].Note);
        Assert.Equal(2, Forwarding.Follow(topology, "inbox").LongestChain);
    }

    [Fact]
    public void A_loop_and_a_missing_target_are_called_out()
    {
        var topology = Forwarding.Annotate(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [Queue("a", "b"), Queue("b", "events"), Queue("lost", "nowhere")],
            [Topic("events", Subscription("events", "back", "A"))]));

        Assert.StartsWith("Forwarding loop: a → b → events → events/back → A", topology.Queues[0].Note, StringComparison.Ordinal);
        Assert.Contains("dead-letters these messages after 4 forwards", topology.Queues[0].Note, StringComparison.Ordinal);
        Assert.Equal("Forwards to nowhere, which does not exist", topology.Queues[2].Note);
    }

    [Fact]
    public void A_chain_longer_than_Service_Bus_allows_is_called_out()
    {
        var topology = Forwarding.Annotate(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [Queue("q1", "q2"), Queue("q2", "q3"), Queue("q3", "q4"), Queue("q4", "q5"), Queue("q5", "q6"), Queue("q6")]));

        var report = Forwarding.Follow(topology, "q1");
        Assert.Equal(5, report.LongestChain);
        Assert.True(report.IsTooLong);
        Assert.Equal("Forwards to q2 → q3 → q4 → q5 → q6; 5 forwards is more than Service Bus allows (4), so messages are dead-lettered on the way",
            topology.Queues[0].Note);
        Assert.False(Forwarding.Follow(topology, "q2").IsTooLong);
    }

    [Fact]
    public void A_topology_without_forwarding_is_left_as_it_is()
    {
        var topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Queue("orders") with { Note = "kept" }]);
        Assert.Same(topology, Forwarding.Annotate(topology));
    }
}
