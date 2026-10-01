using System.Reflection;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Mcp;

namespace QueueLoom.Tests;

public sealed class EntityResolverAuditTests
{
    [Fact]
    public void Resolver_PrefersExactCase()
    {
        var topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Queue("Orders"), Queue("orders")]) { QueueKindName = "topic" };
        Assert.Equal("orders", Resolve(topology, "orders").Name);
    }

    [Fact]
    public void Resolver_RejectsAmbiguousUntypedName()
    {
        Assert.Throws<TargetInvocationException>(() => Resolve(Rabbit(), "orders"));
    }

    [Theory]
    [InlineData("queue:orders", ServiceBusEntityKind.Queue)]
    [InlineData("exchange:orders", ServiceBusEntityKind.Topic)]
    public void Resolver_ResolvesProviderSpecificTypedNames(string name, ServiceBusEntityKind kind)
    {
        Assert.Equal(kind, Resolve(Rabbit(), name).Kind);
    }

    [Fact]
    public void Resolver_RejectsWrongCaseForCaseSensitiveProvider()
    {
        Assert.Throws<TargetInvocationException>(() => Resolve(Rabbit(), "queue:ORDERS"));
    }

    [Fact]
    public void Resolver_RejectsWrongCaseForAwsAndGoogleTopology()
    {
        var topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Queue("orders")]) { EntityNamesCaseSensitive = true };
        Assert.Throws<TargetInvocationException>(() => Resolve(topology, "ORDERS"));
    }

    private static ServiceBusTopology Rabbit() => new(DateTimeOffset.UtcNow, [Queue("orders")],
        [new ServiceBusTopic("orders", ServiceBusEntityRuntime.Empty)]) { TopicKindName = "exchange" };
    private static ServiceBusQueue Queue(string name) => new(name, ServiceBusEntityRuntime.Empty);
    private static ServiceBusEntityReference Resolve(ServiceBusTopology topology, string name) =>
        (ServiceBusEntityReference)Assembly.Load("QueueLoom.Mcp").GetType("QueueLoom.Mcp.EntityResolver")!
            .GetMethod("Resolve", BindingFlags.Static | BindingFlags.Public)!.Invoke(null, [topology, name, false])!;
}
