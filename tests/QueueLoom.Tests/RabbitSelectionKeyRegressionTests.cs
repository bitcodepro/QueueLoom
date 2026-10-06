using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class RabbitSelectionKeyRegressionTests
{
    [Theory]
    [InlineData(true, "4.2.0", "x-delivery-count", true)]
    [InlineData(true, "4.3.0", "x-delivery-count", true)]
    [InlineData(true, "4.3.0", "x-acquired-count", true)]
    [InlineData(true, "4.2.0", "x-acquired-count", false)]
    [InlineData(false, "4.2.0", "x-delivery-count", false)]
    [InlineData(false, "4.2.0", "x-acquired-count", false)]
    [InlineData(false, "4.3.0", "x-delivery-count", false)]
    [InlineData(false, "4.3.0", "x-acquired-count", false)]
    public void BugCycleOne_NoIdSelectionKeyIgnoresOnlyEstablishedBrokerOwnedHeaderNames(bool quorum, string version, string counter, bool stable)
    {
        var owned = RabbitMqWorkspace.BrokerOwnedHeaders(quorum, Version.Parse(version));
        BrowsedMessage Read(bool later)
        {
            var headers = new Dictionary<string, object?> { ["tenant"] = "same" };
            if (later) headers[counter] = 1L;
            return RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(), new BasicProperties
                { Timestamp = new AmqpTimestamp(1720000000), Headers = headers }, "orders",
                ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, owned);
        }
        var first = Read(false);
        var later = Read(true);
        Assert.Equal(stable, first.SequenceNumber == later.SequenceNumber);
        Assert.Equal(stable, MessageFingerprint.Stable(first) == MessageFingerprint.Stable(later));
    }
}
