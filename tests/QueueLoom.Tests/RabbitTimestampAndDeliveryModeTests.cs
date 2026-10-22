using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

// A resent or moved RabbitMQ message was published with timestamp = now and delivery mode persistent, whatever the
// original said. The AMQP timestamp is the producer's data (RabbitMQ never sets it), which consumers use for ordering,
// staleness or idempotency windows; a transient message came back persistent. Both now travel with the message; a new
// message is still persistent and stamped now.
public sealed class RabbitTimestampAndDeliveryModeTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Theory]
    [InlineData(DeliveryModes.Transient)]
    [InlineData(DeliveryModes.Persistent)]
    public void AResentMessageKeepsItsTimestampAndDeliveryMode(DeliveryModes mode)
    {
        var received = new BasicProperties { MessageId = "m-1", Timestamp = new AmqpTimestamp(1_700_000_000), DeliveryMode = mode };
        var browsed = RabbitMqMessageMapper.FromAmqp("{}"u8.ToArray(), received, "orders", Orders, ServiceBusSubQueue.DeadLetter);

        var sent = RabbitMqMessageMapper.ToAmqp(browsed.CreateDraft());

        Assert.Equal(1_700_000_000, sent.Timestamp.UnixTime);
        Assert.Equal(mode, sent.DeliveryMode);
    }

    [Fact]
    public void ANewMessageIsPersistentAndStampedNow()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var sent = RabbitMqMessageMapper.ToAmqp(new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: "new")));

        Assert.Equal(DeliveryModes.Persistent, sent.DeliveryMode);
        Assert.InRange(sent.Timestamp.UnixTime, before, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    }
}
