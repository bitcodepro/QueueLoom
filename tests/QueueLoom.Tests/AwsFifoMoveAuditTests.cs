using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class AwsFifoMoveAuditTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FifoMoveWithOriginalIdNeverDeletesAnOriginalAfterSuppressedDelivery(bool topic)
    {
        var source = ServiceBusEntityReference.Queue("orders.fifo");
        var target = topic ? ServiceBusEntityReference.Topic("events.fifo") : source;
        var original = new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1, "original"u8.ToArray(),
            new EditableMessageProperties(MessageId: "dedup-A", SessionId: "group"));
        var item = new ResendItem(original, target, original.CreateDraft());
        Assert.Equal("dedup-A", AwsMessageMapper.DeduplicationId(item.Message));
        var broker = new DeepAuditResendTests.DeduplicatingBroker("dedup-A") { Provider = MessagingProvider.AmazonSqsSns };
        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(broker.Workspace, [item], ResendMode.Move));
        Assert.Empty(broker.Delivered);
        Assert.Equal(0, broker.Deleted);
        var prepared = item.WithNewMessageId();
        var result = await DeadLetterResender.ResendAsync(broker.Workspace, [prepared], ResendMode.Move);
        Assert.Equal(1, result.MovedCount);
        Assert.Single(broker.Delivered);
        Assert.Equal(1, broker.Deleted);
    }
}
