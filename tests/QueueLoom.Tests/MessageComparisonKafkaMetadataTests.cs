using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class MessageComparisonKafkaMetadataTests
{
    [Fact]
    public void ComparisonAudit_KafkaBinaryHeaderDiffersFromItsBase64Text()
    {
        var left = Browse([0xff]);
        var right = Browse("/w=="u8.ToArray());
        // The same base64 text, but the binary header is typed Binary and the text header String.
        Assert.Equal(left.ApplicationProperties.Select(p => p.Value), right.ApplicationProperties.Select(p => p.Value));

        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }

    [Fact]
    public void ComparisonAudit_KafkaBinaryKeyDiffersFromItsBase64Text()
    {
        var left = Browse([], key: [0xff]);
        var right = Browse([], key: "/w=="u8.ToArray());
        Assert.Equal(left.Properties.PartitionKey, right.Properties.PartitionKey);

        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }

    [Fact]
    public void ComparisonAudit_KafkaNullHeaderDiffersFromEmptyHeader()
    {
        var left = Browse(null);
        var right = Browse([]);
        Assert.Equal(left.ApplicationProperties, right.ApplicationProperties);

        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }

    [Fact]
    public void ComparisonAudit_KafkaEarlierDuplicateHeadersAreNotHiddenByLastValue()
    {
        var left = Browse("same"u8.ToArray(), earlierHeader: "first"u8.ToArray());
        var right = Browse("same"u8.ToArray(), earlierHeader: "other"u8.ToArray());
        Assert.Equal(left.ApplicationProperties, right.ApplicationProperties);

        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }

    [Fact]
    public void ComparisonAudit_KafkaTombstoneDiffersFromEmptyBody()
    {
        var left = Browse([], tombstone: true);
        var right = Browse([], tombstone: false);
        Assert.Equal(left.BodySize, right.BodySize);
        Assert.True(left.Body.Span.SequenceEqual(right.Body.Span));

        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }

    [Fact]
    public void ComparisonAudit_IdenticalKafkaBytesRemainEqualAcrossSeparateArrays()
    {
        var left = Browse([0xff], key: [0x80], earlierHeader: [0x81]);
        var right = Browse([0xff], key: [0x80], earlierHeader: [0x81]);

        Assert.True(MessageComparison.Compare(left, right).AreEqual);
    }

    private static BrowsedMessage Browse(byte[]? header, byte[]? key = null, bool tombstone = false, byte[]? earlierHeader = null)
    {
        var headers = new Headers();
        if (earlierHeader is not null) headers.Add("opaque", earlierHeader);
        headers.Add("opaque", header);
        return KafkaMessageMapper.FromKafka(new ConsumeResult<byte[]?, byte[]?>
        {
            Topic = "orders", Partition = new Partition(0), Offset = new Offset(1),
            Message = new Message<byte[]?, byte[]?> { Key = key, Value = tombstone ? null : [], Headers = headers }
        }, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active);
    }
}
