using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

/// <summary>A null Kafka header reads as a Null property, so it is shown, exported and edited apart from an empty one.</summary>
public sealed class KafkaNullHeaderTests
{
    private static BrowsedMessage Browse(byte[]? header) => KafkaMessageMapper.FromKafka(
        new ConsumeResult<byte[]?, byte[]?> { Topic = "t", Partition = new Partition(0), Offset = new Offset(1),
            Message = new Message<byte[]?, byte[]?> { Value = "body"u8.ToArray(), Headers = new Headers { { "p", header } } } },
        ServiceBusEntityReference.Queue("t"), ServiceBusSubQueue.Active);

    private static byte[]? Sent(BrowsedMessage browsed, MessageApplicationProperty edited)
    {
        var draft = browsed.CreateDraft();
        var changed = new MessageDraft(draft.Body, draft.Properties, [edited]) { KafkaEnvelope = draft.KafkaEnvelope };
        return Assert.Single(KafkaMessageMapper.ToKafka(changed).Headers, header => header.Key == "p").GetValueBytes();
    }

    [Fact]
    public void NullHeaderReadsAsNull()
    {
        var property = Assert.Single(Browse(null).ApplicationProperties);
        Assert.Equal(new MessageApplicationProperty("p", ApplicationPropertyType.Null, string.Empty), property);
    }

    [Fact]
    public void EmptyHeaderStillReadsAsEmptyString()
    {
        var property = Assert.Single(Browse([]).ApplicationProperties);
        Assert.Equal(new MessageApplicationProperty("p", ApplicationPropertyType.String, string.Empty), property);
    }

    [Fact]
    public void NullHeaderEditedToEmptyTextIsSentAsEmptyBytes() =>
        Assert.Empty(Assert.IsType<byte[]>(Sent(Browse(null), new MessageApplicationProperty("p", ApplicationPropertyType.String, string.Empty))));

    [Fact]
    public void EmptyHeaderEditedToNullIsStillSentAsNull() =>
        Assert.Null(Sent(Browse([]), new MessageApplicationProperty("p", ApplicationPropertyType.Null, string.Empty)));

    [Fact]
    public void UntouchedNullHeaderStaysNull()
    {
        var browsed = Browse(null);
        Assert.Null(Sent(browsed, Assert.Single(browsed.CreateDraft().ApplicationProperties)));
    }

    [Fact]
    public async Task NullHeaderSurvivesJsonExportAndImport()
    {
        using var file = new MemoryStream();
        await MessageExport.WriteJsonAsync(file, [new ExportedMessage("Kafka", Browse(null))], default);
        var imported = Assert.Single(await MessageImport.ReadAsync(new MemoryStream(file.ToArray())));

        Assert.Equal(ApplicationPropertyType.Null, Assert.Single(imported.Draft.ApplicationProperties).Type);
        Assert.Null(Assert.Single(KafkaMessageMapper.ToKafka(imported.Draft).Headers, header => header.Key == "p").GetValueBytes());
    }

    [Fact]
    public void ComparisonTellsNullAndEmptyHeadersApart()
    {
        var left = Browse(null);
        var right = Browse([]);
        Assert.NotEqual(left.ApplicationProperties, right.ApplicationProperties);
        Assert.False(MessageComparison.Compare(left, right).AreEqual);
    }
}
