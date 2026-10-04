using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class KafkaBinaryHeaderAndAwsLabelTests
{
    private static BrowsedMessage BrowseKafka(Headers headers) => KafkaMessageMapper.FromKafka(
        new ConsumeResult<byte[]?, byte[]?> { Topic = "t", Partition = new Partition(0), Offset = new Offset(1),
            Message = new Message<byte[]?, byte[]?> { Value = "body"u8.ToArray(), Headers = headers } },
        ServiceBusEntityReference.Queue("t"), ServiceBusSubQueue.Active);

    [Fact]
    public void KafkaNonUtf8HeaderIsTypedBinary()
    {
        var headers = new Headers { { "opaque", new byte[] { 0xff, 0x00, 0xfe } } };
        var property = Assert.Single(BrowseKafka(headers).ApplicationProperties, p => p.Name == "opaque");
        Assert.Equal(ApplicationPropertyType.Binary, property.Type);
    }

    [Fact]
    public void KafkaNonUtf8HeaderSurvivesMoveToAzure()
    {
        var headers = new Headers { { "opaque", new byte[] { 0xff, 0x00, 0xfe } } };
        var azure = AzureMessageMapper.ToAzure(BrowseKafka(headers).CreateDraft());
        // Service Bus rejects byte[] properties, so the bytes go as their Base64 text, not as a lossy UTF-8 guess.
        Assert.Equal(Convert.ToBase64String(new byte[] { 0xff, 0x00, 0xfe }), Assert.IsType<string>(azure.ApplicationProperties["opaque"]));
    }

    [Theory]
    [InlineData("String.42")]
    [InlineData("Number.1")]
    public void AwsNumericCustomLabelIsNotTreatedAsQueueLoomType(string dataType)
    {
        var property = AwsMessageMapper.ToProperty("p", dataType, "7", null);
        Assert.True(Enum.IsDefined(property.Type), $"Got undefined/garbage type {(int)property.Type} ({property.Type})");
        Assert.Equal(dataType.StartsWith("Number") ? ApplicationPropertyType.Int64 : ApplicationPropertyType.String, property.Type);
    }
}
