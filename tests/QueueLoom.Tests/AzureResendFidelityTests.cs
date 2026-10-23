using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class AzureResendFidelityTests
{
    private static ServiceBusReceivedMessage Received(IDictionary<string, object>? properties = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("body"), messageId: "m-1",
            sequenceNumber: 7, properties: properties);

    private static BrowsedMessage Browse(ServiceBusReceivedMessage message) =>
        AzureMessageMapper.FromAzure(message, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

    [Fact]
    public void NullApplicationPropertyReadsAsNullAndConvertsBackToNull()
    {
        var property = ApplicationPropertyValues.FromObject("gone", null);

        Assert.Equal(ApplicationPropertyType.Null, property.Type);
        Assert.Equal(string.Empty, property.Value);
        Assert.Null(ApplicationPropertyValues.ToObject(property));
    }

    [Fact]
    public void AzureResendSendsNullApplicationPropertyAsNull()
    {
        var browsed = Browse(Received(new Dictionary<string, object> { ["gone"] = null!, ["kept"] = "x" }));

        var sent = AzureMessageMapper.ToAzure(browsed.CreateDraft());

        Assert.True(sent.ApplicationProperties.ContainsKey("gone"));
        Assert.Null(sent.ApplicationProperties["gone"]);
        Assert.Equal("x", sent.ApplicationProperties["kept"]);
    }

    [Fact]
    public void NullPropertyIsValidOnlyWithoutText()
    {
        var empty = new MessageDraft(EditableMessageBody.Empty, null, [new MessageApplicationProperty("p", ApplicationPropertyType.Null, string.Empty)]);
        var text = new MessageDraft(EditableMessageBody.Empty, null, [new MessageApplicationProperty("p", ApplicationPropertyType.Null, "x")]);

        Assert.True(MessageDraftValidator.Validate(empty, MessagingProvider.AzureServiceBus).IsValid);
        Assert.False(MessageDraftValidator.Validate(text, MessagingProvider.AzureServiceBus).IsValid);
    }

    [Fact]
    public void NullPropertyGoesToRabbitMqAndKafkaAsNull()
    {
        var draft = new MessageDraft(EditableMessageBody.Empty, null, [new MessageApplicationProperty("p", ApplicationPropertyType.Null, string.Empty)]);

        var headers = RabbitMqMessageMapper.ToAmqp(draft).Headers!;
        Assert.True(headers.ContainsKey("p"));
        Assert.Null(headers["p"]);
        var kafka = Assert.Single(KafkaMessageMapper.ToKafka(draft).Headers, header => header.Key == "p");
        Assert.Null(kafka.GetValueBytes());
    }

    [Fact]
    public void AzureResendCopiesSenderAnnotationsButNotBrokerOnes()
    {
        var received = Received();
        var annotations = received.GetRawAmqpMessage().MessageAnnotations;
        annotations["custom-trace"] = "abc";
        annotations["x-opt-custom-but-reserved"] = "no";
        annotations["x-opt-sequence-number"] = 7L;

        var sent = AzureMessageMapper.ToAzure(Browse(received).CreateDraft()).GetRawAmqpMessage().MessageAnnotations;

        Assert.Equal("abc", sent["custom-trace"]);
        Assert.DoesNotContain(sent.Keys, key => key.StartsWith("x-opt-", StringComparison.Ordinal));
    }

    [Fact]
    public void AnnotationsSurviveANewMessageId()
    {
        var received = Received();
        received.GetRawAmqpMessage().MessageAnnotations["custom-trace"] = "abc";
        var browsed = Browse(received);

        var item = new ResendItem(browsed, browsed.Source, browsed.CreateDraft()).WithNewMessageId();

        Assert.Equal("abc", AzureMessageMapper.ToAzure(item.Message).GetRawAmqpMessage().MessageAnnotations["custom-trace"]);
    }

    [Fact]
    public void ResendSummaryWarnsThatBinaryPropertiesArriveAsBase64OnAzure()
    {
        var binary = Browse(Received(new Dictionary<string, object> { ["raw"] = new byte[] { 1, 2 } }));
        var plain = Browse(Received(new Dictionary<string, object> { ["text"] = "x" }));

        var azure = new ResendDialogViewModel([binary], [], "Orders", false, destinationProvider: MessagingProvider.AzureServiceBus);
        var noBinary = new ResendDialogViewModel([plain], [], "Orders", false, destinationProvider: MessagingProvider.AzureServiceBus);
        var rabbit = new ResendDialogViewModel([binary], [], "Orders", false, destinationProvider: MessagingProvider.RabbitMq);

        Assert.Contains("Base64", azure.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("Base64", noBinary.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("Base64", rabbit.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void BinaryNoticeOnlyForAzureWithABinaryProperty()
    {
        MessageApplicationProperty[] binary = [new("raw", ApplicationPropertyType.Binary, "AQI=")];

        Assert.Contains("Base64", ApplicationPropertyValues.AzureBinaryNotice(MessagingProvider.AzureServiceBus, binary));
        Assert.Null(ApplicationPropertyValues.AzureBinaryNotice(MessagingProvider.Kafka, binary));
        Assert.Null(ApplicationPropertyValues.AzureBinaryNotice(MessagingProvider.AzureServiceBus,
            [new MessageApplicationProperty("t", ApplicationPropertyType.String, "x")]));
    }
}
