using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

// A draft whose Session ID and Partition Key differ was refused for every provider with Azure Service Bus's rule
// ("must be identical"). RabbitMQ uses neither, so such a draft (for example one copied from another broker) is now
// accepted there. SQS, Pub/Sub and Kafka use only one of the two as their key, so a mismatch is still refused, saying
// which one that provider uses instead of quoting Service Bus.
public sealed class SessionPartitionMismatchTests
{
    private static ValidationResult Validate(MessagingProvider provider) => MessageDraftValidator.Validate(
        new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), new EditableMessageProperties(SessionId: "a", PartitionKey: "b")),
        provider);

    [Fact]
    public void RabbitMqAcceptsADraftWithBothSet() =>
        Assert.DoesNotContain(Validate(MessagingProvider.RabbitMq).Errors, error => error.Code == "message.session_partition_mismatch");

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus, "identical")]
    [InlineData(MessagingProvider.AmazonSqsSns, "message group ID")]
    [InlineData(MessagingProvider.GooglePubSub, "ordering key")]
    [InlineData(MessagingProvider.Kafka, "record key")]
    public void ProvidersThatUseOneOfThemSayWhich(MessagingProvider provider, string expected)
    {
        var error = Assert.Single(Validate(provider).Errors, error => error.Code == "message.session_partition_mismatch");
        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }
}
