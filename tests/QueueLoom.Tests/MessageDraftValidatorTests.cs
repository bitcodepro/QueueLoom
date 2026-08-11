using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

public sealed class MessageDraftValidatorTests
{
    [Fact]
    public void ValidJsonAndTypedProperties_AreAccepted()
    {
        var draft = new MessageDraft(
            new EditableMessageBody("{\"orderId\":42}", MessageBodyFormat.Json),
            new EditableMessageProperties(MessageId: "order-42", TimeToLive: TimeSpan.FromMinutes(5)),
            [
                new MessageApplicationProperty("retry", ApplicationPropertyType.Int32, "2"),
                new MessageApplicationProperty("trace", ApplicationPropertyType.Guid, Guid.NewGuid().ToString())
            ]);

        Assert.True(MessageDraftValidator.Validate(draft).IsValid);
    }

    [Fact]
    public void InvalidBodyAndProperties_ReturnStableErrors()
    {
        var draft = new MessageDraft(
            new EditableMessageBody("{broken", MessageBodyFormat.Json),
            new EditableMessageProperties(
                SessionId: "session-a",
                PartitionKey: "partition-b",
                TimeToLive: TimeSpan.Zero),
            [
                new MessageApplicationProperty("attempt", ApplicationPropertyType.Int32, "NaN"),
                new MessageApplicationProperty("attempt", ApplicationPropertyType.Int32, "2")
            ]);

        var result = MessageDraftValidator.Validate(draft);

        Assert.True(result.HasError("message.body.json_invalid"));
        Assert.True(result.HasError("message.ttl.invalid"));
        Assert.True(result.HasError("message.session_partition_mismatch"));
        Assert.True(result.HasError("message.application_property.value_invalid"));
        Assert.True(result.HasError("message.application_property.duplicate"));
    }

    [Fact]
    public void InvalidBase64_IsRejected()
    {
        var draft = new MessageDraft(new EditableMessageBody("not base64!", MessageBodyFormat.Base64));

        Assert.True(MessageDraftValidator.Validate(draft).HasError("message.body.base64_invalid"));
    }
}
