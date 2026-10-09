using System.Text.Json;
using Amazon.SQS.Model;
using QueueLoom.App.Serialization;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class AwsDataTypeRoundTripTests
{
    public static IEnumerable<object[]> RoundTrips()
    {
        (string DataType, string Value, ApplicationPropertyType Type)[] cases =
        [
            ("String.Int32", "42", ApplicationPropertyType.String),
            ("String.Int32", "invoice-42", ApplicationPropertyType.String),
            ("Number.String", "42", ApplicationPropertyType.Int64),
            ("Binary.png", "AP+A", ApplicationPropertyType.Binary),
            ("Number.Int32", "42", ApplicationPropertyType.Int32),
            ("String.Boolean", "true", ApplicationPropertyType.Boolean),
            ("String.Guid", "dc0d2142-2b32-4a25-a750-0e7e6c003a52", ApplicationPropertyType.Guid)
        ];
        foreach (var item in cases)
        foreach (var backup in new[] { false, true })
        foreach (var envelope in new[] { false, true })
            yield return [item.DataType, item.Value, item.Type, backup, envelope];
    }

    [Theory]
    [MemberData(nameof(RoundTrips))]
    public async Task BrowseEditorValidationBackupAndSendPreserveDataTypeAndPayload(
        string dataType, string value, ApplicationPropertyType type, bool backup, bool envelope)
    {
        var binary = dataType.StartsWith("Binary.", StringComparison.Ordinal);
        var incoming = new Message { MessageId = "roundtrip", Body = "body", MessageAttributes = [] };
        if (envelope)
        {
            incoming.Body = JsonSerializer.Serialize(new
            {
                Type = "Notification", MessageId = "notification", TopicArn = "arn:aws:sns:us-east-1:123456789012:orders", Message = "body",
                MessageAttributes = new Dictionary<string, object> { ["p"] = new { Type = dataType, Value = value } }
            });
        }
        else
        {
            incoming.MessageAttributes["p"] = new MessageAttributeValue
            {
                DataType = dataType, StringValue = binary ? null : value,
                BinaryValue = binary ? new MemoryStream(Convert.FromBase64String(value)) : null
            };
        }
        var message = AwsMessageMapper.FromSqs(incoming, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, envelope);
        using var directory = new TemporaryDirectory();
        if (backup)
        {
            var paths = QueueLoomPaths.ForRoot(directory.Path);
            var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development,
                new(AuthenticationKind.AwsDefaultCredentials)) with { Provider = MessagingProvider.AmazonSqsSns };
            var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
            await session.BackupAsync(message, default);
            var repository = new JsonDeadLetterBackupRepository(paths);
            message = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        }
        var originalDraft = message.CreateDraft();
        Assert.Equal(type, Assert.Single(originalDraft.ApplicationProperties).Type);
        // The composer reconstructs a draft from its editable JSON; retained labels must survive that path too.
        var draft = new MessageDraft(originalDraft.Body, originalDraft.Properties,
            ApplicationPropertiesJson.Deserialize(ApplicationPropertiesJson.Serialize(originalDraft.ApplicationProperties)));
        var validation = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);
        Assert.True(validation.IsValid, string.Join("; ", validation.Errors.Select(error => error.Message)));
        var sqs = AwsMessageMapper.ToSqsAttributes(draft)["p"];
        var sns = AwsMessageMapper.ToSnsAttributes(draft)["p"];
        Assert.Equal(dataType, sqs.DataType);
        Assert.Equal(dataType, sns.DataType);
        Assert.Equal(dataType, MessageAttributeConventions.AwsDataType(Assert.Single(draft.ApplicationProperties)));
        var payloadSize = binary ? Convert.FromBase64String(value).Length : System.Text.Encoding.UTF8.GetByteCount(value);
        Assert.Equal(4L + 1 + System.Text.Encoding.UTF8.GetByteCount(dataType) + payloadSize, MessageSizeLimits.AwsSize(draft));
        if (binary)
        {
            Assert.Null(sqs.StringValue);
            Assert.Null(sns.StringValue);
            Assert.Equal(Convert.FromBase64String(value), sqs.BinaryValue.ToArray());
            Assert.Equal(Convert.FromBase64String(value), sns.BinaryValue.ToArray());
        }
        else
        {
            Assert.Equal(value, sqs.StringValue);
            Assert.Equal(value, sns.StringValue);
            Assert.Null(sqs.BinaryValue);
            Assert.Null(sns.BinaryValue);
        }
        Assert.Equal("body", AwsMessageMapper.BodyText(draft));
    }
}
