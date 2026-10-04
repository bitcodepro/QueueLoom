using System.Globalization;
using System.Text;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using SnsAttribute = Amazon.SimpleNotificationService.Model.MessageAttributeValue;

namespace QueueLoom.Infrastructure.Aws;

internal static class AwsMessageMapper
{
    private const int MaximumDelaySeconds = 900;

    public static BrowsedMessage FromSqs(Message message, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        var system = message.Attributes ?? [];
        var attributes = (message.MessageAttributes ?? [])
            .ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);
        var standard = attributes
            .Where(item => IsStringType(item.Value.DataType) && item.Value.StringValue is not null)
            .ToDictionary(item => item.Key, item => item.Value.StringValue, StringComparer.Ordinal);
        var properties = MessageAttributeConventions.ReadStandardAttributes(
            standard,
            message.MessageId,
            sessionId: system.GetValueOrDefault("MessageGroupId"));
        foreach (var name in MessageAttributeConventions.StandardAttributes(properties).Select(item => item.Key))
        {
            attributes.Remove(name);
        }

        var sourceArn = system.GetValueOrDefault("DeadLetterQueueSourceArn");
        var isDeadLetter = subQueue == ServiceBusSubQueue.DeadLetter;
        return new BrowsedMessage(
            source,
            subQueue,
            LeasedMessageIdentity.SequenceNumberFor(message.MessageId ?? message.ReceiptHandle),
            Encoding.UTF8.GetBytes(message.Body ?? string.Empty),
            properties,
            attributes
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => ToProperty(item.Key, item.Value.DataType, item.Value.StringValue, item.Value.BinaryValue)),
            ServiceBusMessageState.Active,
            deliveryCount: ReadInt(system, "ApproximateReceiveCount"),
            enqueuedAt: ReadEpochMilliseconds(system, "SentTimestamp"),
            deadLetterReason: isDeadLetter ? "Moved by the redrive policy" : null,
            deadLetterErrorDescription: isDeadLetter && !string.IsNullOrEmpty(sourceArn)
                ? $"From {AwsQueueInfo.LastSegment(sourceArn)} after {ReadInt(system, "ApproximateReceiveCount")} receives"
                : null)
        { HasSequenceNumber = false };
    }

    public static string BodyText(MessageDraft message) =>
        // SQS and SNS bodies are text. A binary body stays in its base64 form.
        message.Body.Content ?? string.Empty;

    public static Dictionary<string, MessageAttributeValue> ToSqsAttributes(MessageDraft message)
    {
        var result = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(message.Properties))
        {
            result[name] = new MessageAttributeValue { DataType = "String", StringValue = value };
        }
        foreach (var property in message.ApplicationProperties)
        {
            var (dataType, stringValue, binaryValue) = ToAttribute(property);
            result[property.Name] = new MessageAttributeValue
            {
                DataType = dataType,
                StringValue = stringValue,
                BinaryValue = binaryValue is null ? null : new MemoryStream(binaryValue)
            };
        }
        return result;
    }

    public static Dictionary<string, SnsAttribute> ToSnsAttributes(MessageDraft message)
    {
        var result = new Dictionary<string, SnsAttribute>(StringComparer.Ordinal);
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(message.Properties))
        {
            result[name] = new SnsAttribute { DataType = "String", StringValue = value };
        }
        foreach (var property in message.ApplicationProperties)
        {
            var (dataType, stringValue, binaryValue) = ToAttribute(property);
            result[property.Name] = new SnsAttribute
            {
                DataType = dataType,
                StringValue = stringValue,
                BinaryValue = binaryValue is null ? null : new MemoryStream(binaryValue)
            };
        }
        return result;
    }

    /// <summary>FIFO queues and topics need a group: the session ID, else the partition key.</summary>
    public static string GroupId(MessageDraft message) =>
        FirstNonEmpty(message.Properties.SessionId, message.Properties.PartitionKey) ?? "default";

    public static string DeduplicationId(MessageDraft message) =>
        FirstNonEmpty(message.Properties.MessageId) ?? Guid.NewGuid().ToString("N");

    public static int? DelaySeconds(MessageDraft message, DateTimeOffset now)
    {
        if (message.Properties.ScheduledEnqueueTime is not { } scheduledAt)
        {
            return null;
        }

        var seconds = (int)Math.Ceiling((scheduledAt - now).TotalSeconds);
        if (seconds > MaximumDelaySeconds)
        {
            throw new InvalidOperationException("Amazon SQS can delay a message by at most 15 minutes.");
        }
        return seconds > 0 ? seconds : null;
    }

    /// <summary>
    /// SQS attribute types are String, Number and Binary, each with an optional custom label. QueueLoom
    /// writes its type as the label ("Number.Int32", "String.Guid") so the type survives a round trip.
    /// </summary>
    internal static (string DataType, string? StringValue, byte[]? BinaryValue) ToAttribute(MessageApplicationProperty property) =>
        property.Type switch
        {
            ApplicationPropertyType.String => ("String", property.Value, null),
            ApplicationPropertyType.Binary => ("Binary", null, Convert.FromBase64String(property.Value)),
            ApplicationPropertyType.Byte or ApplicationPropertyType.SByte or ApplicationPropertyType.Int16 or
                ApplicationPropertyType.UInt16 or ApplicationPropertyType.Int32 or ApplicationPropertyType.UInt32 or
                ApplicationPropertyType.Int64 or ApplicationPropertyType.UInt64 or ApplicationPropertyType.Single or
                ApplicationPropertyType.Double or ApplicationPropertyType.Decimal =>
                ($"Number.{property.Type}", property.Value, null),
            _ => ($"String.{property.Type}", property.Value, null)
        };

    internal static MessageApplicationProperty ToProperty(string name, string? dataType, string? stringValue, MemoryStream? binaryValue)
    {
        var parts = (dataType ?? "String").Split('.', 2);
        var label = parts.Length > 1 ? parts[1] : null;
        if (parts[0] == "Binary")
        {
            return new MessageApplicationProperty(name, ApplicationPropertyType.Binary,
                Convert.ToBase64String(binaryValue?.ToArray() ?? []));
        }

        var value = stringValue ?? string.Empty;
        // Only a type name counts; Enum.TryParse would also read another producer's "Number.1" as a QueueLoom type.
        if (label is { Length: > 0 } && char.IsAsciiLetter(label[0]) &&
            Enum.TryParse<ApplicationPropertyType>(label, ignoreCase: false, out var labelled) &&
            Enum.IsDefined(labelled) && labelled != ApplicationPropertyType.Binary)
        {
            return new MessageApplicationProperty(name, labelled, value);
        }

        if (parts[0] == "Number")
        {
            var type = long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)
                ? ApplicationPropertyType.Int64
                : decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _)
                    ? ApplicationPropertyType.Decimal
                    : ApplicationPropertyType.String;
            return new MessageApplicationProperty(name, type, value);
        }

        return new MessageApplicationProperty(name, ApplicationPropertyType.String, value);
    }

    private static bool IsStringType(string? dataType) =>
        dataType is not null && (dataType == "String" || dataType.StartsWith("String.", StringComparison.Ordinal));

    private static string? FirstNonEmpty(params string?[] values) =>
        values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));

    private static int ReadInt(Dictionary<string, string> attributes, string name) =>
        int.TryParse(attributes.GetValueOrDefault(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value > 0
            ? value
            : 0;

    private static DateTimeOffset? ReadEpochMilliseconds(Dictionary<string, string> attributes, string name) =>
        long.TryParse(attributes.GetValueOrDefault(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? DateTimeOffset.FromUnixTimeMilliseconds(value)
            : null;
}
