using QueueLoom.Core;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using SnsAttribute = Amazon.SimpleNotificationService.Model.MessageAttributeValue;

namespace QueueLoom.Infrastructure.Aws;

internal static class AwsMessageMapper
{
    private const int MaximumDelaySeconds = 900;

    /// <param name="snsEnvelope">
    /// The messages come through an SNS subscription without raw message delivery, so each body is SNS's JSON envelope.
    /// With raw delivery, an SNS-shaped body is the application's own payload and is left alone.
    /// </param>
    public static BrowsedMessage FromSqs(Message message, ServiceBusEntityReference source, ServiceBusSubQueue subQueue,
        bool snsEnvelope = false)
    {
        var system = message.Attributes ?? [];
        var attributes = (message.MessageAttributes ?? [])
            .ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);
        var body = message.Body ?? string.Empty;
        string? nativeSubject = null;
        // Read through an SNS subscription without raw message delivery, the SQS body is SNS's JSON envelope. A copy
        // goes back to the topic, so it must carry the published body and attributes, not the envelope.
        if (snsEnvelope && attributes.Count == 0 &&
            TryReadSnsEnvelope(body, out var published, out var envelopeAttributes, out var envelopeSubject))
        {
            body = published;
            attributes = envelopeAttributes;
            nativeSubject = envelopeSubject;
        }
        var standard = attributes
            .Where(item => IsStringType(item.Value.DataType) && item.Value.StringValue is not null)
            .ToDictionary(item => item.Key, item => item.Value.StringValue, StringComparer.Ordinal);
        var properties = MessageAttributeConventions.ReadStandardAttributes(
            standard,
            message.MessageId,
            sessionId: system.GetValueOrDefault("MessageGroupId")) with { NativeSubject = nativeSubject };
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
            Encoding.UTF8.GetBytes(body),
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

    /// <summary>
    /// SNS's notification envelope: {"Type":"Notification","MessageId","TopicArn","Message","MessageAttributes":
    /// {"name":{"Type":"String|Number|Binary|String.Array","Value":"..."}},...}. Binary values are base64.
    /// </summary>
    internal static bool TryReadSnsEnvelope(string body, out string message, out Dictionary<string, MessageAttributeValue> attributes,
        out string? subject)
    {
        subject = null;
        message = string.Empty;
        attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        if (!body.AsSpan().TrimStart().StartsWith("{", StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("Type", out var type) || type.ValueKind != JsonValueKind.String || type.GetString() != "Notification" ||
                !root.TryGetProperty("TopicArn", out var topicArn) || topicArn.ValueKind != JsonValueKind.String ||
                !root.TryGetProperty("MessageId", out var messageId) || messageId.ValueKind != JsonValueKind.String ||
                !root.TryGetProperty("Message", out var published) || published.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            if (root.TryGetProperty("MessageAttributes", out var items) && items.ValueKind == JsonValueKind.Object)
            {
                foreach (var item in items.EnumerateObject())
                {
                    if (item.Value.ValueKind != JsonValueKind.Object ||
                        !item.Value.TryGetProperty("Type", out var dataType) || dataType.ValueKind != JsonValueKind.String ||
                        !item.Value.TryGetProperty("Value", out var value) || value.ValueKind != JsonValueKind.String)
                    {
                        attributes.Clear();
                        return false;
                    }
                    var typeName = dataType.GetString()!;
                    attributes[item.Name] = typeName.StartsWith("Binary", StringComparison.Ordinal)
                        ? new MessageAttributeValue { DataType = typeName, BinaryValue = new MemoryStream(Convert.FromBase64String(value.GetString()!)) }
                        : new MessageAttributeValue { DataType = typeName, StringValue = value.GetString() };
                }
            }

            // The publisher's Publish Subject (the e-mail subject line) is part of the envelope, not an attribute. It is
            // kept apart and published again as the native Subject only, so the copy has exactly the original attributes.
            if (root.TryGetProperty("Subject", out var subjectElement) && subjectElement.ValueKind == JsonValueKind.String &&
                subjectElement.GetString() is { Length: > 0 } subjectText)
            {
                subject = subjectText;
            }

            message = published.GetString()!;
            return true;
        }
        catch (Exception exception) when (exception is JsonException or FormatException)
        {
            attributes.Clear();
            return false;
        }
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

    /// <summary>SQS: "Each message can have up to 10 attributes" (SNS raw delivery to SQS has the same limit).</summary>
    public const int MaximumSqsAttributes = 10;

    /// <summary>
    /// Refuses, before the SDK call, attributes SQS (<paramref name="sqs"/>) or SNS would refuse: names outside the
    /// allowed characters, SQS names starting with "AWS." or "Amazon.", and more than 10 attributes on an SQS message,
    /// counting the correlation ID, subject, content type, reply-to and to that travel as attributes. SNS reserves
    /// "AWS." names for its own mobile push attributes, so those stay allowed there.
    /// </summary>
    public static void EnsureAttributesAccepted(IReadOnlyCollection<string> names, bool sqs)
    {
        var service = sqs ? "Amazon SQS" : "Amazon SNS";
        if (names.FirstOrDefault(name => !QueueLoom.Core.Validation.MessageDraftValidator.IsAwsAttributeName(name)) is { } invalid)
        {
            throw new InvalidOperationException(
                $"{service} does not accept the attribute name '{invalid}': use up to 256 letters, digits, '_', '-' and '.', " +
                "not starting or ending with '.' and without '..'. Nothing was sent.");
        }
        if (!sqs)
        {
            return;
        }
        if (names.FirstOrDefault(name => name.StartsWith("AWS.", StringComparison.OrdinalIgnoreCase) ||
                                         name.StartsWith("Amazon.", StringComparison.OrdinalIgnoreCase)) is { } reserved)
        {
            throw new InvalidOperationException(
                $"Amazon SQS reserves attribute names starting with 'AWS.' or 'Amazon.'; rename '{reserved}'. Nothing was sent.");
        }
        if (names.Count > MaximumSqsAttributes)
        {
            throw new InvalidOperationException(
                $"Amazon SQS accepts at most {MaximumSqsAttributes} message attributes; this message has {names.Count} " +
                "(application properties plus correlation ID, subject, content type, reply-to and to, which travel as attributes). " +
                "Remove some before sending. Nothing was sent.");
        }
    }

    /// <summary>
    /// FIFO MessageGroupId and MessageDeduplicationId: at most 128 characters of letters, digits and ASCII punctuation.
    /// </summary>
    public static void EnsureFifoIdentifiers(string groupId, string deduplicationId)
    {
        foreach (var (value, name, property) in new[]
                 {
                     (groupId, "MessageGroupId", "session ID, else partition key"),
                     (deduplicationId, "MessageDeduplicationId", "MessageId")
                 })
        {
            // Printable ASCII without the space: letters, digits and !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~.
            if (value.Length > 128 || value.Any(character => character is <= ' ' or >= '\x7f'))
            {
                throw new InvalidOperationException(
                    $"Amazon SQS and SNS FIFO {name} ({property}) must be at most 128 letters, digits and ASCII punctuation, " +
                    $"without spaces; '{(value.Length > 40 ? TextLimits.Head(value, 40) + "…" : value)}' is not. Nothing was sent.");
            }
        }
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
    internal static (string DataType, string? StringValue, byte[]? BinaryValue) ToAttribute(MessageApplicationProperty property)
    {
        // The DataType choice lives in Core, so the size check before sending counts exactly what is sent.
        var dataType = MessageAttributeConventions.AwsDataType(property);
        return dataType == "Binary" || dataType.StartsWith("Binary.", StringComparison.Ordinal)
            ? (dataType, null, Convert.FromBase64String(property.Value))
            : (dataType, property.Value, null);
    }

    /// <summary>
    /// SNS Publish Subject: "UTF-8 text with no line breaks or control characters, and less than 100 characters long".
    /// The native subject read from an envelope comes first; otherwise the draft's Subject, which also travels as the
    /// Subject attribute. A subject SNS would refuse is not set.
    /// </summary>
    public static string? SnsSubject(MessageDraft message) =>
        AcceptedSubject(message.Properties.NativeSubject) ?? AcceptedSubject(message.Properties.Subject);

    private static string? AcceptedSubject(string? subject) =>
        subject is { Length: > 0 and < 100 } &&
        !string.IsNullOrWhiteSpace(subject) && !subject.Any(character => char.IsControl(character) || (int)character is 0x2028 or 0x2029)
            ? subject
            : null;

    internal static MessageApplicationProperty ToProperty(string name, string? dataType, string? stringValue, MemoryStream? binaryValue)
    {
        var parts = (dataType ?? "String").Split('.', 2);
        var label = parts.Length > 1 ? parts[1] : null;
        if (parts[0] == "Binary")
        {
            return new MessageApplicationProperty(name, ApplicationPropertyType.Binary,
                Convert.ToBase64String(binaryValue?.ToArray() ?? []))
                { WireType = label is { Length: > 0 } ? dataType : null };
        }

        var value = stringValue ?? string.Empty;
        // Only a type name counts; Enum.TryParse would also read another producer's "Number.1" as a QueueLoom type.
        if (label is { Length: > 0 } && char.IsAsciiLetter(label[0]) &&
            Enum.TryParse<ApplicationPropertyType>(label, ignoreCase: false, out var labelled) &&
            Enum.IsDefined(labelled) && labelled != ApplicationPropertyType.Binary)
        {
            var typed = new MessageApplicationProperty(name, labelled, value);
            // A familiar suffix alone is not our convention: String.Int32 is still the producer's String,
            // and Number.String is still a Number. Recognize only the base/label pairs we actually write.
            if (MessageAttributeConventions.AwsDataType(typed) == dataType)
            {
                return typed;
            }
        }

        // A label QueueLoom has no type for is remembered, so a resent copy carries the same type again.
        var wireType = label is { Length: > 0 } ? dataType : null;
        if (parts[0] == "Number")
        {
            var type = long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)
                ? ApplicationPropertyType.Int64
                : decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _)
                    ? ApplicationPropertyType.Decimal
                    : ApplicationPropertyType.String;
            return new MessageApplicationProperty(name, type, value) { WireType = wireType };
        }

        return new MessageApplicationProperty(name, ApplicationPropertyType.String, value) { WireType = wireType };
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
            ? BrokerClock.FromUnixMilliseconds(value)
            : null;
}
