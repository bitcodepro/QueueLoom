
namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// SQS, SNS and Pub/Sub messages only have a body and free-form attributes. Service Bus style properties
/// (correlation ID, subject, content type, reply-to, to) travel as attributes with these names, so a message
/// sent from QueueLoom shows the same properties when it is read back.
/// </summary>
public static class MessageAttributeConventions
{
    public const string CorrelationId = "CorrelationId";
    public const string Subject = "Subject";
    public const string ContentType = "ContentType";
    public const string ReplyTo = "ReplyTo";
    public const string To = "To";

    public static IEnumerable<KeyValuePair<string, string>> StandardAttributes(EditableMessageProperties properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (!string.IsNullOrEmpty(properties.CorrelationId)) yield return new(CorrelationId, properties.CorrelationId);
        if (!string.IsNullOrEmpty(properties.Subject)) yield return new(Subject, properties.Subject);
        if (!string.IsNullOrEmpty(properties.ContentType)) yield return new(ContentType, properties.ContentType);
        if (!string.IsNullOrEmpty(properties.ReplyTo)) yield return new(ReplyTo, properties.ReplyTo);
        if (!string.IsNullOrEmpty(properties.To)) yield return new(To, properties.To);
    }

    /// <summary>
    /// The SQS/SNS DataType an application property is sent with. SQS attribute types are String, Number and Binary,
    /// each with an optional custom label; QueueLoom writes its type as the label ("Number.Int32", "String.Guid") so the
    /// type survives a round trip. A label QueueLoom has no type for (SNS String.Array, another producer's custom
    /// label) goes back unchanged, as long as the property still has the type it was read with (a type changed in the
    /// editor wins).
    /// </summary>
    public static string AwsDataType(MessageApplicationProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);
        if (property.WireType is { } wire &&
            (wire.StartsWith("String.", StringComparison.Ordinal) && property.Type == ApplicationPropertyType.String ||
             wire.StartsWith("Number.", StringComparison.Ordinal) &&
             property.Type is ApplicationPropertyType.Int64 or ApplicationPropertyType.Decimal or ApplicationPropertyType.String))
        {
            return wire;
        }
        return property.Type switch
        {
            ApplicationPropertyType.String => "String",
            ApplicationPropertyType.Binary => "Binary",
            ApplicationPropertyType.Byte or ApplicationPropertyType.SByte or ApplicationPropertyType.Int16 or
                ApplicationPropertyType.UInt16 or ApplicationPropertyType.Int32 or ApplicationPropertyType.UInt32 or
                ApplicationPropertyType.Int64 or ApplicationPropertyType.UInt64 or ApplicationPropertyType.Single or
                ApplicationPropertyType.Double or ApplicationPropertyType.Decimal => $"Number.{property.Type}",
            _ => $"String.{property.Type}"
        };
    }

    /// <summary>Takes the standard attributes out of <paramref name="attributes"/> and returns them as properties.</summary>
    public static EditableMessageProperties ReadStandardAttributes(
        IDictionary<string, string> attributes,
        string? messageId,
        string? sessionId = null,
        string? partitionKey = null)
    {
        ArgumentNullException.ThrowIfNull(attributes);
        return new EditableMessageProperties(
            MessageId: messageId,
            CorrelationId: Take(CorrelationId),
            ContentType: Take(ContentType),
            Subject: Take(Subject),
            To: Take(To),
            ReplyTo: Take(ReplyTo),
            SessionId: sessionId,
            PartitionKey: partitionKey);

        string? Take(string name) => attributes.Remove(name, out var value) ? value : null;
    }
}
