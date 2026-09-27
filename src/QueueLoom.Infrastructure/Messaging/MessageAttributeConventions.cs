using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Messaging;

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
