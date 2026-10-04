using System.Globalization;
using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Validation;

/// <summary>
/// The total message sizes the services accept, counted the way each service counts them, so an oversized message is
/// refused before any network call instead of failing at the service (or being retried by a schedule).
/// </summary>
public static class MessageSizeLimits
{
    /// <summary>
    /// Amazon SQS SendMessage: "The maximum size is 1 MiB or 1,048,576 bytes", and "All components of a message
    /// attribute are included in the 1 MiB message size restriction". It is also the highest MaximumMessageSize an SNS
    /// topic can be configured with ("Valid values are 1024 to 1048576").
    /// </summary>
    public const int AmazonMaximumBytes = 1_048_576;

    /// <summary>
    /// Amazon SNS: a topic's MaximumMessageSize defaults to 262,144 bytes (256 KiB); "All parts of the message
    /// attribute, including name, type, and value, are included in the message size restriction".
    /// </summary>
    public const int AmazonSnsDefaultMaximumBytes = 262_144;

    /// <summary>
    /// Google Pub/Sub: a serialized PublishRequest is at most 10,000,000 bytes (decimal 10 MB), counting the topic, the
    /// protobuf framing, the message data, its attributes and its ordering key (Google's clients enforce this ceiling).
    /// </summary>
    public const int PubSubMaximumRequestBytes = 10_000_000;

    /// <summary>
    /// Azure Service Bus: Premium accepts up to 100 MB per message over AMQP (102,400 KB, the largest
    /// MaxMessageSizeInKilobytes); Basic and Standard accept 256 KB. "The message size includes the size of properties
    /// (system and user) and the size of payload." The tier is not known before sending, so only the Premium ceiling is
    /// certain; the service's own batch check applies the namespace's real limit when sending.
    /// </summary>
    public const int AzureMaximumBytes = 104_857_600;

    /// <summary>What SQS and SNS count: the body text plus each attribute's name, data type and value.</summary>
    public static long AwsSize(MessageDraft draft)
    {
        ArgumentNullException.ThrowIfNull(draft);
        // The same map the send builds: standard attributes, then application properties replacing one of the same name.
        var attributes = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(draft.Properties))
        {
            attributes[name] = Utf8(name) + Utf8("String") + Utf8(value);
        }
        foreach (var property in draft.ApplicationProperties.Where(property => property?.Name is not null))
        {
            var dataType = MessageAttributeConventions.AwsDataType(property);
            attributes[property.Name] = Utf8(property.Name) + Utf8(dataType) +
                                        (dataType == "Binary" ? Base64Length(property.Value) : Utf8(property.Value));
        }
        // SQS and SNS bodies are text: a binary body is sent in its base64 form.
        return Utf8(draft.Body.Content) + attributes.Values.Sum();
    }

    /// <summary>
    /// The serialized size of the PublishRequest's message entry: the PubsubMessage protobuf (data, attribute map entries
    /// and ordering key, each with its tag and length prefix) plus its own framing in the request. The topic field is not
    /// known here; the send adds it and checks the whole request.
    /// </summary>
    public static long PubSubSize(MessageDraft draft)
    {
        ArgumentNullException.ThrowIfNull(draft);
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(draft.Properties))
        {
            attributes[name] = value;
        }
        foreach (var property in draft.ApplicationProperties.Where(property => property?.Name is not null))
        {
            attributes[property.Name] = property.Value ?? string.Empty;
        }
        var orderingKey = new[] { draft.Properties.SessionId, draft.Properties.PartitionKey }
            .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));
        var message = LengthDelimited(BodyBytes(draft.Body)) +
                      attributes.Sum(pair => LengthDelimited(LengthDelimited(Utf8(pair.Key)) + LengthDelimited(Utf8(pair.Value)))) +
                      (orderingKey is null ? 0 : LengthDelimited(Utf8(orderingKey)));
        return LengthDelimited(message);
    }

    /// <summary>A protobuf length-delimited field: one tag byte, the varint length and the bytes (empty fields included).</summary>
    private static long LengthDelimited(long length) => 1 + VarintSize(length) + length;

    private static int VarintSize(long value)
    {
        var size = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            size++;
        }
        return size;
    }

    /// <summary>The payload plus the text of the system and application properties Service Bus carries in the header.</summary>
    public static long AzureSize(MessageDraft draft)
    {
        ArgumentNullException.ThrowIfNull(draft);
        var properties = draft.Properties;
        return BodyBytes(draft.Body) +
               new[]
               {
                   properties.MessageId, properties.CorrelationId, properties.ContentType, properties.Subject, properties.To,
                   properties.ReplyTo, properties.SessionId, properties.ReplyToSessionId, properties.PartitionKey,
                   properties.TransactionPartitionKey
               }.Sum(Utf8) +
               draft.ApplicationProperties.Where(property => property is not null)
                   .Sum(property => Utf8(property.Name) + Utf8(property.Value));
    }

    /// <summary>The size error for <paramref name="draft"/> sent to <paramref name="provider"/>, or null when it fits.</summary>
    public static string? Check(MessageDraft draft, MessagingProvider provider)
    {
        ArgumentNullException.ThrowIfNull(draft);
        return provider switch
        {
            MessagingProvider.AmazonSqsSns when AwsSize(draft) is var size && size > AmazonMaximumBytes =>
                $"Amazon SQS and SNS accept at most 1 MiB ({Bytes(AmazonMaximumBytes)}) per message, counting the body and every " +
                $"attribute's name, type and value; this message is {Bytes(size)}.",
            MessagingProvider.GooglePubSub when PubSubSize(draft) is var size && size > PubSubMaximumRequestBytes =>
                $"Google Pub/Sub accepts at most 10 MB ({Bytes(PubSubMaximumRequestBytes)}) per publish request, counting the " +
                $"data, attributes, ordering key and their framing; this message needs {Bytes(size)}.",
            MessagingProvider.AzureServiceBus when AzureSize(draft) is var size && size > AzureMaximumBytes =>
                $"Azure Service Bus accepts at most 100 MB ({Bytes(AzureMaximumBytes)}) per message even on the Premium tier " +
                $"(Basic and Standard accept 256 KB), counting the body and properties; this message is {Bytes(size)}.",
            _ => null
        };
    }

    /// <summary>
    /// Refuses <paramref name="size"/> over <paramref name="limit"/> for <paramref name="destination"/> (an SQS queue's or
    /// SNS topic's configured MaximumMessageSize) before the SDK call. Nothing was sent, which is proven, so it throws
    /// <see cref="DeliveryRejectedException"/>: a durable resend records a rejection, not an uncertain delivery.
    /// </summary>
    public static void EnsureWithin(long size, long limit, string destination)
    {
        if (size > limit)
        {
            throw new DeliveryRejectedException(
                $"{destination} accepts messages up to {Bytes(limit)} (its MaximumMessageSize), counting the body and every " +
                $"attribute's name, type and value; this message is {Bytes(size)}. Nothing was sent.");
        }
    }

    public static string Bytes(long bytes) => bytes.ToString("N0", CultureInfo.InvariantCulture) + " bytes";

    private static long Utf8(string? value) => value is null ? 0 : Encoding.UTF8.GetByteCount(value);

    private static long BodyBytes(EditableMessageBody body) => body.Format == MessageBodyFormat.Base64
        ? Base64Length(body.Content)
        : Utf8(body.Content);

    /// <summary>The decoded length of valid base64 text (whitespace and padding are not data).</summary>
    private static long Base64Length(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return 0;
        }
        long characters = 0;
        long padding = 0;
        foreach (var character in value)
        {
            if (char.IsWhiteSpace(character)) continue;
            if (character == '=') padding++;
            characters++;
        }
        return characters / 4 * 3 - Math.Min(padding, 2);
    }
}
