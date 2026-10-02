using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Infrastructure.Kafka;

internal static class KafkaMessageMapper
{
    internal const string MessageIdHeader = "MessageId";

    /// <summary>Headers written by Spring Kafka's dead-letter recoverer and by Kafka Connect's error handler.</summary>
    private static readonly string[] DeadLetterHeaderPrefixes = ["kafka_dlt-", "__connect.errors."];
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    public static BrowsedMessage FromKafka(ConsumeResult<byte[]?, byte[]?> result, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        var message = result.Message;
        var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var header in message.Headers ?? [])
        {
            headers[header.Key] = header.GetValueBytes() ?? [];
        }

        var texts = headers
            .Where(header => !DeadLetterHeaderPrefixes.Any(prefix => header.Key.StartsWith(prefix, StringComparison.Ordinal)))
            .ToDictionary(header => header.Key, header => Text(header.Value), StringComparer.Ordinal);
        texts.Remove(MessageIdHeader, out var messageId);
        var properties = MessageAttributeConventions.ReadStandardAttributes(texts, messageId, partitionKey: message.Key is { Length: > 0 } key ? Text(key) : null);

        var applicationProperties = texts
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => new MessageApplicationProperty(pair.Key, ApplicationPropertyType.String, pair.Value))
            .Concat(headers
                .Where(header => DeadLetterHeaderPrefixes.Any(prefix => header.Key.StartsWith(prefix, StringComparison.Ordinal)))
                .OrderBy(header => header.Key, StringComparer.Ordinal)
                .Select(header => new MessageApplicationProperty(header.Key, ApplicationPropertyType.String, DeadLetterHeaderText(header.Key, header.Value))))
            .ToArray();

        var (reason, description) = DeadLetterInfo(headers);
        return new BrowsedMessage(
            source,
            subQueue,
            LeasedMessageIdentity.SequenceNumberFor($"{result.Topic}:{result.Partition.Value}:{result.Offset.Value}"),
            message.Value ?? [],
            properties,
            applicationProperties,
            ServiceBusMessageState.Active,
            enqueuedAt: message.Timestamp.Type == TimestampType.NotAvailable ? null : new DateTimeOffset(message.Timestamp.UtcDateTime),
            deadLetterReason: reason,
            deadLetterErrorDescription: description)
        {
            HasSequenceNumber = false, Position = new LogPosition(result.Partition.Value, result.Offset.Value),
            KafkaEnvelope = new KafkaEnvelope(message.Key?.ToArray(), message.Value is null,
                (message.Headers ?? []).Select(header => new KafkaRawHeader(header.Key, header.GetValueBytes()?.ToArray())).ToArray(),
                properties, applicationProperties)
        };
    }

    /// <summary>The topic a dead-lettered message came from, when the dead-letter headers say so.</summary>
    public static string? OriginalTopic(Headers? headers)
    {
        foreach (var name in new[] { "kafka_dlt-original-topic", "__connect.errors.topic" })
        {
            if (headers is not null && headers.TryGetLastBytes(name, out var value))
            {
                return Text(value);
            }
        }
        return null;
    }

    public static Message<byte[]?, byte[]?> ToKafka(MessageDraft draft)
    {
        var headers = new Headers();
        if (!string.IsNullOrEmpty(draft.Properties.MessageId))
        {
            headers.Add(MessageIdHeader, Encoding.UTF8.GetBytes(draft.Properties.MessageId));
        }
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(draft.Properties))
        {
            headers.Add(name, Encoding.UTF8.GetBytes(value));
        }
        foreach (var property in draft.ApplicationProperties)
        {
            // Position is stored separately. Position-like header names belong to the sender.
            if (DeadLetterHeaderPrefixes.Any(prefix => property.Name.StartsWith(prefix, StringComparison.Ordinal)))
            {
                continue;
            }
            headers.Add(property.Name, property.Type == ApplicationPropertyType.Binary
                ? Convert.FromBase64String(property.Value)
                : Encoding.UTF8.GetBytes(property.Value));
        }

        var key = draft.Properties.PartitionKey ?? draft.Properties.SessionId;
        var result = new Message<byte[]?, byte[]?>
        {
            Key = string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key),
            Value = draft.Body.GetBytes(),
            Headers = headers
        };
        if (draft.KafkaEnvelope is not { } envelope) return result;

        // Preserve raw metadata for every name whose editable projection has not changed.
        // An explicit edit replaces that name's raw occurrences with the edited representation.
        var baseline = ToKafka(new MessageDraft(draft.Body, envelope.OriginalProperties, envelope.OriginalApplicationProperties));
        var names = baseline.Headers.Select(header => header.Key).Concat(headers.Select(header => header.Key)).Distinct(StringComparer.Ordinal);
        var changed = names.Where(name => !baseline.Headers.Where(header => header.Key == name).Select(header => Convert.ToBase64String(header.GetValueBytes() ?? []))
            .SequenceEqual(headers.Where(header => header.Key == name).Select(header => Convert.ToBase64String(header.GetValueBytes() ?? []))))
            .ToHashSet(StringComparer.Ordinal);
        var raw = new Headers();
        foreach (var header in envelope.Headers)
        {
            if (!changed.Contains(header.Name) && !DeadLetterHeaderPrefixes.Any(prefix => header.Name.StartsWith(prefix, StringComparison.Ordinal)))
                raw.Add(header.Name, header.Value?.ToArray());
        }
        foreach (var header in headers.Where(header => changed.Contains(header.Key))) raw.Add(header.Key, header.GetValueBytes());
        result.Headers = raw;
        if (draft.Properties.PartitionKey == envelope.OriginalProperties.PartitionKey && draft.Properties.SessionId == envelope.OriginalProperties.SessionId)
            result.Key = envelope.Key?.ToArray();
        if (envelope.IsTombstone && result.Value is { Length: 0 }) result.Value = null;
        return result;
    }

    private static (string? Reason, string? Description) DeadLetterInfo(IReadOnlyDictionary<string, byte[]> headers)
    {
        string? Header(params string[] names) => names.Select(name => headers.TryGetValue(name, out var value) ? DeadLetterHeaderText(name, value) : null)
            .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));

        var exceptionClass = Header("kafka_dlt-exception-fqcn", "__connect.errors.exception.class.name");
        var exceptionMessage = Header("kafka_dlt-exception-message", "__connect.errors.exception.message");
        var topic = Header("kafka_dlt-original-topic", "__connect.errors.topic");
        var partition = Header("kafka_dlt-original-partition", "__connect.errors.partition");
        var offset = Header("kafka_dlt-original-offset", "__connect.errors.offset");
        if (exceptionClass is null && exceptionMessage is null && topic is null)
        {
            return (null, null);
        }

        var reason = exceptionClass is null ? "Dead-lettered" : exceptionClass[(exceptionClass.LastIndexOf('.') + 1)..];
        var origin = topic is null ? null : $"from {topic}" + (partition is null ? string.Empty : $", partition {partition}") +
                                              (offset is null ? string.Empty : $", offset {offset}");
        var description = string.Join(" · ", new[] { exceptionMessage, origin }.Where(part => !string.IsNullOrWhiteSpace(part)));
        return (reason, description.Length == 0 ? null : description);
    }

    /// <summary>Spring writes the original partition and offset as big-endian numbers, everything else as text.</summary>
    private static string DeadLetterHeaderText(string name, byte[] value) =>
        name.EndsWith("original-partition", StringComparison.Ordinal) && value.Length == 4
            ? BinaryPrimitives.ReadInt32BigEndian(value).ToString(CultureInfo.InvariantCulture)
            : name.EndsWith("original-offset", StringComparison.Ordinal) && value.Length == 8
                ? BinaryPrimitives.ReadInt64BigEndian(value).ToString(CultureInfo.InvariantCulture)
                : Text(value);

    private static string Text(byte[] bytes)
    {
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return Convert.ToBase64String(bytes);
        }
    }
}
