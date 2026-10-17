using System.Globalization;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

/// <summary>A message read from an export file, as a draft to send, and where it was read from.</summary>
public sealed record ImportedMessage(MessageDraft Draft, string Origin);

/// <summary>
/// Reads a JSON export written by <see cref="MessageExport"/> back into drafts, so the messages can be sent again,
/// to another queue or another environment. Only the JSON export is read: the CSV export leaves out property types,
/// the Kafka record and repeated property names. The limits are those of a replay.
/// </summary>
public static class MessageImport
{
    public const int MaximumMessages = 1_000;
    public const long MaximumBodyBytes = 32L * 1024 * 1024;

    /// <summary>The file itself: base64 bodies and the JSON around them take more room than the bodies.</summary>
    public const long MaximumFileBytes = 64L * 1024 * 1024;

    public static async Task<IReadOnlyList<ImportedMessage>> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (MessageExport.FormatFor(path) == MessageExportFormat.Csv)
        {
            throw new InvalidOperationException(
                "Choose a JSON export. A CSV export leaves out property types and Kafka keys and headers, so its messages cannot be sent again as they were.");
        }
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 81_920, useAsync: true);
        return await ReadAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<ImportedMessage>> ReadAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        // Read at most one byte past the limit: the length a file reports is not trusted (it can grow while read).
        using var buffer = new MemoryStream();
        var chunk = new byte[81_920];
        int read;
        while ((read = await stream.ReadAsync(chunk, cancellationToken).ConfigureAwait(false)) > 0)
        {
            buffer.Write(chunk, 0, read);
            if (buffer.Length > MaximumFileBytes)
            {
                throw new InvalidOperationException($"The file is larger than {MaximumFileBytes / (1024 * 1024)} MiB. Export fewer messages.");
            }
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(buffer.GetBuffer().AsMemory(0, (int)buffer.Length));
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException($"The file is not a QueueLoom JSON export: {exception.Message}", exception);
        }
        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException("The file is not a QueueLoom JSON export: it should be a list of messages.");
            }
            var count = document.RootElement.GetArrayLength();
            if (count == 0)
            {
                throw new InvalidOperationException("The export holds no messages.");
            }
            if (count > MaximumMessages)
            {
                throw new InvalidOperationException(
                    $"The export holds {count:N0} messages; at most {MaximumMessages:N0} are sent at a time. Export fewer messages.");
            }
            var messages = new List<ImportedMessage>(count);
            long bodyBytes = 0;
            var index = 0;
            foreach (var element in document.RootElement.EnumerateArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                index++;
                try
                {
                    var message = Read(element);
                    bodyBytes += message.Draft.Body.GetBytes().LongLength;
                    if (bodyBytes > MaximumBodyBytes)
                    {
                        throw new InvalidOperationException(
                            $"The bodies add up to more than {MaximumBodyBytes / (1024 * 1024)} MiB; at most that is sent at a time. Export fewer messages.");
                    }
                    messages.Add(message);
                }
                catch (Exception exception) when (exception is FormatException or InvalidOperationException or KeyNotFoundException
                                                      or OverflowException or ArgumentException)
                {
                    throw new InvalidOperationException($"Message {index:N0} of the export cannot be read: {exception.Message}", exception);
                }
            }
            return messages;
        }
    }

    private static ImportedMessage Read(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException("it is not an object.");
        }
        var version = element.TryGetProperty("exportVersion", out var versionValue) && versionValue.TryGetInt32(out var number) ? number : 1;
        if (version > MessageExport.ExportVersion)
        {
            throw new InvalidOperationException("it was written by a newer QueueLoom. Update QueueLoom to read it.");
        }
        if (element.TryGetProperty("bodyTruncated", out var truncated) && truncated.ValueKind == JsonValueKind.True)
        {
            throw new InvalidOperationException("its body was cut when it was exported, so it cannot be sent as it was.");
        }
        var encoding = Text(element, "bodyEncoding") ?? "text";
        var bodyText = Text(element, "body") ?? throw new FormatException("it has no body.");
        var body = encoding switch
        {
            "text" => Encoding.UTF8.GetBytes(bodyText),
            "base64" => Convert.FromBase64String(bodyText),
            _ => throw new FormatException($"its body encoding '{encoding}' is unknown.")
        };

        var properties = new EditableMessageProperties(
            MessageId: Text(element, "messageId"),
            CorrelationId: Text(element, "correlationId"),
            ContentType: Text(element, "contentType"),
            Subject: Text(element, "subject"),
            To: Text(element, "to"),
            ReplyTo: Text(element, "replyTo"),
            SessionId: Text(element, "sessionId"),
            ReplyToSessionId: Text(element, "replyToSessionId"),
            PartitionKey: Text(element, "partitionKey"),
            TransactionPartitionKey: Text(element, "transactionPartitionKey"),
            TimeToLive: Text(element, "timeToLive") is { } timeToLive
                ? TimeSpan.ParseExact(timeToLive, "c", CultureInfo.InvariantCulture)
                : null,
            AmqpType: Text(element, "amqpType"),
            AmqpAppId: Text(element, "amqpAppId"),
            AmqpContentEncoding: Text(element, "amqpContentEncoding"),
            AmqpPriority: element.TryGetProperty("amqpPriority", out var priority) ? priority.GetByte() : null)
        {
            NativeSubject = Text(element, "nativeSubject")
        };

        var applicationProperties = new List<MessageApplicationProperty>();
        if (element.TryGetProperty("typedApplicationProperties", out var typed) && typed.ValueKind == JsonValueKind.Array)
        {
            foreach (var property in typed.EnumerateArray())
            {
                var type = Enum.Parse<ApplicationPropertyType>(Text(property, "type") ?? throw new FormatException("a property has no type."));
                applicationProperties.Add(new MessageApplicationProperty(
                    Text(property, "name") ?? throw new FormatException("a property has no name."), type,
                    Text(property, "value") ?? string.Empty) { WireType = Text(property, "wireType") });
            }
        }
        else if (element.TryGetProperty("applicationProperties", out var plain) && plain.ValueKind == JsonValueKind.Object)
        {
            // Version 1 exports kept one text value per name.
            foreach (var property in plain.EnumerateObject())
            {
                applicationProperties.Add(new MessageApplicationProperty(property.Name, ApplicationPropertyType.String,
                    property.Value.ValueKind == JsonValueKind.String ? property.Value.GetString()! : property.Value.GetRawText()));
            }
        }

        KafkaEnvelope? kafka = null;
        if (element.TryGetProperty("kafka", out var record) && record.ValueKind == JsonValueKind.Object)
        {
            var headers = new List<KafkaRawHeader>();
            if (record.TryGetProperty("headers", out var headerList) && headerList.ValueKind == JsonValueKind.Array)
            {
                foreach (var header in headerList.EnumerateArray())
                {
                    headers.Add(new KafkaRawHeader(Text(header, "name") ?? throw new FormatException("a Kafka header has no name."),
                        Text(header, "valueBase64") is { } value ? Convert.FromBase64String(value) : null));
                }
            }
            kafka = new KafkaEnvelope(
                Text(record, "keyBase64") is { } key ? Convert.FromBase64String(key) : null,
                record.TryGetProperty("tombstone", out var tombstone) && tombstone.ValueKind == JsonValueKind.True,
                headers, properties, applicationProperties);
        }

        var origin = $"{Text(element, "environment") ?? "export"} / {Text(element, "source") ?? "?"} / {properties.MessageId ?? "no Message ID"}";
        return new ImportedMessage(
            new MessageDraft(EditableMessageBody.FromBytes(body), properties, applicationProperties) { KafkaEnvelope = kafka },
            origin);
    }

    private static string? Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
}
