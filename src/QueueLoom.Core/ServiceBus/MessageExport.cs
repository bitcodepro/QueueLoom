using System.Globalization;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

public enum MessageExportFormat
{
    Json,
    Csv
}

/// <summary>A message to export and the environment it was read from.</summary>
public sealed record ExportedMessage(string Environment, BrowsedMessage Message);

/// <summary>
/// Writes messages to JSON (complete, one object per message) or CSV (one row per message, for spreadsheets).
/// Text bodies are written as text; other bodies as base64.
/// </summary>
public static class MessageExport
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static readonly string[] CsvColumns =
    [
        "environment", "source", "subQueue", "messageId", "correlationId", "subject", "contentType", "sessionId",
        "enqueuedAtUtc", "deliveryCount", "deadLetterReason", "deadLetterDescription", "applicationProperties",
        "amqpType", "amqpAppId", "bodySize", "bodyTruncated", "partition", "offset", "bodyEncoding", "body"
    ];

    public static MessageExportFormat FormatFor(string path) =>
        string.Equals(Path.GetExtension(path), ".csv", StringComparison.OrdinalIgnoreCase)
            ? MessageExportFormat.Csv
            : MessageExportFormat.Json;

    /// <summary>
    /// Writes the export through <see cref="QueueLoom.Core.IO.SafeFileWriter"/>: the message bodies go into a file only
    /// the current user can open, and an existing export is replaced in one step keeping its access (narrowed, never
    /// widened, when a rename cannot keep its group). A new export is private.
    /// </summary>
    /// <returns>True when an existing file's group access had to be narrowed.</returns>
    public static Task<bool> WriteAsync(
        string path,
        IReadOnlyList<ExportedMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(messages);
        cancellationToken.ThrowIfCancellationRequested();
        var csv = FormatFor(path) == MessageExportFormat.Csv;
        return QueueLoom.Core.IO.SafeFileWriter.WriteAsync(Path.GetFullPath(path), async (stream, token) =>
        {
            if (csv)
            {
                await using var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true), leaveOpen: true);
                await WriteCsvAsync(writer, messages, token).ConfigureAwait(false);
            }
            else
            {
                await WriteJsonAsync(stream, messages, token).ConfigureAwait(false);
            }
        }, cancellationToken);
    }

    public static async Task WriteJsonAsync(Stream stream, IReadOnlyList<ExportedMessage> messages, CancellationToken cancellationToken)
    {
        await using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });
        writer.WriteStartArray();
        foreach (var (environment, message) in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var properties = message.Properties;
            var (encoding, body) = Body(message);
            writer.WriteStartObject();
            // 2: typed application properties, every standard property and the Kafka record are included.
            writer.WriteNumber("exportVersion", ExportVersion);
            writer.WriteString("environment", environment);
            writer.WriteString("source", message.Source.DisplayName);
            writer.WriteString("sourceKind", message.Source.Kind.ToString());
            writer.WriteString("subQueue", message.SubQueue.ToString());
            if (message.HasSequenceNumber)
            {
                writer.WriteNumber("sequenceNumber", message.SequenceNumber);
            }
            if (message.Position is { } position)
            {
                writer.WriteNumber("partition", position.Partition);
                writer.WriteNumber("offset", position.Offset);
            }
            WriteOptional(writer, "messageId", properties.MessageId);
            WriteOptional(writer, "correlationId", properties.CorrelationId);
            WriteOptional(writer, "subject", properties.Subject);
            WriteOptional(writer, "contentType", properties.ContentType);
            WriteOptional(writer, "sessionId", properties.SessionId);
            WriteOptional(writer, "to", properties.To);
            WriteOptional(writer, "replyTo", properties.ReplyTo);
            WriteOptional(writer, "amqpType", properties.AmqpType);
            WriteOptional(writer, "amqpAppId", properties.AmqpAppId);
            WriteOptional(writer, "amqpContentEncoding", properties.AmqpContentEncoding);
            if (properties.AmqpPriority is { } priority)
            {
                writer.WriteNumber("amqpPriority", priority);
            }
            WriteOptional(writer, "replyToSessionId", properties.ReplyToSessionId);
            WriteOptional(writer, "partitionKey", properties.PartitionKey);
            WriteOptional(writer, "transactionPartitionKey", properties.TransactionPartitionKey);
            WriteOptional(writer, "nativeSubject", properties.NativeSubject);
            if (properties.TimeToLive is { } timeToLive)
            {
                writer.WriteString("timeToLive", timeToLive.ToString("c", CultureInfo.InvariantCulture));
            }
            if (properties.ScheduledEnqueueTime is { } scheduled)
            {
                writer.WriteString("scheduledEnqueueTimeUtc", scheduled.ToUniversalTime());
            }
            if (message.EnqueuedAt is { } enqueuedAt)
            {
                writer.WriteString("enqueuedAtUtc", enqueuedAt.ToUniversalTime());
            }
            writer.WriteNumber("deliveryCount", message.DeliveryCount);
            WriteOptional(writer, "deadLetterReason", message.DeadLetterReason);
            WriteOptional(writer, "deadLetterDescription", message.DeadLetterErrorDescription);
            writer.WriteStartObject("applicationProperties");
            foreach (var property in message.ApplicationProperties)
            {
                writer.WriteString(property.Name, property.Value);
            }
            writer.WriteEndObject();
            // The object above keeps one text value per name (as before); this list keeps each property's type, the
            // service's own type label and repeated names, so Int64 "42" and String "42" stay different.
            writer.WriteStartArray("typedApplicationProperties");
            foreach (var property in message.ApplicationProperties)
            {
                writer.WriteStartObject();
                writer.WriteString("name", property.Name);
                writer.WriteString("type", property.Type.ToString());
                if (property.WireType is not null)
                {
                    writer.WriteString("wireType", property.WireType);
                }
                writer.WriteString("value", property.Value);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            if (message.KafkaEnvelope is { } kafka)
            {
                // The record as Kafka holds it: a binary key, a tombstone, and headers (repeated, binary or null).
                writer.WriteStartObject("kafka");
                if (kafka.Key is { } key)
                {
                    writer.WriteString("keyBase64", Convert.ToBase64String(key));
                }
                else
                {
                    writer.WriteNull("keyBase64");
                }
                writer.WriteBoolean("tombstone", kafka.IsTombstone);
                writer.WriteStartArray("headers");
                foreach (var header in kafka.Headers)
                {
                    writer.WriteStartObject();
                    writer.WriteString("name", header.Name);
                    if (header.Value is { } value)
                    {
                        writer.WriteString("valueBase64", Convert.ToBase64String(value));
                    }
                    else
                    {
                        writer.WriteNull("valueBase64");
                    }
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            writer.WriteString("bodyEncoding", encoding);
            writer.WriteString("body", body);
            if (message.IsBodyTruncated)
            {
                writer.WriteBoolean("bodyTruncated", true);
            }
            writer.WriteEndObject();
            // Utf8JsonWriter buffers everything until it is flushed. Hand each message to the stream so a large
            // export never needs one buffer the size of the whole file, which cannot grow past the largest array.
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task WriteCsvAsync(TextWriter writer, IReadOnlyList<ExportedMessage> messages, CancellationToken cancellationToken)
    {
        await writer.WriteLineAsync(string.Join(",", CsvColumns)).ConfigureAwait(false);
        foreach (var (environment, message) in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var properties = message.Properties;
            var (encoding, body) = Body(message);
            var applicationProperties = JsonSerializer.Serialize(
                message.ApplicationProperties.ToDictionary(property => property.Name, property => property.Value));
            string?[] row =
            [
                environment,
                message.Source.DisplayName,
                message.SubQueue.ToString(),
                properties.MessageId,
                properties.CorrelationId,
                properties.Subject,
                properties.ContentType,
                properties.SessionId,
                message.EnqueuedAt?.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
                message.DeliveryCount.ToString(CultureInfo.InvariantCulture),
                message.DeadLetterReason,
                message.DeadLetterErrorDescription,
                applicationProperties,
                properties.AmqpType,
                properties.AmqpAppId,
                message.BodySize.ToString(CultureInfo.InvariantCulture),
                message.IsBodyTruncated ? "true" : "false",
                message.Position?.Partition.ToString(CultureInfo.InvariantCulture),
                message.Position?.Offset.ToString(CultureInfo.InvariantCulture),
                encoding,
                body
            ];
            await writer.WriteLineAsync(string.Join(",", row.Select(Csv))).ConfigureAwait(false);
        }
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// RFC 4180 quoting; a leading =, +, - or @ is prefixed so spreadsheets do not run it as a formula, and so is a
    /// leading tab or carriage return, which spreadsheets skip before reading the formula behind it.
    /// </summary>
    internal static string Csv(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (value[0] is '=' or '+' or '-' or '@' or '\t' or '\r')
        {
            value = "'" + value;
        }
        return value.IndexOfAny([',', '"', '\n', '\r']) >= 0 || value != value.Trim()
            ? $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\""
            : value;
    }

    private static (string Encoding, string Body) Body(BrowsedMessage message)
    {
        try
        {
            return ("text", StrictUtf8.GetString(message.Body.Span));
        }
        catch (DecoderFallbackException)
        {
            return ("base64", Convert.ToBase64String(message.Body.Span));
        }
    }

    /// <summary>The JSON export's format version, written on every message.</summary>
    public const int ExportVersion = 2;

    /// <summary>A missing (null) property is left out; an empty one is written, so the two stay apart.</summary>
    private static void WriteOptional(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is not null)
        {
            writer.WriteString(name, value);
        }
    }
}
