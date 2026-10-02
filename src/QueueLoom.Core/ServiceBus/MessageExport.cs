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
        "amqpType", "amqpAppId", "partition", "offset", "bodyEncoding", "body"
    ];

    public static MessageExportFormat FormatFor(string path) =>
        string.Equals(Path.GetExtension(path), ".csv", StringComparison.OrdinalIgnoreCase)
            ? MessageExportFormat.Csv
            : MessageExportFormat.Json;

    public static async Task WriteAsync(
        string path,
        IReadOnlyList<ExportedMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(messages);
        cancellationToken.ThrowIfCancellationRequested();
        var destination = Path.GetFullPath(path);
        var temporary = Path.Combine(Path.GetDirectoryName(destination)!, $".{Path.GetFileName(destination)}.{Guid.NewGuid():N}.tmp");
        var ownsTemporary = false;
        try
        {
            await using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, useAsync: true))
            {
                ownsTemporary = true;
                if (FormatFor(path) == MessageExportFormat.Csv)
                {
                    await using var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true), leaveOpen: true);
                    await WriteCsvAsync(writer, messages, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await WriteJsonAsync(stream, messages, cancellationToken).ConfigureAwait(false);
                }
                stream.Flush(flushToDisk: true);
            }
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporary, destination, overwrite: true);
        }
        finally
        {
            if (ownsTemporary && File.Exists(temporary)) File.Delete(temporary);
        }
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
            writer.WriteString("bodyEncoding", encoding);
            writer.WriteString("body", body);
            if (message.IsBodyTruncated)
            {
                writer.WriteBoolean("bodyTruncated", true);
            }
            writer.WriteEndObject();
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
                message.Position?.Partition.ToString(CultureInfo.InvariantCulture),
                message.Position?.Offset.ToString(CultureInfo.InvariantCulture),
                encoding,
                body
            ];
            await writer.WriteLineAsync(string.Join(",", row.Select(Csv))).ConfigureAwait(false);
        }
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>RFC 4180 quoting; a leading =, +, - or @ is prefixed so spreadsheets do not run it as a formula.</summary>
    internal static string Csv(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (value[0] is '=' or '+' or '-' or '@')
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

    private static void WriteOptional(Utf8JsonWriter writer, string name, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            writer.WriteString(name, value);
        }
    }
}
