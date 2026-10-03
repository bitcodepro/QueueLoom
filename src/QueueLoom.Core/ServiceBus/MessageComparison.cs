using System.Globalization;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

public enum DiffKind
{
    Same,
    Removed,
    Added
}

/// <summary>One line of a body comparison: in both messages, only in the first, or only in the second.</summary>
public sealed record DiffLine(DiffKind Kind, string Text, int? LeftNumber, int? RightNumber);

/// <summary>A property with its value in each message; null where the message does not have it.</summary>
public sealed record PropertyDifference(string Name, string? Left, string? Right)
{
    public bool Differs => !string.Equals(Left, Right, StringComparison.Ordinal);
}

public sealed record MessageComparisonResult(IReadOnlyList<DiffLine> BodyLines, IReadOnlyList<PropertyDifference> Properties, bool BodyTruncated)
{
    public bool SourceBodyTruncated { get; init; }

    public bool BodyLineLimitReached { get; init; }

    public int ChangedLines => BodyLines.Count(line => line.Kind != DiffKind.Same);

    public int ChangedProperties => Properties.Count(property => property.Differs);

    public bool AreEqual => !BodyTruncated && ChangedLines == 0 && ChangedProperties == 0;
}

/// <summary>
/// Compares two messages: bodies line by line (JSON indented first, so formatting does not count) and every
/// broker and application property side by side.
/// </summary>
public static class MessageComparison
{
    public const int MaximumLines = 4_000;

    public static MessageComparisonResult Compare(BrowsedMessage left, BrowsedMessage right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var leftLines = Lines(left.Body);
        var rightLines = Lines(right.Body);
        var sourceTruncated = left.IsBodyTruncated || right.IsBodyTruncated;
        var lineLimitReached = leftLines.Length > MaximumLines || rightLines.Length > MaximumLines;
        var lines = Diff(leftLines.Take(MaximumLines).ToArray(), rightLines.Take(MaximumLines).ToArray());
        return new MessageComparisonResult(lines, CompareProperties(left, right), sourceTruncated || lineLimitReached)
        {
            SourceBodyTruncated = sourceTruncated,
            BodyLineLimitReached = lineLimitReached
        };
    }

    /// <summary>A longest-common-subsequence diff; lines only in the first come before lines only in the second.</summary>
    public static IReadOnlyList<DiffLine> Diff(IReadOnlyList<string> left, IReadOnlyList<string> right)
    {
        // Common start and end are cut first: messages of one kind usually differ in a few lines only.
        var start = 0;
        while (start < left.Count && start < right.Count && left[start] == right[start])
        {
            start++;
        }
        var end = 0;
        while (end < left.Count - start && end < right.Count - start && left[left.Count - 1 - end] == right[right.Count - 1 - end])
        {
            end++;
        }

        var result = new List<DiffLine>(left.Count + right.Count);
        for (var index = 0; index < start; index++)
        {
            result.Add(new DiffLine(DiffKind.Same, left[index], index + 1, index + 1));
        }

        var rows = left.Count - start - end;
        var columns = right.Count - start - end;
        if ((long)rows * columns > 4_000_000)
        {
            // Too different to align cheaply: show the first message's middle, then the second's.
            result.AddRange(Enumerable.Range(start, rows).Select(index => new DiffLine(DiffKind.Removed, left[index], index + 1, null)));
            result.AddRange(Enumerable.Range(start, columns).Select(index => new DiffLine(DiffKind.Added, right[index], null, index + 1)));
            rows = columns = 0;
        }
        var lengths = new int[rows + 1, columns + 1];
        for (var row = rows - 1; row >= 0; row--)
        {
            for (var column = columns - 1; column >= 0; column--)
            {
                lengths[row, column] = left[start + row] == right[start + column]
                    ? lengths[row + 1, column + 1] + 1
                    : Math.Max(lengths[row + 1, column], lengths[row, column + 1]);
            }
        }

        int i = 0, j = 0;
        while (i < rows || j < columns)
        {
            if (i < rows && j < columns && left[start + i] == right[start + j])
            {
                result.Add(new DiffLine(DiffKind.Same, left[start + i], start + i + 1, start + j + 1));
                i++;
                j++;
            }
            else if (j >= columns || i < rows && lengths[i + 1, j] >= lengths[i, j + 1])
            {
                result.Add(new DiffLine(DiffKind.Removed, left[start + i], start + i + 1, null));
                i++;
            }
            else
            {
                result.Add(new DiffLine(DiffKind.Added, right[start + j], null, start + j + 1));
                j++;
            }
        }

        for (var index = 0; index < end; index++)
        {
            result.Add(new DiffLine(DiffKind.Same, left[left.Count - end + index], left.Count - end + index + 1, right.Count - end + index + 1));
        }
        return result;
    }

    private static string[] Lines(ReadOnlyMemory<byte> body)
    {
        var text = EditableMessageBody.FromBytes(body.Span) is { Format: not MessageBodyFormat.Base64 } editable
            ? editable.Content
            : BodyDecoder.HexDump(body.Span);
        try
        {
            using var document = JsonDocument.Parse(text);
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions
                   {
                       Indented = true,
                       Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                   }))
            {
                document.WriteTo(writer);
            }
            text = Encoding.UTF8.GetString(stream.ToArray());
        }
        catch (JsonException)
        {
        }
        return text.ReplaceLineEndings("\n").Split('\n');
    }

    private static IReadOnlyList<PropertyDifference> CompareProperties(BrowsedMessage left, BrowsedMessage right)
    {
        var a = Properties(left);
        var b = Properties(right);
        return a.Keys.Union(b.Keys, StringComparer.Ordinal)
            .Select(name => new PropertyDifference(name, a.GetValueOrDefault(name), b.GetValueOrDefault(name)))
            .OrderByDescending(property => property.Differs)
            .ThenBy(property => property.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static Dictionary<string, string?> Properties(BrowsedMessage message)
    {
        var properties = message.Properties;
        var values = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Source"] = message.Source.DisplayName,
            ["Message ID"] = properties.MessageId,
            ["Correlation ID"] = properties.CorrelationId,
            ["Content type"] = properties.ContentType,
            ["Subject"] = properties.Subject,
            ["To"] = properties.To,
            ["Reply to"] = properties.ReplyTo,
            ["Session ID"] = properties.SessionId,
            ["Reply to session ID"] = properties.ReplyToSessionId,
            ["Partition key"] = properties.PartitionKey,
            ["Transaction partition key"] = properties.TransactionPartitionKey,
            ["Time to live"] = properties.TimeToLive?.ToString("c", CultureInfo.InvariantCulture),
            ["Scheduled enqueue"] = properties.ScheduledEnqueueTime?.ToString("O", CultureInfo.InvariantCulture),
            ["AMQP type"] = properties.AmqpType,
            ["AMQP app ID"] = properties.AmqpAppId,
            ["Enqueued"] = message.EnqueuedAt?.ToString("O", CultureInfo.InvariantCulture),
            ["Delivery count"] = message.DeliveryCount.ToString(CultureInfo.InvariantCulture),
            ["Dead-letter reason"] = message.DeadLetterReason,
            ["Dead-letter description"] = message.DeadLetterErrorDescription,
            ["Body size"] = message.BodySize.ToString("N0", CultureInfo.InvariantCulture) + " bytes"
        };
        foreach (var property in message.ApplicationProperties)
        {
            values["Application: " + property.Name] = $"[{property.Type}] {property.Value}";
        }
        return values.Where(pair => pair.Value is not null).ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
    }
}
