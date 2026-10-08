using System.Text.RegularExpressions;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// One cause among dead letters: a reason and the shape of its error description, with the parts that differ from
/// message to message (IDs, numbers, dates) replaced, for example "Order {n} was not found".
/// </summary>
/// <param name="Pattern">The description with its varying parts replaced; null when the messages have none.</param>
/// <param name="Example">One description as it was, to show what the pattern stands for.</param>
public sealed record DeadLetterCause(string Reason, string? Pattern, string? Example, int Count)
{
    /// <summary>"MaxDeliveryCountExceeded: Order {n} was not found", or just the reason.</summary>
    public string Label => Pattern is null ? Reason : $"{Reason}: {Pattern}";
}

/// <summary>Groups dead letters by why they failed, so a queue with thousands of them reads as a handful of causes.</summary>
public static partial class DeadLetterCauses
{
    public const string NoReason = "(no reason)";

    private const int MaximumPatternLength = 160;

    /// <summary>The causes of the messages, most frequent first.</summary>
    public static IReadOnlyList<DeadLetterCause> Group(IEnumerable<BrowsedMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        return messages
            .Select(message => (Key: KeyOf(message), message.DeadLetterErrorDescription))
            .GroupBy(item => item.Key)
            .Select(group => new DeadLetterCause(group.Key.Reason, group.Key.Pattern,
                group.Select(item => FirstLine(item.DeadLetterErrorDescription)).FirstOrDefault(text => text is not null), group.Count()))
            .OrderByDescending(cause => cause.Count)
            .ThenBy(cause => cause.Label, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    /// <summary>The reason and description pattern a message is grouped under.</summary>
    public static (string Reason, string? Pattern) KeyOf(BrowsedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var reason = string.IsNullOrWhiteSpace(message.DeadLetterReason) ? NoReason : message.DeadLetterReason.Trim();
        return (reason, Pattern(message.DeadLetterErrorDescription));
    }

    /// <summary>
    /// The first line of a description with the parts that vary replaced: GUIDs and long hex IDs by {id}, dates and
    /// times by {time}, quoted values holding digits by '{value}', other numbers by {n}. Words stay, so
    /// "Field 'amount' is required" keeps its field name.
    /// </summary>
    public static string? Pattern(string? description)
    {
        var text = FirstLine(description);
        if (text is null)
        {
            return null;
        }
        text = Guid().Replace(text, "{id}");
        text = Timestamp().Replace(text, "{time}");
        text = QuotedWithDigits().Replace(text, match => $"{match.Groups["quote"].Value}{{value}}{match.Groups["quote"].Value}");
        text = HexId().Replace(text, "{id}");
        text = Number().Replace(text, "{n}");
        text = Spaces().Replace(text, " ").Trim();
        return text.Length > MaximumPatternLength ? text[..(MaximumPatternLength - 1)] + "…" : text;
    }

    private static string? FirstLine(string? description)
    {
        if (string.IsNullOrWhiteSpace(description))
        {
            return null;
        }
        var line = description.Trim().Split('\n', 2)[0].Trim();
        return line.Length == 0 ? null : line;
    }

    [GeneratedRegex(@"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")]
    private static partial Regex Guid();

    [GeneratedRegex(@"\b\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?\b|\b\d{1,2}:\d{2}:\d{2}(\.\d+)?\b")]
    private static partial Regex Timestamp();

    // A quote opens after something other than a letter or digit and closes before one: an apostrophe inside a word
    // ("can't", "customer's", "customers' ") is not a quote, and pairing two of them swallowed the words in between
    // ("Can't ship order 42, it doesn't exist" became "Can'{value}'t exist"), merging different causes.
    [GeneratedRegex(@"(?<![\p{L}\p{N}])(?<quote>['""`])[^'""`\r\n]*\d[^'""`\r\n]*\k<quote>(?![\p{L}\p{N}])")]
    private static partial Regex QuotedWithDigits();

    [GeneratedRegex(@"\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{12,}\b")]
    private static partial Regex HexId();

    [GeneratedRegex(@"(?<![\p{L}_\d])(?>\d+(?:[.,]\d+)?)(?![_\d])")]
    private static partial Regex Number();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();
}
