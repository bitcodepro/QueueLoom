using System.Globalization;
using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Routing;

/// <summary>
/// How a RabbitMQ exchange decides whether a binding takes a message. The routing key is the message's Subject,
/// as QueueLoom sends it; the headers are its application properties.
/// </summary>
public static class RabbitBindings
{
    /// <summary>Headers QueueLoom sends as AMQP properties rather than headers.</summary>
    private static readonly string[] PropertyNames = ["amqp-type", "amqp-app-id"];

    /// <summary>AMQP topic matching: words are separated by dots, '*' is exactly one word, '#' is zero or more.</summary>
    public static bool TopicMatches(string pattern, string routingKey)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        ArgumentNullException.ThrowIfNull(routingKey);
        var words = routingKey.Length == 0 ? [] : routingKey.Split('.');
        // Dynamic programming keeps adjacent # wildcards independent, including their
        // zero-word alternatives, without regex rewriting or exponential backtracking.
        var previous = new bool[words.Length + 1];
        previous[0] = true;
        foreach (var word in pattern.Length == 0 ? Array.Empty<string>() : pattern.Split('.'))
        {
            var current = new bool[words.Length + 1];
            current[0] = word == "#" && previous[0];
            for (var i = 1; i <= words.Length; i++)
                current[i] = word == "#"
                    ? previous[i] || current[i - 1]
                    : previous[i - 1] && (word == "*" || word == words[i - 1]);
            previous = current;
        }
        return previous[words.Length];
    }

    /// <summary>
    /// A headers binding: with x-match "all" (the default) every argument must match a header, with "any" one is
    /// enough. Arguments whose names start with "x-" are not compared unless x-match ends in "-with-x". A value must
    /// equal the header's exactly, type included (RabbitMQ confirms 250 matches neither '250' nor 250.0); an
    /// argument without a value only needs the header to be there.
    /// </summary>
    public static (RoutingOutcome Outcome, string Explanation) MatchHeaders(IReadOnlyDictionary<string, object?> arguments, RoutingMessage message)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        ArgumentNullException.ThrowIfNull(message);
        var mode = arguments.TryGetValue("x-match", out var value) && value is string text ? text : "all";
        var withX = mode.EndsWith("-with-x", StringComparison.Ordinal);
        var any = mode.StartsWith("any", StringComparison.Ordinal);
        var compared = arguments
            .Where(pair => pair.Key != "x-match" && (withX || !pair.Key.StartsWith("x-", StringComparison.Ordinal)))
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToArray();
        if (compared.Length == 0)
        {
            // RabbitMQ: "all" of nothing is true, "any" of nothing is false.
            return any
                ? (RoutingOutcome.Skips, "The binding lists no headers, and x-match any needs one to match.")
                : (RoutingOutcome.Receives, "The binding lists no headers, so every message matches.");
        }

        var misses = new List<string>();
        var unknowns = new List<string>();
        var matched = 0;
        foreach (var (name, expected) in compared)
        {
            var exists = TryHeader(message, name, out var actual);
            if (!exists)
            {
                misses.Add($"header {name} is missing");
                continue;
            }
            switch (Compare(expected, actual))
            {
                case true:
                    matched++;
                    break;
                case false:
                    misses.Add($"header {name} should be {RoutingValue.Format(expected)} but is {RoutingValue.Format(message.UserProperties[name])}");
                    break;
                default:
                    unknowns.Add($"header {name} is a {message.UserProperties[name]?.GetType().Name ?? "value"} that QueueLoom cannot compare; RabbitMQ decides");
                    break;
            }
        }

        if (any)
        {
            return matched > 0
                ? (RoutingOutcome.Receives, "At least one header matches (x-match any).")
                : unknowns.Count > 0
                    ? (RoutingOutcome.Unknown, string.Join("; ", unknowns.Take(3)))
                    : (RoutingOutcome.Skips, "No header matches (x-match any): " + string.Join("; ", misses.Take(3)));
        }
        return misses.Count > 0
            ? (RoutingOutcome.Skips, string.Join("; ", misses.Take(3)))
            : unknowns.Count > 0
                ? (RoutingOutcome.Unknown, string.Join("; ", unknowns.Take(3)))
                : (RoutingOutcome.Receives, "Every header matches.");
    }

    /// <summary>The header as RabbitMQ receives it from QueueLoom; names are case-sensitive.</summary>
    private static bool TryHeader(RoutingMessage message, string name, out object? value)
    {
        value = null;
        if (PropertyNames.Contains(name, StringComparer.Ordinal) || !message.UserProperties.TryGetValue(name, out var typed))
        {
            return false;
        }
        value = ToHeader(typed);
        return true;
    }

    /// <summary>The value as QueueLoom writes it into an AMQP header (see the RabbitMQ message mapper).</summary>
    private static object? ToHeader(object? value) => value switch
    {
        null => null,
        bool flag => flag,
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        // A Single is sent as the double its text names (0.1, not 0.10000000149011612), as the mapper parses it.
        float or double => RoutingValue.Normalize(value),
        decimal number => number,
        DateTime or DateTimeOffset => value,
        byte[] bytes => bytes,
        string text => Encoding.UTF8.GetBytes(text),
        _ => Encoding.UTF8.GetBytes(ApplicationPropertyValues.FromObject(string.Empty, value).Value)
    };

    /// <summary>True or false when RabbitMQ's answer is certain, null when QueueLoom cannot tell.</summary>
    private static bool? Compare(object? expected, object? header)
    {
        if (expected is null)
        {
            return true;
        }
        if (header is DateTime or DateTimeOffset or decimal || expected is not (string or long or int or double or bool))
        {
            return null;
        }
        return (expected, header) switch
        {
            (string text, byte[] bytes) => Encoding.UTF8.GetBytes(text).AsSpan().SequenceEqual(bytes),
            (long or int, long number) => Convert.ToInt64(expected, CultureInfo.InvariantCulture) == number,
            // OTP 27+ term equivalence distinguishes +0.0 from -0.0, as the broker's headers matcher does.
            (double x, double y) => double.IsFinite(x) && double.IsFinite(y) &&
                BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y),
            (bool x, bool y) => x == y,
            _ => false
        };
    }
}
