using System.Text;

namespace QueueLoom.Core.ServiceBus;

public static class DeadLetterSearchMatcher
{
    public static bool IsMatch(BrowsedMessage message, string query)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(query);

        var needle = query.Trim();
        var properties = message.Properties;
        if (Contains(properties.CorrelationId, needle) ||
            Contains(properties.MessageId, needle) ||
            Contains(properties.Subject, needle) ||
            Contains(properties.ContentType, needle) ||
            Contains(properties.SessionId, needle) ||
            Contains(properties.To, needle) ||
            Contains(properties.ReplyTo, needle) ||
            Contains(message.DeadLetterReason, needle) ||
            Contains(message.DeadLetterErrorDescription, needle))
        {
            return true;
        }

        foreach (var property in message.ApplicationProperties)
        {
            if (Contains(property.Name, needle) || Contains(property.Value, needle))
            {
                return true;
            }
        }

        return message.Body.Length > 0 &&
               Encoding.UTF8.GetString(message.Body.Span)
                   .Contains(needle, StringComparison.OrdinalIgnoreCase);
    }

    private static bool Contains(string? value, string query) =>
        !string.IsNullOrEmpty(value) && value.Contains(query, StringComparison.OrdinalIgnoreCase);
}
