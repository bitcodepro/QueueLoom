namespace QueueLoom.Core.ServiceBus;

public static class DeadLetterSearchMatcher
{
    /// <summary>Whether the message matches the search: text, a /regular expression/ or a $.json.path condition.</summary>
    public static bool IsMatch(BrowsedMessage message, string query)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        return MessageSearchQuery.Parse(query).Matches(message);
    }

    public static bool IsMatch(BrowsedMessage message, MessageSearchQuery query)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(query);
        return query.Matches(message);
    }
}
