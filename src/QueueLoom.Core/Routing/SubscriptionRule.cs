namespace QueueLoom.Core.Routing;

public enum RuleFilterKind
{
    /// <summary>A SQL-like condition on the message's properties, for example <c>region = 'EU' AND amount &gt; 100</c>.</summary>
    Sql,

    /// <summary>Exact matches on system properties (Subject, CorrelationId…) and application properties.</summary>
    Correlation,

    /// <summary>Every message matches (<c>1=1</c>, the <c>$Default</c> rule).</summary>
    True,

    /// <summary>No message matches (<c>1=0</c>).</summary>
    False
}

/// <summary>
/// The fields of a correlation filter. A field that is null is not checked; every field that is set must match
/// exactly, and so must every listed application property.
/// </summary>
public sealed record CorrelationFilterFields(
    string? CorrelationId = null,
    string? MessageId = null,
    string? To = null,
    string? ReplyTo = null,
    string? Subject = null,
    string? SessionId = null,
    string? ReplyToSessionId = null,
    string? ContentType = null)
{
    /// <summary>Application properties with their typed values: text, long, double or bool (and as Service Bus returns others).</summary>
    public IReadOnlyDictionary<string, object> Properties { get; init; } = new Dictionary<string, object>(StringComparer.Ordinal);

    public bool IsEmpty => CorrelationId is null && MessageId is null && To is null && ReplyTo is null && Subject is null &&
                           SessionId is null && ReplyToSessionId is null && ContentType is null && Properties.Count == 0;

    /// <summary>The set fields as "Subject = 'order.created' AND tenant = 'acme' AND amount = 250".</summary>
    public string Describe()
    {
        var parts = new List<string>();
        void Add(string name, string? value)
        {
            if (value is not null)
            {
                parts.Add($"{name} = '{value}'");
            }
        }
        Add("CorrelationId", CorrelationId);
        Add("MessageId", MessageId);
        Add("To", To);
        Add("ReplyTo", ReplyTo);
        Add("Subject", Subject);
        Add("SessionId", SessionId);
        Add("ReplyToSessionId", ReplyToSessionId);
        Add("ContentType", ContentType);
        foreach (var (name, value) in Properties.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            parts.Add($"{name} = {RoutingValue.Format(value)}");
        }
        return parts.Count == 0 ? "(no fields: matches every message)" : string.Join(" AND ", parts);
    }
}

/// <summary>A rule of a topic subscription: a filter that decides whether a message is copied to it, and an optional action.</summary>
/// <param name="Action">A SQL action applied to matching messages, for example <c>SET priority = 'high'</c>; null for none.</param>
public sealed record SubscriptionRule(
    string Name,
    RuleFilterKind Kind,
    string? SqlExpression = null,
    CorrelationFilterFields? Correlation = null,
    string? Action = null)
{
    public const string DefaultRuleName = "$Default";

    public string FilterText => Kind switch
    {
        RuleFilterKind.Sql => SqlExpression ?? string.Empty,
        RuleFilterKind.Correlation => (Correlation ?? new CorrelationFilterFields()).Describe(),
        RuleFilterKind.True => "1=1 (every message)",
        _ => "1=0 (no message)"
    };

    public string KindLabel => Kind switch
    {
        RuleFilterKind.Sql => "SQL",
        RuleFilterKind.Correlation => "Correlation",
        RuleFilterKind.True => "All messages",
        _ => "No messages"
    };
}

/// <summary>The rules of one subscription of a topic.</summary>
public sealed record SubscriptionRules(string Subscription, IReadOnlyList<SubscriptionRule> Rules)
{
    /// <summary>Why this subscription may not receive what one would expect, or null.</summary>
    public string? Warning => Rules.Count == 0
        ? "Has no rules, so it receives no messages at all."
        : Rules.All(rule => rule.Kind == RuleFilterKind.False)
            ? "Every rule is 1=0, so it receives no messages."
            : null;
}
