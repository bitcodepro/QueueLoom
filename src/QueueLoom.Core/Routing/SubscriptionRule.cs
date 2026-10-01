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
    False,

    /// <summary>An Amazon SNS filter policy (JSON) on the message attributes or the JSON body.</summary>
    SnsFilterPolicy,

    /// <summary>A Google Cloud Pub/Sub filter on the message attributes.</summary>
    PubSubFilter,

    /// <summary>A RabbitMQ binding of a direct exchange: the routing key must equal the binding key.</summary>
    DirectBinding,

    /// <summary>A RabbitMQ binding of a topic exchange: the routing key must match the pattern (* and #).</summary>
    TopicBinding,

    /// <summary>A RabbitMQ binding of a fanout exchange: every message.</summary>
    FanoutBinding,

    /// <summary>A RabbitMQ binding of a headers exchange: headers compared with the binding's arguments.</summary>
    HeadersBinding,

    /// <summary>A RabbitMQ binding of an exchange type QueueLoom does not know (a plugin's).</summary>
    OtherBinding
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

    /// <summary>The SNS policy (JSON), the Pub/Sub filter or the RabbitMQ binding key.</summary>
    public string? Expression { get; init; }

    /// <summary>SNS: the policy looks at the JSON body (FilterPolicyScope MessageBody) instead of the attributes.</summary>
    public bool OnMessageBody { get; init; }

    /// <summary>RabbitMQ: the binding's arguments (for a headers binding, x-match and the headers to compare).</summary>
    public IReadOnlyDictionary<string, object?> Arguments { get; init; } = NoArguments;

    // Shared so that rules without arguments stay equal as records.
    private static readonly IReadOnlyDictionary<string, object?> NoArguments = new Dictionary<string, object?>(StringComparer.Ordinal);

    /// <summary>A readable name where <see cref="Name"/> is an identifier (RabbitMQ's binding key, for example).</summary>
    public string? Title { get; init; }

    public string DisplayName => Title ?? Name;

    /// <summary>
    /// RabbitMQ: whether the binding leads to an exchange (true) or a queue (false). A queue and an exchange may
    /// share a name, so a change to a binding says which one it means; null when unknown.
    /// </summary>
    public bool? ToExchange { get; init; }

    public bool IsBinding => Kind is RuleFilterKind.DirectBinding or RuleFilterKind.TopicBinding or RuleFilterKind.FanoutBinding
        or RuleFilterKind.HeadersBinding or RuleFilterKind.OtherBinding;

    public string FilterText => Kind switch
    {
        RuleFilterKind.Sql => SqlExpression ?? string.Empty,
        RuleFilterKind.Correlation => (Correlation ?? new CorrelationFilterFields()).Describe(),
        RuleFilterKind.True => "1=1 (every message)",
        RuleFilterKind.False => "1=0 (no message)",
        RuleFilterKind.SnsFilterPolicy or RuleFilterKind.PubSubFilter => Expression ?? string.Empty,
        RuleFilterKind.DirectBinding => $"routing key = '{Expression}'",
        RuleFilterKind.TopicBinding => $"routing key matches '{Expression}'",
        RuleFilterKind.FanoutBinding => "every message (fanout)",
        RuleFilterKind.HeadersBinding => DescribeHeaders(),
        _ => Expression is { Length: > 0 } key ? $"binding key '{key}'" + ArgumentsText() : "binding" + ArgumentsText()
    };

    public string KindLabel => Kind switch
    {
        RuleFilterKind.Sql => "SQL",
        RuleFilterKind.Correlation => "Correlation",
        RuleFilterKind.True => "All messages",
        RuleFilterKind.False => "No messages",
        RuleFilterKind.SnsFilterPolicy => OnMessageBody ? "Message body" : "Message attributes",
        RuleFilterKind.PubSubFilter => "Attributes",
        RuleFilterKind.DirectBinding => "Direct binding",
        RuleFilterKind.TopicBinding => "Topic binding",
        RuleFilterKind.FanoutBinding => "Fanout binding",
        RuleFilterKind.HeadersBinding => "Headers binding",
        _ => "Binding"
    };

    /// <summary>"x-match all: region = 'EU' AND tier = 'gold'".</summary>
    private string DescribeHeaders()
    {
        var mode = Arguments.TryGetValue("x-match", out var value) && value is string text ? text : "all";
        var joiner = mode.StartsWith("any", StringComparison.Ordinal) ? " OR " : " AND ";
        var headers = Arguments.Where(pair => pair.Key != "x-match").OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => pair.Value is null ? $"has {pair.Key}" : $"{pair.Key} = {RoutingValue.Format(pair.Value)}")
            .ToArray();
        return $"x-match {mode}: " + (headers.Length == 0 ? "(no headers)" : string.Join(joiner, headers));
    }

    private string ArgumentsText() => Arguments.Count == 0
        ? string.Empty
        : " with " + string.Join(", ", Arguments.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => $"{pair.Key} = {RoutingValue.Format(pair.Value)}"));
}

/// <summary>The rules of one subscription of a topic (in RabbitMQ: the bindings of one queue or exchange to the exchange).</summary>
public sealed record SubscriptionRules(string Subscription, IReadOnlyList<SubscriptionRule> Rules)
{
    public RoutingService Service { get; init; } = RoutingService.ServiceBus;

    /// <summary>Why the subscription receives nothing whatever its rules say (pending confirmation, detached…), or null.</summary>
    public string? Problem { get; init; }

    /// <summary>A short remark shown under the name, such as "exchange" or "alternate exchange".</summary>
    public string? Note { get; init; }

    /// <summary>RabbitMQ's alternate exchange: it gets a message only when no binding takes it.</summary>
    public bool IsFallback { get; init; }

    /// <summary>RabbitMQ: the destination is an exchange that routes the message on with its own bindings.</summary>
    public bool IsExchange { get; init; }

    /// <summary>A subscription without rules: none in Service Bus, every message in SNS and Pub/Sub (no filter).</summary>
    public bool ReceivesAllWithoutRules => Service is RoutingService.Sns or RoutingService.PubSub;

    /// <summary>Why this subscription may not receive what one would expect, or null.</summary>
    public string? Warning => Problem ?? (Rules.Count == 0 && !ReceivesAllWithoutRules && !IsFallback
        ? "Has no rules, so it receives no messages at all."
        : Rules.Count > 0 && Rules.All(rule => rule.Kind == RuleFilterKind.False)
            ? "Every rule is 1=0, so it receives no messages."
            : null);
}
