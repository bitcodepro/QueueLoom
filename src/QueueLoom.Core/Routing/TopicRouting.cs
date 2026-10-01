namespace QueueLoom.Core.Routing;

public enum RoutingOutcome
{
    /// <summary>The message is copied to the subscription.</summary>
    Receives,

    /// <summary>No rule matches: the subscription does not get the message.</summary>
    Skips,

    /// <summary>A rule uses something QueueLoom cannot evaluate before sending; Service Bus decides.</summary>
    Unknown
}

public sealed record RuleResult(SubscriptionRule Rule, RoutingOutcome Outcome, string Explanation);

public sealed record SubscriptionRouting(string Subscription, RoutingOutcome Outcome, IReadOnlyList<RuleResult> Rules, string? Warning)
{
    /// <summary>One line: which rule let the message in, or why none did.</summary>
    public string Summary => Outcome switch
    {
        RoutingOutcome.Receives => $"Receives it through rule {Rules.First(rule => rule.Outcome == RoutingOutcome.Receives).Rule.Name}",
        RoutingOutcome.Unknown => Rules.First(rule => rule.Outcome == RoutingOutcome.Unknown).Explanation,
        _ => Warning ?? string.Join("; ", Rules.Select(rule => $"{rule.Rule.Name}: {rule.Explanation}"))
    };
}

public sealed record TopicRoutingResult(string Topic, IReadOnlyList<SubscriptionRouting> Subscriptions)
{
    public int ReceivingCount => Subscriptions.Count(subscription => subscription.Outcome == RoutingOutcome.Receives);

    public int UnknownCount => Subscriptions.Count(subscription => subscription.Outcome == RoutingOutcome.Unknown);

    /// <summary>Service Bus accepts the message and silently drops it: no subscription takes it.</summary>
    public bool IsDropped => Subscriptions.All(subscription => subscription.Outcome == RoutingOutcome.Skips);

    public string Headline => Subscriptions.Count == 0
        ? $"Topic {Topic} has no subscriptions: every message sent to it is dropped."
        : IsDropped
            ? "No subscription takes this message. Service Bus accepts it and drops it without an error."
            : $"{ReceivingCount} of {Subscriptions.Count} subscriptions receive it" +
              (UnknownCount > 0 ? $"; {UnknownCount} {(UnknownCount == 1 ? "depends" : "depend")} on what only Service Bus knows." : ".");
}

/// <summary>Works out which subscriptions of a topic a message would be copied to, rule by rule.</summary>
public static class TopicRouting
{
    public static TopicRoutingResult Route(string topic, IReadOnlyList<SubscriptionRules> subscriptions, RoutingMessage message)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(message);
        return new TopicRoutingResult(topic, subscriptions.Select(subscription =>
        {
            var results = subscription.Rules.Select(rule => Check(rule, message)).ToArray();
            var outcome = results.Any(result => result.Outcome == RoutingOutcome.Receives) ? RoutingOutcome.Receives
                : results.Any(result => result.Outcome == RoutingOutcome.Unknown) ? RoutingOutcome.Unknown
                : RoutingOutcome.Skips;
            return new SubscriptionRouting(subscription.Subscription, outcome, results, subscription.Warning);
        }).ToArray());
    }

    public static RuleResult Check(SubscriptionRule rule, RoutingMessage message)
    {
        ArgumentNullException.ThrowIfNull(rule);
        switch (rule.Kind)
        {
            case RuleFilterKind.True:
                return new RuleResult(rule, RoutingOutcome.Receives, "Takes every message.");
            case RuleFilterKind.False:
                return new RuleResult(rule, RoutingOutcome.Skips, "Takes no message (1=0).");
            case RuleFilterKind.Correlation:
                return CheckCorrelation(rule, rule.Correlation ?? new CorrelationFilterFields(), message);
        }

        try
        {
            var steps = new List<SqlFilterStep>();
            var result = SqlFilter.Parse(rule.SqlExpression ?? string.Empty).Evaluate(message, steps);
            if (result == true)
            {
                return new RuleResult(rule, RoutingOutcome.Receives, $"{rule.SqlExpression} is true.");
            }
            var failed = steps.Where(step => step.Result != true).Take(3)
                .Select(step => $"{step.Text} is {(step.Result == false ? "false" : "unknown")}" + (step.Actual is null ? string.Empty : $" ({step.Actual})"))
                .ToArray();
            return new RuleResult(rule, RoutingOutcome.Skips, failed.Length > 0 ? string.Join("; ", failed) : $"{rule.SqlExpression} is not true.");
        }
        catch (SqlFilterNotSupportedException exception)
        {
            return new RuleResult(rule, RoutingOutcome.Unknown, exception.Message);
        }
        catch (SqlFilterSyntaxException exception)
        {
            return new RuleResult(rule, RoutingOutcome.Unknown, $"QueueLoom cannot read this filter ({exception.Message}).");
        }
    }

    private static RuleResult CheckCorrelation(SubscriptionRule rule, CorrelationFilterFields filter, RoutingMessage message)
    {
        var properties = message.Properties;
        var misses = new List<string>();
        void Field(string name, string? expected, string? actual)
        {
            if (expected is not null && !string.Equals(expected, actual, StringComparison.Ordinal))
            {
                misses.Add(actual is null ? $"{name} should be '{expected}' but the message has none" : $"{name} should be '{expected}' but is '{actual}'");
            }
        }
        Field("CorrelationId", filter.CorrelationId, properties.CorrelationId);
        Field("MessageId", filter.MessageId, properties.MessageId);
        Field("To", filter.To, properties.To);
        Field("ReplyTo", filter.ReplyTo, properties.ReplyTo);
        Field("Subject", filter.Subject, properties.Subject);
        Field("SessionId", filter.SessionId, properties.SessionId);
        Field("ReplyToSessionId", filter.ReplyToSessionId, properties.ReplyToSessionId);
        Field("ContentType", filter.ContentType, properties.ContentType);
        var unknowns = new List<string>();
        foreach (var (name, expected) in filter.Properties)
        {
            var value = message.User(name, out var exists);
            if (!RoutingValue.IsComparable(expected) || exists && !RoutingValue.IsComparable(value))
            {
                unknowns.Add($"{name} holds a {(RoutingValue.IsComparable(expected) ? value : expected)!.GetType().Name}, which QueueLoom cannot compare; Service Bus decides");
            }
            else if (!exists)
            {
                misses.Add($"{name} should be {RoutingValue.Format(expected)} but the message has none");
            }
            else if (!RoutingValue.AreEqual(expected, value))
            {
                misses.Add($"{name} should be {RoutingValue.Format(expected)} but is {RoutingValue.Format(value)}");
            }
        }
        return misses.Count > 0
            ? new RuleResult(rule, RoutingOutcome.Skips, string.Join("; ", misses.Take(3)))
            : unknowns.Count > 0
                ? new RuleResult(rule, RoutingOutcome.Unknown, string.Join("; ", unknowns.Take(3)))
                : new RuleResult(rule, RoutingOutcome.Receives, "Every field matches.");
    }
}
