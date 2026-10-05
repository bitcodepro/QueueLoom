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
        RoutingOutcome.Receives => Reason ?? $"Receives it through {Describe(Rules.First(rule => rule.Outcome == RoutingOutcome.Receives).Rule)}",
        RoutingOutcome.Unknown => Reason ?? Rules.First(rule => rule.Outcome == RoutingOutcome.Unknown).Explanation,
        _ => Reason ?? Warning ?? string.Join("; ", Rules.Select(rule => $"{rule.Rule.DisplayName}: {rule.Explanation}"))
    };

    /// <summary>Set when the outcome does not come from one rule: no filter at all, an alternate exchange, a problem.</summary>
    public string? Reason { get; init; }

    private static string Describe(SubscriptionRule rule) => rule.Kind switch
    {
        RuleFilterKind.SnsFilterPolicy => "its filter policy",
        RuleFilterKind.PubSubFilter => "its filter",
        _ when rule.IsBinding => $"binding {rule.DisplayName}",
        _ => $"rule {rule.DisplayName}"
    };
}

public sealed record TopicRoutingResult(string Topic, IReadOnlyList<SubscriptionRouting> Subscriptions)
{
    public RoutingService Service { get; init; } = RoutingService.ServiceBus;

    public int ReceivingCount => Subscriptions.Count(subscription => subscription.Outcome == RoutingOutcome.Receives);

    public int UnknownCount => Subscriptions.Count(subscription => subscription.Outcome == RoutingOutcome.Unknown);

    /// <summary>Service Bus accepts the message and silently drops it: no subscription takes it.</summary>
    public bool IsDropped => Subscriptions.All(subscription => subscription.Outcome == RoutingOutcome.Skips);

    public string Headline => Subscriptions.Count == 0
        ? Service == RoutingService.RabbitMq
            ? $"Exchange {Topic} has no bindings: {Service.DropText()}"
            : $"Topic {Topic} has no subscriptions: every message sent to it is dropped."
        : IsDropped
            ? $"No {Service.SubscriptionWord()} takes this message. {Service.DropText()}"
            : $"{ReceivingCount} of {Subscriptions.Count} {Service.SubscriptionWord()}s receive it" +
              (UnknownCount > 0 ? $"; {UnknownCount} {(UnknownCount == 1 ? "depends" : "depend")} on what only {Service.Name()} knows." : ".");
}

/// <summary>Works out which subscriptions of a topic a message would be copied to, rule by rule.</summary>
public static class TopicRouting
{
    public static TopicRoutingResult Route(string topic, IReadOnlyList<SubscriptionRules> subscriptions, RoutingMessage message,
        RoutingService service = RoutingService.ServiceBus)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(message);
        var routed = subscriptions.Select(subscription =>
        {
            if (subscription.IsFallback)
            {
                return null;
            }
            if (subscription.Problem is { } problem)
            {
                return new SubscriptionRouting(subscription.Subscription, RoutingOutcome.Skips, [], subscription.Warning) { Reason = problem };
            }
            if (subscription.Unreadable is { } unreadable)
            {
                return new SubscriptionRouting(subscription.Subscription, RoutingOutcome.Unknown, [], subscription.Warning) { Reason = unreadable };
            }
            if (subscription.Rules.Count == 0 && subscription.ReceivesAllWithoutRules)
            {
                return new SubscriptionRouting(subscription.Subscription, RoutingOutcome.Receives, [], subscription.Warning)
                {
                    Reason = "Has no filter, so it receives every message."
                };
            }
            var results = subscription.Rules.Select(rule => Check(rule, message)).ToArray();
            var outcome = results.Any(result => result.Outcome == RoutingOutcome.Receives) ? RoutingOutcome.Receives
                : results.Any(result => result.Outcome == RoutingOutcome.Unknown) ? RoutingOutcome.Unknown
                : RoutingOutcome.Skips;
            return new SubscriptionRouting(subscription.Subscription, outcome, results, subscription.Warning);
        }).ToArray();

        // An alternate exchange gets the message when it reaches no queue. A queue bound directly settles that; an
        // exchange bound in between may itself route the message nowhere, which only its own bindings tell.
        var regular = subscriptions.Select((subscription, index) => (subscription.IsExchange, Routing: routed[index]))
            .Where(item => item.Routing is not null)
            .ToArray();
        var fallback = regular.Any(item => !item.IsExchange && item.Routing!.Outcome == RoutingOutcome.Receives)
            ? (RoutingOutcome.Skips, "A queue takes the message, so the alternate exchange does not get it.")
            : regular.Any(item => item.IsExchange && item.Routing!.Outcome == RoutingOutcome.Receives)
                ? (RoutingOutcome.Unknown, "Only an exchange takes the message; the alternate exchange gets it if that exchange reaches no queue, which RabbitMQ decides.")
                : regular.Any(item => item.Routing!.Outcome == RoutingOutcome.Unknown)
                    ? (RoutingOutcome.Unknown, "Gets the message only if no binding takes it, which RabbitMQ decides.")
                    : (RoutingOutcome.Receives, "No binding takes the message, so the alternate exchange gets it.");
        var result = subscriptions.Select((subscription, index) => routed[index] ??
            new SubscriptionRouting(subscription.Subscription, fallback.Item1, [], subscription.Warning) { Reason = fallback.Item2 }).ToArray();
        return new TopicRoutingResult(topic, result) { Service = service };
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
            case RuleFilterKind.SnsFilterPolicy:
                var (snsOutcome, snsExplanation) = SnsFilterPolicy.Evaluate(rule.Expression ?? string.Empty, rule.OnMessageBody, message);
                return new RuleResult(rule, snsOutcome, snsExplanation);
            case RuleFilterKind.PubSubFilter:
                return CheckPubSub(rule, message);
            case RuleFilterKind.FanoutBinding:
                return new RuleResult(rule, RoutingOutcome.Receives, "A fanout exchange copies every message to every binding.");
            case RuleFilterKind.DirectBinding or RuleFilterKind.TopicBinding:
                var key = message.Properties.Subject ?? string.Empty;
                var pattern = rule.Expression ?? string.Empty;
                var matches = rule.Kind == RuleFilterKind.DirectBinding
                    ? string.Equals(key, pattern, StringComparison.Ordinal)
                    : RabbitBindings.TopicMatches(pattern, key);
                return matches
                    ? new RuleResult(rule, RoutingOutcome.Receives, $"The routing key '{key}' {(rule.Kind == RuleFilterKind.DirectBinding ? "is" : "matches")} '{pattern}'.")
                    : new RuleResult(rule, RoutingOutcome.Skips, key.Length == 0
                        ? $"The message has no routing key (Subject); the binding wants '{pattern}'"
                        : $"The routing key '{key}' {(rule.Kind == RuleFilterKind.DirectBinding ? "is not" : "does not match")} '{pattern}'");
            case RuleFilterKind.HeadersBinding:
                var (headersOutcome, headersExplanation) = RabbitBindings.MatchHeaders(rule.Arguments, message);
                return new RuleResult(rule, headersOutcome, headersExplanation);
            case RuleFilterKind.OtherBinding:
                return new RuleResult(rule, RoutingOutcome.Unknown, "The exchange type comes from a plugin QueueLoom does not know; RabbitMQ decides.");
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

    private static RuleResult CheckPubSub(SubscriptionRule rule, RoutingMessage message)
    {
        try
        {
            var steps = new List<SqlFilterStep>();
            if (PubSubFilter.Parse(rule.Expression ?? string.Empty).Evaluate(message, steps))
            {
                return new RuleResult(rule, RoutingOutcome.Receives, "The filter matches.");
            }
            var failed = steps.Where(step => step.Result != true).Take(3)
                .Select(step => $"{step.Text} is false" + (step.Actual is null ? string.Empty : $" ({step.Actual})"))
                .ToArray();
            return new RuleResult(rule, RoutingOutcome.Skips, failed.Length > 0 ? string.Join("; ", failed) : "The filter does not match.");
        }
        catch (SqlFilterSyntaxException exception)
        {
            return new RuleResult(rule, RoutingOutcome.Unknown, $"QueueLoom cannot read this filter ({exception.Message}); Pub/Sub decides.");
        }
        catch (SqlFilterNotSupportedException exception)
        {
            return new RuleResult(rule, RoutingOutcome.Unknown, exception.Message);
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
