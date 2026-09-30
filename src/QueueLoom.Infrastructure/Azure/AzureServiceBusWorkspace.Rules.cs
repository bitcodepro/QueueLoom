using Azure;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Routing;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>
/// Subscription rules: the filters that decide which messages of a topic each subscription receives. Reading needs
/// the Manage right (Service Bus Data Owner); changing them is allowed only with queue management turned on.
/// </summary>
public sealed partial class AzureServiceBusWorkspace
{
    public bool SupportsSubscriptionRules => true;

    public async Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ThrowIfDisposed();
        var administration = GetAdministrationClient();
        var result = new List<SubscriptionRules>();
        try
        {
            await foreach (var subscription in administration.GetSubscriptionsAsync(topic, cancellationToken).ConfigureAwait(false))
            {
                var rules = new List<SubscriptionRule>();
                await foreach (var rule in administration.GetRulesAsync(topic, subscription.SubscriptionName, cancellationToken).ConfigureAwait(false))
                {
                    rules.Add(ToRule(rule));
                }
                result.Add(new SubscriptionRules(subscription.SubscriptionName, rules));
            }
        }
        catch (RequestFailedException exception) when (exception.Status is 401 or 403)
        {
            throw new InvalidOperationException(
                "Reading subscription rules needs the Azure Service Bus Data Owner role (or a Manage connection string).", exception);
        }
        return result.OrderBy(item => item.Subscription, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public async Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        var administration = GetAdministrationClient();
        if (replace)
        {
            var existing = (await administration.GetRuleAsync(topic, subscription, rule.Name, cancellationToken).ConfigureAwait(false)).Value;
            existing.Filter = ToFilter(rule);
            existing.Action = string.IsNullOrWhiteSpace(rule.Action) ? null : new SqlRuleAction(rule.Action);
            await AdministerRule(() => administration.UpdateRuleAsync(topic, subscription, existing, cancellationToken)).ConfigureAwait(false);
            return;
        }

        var options = new CreateRuleOptions(rule.Name, ToFilter(rule));
        if (!string.IsNullOrWhiteSpace(rule.Action))
        {
            options.Action = new SqlRuleAction(rule.Action);
        }
        await AdministerRule(() => administration.CreateRuleAsync(topic, subscription, options, cancellationToken)).ConfigureAwait(false);
    }

    public async Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        await Administer(() => GetAdministrationClient().DeleteRuleAsync(topic, subscription, rule, cancellationToken)).ConfigureAwait(false);
    }

    private static async Task AdministerRule(Func<Task> action)
    {
        try
        {
            await Administer(action).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is ArgumentException || exception is RequestFailedException { Status: 400 })
        {
            // "There was an error parsing the SQL expression. [...] . TrackingId:…": keep the part a person can use.
            var message = exception.Message.Split('\n')[0];
            var tracking = message.IndexOf(" TrackingId:", StringComparison.Ordinal);
            throw new InvalidOperationException(
                $"Service Bus did not accept the rule: {(tracking > 0 ? message[..tracking].TrimEnd(' ', '.') : message)}", exception);
        }
    }

    public static SubscriptionRule ToRule(RuleProperties rule)
    {
        var action = (rule.Action as SqlRuleAction)?.SqlExpression;
        return rule.Filter switch
        {
            TrueRuleFilter => new SubscriptionRule(rule.Name, RuleFilterKind.True, "1=1", Action: action),
            FalseRuleFilter => new SubscriptionRule(rule.Name, RuleFilterKind.False, "1=0", Action: action),
            SqlRuleFilter sql => new SubscriptionRule(rule.Name, RuleFilterKind.Sql, sql.SqlExpression, Action: action),
            CorrelationRuleFilter correlation => new SubscriptionRule(rule.Name, RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(
                Empty(correlation.CorrelationId), Empty(correlation.MessageId), Empty(correlation.To), Empty(correlation.ReplyTo),
                Empty(correlation.Subject), Empty(correlation.SessionId), Empty(correlation.ReplyToSessionId), Empty(correlation.ContentType))
            {
                Properties = correlation.ApplicationProperties.ToDictionary(
                    pair => pair.Key,
                    pair => Convert.ToString(pair.Value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
                    StringComparer.Ordinal)
            }, Action: action),
            _ => new SubscriptionRule(rule.Name, RuleFilterKind.Sql, rule.Filter?.ToString(), Action: action)
        };
    }

    public static RuleFilter ToFilter(SubscriptionRule rule)
    {
        switch (rule.Kind)
        {
            case RuleFilterKind.True:
                return new TrueRuleFilter();
            case RuleFilterKind.False:
                return new FalseRuleFilter();
            case RuleFilterKind.Sql:
                return new SqlRuleFilter(rule.SqlExpression ?? throw new InvalidOperationException("Enter the SQL filter."));
        }

        var fields = rule.Correlation ?? throw new InvalidOperationException("Fill in at least one correlation field.");
        var filter = new CorrelationRuleFilter
        {
            CorrelationId = fields.CorrelationId,
            MessageId = fields.MessageId,
            To = fields.To,
            ReplyTo = fields.ReplyTo,
            Subject = fields.Subject,
            SessionId = fields.SessionId,
            ReplyToSessionId = fields.ReplyToSessionId,
            ContentType = fields.ContentType
        };
        foreach (var (name, value) in fields.Properties)
        {
            filter.ApplicationProperties[name] = value;
        }
        return filter;
    }

    private static string? Empty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
