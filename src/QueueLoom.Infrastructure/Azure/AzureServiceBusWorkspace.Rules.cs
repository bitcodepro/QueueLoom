using Azure;
using Azure.Messaging.ServiceBus;
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
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
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
                result.Add(new SubscriptionRules(subscription.SubscriptionName, rules) { Note = ForwardingNote(topic, subscription) });
            }
        }
        catch (RequestFailedException exception) when (exception.Status is 401 or 403)
        {
            throw new InvalidOperationException(
                "Reading subscription rules needs the Azure Service Bus Data Owner role (or a Manage connection string).", exception);
        }
        return result.OrderBy(item => item.Subscription, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    /// <summary>Where a subscription that auto-forwards sends what it receives, followed through the last topology read.</summary>
    private string? ForwardingNote(string topic, SubscriptionProperties subscription)
    {
        var target = Forwarding.TargetName(subscription.ForwardTo);
        var deadLetters = Forwarding.TargetName(subscription.ForwardDeadLetteredMessagesTo);
        var notes = new List<string>();
        if (target is not null)
        {
            notes.Add(_cachedTopology is { } topology
                ? Forwarding.Follow(topology, $"{topic}/{subscription.SubscriptionName}").Describe(target)
                : $"Forwards to {target}");
        }
        if (deadLetters is not null)
        {
            notes.Add($"Dead letters are forwarded to {deadLetters}");
        }
        return notes.Count == 0 ? null : string.Join(" · ", notes);
    }

    public async Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        ThrowIfDisposed();
        // Like sends, purges and deletes here (and every leased provider's ManageAsync), a rule write holds the operation
        // gate, so a disconnect or environment switch waits for it instead of tearing the client away mid-write.
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        var administration = GetAdministrationClient();
        if (replace)
        {
            var existing = await GetUnchangedRuleAsync(administration, topic, subscription, rule.Original, rule.Name, cancellationToken)
                .ConfigureAwait(false);
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

    public Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        DeleteRuleAsync(topic, subscription, rule, null, cancellationToken);

    /// <summary>Deletes the rule as it was read: refused when someone changed it since (see <see cref="SubscriptionRule.Original"/>).</summary>
    public Task DeleteSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return DeleteRuleAsync(topic, subscription, rule.Name, rule.Original ?? rule, cancellationToken);
    }

    private async Task DeleteRuleAsync(string topic, string subscription, string rule, SubscriptionRule? asRead,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        var administration = GetAdministrationClient();
        if (asRead is not null)
        {
            await GetUnchangedRuleAsync(administration, topic, subscription, asRead, rule, cancellationToken).ConfigureAwait(false);
        }
        await Administer(() => administration.DeleteRuleAsync(topic, subscription, rule, cancellationToken)).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the live rule and, when the caller says how it read the rule, refuses if it differs. RuleProperties has no
    /// ETag or version (only Name, Filter and Action: https://learn.microsoft.com/dotnet/api/azure.messaging.servicebus.administration.ruleproperties),
    /// and UpdateRuleAsync / DeleteRuleAsync overwrite or remove whatever is there, so the definitions themselves are
    /// compared right before the write. That shrinks the window for a lost update from the time the dialog was open to
    /// the two calls here; the service offers nothing that closes it completely.
    /// </summary>
    private static async Task<RuleProperties> GetUnchangedRuleAsync(ServiceBusAdministrationClient administration, string topic,
        string subscription, SubscriptionRule? asRead, string name, CancellationToken cancellationToken)
    {
        RuleProperties? live = null;
        try
        {
            await Administer(async () => live = (await administration.GetRuleAsync(topic, subscription, name, cancellationToken)
                .ConfigureAwait(false)).Value).ConfigureAwait(false);
        }
        catch (Exception exception) when (asRead is not null &&
                                          exception is ServiceBusException { Reason: ServiceBusFailureReason.MessagingEntityNotFound } or
                                              RequestFailedException { Status: 404 })
        {
            throw new InvalidOperationException(
                $"Rule {name} of {topic} / {subscription} no longer exists: it changed since you opened it. Refresh and review again.", exception);
        }
        if (asRead is not null && !SameDefinition(ToRule(live!), asRead))
        {
            throw new InvalidOperationException(
                $"Rule {name} of {topic} / {subscription} changed since you opened it; nothing was saved. Refresh and review again.");
        }
        return live!;
    }

    /// <summary>Whether two rules filter and annotate alike: same kind, expression, correlation fields, typed properties and action.</summary>
    internal static bool SameDefinition(SubscriptionRule left, SubscriptionRule right)
    {
        if (left.Kind != right.Kind || !string.Equals(Blank(left.Action), Blank(right.Action), StringComparison.Ordinal))
        {
            return false;
        }
        switch (left.Kind)
        {
            case RuleFilterKind.True or RuleFilterKind.False:
                return true;
            case RuleFilterKind.Correlation:
                var a = left.Correlation ?? new CorrelationFilterFields();
                var b = right.Correlation ?? new CorrelationFilterFields();
                // Values keep their type: amount = 250 (Int64) and amount = '250' are different rules.
                return a with { Properties = b.Properties } == b && a.Properties.Count == b.Properties.Count &&
                       a.Properties.All(pair => b.Properties.TryGetValue(pair.Key, out var other) &&
                                                other.GetType() == pair.Value.GetType() && Equals(other, pair.Value));
            default:
                return string.Equals(left.SqlExpression, right.SqlExpression, StringComparison.Ordinal);
        }
    }

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

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
                // Values keep their type: a rule on amount = 250 must not turn into amount = '250' when it is saved again.
                Properties = correlation.ApplicationProperties.ToDictionary(
                    pair => pair.Key,
                    pair => pair.Value ?? string.Empty,
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
