using System.Collections.Concurrent;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using QueueLoom.Core.Routing;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Infrastructure.Aws;

/// <summary>
/// SNS subscription filter policies as rules: a subscription has at most one, named "FilterPolicy"; without one it
/// receives every message. Changing a policy needs queue management allowed for the environment.
/// </summary>
public sealed partial class AwsSqsSnsWorkspace
{
    public const string FilterPolicyRule = "FilterPolicy";

    /// <summary>Per topic, the subscription names the last rules read showed, with the ARN each one stood for.</summary>
    private readonly ConcurrentDictionary<string, IReadOnlyDictionary<string, string>> _shownSubscriptions = new(StringComparer.Ordinal);

    public override bool SupportsSubscriptionRules => true;

    public override RoutingService RoutingService => RoutingService.Sns;

    public override Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        ReadRulesAsync<IReadOnlyList<SubscriptionRules>>(async token =>
        {
            var info = await ReadTopicAsync(FindTopicArn(topic), token).ConfigureAwait(false);
            // The names are derived and numbered in list order; a change addresses the subscription shown, by its ARN.
            _shownSubscriptions[topic] = info.Subscriptions.ToDictionary(item => item.Name, item => item.Arn, StringComparer.Ordinal);
            return info.Subscriptions.Select(subscription => new SubscriptionRules(subscription.Name, subscription.FilterPolicy is null
                ? []
                :
                [
                    new SubscriptionRule(FilterPolicyRule, RuleFilterKind.SnsFilterPolicy)
                    {
                        Expression = subscription.FilterPolicy,
                        OnMessageBody = subscription.FilterPolicyOnBody,
                        Title = "Filter policy"
                    }
                ])
            {
                Service = RoutingService.Sns,
                Note = subscription.Protocol == "sqs" ? null : $"{subscription.Protocol}: {subscription.Endpoint}",
                Problem = subscription.IsConfirmed ? null : "Pending confirmation: SNS delivers nothing to it until the endpoint confirms."
            }).ToArray();
        }, cancellationToken);

    public override Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(rule);
            if (rule.Kind != RuleFilterKind.SnsFilterPolicy || string.IsNullOrWhiteSpace(rule.Expression))
            {
                throw new ArgumentException("SNS subscriptions take a filter policy.", nameof(rule));
            }
            SnsFilterPolicy.Validate(rule.Expression, rule.OnMessageBody);
            var current = await FindSubscriptionAsync(topic, subscription, token).ConfigureAwait(false);
            var scope = rule.OnMessageBody ? "MessageBody" : "MessageAttributes";
            if (current.FilterPolicyOnBody == rule.OnMessageBody)
            {
                await SetAttributeAsync(current.Arn, "FilterPolicy", rule.Expression, token).ConfigureAwait(false);
                return;
            }
            // SNS checks a policy against the scope in force, so the order follows the new policy: a body policy (it may
            // nest) is set once the scope is MessageBody, as AWS documents; an attribute policy (always flat, so valid
            // under either scope) goes first and the scope follows.
            var (first, value, previous, second, next) = rule.OnMessageBody
                ? ("FilterPolicyScope", scope, current.FilterPolicyOnBody ? "MessageBody" : "MessageAttributes", "FilterPolicy", rule.Expression)
                : ("FilterPolicy", rule.Expression, current.FilterPolicy ?? "{}", "FilterPolicyScope", scope);
            await SetAttributeAsync(current.Arn, first, value, token).ConfigureAwait(false);
            try
            {
                await SetAttributeAsync(current.Arn, second, next, token).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // The two attributes are separate calls. When SNS refuses the second (a policy over its five keys or 150
                // combinations, which QueueLoom does not check), the first must be undone: otherwise the old policy is
                // applied to the other scope (attribute keys looked up in the body), or the new one to the old scope,
                // and the subscription silently stops receiving what it received before.
                try
                {
                    await SetAttributeAsync(current.Arn, first, previous, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception restore) when (restore is AmazonServiceException or InvalidOperationException)
                {
                    throw new InvalidOperationException(
                        $"SNS changed {first} but refused {second}, and restoring {first} failed too, so the subscription filters " +
                        $"with a mix of the old and new settings. Check it in AWS. ({exception.Message})", exception);
                }
                throw;
            }
        }, cancellationToken);

    public override Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            var current = await FindSubscriptionAsync(topic, subscription, token).ConfigureAwait(false);
            // An empty policy removes it: the subscription receives every message again.
            await SetAttributeAsync(current.Arn, "FilterPolicy", "{}", token).ConfigureAwait(false);
        }, cancellationToken);

    private string FindTopicArn(string topic) =>
        _index.FindTopic(topic)?.Arn ?? throw new InvalidOperationException($"Topic '{topic}' was not found. Refresh and try again.");

    private async Task<AwsSubscriptionInfo> FindSubscriptionAsync(string topic, string subscription, CancellationToken cancellationToken)
    {
        // SNS subscriptions have no names: QueueLoom derives "sqs:orders" from the endpoint and numbers clashes
        // ("sqs:orders (2)", queues of one name in two regions or accounts) in list order. Once a namesake is
        // unsubscribed, a name can point at another subscription, so the one that was shown is found by its ARN.
        var shown = _shownSubscriptions.TryGetValue(topic, out var names) && names.TryGetValue(subscription, out var arn)
            ? arn
            : _index.FindSubscription(topic, subscription)?.Arn;
        var info = await ReadTopicAsync(FindTopicArn(topic), cancellationToken).ConfigureAwait(false);
        var found = (shown is not null && shown.StartsWith("arn:", StringComparison.Ordinal)
                        ? info.Subscriptions.FirstOrDefault(item => item.Arn == shown)
                        : info.Subscriptions.FirstOrDefault(item => item.Name == subscription))
                    ?? throw new InvalidOperationException($"Subscription '{subscription}' of {topic} was not found. Refresh and try again.");
        return found.IsConfirmed
            ? found
            : throw new InvalidOperationException($"Subscription '{subscription}' is pending confirmation, so it has no filter policy yet.");
    }

    private async Task SetAttributeAsync(string subscriptionArn, string name, string value, CancellationToken cancellationToken)
    {
        try
        {
            await Sns.SetSubscriptionAttributesAsync(new Sns.SetSubscriptionAttributesRequest
            {
                SubscriptionArn = subscriptionArn,
                AttributeName = name,
                AttributeValue = value
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonSimpleNotificationServiceException exception) when (exception.ErrorCode is "InvalidParameter" or "InvalidParameterException")
        {
            throw new InvalidOperationException($"SNS did not accept the filter policy: {exception.Message}", exception);
        }
    }
}
