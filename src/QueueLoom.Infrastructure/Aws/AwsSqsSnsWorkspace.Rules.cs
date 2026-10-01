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

    public override bool SupportsSubscriptionRules => true;

    public override RoutingService RoutingService => RoutingService.Sns;

    public override Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        ReadRulesAsync<IReadOnlyList<SubscriptionRules>>(async token =>
        {
            var info = await ReadTopicAsync(FindTopicArn(topic), token).ConfigureAwait(false);
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
            if (current.FilterPolicyOnBody != rule.OnMessageBody && current.FilterPolicy is not null)
            {
                // The scope decides how SNS reads the policy, so it changes first and the new policy follows.
                await SetAttributeAsync(current.Arn, "FilterPolicyScope", scope, token).ConfigureAwait(false);
                await SetAttributeAsync(current.Arn, "FilterPolicy", rule.Expression, token).ConfigureAwait(false);
                return;
            }
            await SetAttributeAsync(current.Arn, "FilterPolicy", rule.Expression, token).ConfigureAwait(false);
            if (current.FilterPolicyOnBody != rule.OnMessageBody)
            {
                await SetAttributeAsync(current.Arn, "FilterPolicyScope", scope, token).ConfigureAwait(false);
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
        var info = await ReadTopicAsync(FindTopicArn(topic), cancellationToken).ConfigureAwait(false);
        var found = info.Subscriptions.FirstOrDefault(item => item.Name == subscription)
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
