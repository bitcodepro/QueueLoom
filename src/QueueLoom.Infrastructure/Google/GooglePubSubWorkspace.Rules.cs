using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using QueueLoom.Core.Routing;

namespace QueueLoom.Infrastructure.Google;

/// <summary>
/// Pub/Sub subscription filters as rules: a subscription has at most one, named "filter"; without one it receives
/// every message. Pub/Sub fixes the filter when the subscription is created, so QueueLoom only reads it.
/// </summary>
public sealed partial class GooglePubSubWorkspace
{
    internal const string FilterRule = "filter";

    public override bool SupportsSubscriptionRules => true;

    public override RoutingService RoutingService => RoutingService.PubSub;

    public override string? RuleEditingNote =>
        "Pub/Sub cannot change a subscription's filter after it is created; create a new subscription with the filter instead.";

    public override Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        ReadRulesAsync<IReadOnlyList<SubscriptionRules>>(async token =>
        {
            var result = new List<SubscriptionRules>();
            await foreach (var subscription in Subscriber.ListSubscriptionsAsync(new ProjectName(_projectId)).WithCancellation(token)
                               .ConfigureAwait(false))
            {
                if (subscription.Topic != TopicResource(topic).ToString())
                {
                    continue;
                }
                result.Add(ToRules(subscription));
            }
            return result.OrderBy(item => item.Subscription, StringComparer.Ordinal).ToArray();
        }, cancellationToken);

    internal static SubscriptionRules ToRules(Subscription subscription) =>
        new(subscription.SubscriptionName.SubscriptionId, string.IsNullOrWhiteSpace(subscription.Filter)
            ? []
            : [new SubscriptionRule(FilterRule, RuleFilterKind.PubSubFilter) { Expression = subscription.Filter, Title = "Filter" }])
        {
            Service = RoutingService.PubSub,
            Problem = subscription.Detached ? "Detached from the topic: it receives no new messages." : null
        };
}
