using Google.Api.Gax.Grpc;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
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

    /// <summary>
    /// Every subscription attached to the topic, whichever project it lives in. A topic's subscriptions may belong to
    /// other projects, which a project listing (projects.subscriptions.list) never shows, so a topic read only through
    /// it could look as if it had no subscriptions and dropped every message. projects.topics.subscriptions.list
    /// "Lists the names of the attached subscriptions on this topic"
    /// (https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics.subscriptions/list), paged with
    /// nextPageToken, which the client library's paged enumerable follows.
    /// </summary>
    public override Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        ReadRulesAsync<IReadOnlyList<SubscriptionRules>>(async token =>
        {
            var topicName = TopicResource(topic);
            var result = new List<SubscriptionRules>();
            var listed = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var name in Publisher.ListTopicSubscriptionsAsync(topicName).WithCancellation(token).ConfigureAwait(false))
            {
                if (!listed.Add(name))
                {
                    continue;
                }
                var parsed = SubscriptionName.TryParse(name, out var subscriptionName) ? subscriptionName : null;
                var local = parsed?.ProjectId == _projectId;
                var display = local ? parsed!.SubscriptionId : name;
                var elsewhere = local ? null : $"In project {parsed?.ProjectId ?? "unknown"}; QueueLoom shows it but does not manage it.";
                try
                {
                    var subscription = await Subscriber.GetSubscriptionAsync(new GetSubscriptionRequest { Subscription = name },
                        CallSettings.FromCancellationToken(token)).ConfigureAwait(false);
                    result.Add(ToRules(subscription, display) with { Note = elsewhere });
                }
                catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound)
                {
                    // Deleted between the listing and this read: it no longer takes messages.
                }
                catch (RpcException exception) when (exception.StatusCode == StatusCode.PermissionDenied)
                {
                    // Attached, so Pub/Sub delivers to it, but its filter cannot be read with these credentials.
                    result.Add(new SubscriptionRules(display, [])
                    {
                        Service = RoutingService.PubSub,
                        Note = elsewhere,
                        Unreadable = "Attached to the topic, but QueueLoom may not read its filter, so only Pub/Sub knows whether it takes the message."
                    });
                }
            }
            await AddDetachedAsync(topicName, listed, result, token).ConfigureAwait(false);
            return result.OrderBy(item => item.Subscription, StringComparer.Ordinal).ToArray();
        }, cancellationToken);

    /// <summary>
    /// The topic listing names attached subscriptions only. Detached ones of this project still point at the topic and
    /// are shown, as before, with the reason they receive nothing; this is best effort and skipped without permission.
    /// </summary>
    private async Task AddDetachedAsync(TopicName topic, HashSet<string> listed, List<SubscriptionRules> result, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var subscription in Subscriber.ListSubscriptionsAsync(new ProjectName(_projectId)).WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                if (subscription.Detached && subscription.Topic == topic.ToString() && listed.Add(subscription.Name))
                {
                    result.Add(ToRules(subscription, subscription.SubscriptionName.SubscriptionId));
                }
            }
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.PermissionDenied)
        {
        }
    }

    internal static SubscriptionRules ToRules(Subscription subscription, string name) =>
        new(name, string.IsNullOrWhiteSpace(subscription.Filter)
            ? []
            : [new SubscriptionRule(FilterRule, RuleFilterKind.PubSubFilter) { Expression = subscription.Filter, Title = "Filter" }])
        {
            Service = RoutingService.PubSub,
            Problem = subscription.Detached ? "Detached from the topic: it receives no new messages." : null
        };
}
