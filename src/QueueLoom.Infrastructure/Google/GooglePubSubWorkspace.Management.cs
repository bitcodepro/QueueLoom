using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Google;

/// <summary>
/// Creating, changing and deleting Pub/Sub subscriptions. Pub/Sub has no queues: a subscription is what holds
/// messages, and its dead letters go to a dead-letter topic, read through a subscription of its own.
/// </summary>
public sealed partial class GooglePubSubWorkspace
{
    private const string DeadLetterEnding = "-dead-letter";

    public override QueueManagementCapabilities? QueueManagement => new(
        "subscription",
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration,
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration,
        CanCreateDeadLetterQueue: true,
        UpdateNote: "Deliveries before dead-lettering apply only when the subscription has a dead-letter topic. " +
                    "In Google Cloud, the Pub/Sub service agent needs permission to publish to that topic.")
    {
        ManagesSubscriptions = true
    };

    public override async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        var subscription = await Subscriber.GetSubscriptionAsync(SubscriptionName.FromProjectSubscription(_projectId, queue), cancellationToken)
            .ConfigureAwait(false);
        return new QueueSettings(
            subscription.MessageRetentionDuration?.ToTimeSpan(),
            subscription.DeadLetterPolicy is { MaxDeliveryAttempts: > 0 } policy ? policy.MaxDeliveryAttempts : null,
            subscription.AckDeadlineSeconds > 0 ? TimeSpan.FromSeconds(subscription.AckDeadlineSeconds) : null);
    }

    public override Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(definition);
            var topic = definition.TopicName ?? throw new InvalidOperationException("Select the topic the subscription reads from.");
            var subscription = new Subscription
            {
                SubscriptionName = SubscriptionName.FromProjectSubscription(_projectId, definition.Name),
                TopicAsTopicName = TopicResource(topic)
            };
            Apply(subscription, definition.Settings);
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetterTopic = TopicName.FromProjectTopic(_projectId, definition.Name + DeadLetterEnding);
                await CreateIfMissingAsync(() => Publisher.CreateTopicAsync(deadLetterTopic, token)).ConfigureAwait(false);
                // Dead letters are read through a subscription, so the topic gets one with the same name.
                await CreateIfMissingAsync(() => Subscriber.CreateSubscriptionAsync(new Subscription
                {
                    SubscriptionName = SubscriptionName.FromProjectSubscription(_projectId, definition.Name + DeadLetterEnding),
                    TopicAsTopicName = deadLetterTopic,
                    MessageRetentionDuration = Duration.FromTimeSpan(TimeSpan.FromDays(7))
                }, token)).ConfigureAwait(false);
                subscription.DeadLetterPolicy = new DeadLetterPolicy
                {
                    DeadLetterTopic = deadLetterTopic.ToString(),
                    MaxDeliveryAttempts = definition.Settings.MaxDeliveryCount ?? 5
                };
            }
            else if (definition.Settings.MaxDeliveryCount is not null)
            {
                throw new InvalidOperationException("Deliveries before dead-lettering need a dead-letter topic. Tick the dead-letter option.");
            }

            try
            {
                await Subscriber.CreateSubscriptionAsync(subscription, token).ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
            {
                throw new InvalidOperationException($"A subscription named '{definition.Name}' already exists.", exception);
            }
        }, cancellationToken);

    public override Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(settings);
            var name = SubscriptionName.FromProjectSubscription(_projectId, queue);
            var current = await Subscriber.GetSubscriptionAsync(name, token).ConfigureAwait(false);
            var subscription = new Subscription { SubscriptionName = name };
            var mask = new FieldMask();
            Apply(subscription, settings);
            if (settings.MessageTimeToLive is not null)
            {
                mask.Paths.Add("message_retention_duration");
            }
            if (settings.LockDuration is not null)
            {
                mask.Paths.Add("ack_deadline_seconds");
            }
            if (settings.MaxDeliveryCount is { } attempts)
            {
                if (current.DeadLetterPolicy is not { DeadLetterTopic.Length: > 0 } policy)
                {
                    throw new InvalidOperationException($"Subscription {queue} has no dead-letter topic, so deliveries before dead-lettering cannot be set.");
                }
                subscription.DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = policy.DeadLetterTopic, MaxDeliveryAttempts = attempts };
                mask.Paths.Add("dead_letter_policy");
            }
            if (mask.Paths.Count > 0)
            {
                await Subscriber.UpdateSubscriptionAsync(new UpdateSubscriptionRequest { Subscription = subscription, UpdateMask = mask }, token)
                    .ConfigureAwait(false);
            }
        }, cancellationToken);

    /// <summary>Deletes the subscription only; its messages go with it, the topic and any dead-letter topic stay.</summary>
    public override Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        ManageAsync(token => Subscriber.DeleteSubscriptionAsync(SubscriptionName.FromProjectSubscription(_projectId, queue), token), cancellationToken);

    private static void Apply(Subscription subscription, QueueSettings settings)
    {
        if (settings.MessageTimeToLive is { } retention)
        {
            // projects.subscriptions messageRetentionDuration: "Cannot be more than 31 days or less than 10 minutes."
            if (retention < TimeSpan.FromMinutes(10) || retention > TimeSpan.FromDays(31))
            {
                throw new InvalidOperationException("Pub/Sub keeps unacknowledged messages for 10 minutes to 31 days.");
            }
            subscription.MessageRetentionDuration = Duration.FromTimeSpan(retention);
        }
        if (settings.LockDuration is { } deadline)
        {
            if (deadline < TimeSpan.FromSeconds(10) || deadline > TimeSpan.FromSeconds(600))
            {
                throw new InvalidOperationException("The acknowledgement deadline must be 10 to 600 seconds.");
            }
            subscription.AckDeadlineSeconds = (int)deadline.TotalSeconds;
        }
        if (settings.MaxDeliveryCount is < 5 or > 100)
        {
            throw new InvalidOperationException("Pub/Sub allows 5 to 100 delivery attempts before dead-lettering.");
        }
    }

    private static async Task CreateIfMissingAsync(Func<Task> create)
    {
        try
        {
            await create().ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
        {
        }
    }
}
