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
        ManagesSubscriptions = true,
        // projects.subscriptions: messageRetentionDuration 10 minutes to 31 days, ackDeadlineSeconds 10 to 600,
        // deadLetterPolicy.maxDeliveryAttempts 5 to 100.
        Limits = new QueueSettingLimits("Google Pub/Sub")
        {
            MinTimeToLive = TimeSpan.FromMinutes(10),
            MaxTimeToLive = TimeSpan.FromDays(31),
            TimeToLiveName = "retention",
            MinDeliveryCount = 5,
            MaxDeliveryCount = 100,
            DeliveryCountName = "number of delivery attempts",
            MinLock = TimeSpan.FromSeconds(10),
            MaxLock = TimeSpan.FromSeconds(600),
            LockName = "acknowledgement deadline"
        }
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
            if (!definition.CreateDeadLetterQueue && definition.Settings.MaxDeliveryCount is not null)
            {
                throw new InvalidOperationException("Deliveries before dead-lettering need a dead-letter topic. Tick the dead-letter option.");
            }

            // projects.subscriptions.create "returns ALREADY_EXISTS" for a taken name, but only after the dead-letter
            // topic and subscription would have been made. A taken name is refused here, before anything is created.
            await EnsureSubscriptionMissingAsync(subscription.SubscriptionName, token).ConfigureAwait(false);
            var created = new List<string>();
            var mainAttempted = false;
            try
            {
                if (definition.CreateDeadLetterQueue)
                {
                    var deadLetterTopic = TopicName.FromProjectTopic(_projectId, definition.Name + DeadLetterEnding);
                    if (await CreateIfMissingAsync(() => Publisher.CreateTopicAsync(deadLetterTopic, token)).ConfigureAwait(false))
                    {
                        created.Add($"dead-letter topic {deadLetterTopic}");
                    }
                    // Dead letters are read through a subscription, so the topic gets one with the same name.
                    var deadLetterSubscription = SubscriptionName.FromProjectSubscription(_projectId, definition.Name + DeadLetterEnding);
                    if (await CreateIfMissingAsync(() => Subscriber.CreateSubscriptionAsync(new Subscription
                        {
                            SubscriptionName = deadLetterSubscription,
                            TopicAsTopicName = deadLetterTopic,
                            MessageRetentionDuration = Duration.FromTimeSpan(TimeSpan.FromDays(7))
                        }, token)).ConfigureAwait(false))
                    {
                        created.Add($"dead-letter subscription {deadLetterSubscription}");
                    }
                    subscription.DeadLetterPolicy = new DeadLetterPolicy
                    {
                        DeadLetterTopic = deadLetterTopic.ToString(),
                        MaxDeliveryAttempts = definition.Settings.MaxDeliveryCount ?? 5
                    };
                }

                mainAttempted = true;
                await Subscriber.CreateSubscriptionAsync(subscription, token).ConfigureAwait(false);
            }
            catch (RpcException exception) when (!mainAttempted && !IsDefiniteRejection(exception.StatusCode))
            {
                // The dead-letter setup failed before the subscription itself was requested, so this call did not create
                // it, whatever a read-back would find (another operator may be creating the same name meanwhile).
                var leftovers = created.Count == 0 ? string.Empty : $" Created before that: {string.Join(" and ", created)}.";
                throw new InvalidOperationException(
                    $"Pub/Sub did not create the subscription '{definition.Name}': setting up its dead-letter topic failed " +
                    $"({exception.Status.Detail}), and whether that step took effect is unknown.{leftovers} Check the " +
                    $"'{definition.Name}{DeadLetterEnding}' topic and subscription in Google Cloud before retrying.", exception);
            }
            catch (RpcException exception) when (!IsDefiniteRejection(exception.StatusCode))
            {
                // A timeout, a cancellation or a lost response does not say whether Pub/Sub created the subscription:
                // a read-back decides. Present with the requested topic is done; absent is a definite non-creation.
                Subscription? existing = null;
                var known = true;
                try
                {
                    existing = await Subscriber.GetSubscriptionAsync(subscription.SubscriptionName, CancellationToken.None).ConfigureAwait(false);
                }
                catch (RpcException readBack) when (readBack.StatusCode == StatusCode.NotFound)
                {
                }
                catch (RpcException)
                {
                    known = false;
                }
                if (existing is not null)
                {
                    if (HasRequestedConfiguration(existing, subscription))
                    {
                        return;
                    }
                    // Someone else created the same name meanwhile with other settings: it is theirs, not changed here.
                    throw new InvalidOperationException(
                        $"Whether Pub/Sub created the subscription '{definition.Name}' is unknown ({exception.Status.Detail}): a " +
                        "subscription with that name exists now, but not with the requested topic, dead-letter policy, " +
                        "retention or acknowledgement deadline, so it was probably created by someone else. It was left " +
                        "unchanged; check it in Google Cloud before deleting anything.", exception);
                }
                var leftovers = created.Count == 0 ? string.Empty : $" Created before that: {string.Join(" and ", created)}.";
                throw new InvalidOperationException(known
                    ? $"Pub/Sub did not create the subscription '{definition.Name}' ({exception.Status.Detail}).{leftovers}" +
                      (created.Count == 0 ? string.Empty : " Delete them in Google Cloud if nothing else uses them.")
                    : $"Whether Pub/Sub created the subscription '{definition.Name}' is unknown ({exception.Status.Detail}), and it " +
                      $"could not be read back.{leftovers} Check the subscription in Google Cloud before deleting anything: those " +
                      "resources may already serve it.", exception);
            }
            catch (RpcException exception)
            {
                // What this call created is never deleted here: another operator creating the same subscription at the
                // same moment adopts the same "-dead-letter" topic and subscription (they already exist for it), so
                // ownership cannot be proven and deleting them could break their dead-lettering. They are named instead.
                var reason = exception.StatusCode == StatusCode.AlreadyExists
                    ? $"A subscription named '{definition.Name}' already exists."
                    : $"Pub/Sub did not create the subscription '{definition.Name}': {exception.Status.Detail}";
                throw new InvalidOperationException(created.Count == 0
                    ? reason
                    : $"{reason} Created before that and left in place: {string.Join(" and ", created)}. " +
                      "Delete them in Google Cloud if nothing else uses them.", exception);
            }
        }, cancellationToken);

    /// <summary>Whether a read-back subscription carries everything this request asked for.</summary>
    private static bool HasRequestedConfiguration(Subscription existing, Subscription requested) =>
        existing.Topic == requested.Topic
        && (requested.AckDeadlineSeconds == 0 || existing.AckDeadlineSeconds == requested.AckDeadlineSeconds)
        && (requested.MessageRetentionDuration is null || Equals(existing.MessageRetentionDuration, requested.MessageRetentionDuration))
        && (requested.DeadLetterPolicy is null
            ? existing.DeadLetterPolicy is null
            : existing.DeadLetterPolicy is { } policy && policy.DeadLetterTopic == requested.DeadLetterPolicy.DeadLetterTopic
              && policy.MaxDeliveryAttempts == requested.DeadLetterPolicy.MaxDeliveryAttempts);

    /// <summary>Pub/Sub answered and refused the request, so nothing was created by it.</summary>
    private static bool IsDefiniteRejection(StatusCode code) => code is StatusCode.AlreadyExists or StatusCode.InvalidArgument
        or StatusCode.PermissionDenied or StatusCode.NotFound or StatusCode.FailedPrecondition or StatusCode.Unauthenticated
        or StatusCode.OutOfRange or StatusCode.ResourceExhausted or StatusCode.Unimplemented;

    private async Task EnsureSubscriptionMissingAsync(SubscriptionName name, CancellationToken cancellationToken)
    {
        try
        {
            await Subscriber.GetSubscriptionAsync(name, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound)
        {
            return;
        }
        throw new InvalidOperationException($"A subscription named '{name.SubscriptionId}' already exists.");
    }

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

    /// <summary>True when this call created it; false when it already existed (and is adopted, not owned).</summary>
    private static async Task<bool> CreateIfMissingAsync(Func<Task> create)
    {
        try
        {
            await create().ConfigureAwait(false);
            return true;
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.AlreadyExists)
        {
            return false;
        }
    }
}
