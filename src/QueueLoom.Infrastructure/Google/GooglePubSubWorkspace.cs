using System.Text;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Api.Gax.ResourceNames;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Monitoring = Google.Cloud.Monitoring.V3;
using WellKnownTypes = Google.Protobuf.WellKnownTypes;
using Google.Protobuf;
using Grpc.Core;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Google;

/// <summary>
/// Topics and subscriptions of one Google Cloud project. Pub/Sub has no queues. The dead letters of a
/// subscription are read from a subscription attached to the dead-letter topic of its dead-letter policy.
/// </summary>
public sealed partial class GooglePubSubWorkspace : LeasedMessagingWorkspace
{
    /// <summary>How long a pulled message stays unacknowledged while QueueLoom looks at it (Pub/Sub allows up to 600).</summary>
    internal const int HoldSeconds = 180;
    private const int MaximumBatch = 100;
    private static readonly TimeSpan PullWait = TimeSpan.FromSeconds(2);

    // Pub/Sub adds these to a message it forwards to a dead-letter topic.
    internal const string DeadLetterSourceSubscription = "CloudPubSubDeadLetterSourceSubscription";
    internal const string DeadLetterSourceSubscriptionProject = "CloudPubSubDeadLetterSourceSubscriptionProject";
    internal const string DeadLetterSourceDeliveryCount = "CloudPubSubDeadLetterSourceDeliveryCount";
    private const string DeadLetterAttributePrefix = "CloudPubSubDeadLetter";

    private readonly ISecretVault _secretVault;
    private PublisherServiceApiClient? _publisher;
    private SubscriberServiceApiClient? _subscriber;
    private Monitoring.MetricServiceClient? _metrics;
    private string _projectId = string.Empty;
    private IReadOnlyDictionary<(string Topic, string Subscription), Subscription> _subscriptions =
        new Dictionary<(string, string), Subscription>();
    private IReadOnlyDictionary<string, Subscription> _deadLetterReaders = new Dictionary<string, Subscription>();

    public GooglePubSubWorkspace(
        ISecretVault secretVault,
        TimeProvider? timeProvider = null,
        DeadLetterJsonBackupStore? backupStore = null)
        : base(backupStore, timeProvider)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
    }

    public override MessagingProvider Provider => MessagingProvider.GooglePubSub;

    protected override async Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        var settings = profile.GooglePubSub ?? throw new InvalidOperationException("The Google Cloud project is missing.");
        var publisherBuilder = new PublisherServiceApiClientBuilder();
        var subscriberBuilder = new SubscriberServiceApiClientBuilder();

        if (!string.IsNullOrWhiteSpace(settings.EmulatorHost))
        {
            // The emulator speaks plain gRPC and ignores credentials.
            publisherBuilder.Endpoint = subscriberBuilder.Endpoint = settings.EmulatorHost.Trim();
            publisherBuilder.ChannelCredentials = subscriberBuilder.ChannelCredentials = ChannelCredentials.Insecure;
        }
        else if (profile.Authentication.Kind == AuthenticationKind.GoogleServiceAccountKey)
        {
            var json = await _secretVault.RetrieveForProfileAsync(profile, ProfileSecretKind.ConnectionString, cancellationToken)
                .ConfigureAwait(false);
            var credential = ParseServiceAccountKey(json);
            publisherBuilder.GoogleCredential = subscriberBuilder.GoogleCredential = credential;
        }

        _publisher = await publisherBuilder.BuildAsync(cancellationToken).ConfigureAwait(false);
        _subscriber = await subscriberBuilder.BuildAsync(cancellationToken).ConfigureAwait(false);
        // Counts come from Cloud Monitoring. The emulator has none, and without monitoring.timeSeries.list
        // permission the counts fall back to reading dead letters, as before.
        _metrics = string.IsNullOrWhiteSpace(settings.EmulatorHost)
            ? await new Monitoring.MetricServiceClientBuilder { GoogleCredential = publisherBuilder.GoogleCredential }
                .BuildAsync(cancellationToken).ConfigureAwait(false)
            : null;
        _projectId = settings.ProjectId.Trim();

        // Proves the endpoint, the credentials and pubsub.topics.list without touching messages.
        await foreach (var _ in _publisher.ListTopicsAsync(new ProjectName(_projectId), pageSize: 1)
                           .AsRawResponses()
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            break;
        }
    }

    protected override ValueTask CloseAsync()
    {
        _publisher = null;
        _subscriber = null;
        _metrics = null;
        _subscriptions = new Dictionary<(string, string), Subscription>();
        _deadLetterReaders = new Dictionary<string, Subscription>();
        return ValueTask.CompletedTask;
    }

    protected override async Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken)
    {
        var project = new ProjectName(_projectId);
        var topicIds = new List<string>();
        await foreach (var topic in Publisher.ListTopicsAsync(project).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            topicIds.Add(topic.TopicName.TopicId);
        }

        var subscriptions = new List<Subscription>();
        await foreach (var subscription in Subscriber.ListSubscriptionsAsync(project).WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            subscriptions.Add(subscription);
        }

        var undelivered = await ReadUndeliveredCountsAsync(cancellationToken).ConfigureAwait(false);
        var index = GooglePubSubTopology.Build(_projectId, topicIds, subscriptions, TimeProvider.GetUtcNow(), undelivered);
        _subscriptions = index.Subscriptions;
        _deadLetterReaders = index.DeadLetterReaders;
        return index.Topology;
    }

    protected override ILeasedMessageChannel OpenChannel(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        if (source.Kind != ServiceBusEntityKind.Subscription)
        {
            throw new InvalidOperationException("Google Pub/Sub keeps messages in subscriptions only. Pick a subscription.");
        }
        if (subQueue == ServiceBusSubQueue.TransferDeadLetter)
        {
            throw new InvalidOperationException("Google Pub/Sub has no transfer dead-letter queues.");
        }
        if (!_subscriptions.TryGetValue((source.TopicName!, source.Name), out var subscription))
        {
            throw new InvalidOperationException($"Subscription '{source.DisplayName}' was not found. Refresh and try again.");
        }
        if (subQueue == ServiceBusSubQueue.Active)
        {
            return new PubSubChannel(this, source, subQueue, subscription.SubscriptionName, belongsTo: null);
        }

        var deadLetterTopic = subscription.DeadLetterPolicy?.DeadLetterTopic;
        if (string.IsNullOrEmpty(deadLetterTopic))
        {
            throw new InvalidOperationException(
                $"Subscription '{source.Name}' has no dead-letter topic. Add a dead-letter policy to it in Google Cloud first.");
        }
        if (!_deadLetterReaders.TryGetValue(deadLetterTopic, out var reader))
        {
            throw new InvalidOperationException(
                $"Dead-letter topic '{GooglePubSubTopology.TopicIdOf(deadLetterTopic)}' has no subscription, so its messages cannot be read. " +
                "Create a subscription on it first; messages published before that are not kept.");
        }

        return new PubSubChannel(this, source, subQueue, reader.SubscriptionName, belongsTo: subscription.SubscriptionName);
    }

    protected override async Task SendCoreAsync(
        ServiceBusTopology topology,
        ServiceBusEntityReference destination,
        MessageDraft message,
        CancellationToken cancellationToken)
    {
        if (destination.Kind != ServiceBusEntityKind.Topic)
        {
            throw new InvalidOperationException("Google Pub/Sub messages are published to topics.");
        }
        if (message.Properties.ScheduledEnqueueTime is not null)
        {
            throw new InvalidOperationException("Google Pub/Sub cannot schedule messages. Clear the scheduled time.");
        }

        var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFrom(message.Body.GetBytes()) };
        foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(message.Properties))
        {
            pubsubMessage.Attributes[name] = value;
        }
        foreach (var property in message.ApplicationProperties)
        {
            pubsubMessage.Attributes[property.Name] = property.Value;
        }
        var orderingKey = new[] { message.Properties.SessionId, message.Properties.PartitionKey }
            .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));
        if (orderingKey is not null)
        {
            pubsubMessage.OrderingKey = orderingKey;
        }

        await Publisher.PublishAsync(TopicResource(destination.Name), [pubsubMessage], cancellationToken)
            .ConfigureAwait(false);
    }

    private TopicName TopicResource(string topic) => TopicName.TryParse(topic, out var resource)
        ? resource : new TopicName(_projectId, topic);

    /// <summary>
    /// The latest pubsub.googleapis.com/subscription/num_undelivered_messages per subscription ID, or null when
    /// Cloud Monitoring is not available (emulator, missing permission). The metric trails by a minute or two.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, long>?> ReadUndeliveredCountsAsync(CancellationToken cancellationToken)
    {
        if (_metrics is null)
        {
            return null;
        }

        try
        {
            var now = TimeProvider.GetUtcNow();
            var counts = new Dictionary<string, long>(StringComparer.Ordinal);
            var series = _metrics.ListTimeSeriesAsync(
                new ProjectName(_projectId),
                "metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.type = \"pubsub_subscription\"",
                new Monitoring.TimeInterval
                {
                    StartTime = WellKnownTypes.Timestamp.FromDateTimeOffset(now.AddMinutes(-10)),
                    EndTime = WellKnownTypes.Timestamp.FromDateTimeOffset(now)
                },
                Monitoring.ListTimeSeriesRequest.Types.TimeSeriesView.Full);
            await foreach (var item in series.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (item.Resource.Labels.TryGetValue("subscription_id", out var subscriptionId) && item.Points.Count > 0)
                {
                    // Points come newest first.
                    counts[subscriptionId] = Math.Max(0, item.Points[0].Value.Int64Value);
                }
            }
            return counts;
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.PermissionDenied or StatusCode.Unauthenticated
                                                 or StatusCode.Unavailable or StatusCode.NotFound or StatusCode.InvalidArgument)
        {
            return null;
        }
    }

    internal static BrowsedMessage ToBrowsedMessage(
        ReceivedMessage received,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        var message = received.Message;
        var attributes = message.Attributes.ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);
        var properties = MessageAttributeConventions.ReadStandardAttributes(
            attributes,
            message.MessageId,
            sessionId: string.IsNullOrEmpty(message.OrderingKey) ? null : message.OrderingKey);

        var sourceSubscription = attributes.GetValueOrDefault(DeadLetterSourceSubscription);
        var deliveryCount = int.TryParse(attributes.GetValueOrDefault(DeadLetterSourceDeliveryCount), out var sourceCount)
            ? sourceCount
            : received.DeliveryAttempt;
        var isDeadLetter = subQueue == ServiceBusSubQueue.DeadLetter;
        return new BrowsedMessage(
            source,
            subQueue,
            LeasedMessageIdentity.SequenceNumberFor(message.MessageId),
            message.Data.Memory,
            properties,
            attributes
                // Pub/Sub's own dead-letter bookkeeping must not travel with a resent copy.
                .Where(item => !item.Key.StartsWith(DeadLetterAttributePrefix, StringComparison.Ordinal))
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => new MessageApplicationProperty(item.Key, ApplicationPropertyType.String, item.Value)),
            ServiceBusMessageState.Active,
            deliveryCount: Math.Max(0, deliveryCount),
            enqueuedAt: message.PublishTime?.ToDateTimeOffset(),
            deadLetterReason: isDeadLetter ? "Exceeded the maximum delivery attempts" : null,
            deadLetterErrorDescription: isDeadLetter && !string.IsNullOrEmpty(sourceSubscription)
                ? $"From {sourceSubscription} after {deliveryCount} delivery attempts"
                : null)
        { HasSequenceNumber = false };
    }

    internal static GoogleCredential ParseServiceAccountKey(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new InvalidOperationException("This environment has no saved service account key.");
        }

        try
        {
            return CredentialFactory.FromJson<ServiceAccountCredential>(json)
                .ToGoogleCredential()
                .CreateScoped(PublisherServiceApiClient.DefaultScopes);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or Newtonsoft.Json.JsonException)
        {
            throw new InvalidOperationException(
                "The saved key is not a Google service account key. Download a JSON key for a service account and enter it again.",
                exception);
        }
    }

    private PublisherServiceApiClient Publisher =>
        _publisher ?? throw new InvalidOperationException("Connect to an environment first.");

    private SubscriberServiceApiClient Subscriber =>
        _subscriber ?? throw new InvalidOperationException("Connect to an environment first.");

    private sealed class PubSubChannel(
        GooglePubSubWorkspace owner,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        SubscriptionName subscription,
        SubscriptionName? belongsTo) : ILeasedMessageChannel
    {
        public string PhysicalName => subscription.SubscriptionId;

        public int MaximumBatchSize => MaximumBatch;

        public async Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            PullResponse response;
            try
            {
                response = await owner.Subscriber.PullAsync(
                        new PullRequest { SubscriptionAsSubscriptionName = subscription, MaxMessages = Math.Clamp(maxMessages, 1, MaximumBatch) },
                        CallSettings.FromCancellationToken(cancellationToken)
                            .WithExpiration(Expiration.FromTimeout(PullWait)))
                    .ConfigureAwait(false);
            }
            catch (RpcException exception) when (exception.StatusCode == StatusCode.DeadlineExceeded)
            {
                return [];
            }

            if (response.ReceivedMessages.Count == 0)
            {
                return [];
            }

            // The subscription's own ack deadline can be as short as 10 seconds; hold messages longer.
            await owner.Subscriber.ModifyAckDeadlineAsync(
                    subscription,
                    response.ReceivedMessages.Select(message => message.AckId),
                    HoldSeconds,
                    cancellationToken)
                .ConfigureAwait(false);

            return response.ReceivedMessages
                .Select(message => new LeasedMessage(
                    ToBrowsedMessage(message, source, subQueue),
                    message.AckId,
                    BelongsToSource(message.Message)))
                .ToArray();
        }

        public async Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            foreach (var chunk in messages.Chunk(1_000))
            {
                await owner.Subscriber.ModifyAckDeadlineAsync(
                        subscription,
                        chunk.Select(message => message.LeaseHandle),
                        0,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        public async Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(
            IReadOnlyCollection<LeasedMessage> messages,
            CancellationToken cancellationToken)
        {
            if (messages.Count == 0)
            {
                return [];
            }

            try
            {
                foreach (var chunk in messages.Chunk(1_000))
                {
                    await owner.Subscriber.AcknowledgeAsync(subscription, chunk.Select(message => message.LeaseHandle), cancellationToken)
                        .ConfigureAwait(false);
                }
                return [];
            }
            catch (RpcException)
            {
                return messages;
            }
        }

        private bool BelongsToSource(PubsubMessage message)
        {
            if (belongsTo is null)
            {
                return true;
            }
            if (!message.Attributes.TryGetValue(DeadLetterSourceSubscription, out var sourceSubscription) ||
                string.IsNullOrEmpty(sourceSubscription)) return false;
            message.Attributes.TryGetValue(DeadLetterSourceSubscriptionProject, out var sourceProject);
            if (!string.IsNullOrEmpty(sourceProject) && sourceProject != belongsTo.ProjectId) return false;
            if (SubscriptionName.TryParse(sourceSubscription, out var resource))
                return resource == belongsTo;
            return sourceSubscription == belongsTo.SubscriptionId && sourceProject == belongsTo.ProjectId;
        }
    }
}

internal sealed record GooglePubSubTopology(
    ServiceBusTopology Topology,
    IReadOnlyDictionary<(string Topic, string Subscription), Subscription> Subscriptions,
    IReadOnlyDictionary<string, Subscription> DeadLetterReaders)
{
    public static GooglePubSubTopology Build(
        string projectId,
        IEnumerable<string> topicIds,
        IReadOnlyCollection<Subscription> subscriptions,
        DateTimeOffset fetchedAt,
        IReadOnlyDictionary<string, long>? undelivered = null)
    {
        var byTopic = subscriptions
            .GroupBy(subscription => subscription.Topic, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.OrderBy(item => item.Name, StringComparer.Ordinal).ToArray(),
                StringComparer.Ordinal);
        // The subscription QueueLoom reads a dead-letter topic through: the first one by name.
        var deadLetterReaders = subscriptions
            .Select(subscription => subscription.DeadLetterPolicy?.DeadLetterTopic)
            .Where(topic => !string.IsNullOrEmpty(topic) && byTopic.ContainsKey(topic))
            .Distinct(StringComparer.Ordinal)
            .ToDictionary(topic => topic!, topic => byTopic[topic!][0], StringComparer.Ordinal);
        var deadLetterUsers = subscriptions
            .Where(subscription => !string.IsNullOrEmpty(subscription.DeadLetterPolicy?.DeadLetterTopic))
            .GroupBy(subscription => subscription.DeadLetterPolicy.DeadLetterTopic, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Select(item => item.SubscriptionName.SubscriptionId).ToArray(),
                StringComparer.Ordinal);

        var index = new Dictionary<(string, string), Subscription>();
        var topics = new List<ServiceBusTopic>();
        // Subscriptions may belong to a topic of another project or to a deleted topic; show them too.
        var allTopics = topicIds.Select(topicId => new TopicName(projectId, topicId).ToString())
            .Concat(byTopic.Keys)
            .Distinct(StringComparer.Ordinal)
            .Select(resource => (Id: TopicName.TryParse(resource, out var parsed) && parsed.ProjectId == projectId
                ? parsed.TopicId : resource, Resource: resource))
            .OrderBy(topic => topic.Id, StringComparer.Ordinal);

        foreach (var (topicId, resource) in allTopics)
        {
            var topicSubscriptions = byTopic
                .Where(pair => pair.Key == resource)
                .SelectMany(pair => pair.Value)
                .ToArray();

            var mapped = topicSubscriptions.Select(subscription =>
            {
                var subscriptionId = subscription.SubscriptionName.SubscriptionId;
                index[(topicId, subscriptionId)] = subscription;
                var deadLetterTopic = subscription.DeadLetterPolicy?.DeadLetterTopic;
                var hasDeadLetters = !string.IsNullOrEmpty(deadLetterTopic) && deadLetterReaders.ContainsKey(deadLetterTopic);
                string? note = null;
                if (!string.IsNullOrEmpty(deadLetterTopic))
                {
                    var deadLetterTopicId = TopicIdOf(deadLetterTopic);
                    note = hasDeadLetters
                        ? $"Dead letters go to {deadLetterTopicId}"
                        : $"Dead letters go to {deadLetterTopicId}, which has no subscription to read them from";
                }
                else if (deadLetterUsers.TryGetValue(resource, out var users))
                {
                    note = $"Holds dead letters of {string.Join(", ", users)}";
                }
                else if (subscription.PushConfig?.PushEndpoint is { Length: > 0 } endpoint)
                {
                    note = $"Pushes to {endpoint}";
                }

                return new ServiceBusSubscription(
                    topicId,
                    subscriptionId,
                    Runtime(subscription, hasDeadLetters ? deadLetterReaders[deadLetterTopic!] : null),
                    subscription.State == Subscription.Types.State.Active
                        ? ServiceBusEntityStatus.Active
                        : ServiceBusEntityStatus.Unknown)
                {
                    HasDeadLetterQueue = hasDeadLetters,
                    Note = note
                };
            }).ToArray();

            topics.Add(new ServiceBusTopic(
                topicId,
                new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty)
                {
                    CountsUnavailable = undelivered is null,
                    HasTransferDeadLetterCount = false
                },
                mapped,
                ServiceBusEntityStatus.Active));
        }

        // Without Cloud Monitoring there are no counts; scans and monitors then count dead letters by reading them.
        var topology = new ServiceBusTopology(fetchedAt, topics: topics)
        {
            SupportsTransferDeadLetter = false,
            HasMessageCounts = undelivered is not null,
            UsesSampledCounts = undelivered is null
        };
        return new GooglePubSubTopology(topology, index, deadLetterReaders);

        // Active: the subscription's undelivered messages. Dead letters: those of the subscription reading its
        // dead-letter topic, which covers every subscription that shares the topic.
        ServiceBusEntityRuntime Runtime(Subscription subscription, Subscription? deadLetterReader)
        {
            if (undelivered is null)
            {
                return new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty)
                {
                    CountsUnavailable = true,
                    HasTransferDeadLetterCount = false
                };
            }

            return new ServiceBusEntityRuntime(new ServiceBusMessageCounts(
                active: undelivered.GetValueOrDefault(subscription.SubscriptionName.SubscriptionId),
                deadLetter: deadLetterReader is null ? 0 : undelivered.GetValueOrDefault(deadLetterReader.SubscriptionName.SubscriptionId)))
            {
                HasTransferDeadLetterCount = false
            };
        }
    }

    /// <summary>
    /// The topic ID of "projects/p/topics/id". Subscriptions whose topic was deleted point at "_deleted-topic_",
    /// which is kept as is so they still show up (under that name) and can be drained.
    /// </summary>
    internal static string TopicIdOf(string resource) =>
        TopicName.TryParse(resource, out var parsed) && !string.IsNullOrEmpty(parsed.TopicId) ? parsed.TopicId : resource;
}
