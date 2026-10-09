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
    // Attribute keys must be non-empty and must not begin with 'goog' (case-insensitive).
    private const string ReservedAttributePrefix = "goog";

    private readonly ISecretVault _secretVault;
    private PublisherServiceApiClient? _publisher;
    private SubscriberServiceApiClient? _subscriber;
    private Monitoring.MetricServiceClient? _metrics;
    // gRPC channels the builders created for this connection (an emulator or a service account key; the default
    // credentials share GAX's channel pool instead). A dropped client does not close its channel, so each one is
    // shut down when the connection closes; otherwise every reconnect would leave a connection open.
    private readonly List<ChannelBase> _channels = [];
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
        TrackChannel(publisherBuilder.LastCreatedChannel);
        _subscriber = await subscriberBuilder.BuildAsync(cancellationToken).ConfigureAwait(false);
        TrackChannel(subscriberBuilder.LastCreatedChannel);
        // Counts come from Cloud Monitoring. The emulator has none, and without monitoring.timeSeries.list
        // permission the counts fall back to reading dead letters, as before.
        if (string.IsNullOrWhiteSpace(settings.EmulatorHost))
        {
            var metricsBuilder = new Monitoring.MetricServiceClientBuilder { GoogleCredential = publisherBuilder.GoogleCredential };
            _metrics = await metricsBuilder.BuildAsync(cancellationToken).ConfigureAwait(false);
            TrackChannel(metricsBuilder.LastCreatedChannel);
        }
        else
        {
            _metrics = null;
        }
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

    protected override async ValueTask CloseAsync()
    {
        _publisher = null;
        _subscriber = null;
        _metrics = null;
        _subscriptions = new Dictionary<(string, string), Subscription>();
        _deadLetterReaders = new Dictionary<string, Subscription>();
        var channels = _channels.ToArray();
        _channels.Clear();
        foreach (var channel in channels)
        {
            try
            {
                await channel.ShutdownAsync().ConfigureAwait(false);
                // Grpc.Net.Client's GrpcChannel keeps its HTTP/2 connection until it is disposed; ShutdownAsync alone
                // does not release it.
                (channel as IDisposable)?.Dispose();
            }
            catch (Exception exception) when (exception is InvalidOperationException or ObjectDisposedException or RpcException)
            {
                // Closing goes on: the other channels are still shut down.
            }
        }
    }

    private void TrackChannel(ChannelBase? channel)
    {
        if (channel is not null && !_channels.Contains(channel))
        {
            _channels.Add(channel);
        }
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
        if (QueueLoom.Core.Validation.MessageSizeLimits.Check(message, MessagingProvider.GooglePubSub) is { } tooLarge)
        {
            throw new DeliveryRejectedException(tooLarge + " Nothing was sent.");
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

        // The whole serialized request (topic and framing included) must stay within Pub/Sub's ceiling; refusing here
        // keeps an oversized message a proven non-delivery instead of an RPC failure of unknown outcome.
        var topic = TopicResource(destination.Name);
        var requestSize = new PublishRequest { Topic = topic.ToString(), Messages = { pubsubMessage } }.CalculateSize();
        if (requestSize > QueueLoom.Core.Validation.MessageSizeLimits.PubSubMaximumRequestBytes)
        {
            throw new DeliveryRejectedException(
                $"Google Pub/Sub accepts at most {QueueLoom.Core.Validation.MessageSizeLimits.PubSubMaximumRequestBytes:N0} bytes " +
                $"per publish request; this one is {requestSize:N0} bytes. Nothing was sent.");
        }

        await Publisher.PublishAsync(topic, [pubsubMessage], cancellationToken)
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
                // Pub/Sub's own dead-letter bookkeeping must not travel with a resent copy, nor attributes Pub/Sub adds
                // itself (googclient_schemaname and the like): publishing a key that begins with "goog" is refused.
                .Where(item => !item.Key.StartsWith(DeadLetterAttributePrefix, StringComparison.Ordinal) &&
                               !item.Key.StartsWith(ReservedAttributePrefix, StringComparison.OrdinalIgnoreCase))
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
            await HoldAsync(response.ReceivedMessages.Select(message => message.AckId).ToArray(), cancellationToken)
                .ConfigureAwait(false);

            return response.ReceivedMessages
                .Select(message => new LeasedMessage(
                    ToBrowsedMessage(message, source, subQueue),
                    message.AckId,
                    BelongsToSource(message.Message)))
                .ToArray();
        }

        /// <summary>
        /// Extends the hold on messages just pulled. Pulling already counted a delivery attempt, so a transient failure is
        /// retried rather than dropping the batch: dropped, the messages would come back only after the subscription's
        /// ack deadline, and be pulled (and counted) again. If the hold cannot be set, they are released at once.
        /// </summary>
        private async Task HoldAsync(string[] ackIds, CancellationToken cancellationToken)
        {
            // One cleanup scope covers every attempt and every back-off: a cancellation during a delay must release the
            // batch too, since the caller never received these ack IDs and cannot release them itself.
            try
            {
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        await owner.Subscriber.ModifyAckDeadlineAsync(subscription, ackIds, HoldSeconds, cancellationToken)
                            .ConfigureAwait(false);
                        return;
                    }
                    catch (RpcException exception) when (attempt < HoldAttempts && IsTransient(exception.StatusCode)
                                                         && !cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt), owner.TimeProvider, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception exception) when (exception is RpcException or OperationCanceledException)
            {
                try
                {
                    // Returned now instead of after the ack deadline; with a dead-letter policy this is the same
                    // one delivery attempt the pull already counted.
                    await owner.Subscriber.ModifyAckDeadlineAsync(subscription, ackIds, 0, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (RpcException)
                {
                    // They return by themselves when the subscription's ack deadline passes.
                }
                if (exception is OperationCanceledException)
                {
                    throw;
                }
                // Cancelled while the hold was failing: the caller asked to stop, so that is what it is told.
                cancellationToken.ThrowIfCancellationRequested();
                throw new InvalidOperationException(
                    $"Pub/Sub did not hold the {ackIds.Length:N0} message(s) just read on '{subscription.SubscriptionId}' " +
                    $"({((RpcException)exception).Status.Detail}); they were returned to the subscription unchanged.", exception);
            }
        }

        private const int HoldAttempts = 3;

        private static bool IsTransient(StatusCode code) =>
            code is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Internal or StatusCode.Aborted;

        public async Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            // A 0 deadline is a NACK; with a dead-letter policy each one is a delivery attempt (see MaxDeliveryAttempts).
            // A chunk that fails does not stop the others: they would stay held until their deadline (up to 180 s).
            var unreleased = 0;
            var reasons = new List<string>();
            foreach (var chunk in messages.Chunk(1_000))
            {
                try
                {
                    await owner.Subscriber.ModifyAckDeadlineAsync(
                            subscription,
                            chunk.Select(message => message.LeaseHandle),
                            0,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (RpcException exception) when (!cancellationToken.IsCancellationRequested)
                {
                    unreleased += chunk.Length;
                    reasons.Add(exception.Status.Detail);
                }
            }
            if (unreleased > 0)
            {
                throw new IOException(
                    $"{unreleased:N0} message(s) read from '{subscription.SubscriptionId}' could not be returned to the subscription " +
                    $"({string.Join(", ", reasons.Distinct().Take(3))}); they become available again when their acknowledgement " +
                    $"deadline ({HoldSeconds} s) ends. Nothing was lost.");
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

            // Chunks acknowledged before a failure are gone; only the rest are reported as not settled.
            var acknowledged = 0;
            try
            {
                foreach (var chunk in messages.Chunk(1_000))
                {
                    await owner.Subscriber.AcknowledgeAsync(subscription, chunk.Select(message => message.LeaseHandle), cancellationToken)
                        .ConfigureAwait(false);
                    acknowledged += chunk.Length;
                }
                return [];
            }
            catch (RpcException)
            {
                return messages.Skip(acknowledged).ToList();
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
                    MaxDeliveryCount = MaxDeliveryAttempts(subscription),
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
    /// The dead-letter policy's maxDeliveryAttempts, or null without a dead-letter topic. Pub/Sub only counts delivery
    /// attempts then, and a NACK (ModifyAckDeadline with a 0 deadline, which is how QueueLoom releases what it read)
    /// adds one: deliveryAttempt is "1 + (number of NACKs) + (number of ack_deadline exceeds)" (Pub/Sub REST reference,
    /// ReceivedMessage.deliveryAttempt; "Handle message failures", dead-letter topics). An unset value (0) means the
    /// documented default of 5.
    /// </summary>
    internal static int? MaxDeliveryAttempts(Subscription subscription) =>
        string.IsNullOrEmpty(subscription.DeadLetterPolicy?.DeadLetterTopic)
            ? null
            : subscription.DeadLetterPolicy.MaxDeliveryAttempts > 0 ? subscription.DeadLetterPolicy.MaxDeliveryAttempts : 5;

    /// <summary>
    /// The topic ID of "projects/p/topics/id". Subscriptions whose topic was deleted point at "_deleted-topic_",
    /// which is kept as is so they still show up (under that name) and can be drained.
    /// </summary>
    internal static string TopicIdOf(string resource) =>
        TopicName.TryParse(resource, out var parsed) && !string.IsNullOrEmpty(parsed.TopicId) ? parsed.TopicId : resource;
}
