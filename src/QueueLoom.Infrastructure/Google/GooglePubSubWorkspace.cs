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
    // What QueueLoom last observed about each source subscription (its dead-letter policy, or why it could not read
    // it) and each dead-letter topic (its attached readers, or why it could not list them). Whether a deletion may go
    // ahead is decided per binding (source, topic, reader) from these facts, so a fact observed through any reader or
    // by any check (sharing, a replaced reader, a changed policy, a failed read) applies to every binding it
    // contradicts, while one reader's eligibility never makes another reader eligible. See Observe for ordering.
    private readonly Dictionary<string, SourceObservation> _sourceObservations = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TopicObservation> _topicObservations = new(StringComparer.Ordinal);
    private long _observationClock;

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
        lock (_topicObservations)
        {
            _sourceObservations.Clear();
            _topicObservations.Clear();
        }
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
        var started = Interlocked.Increment(ref _observationClock);
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
        var index = GooglePubSubTopology.Build(_projectId, topicIds, subscriptions, TimeProvider.GetUtcNow(),
            undelivered?.ToDictionary(pair => pair.Key, pair => pair.Value.Count, StringComparer.Ordinal),
            undelivered?.ToDictionary(pair => pair.Key, pair => pair.Value.At, StringComparer.Ordinal));
        _subscriptions = index.Subscriptions;
        _deadLetterReaders = index.DeadLetterReaders;
        // This project's subscriptions are listed completely, so their policies are observations. Their readers are not
        // (another project may read a topic too): only sharing seen here is recorded; one local reader proves nothing.
        foreach (var subscription in subscriptions.Where(subscription => subscription.SubscriptionName.ProjectId == _projectId))
        {
            Observe(_sourceObservations, subscription.SubscriptionName.SubscriptionId,
                new SourceObservation(started, null, subscription.DeadLetterPolicy?.DeadLetterTopic is { Length: > 0 } policy ? policy : null));
        }
        foreach (var (topic, readers) in index.SharedDeadLetterTopics)
        {
            Observe(_topicObservations, topic, new TopicObservation(started, SharedRefusal(topic, readers), []));
        }
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

        // Several subscriptions on the dead-letter topic each hold a copy, and some may be another consumer's: what is
        // acknowledged through one stays in the others. Such a topic is viewed, never acknowledged through.
        return new PubSubChannel(this, source, subQueue, reader.SubscriptionName, belongsTo: subscription.SubscriptionName)
        {
            DeadLetterTopic = deadLetterTopic
        };
    }

    private static string SharedRefusal(string topic, IReadOnlyCollection<string> readers) =>
        $"Dead-letter topic '{GooglePubSubTopology.TopicIdOf(topic)}' has {readers.Count:N0} subscriptions ({string.Join(", ", readers)}). " +
        "Each keeps its own copy and some may belong to another consumer, so QueueLoom does not delete or move its messages; " +
        "it only shows them. Keep one subscription on the topic to manage them here.";

    /// <summary>A source subscription's dead-letter topic as observed (null: none), or why it could not be read.</summary>
    private sealed record SourceObservation(long Started, string? Problem, string? Policy);

    /// <summary>A dead-letter topic's attached readers as observed, or why they could not be listed.</summary>
    private sealed record TopicObservation(long Started, string? Problem, string[] Readers)
    {
        public bool SameFacts(TopicObservation other) => Problem == other.Problem && Readers.SequenceEqual(other.Readers);
    }

    /// <summary>
    /// Records an observation. One that began after the recorded one replaces it. One that began earlier but reports
    /// different facts (it finished late) never replaces newer facts silently: the scope is marked as contradicted, so
    /// nothing is admitted on it until an observation that begins later settles it. Same facts change nothing.
    /// </summary>
    private void Observe<T>(Dictionary<string, T> observations, string key, T observation) where T : class
    {
        lock (_topicObservations)
        {
            if (!observations.TryGetValue(key, out var recorded) || Started(observation) > Started(recorded))
            {
                observations[key] = observation;
            }
            else if (!SameFacts(observation, recorded))
            {
                observations[key] = Contradicted(recorded, observation);
            }
        }
    }

    private static long Started(object observation) => observation switch
    {
        SourceObservation source => source.Started,
        TopicObservation topic => topic.Started,
        _ => throw new ArgumentException("Unknown observation.", nameof(observation))
    };

    private static bool SameFacts(object first, object second) => (first, second) switch
    {
        (SourceObservation a, SourceObservation b) => a.Problem == b.Problem && a.Policy == b.Policy,
        (TopicObservation a, TopicObservation b) => a.SameFacts(b),
        _ => false
    };

    private static T Contradicted<T>(T recorded, T late) where T : class
    {
        const string changed = "was observed differently by checks running at the same time. Refresh and try again; nothing was deleted or moved.";
        return (recorded, late) switch
        {
            (SourceObservation a, SourceObservation b) => (T)(object)(a with { Problem = b.Problem ?? a.Problem ?? $"The dead-letter policy of this subscription {changed}" }),
            (TopicObservation a, TopicObservation b) => (T)(object)(a with { Problem = b.Problem ?? a.Problem ?? $"The readers of this dead-letter topic {changed}" }),
            _ => recorded
        };
    }

    /// <summary>What a deletion of a source's dead letters reads: the dead-letter topic and the subscription read from.</summary>
    private readonly record struct DeadLetterBinding(string Source, string Topic, SubscriptionName Reader);

    /// <summary>Why a deletion bound to (source, topic, reader) must not go ahead now, from the latest facts; null if it may.</summary>
    internal string? DeadLetterRefusal(string source, string topic, SubscriptionName reader)
    {
        lock (_topicObservations)
        {
            return Refusal(new DeadLetterBinding(source, topic, reader),
                _sourceObservations.GetValueOrDefault(source), _topicObservations.GetValueOrDefault(topic));
        }
    }

    /// <summary>Whether these facts allow a deletion bound to <paramref name="binding"/>; null if they do.</summary>
    private static string? Refusal(DeadLetterBinding binding, SourceObservation? source, TopicObservation? topic)
    {
        var topicId = GooglePubSubTopology.TopicIdOf(binding.Topic);
        if (source is null)
        {
            return $"QueueLoom has not checked the dead letters of '{binding.Source}' yet. Nothing was deleted or moved.";
        }
        if (source.Problem is { } sourceProblem)
        {
            return sourceProblem;
        }
        if (!string.Equals(source.Policy, binding.Topic, StringComparison.Ordinal))
        {
            return source.Policy is null
                ? $"Subscription '{binding.Source}' has no dead-letter topic now. Refresh and try again; nothing was deleted or moved."
                : $"The dead-letter topic of '{binding.Source}' is now '{GooglePubSubTopology.TopicIdOf(source.Policy)}', not '{topicId}'. " +
                  "Refresh and try again; nothing was deleted or moved.";
        }
        if (topic is null)
        {
            return $"QueueLoom has not checked dead-letter topic '{topicId}' yet. Nothing was deleted or moved.";
        }
        if (topic.Problem is { } topicProblem)
        {
            return topicProblem;
        }
        if (topic.Readers.Length > 1)
        {
            return SharedRefusal(binding.Topic, topic.Readers);
        }
        if (topic.Readers.Length == 0 || !string.Equals(topic.Readers[0], binding.Reader.ToString(), StringComparison.Ordinal))
        {
            return $"Dead-letter topic '{topicId}' is no longer read by '{binding.Reader.SubscriptionId}'" +
                   (topic.Readers.Length == 0 ? " (no subscription reads it now)" : $" but by '{topic.Readers[0]}'") +
                   ". Refresh and try again; nothing was deleted or moved.";
        }
        return null;
    }

    /// <summary>
    /// Right before a deletion or a move, observes each source against the service, bound to what the operation will
    /// actually read: the open channel's dead-letter topic and reader, or (for a move, which opens its channel later)
    /// the ones the cached topology names when the check starts. The source's current policy must still name that
    /// topic, and the topic must have exactly one attached subscription, that very reader, in any project and over every
    /// page. What a check observes decides its own outcome; once every source passed, every binding is judged again
    /// from the latest facts, so anything another check or a refresh observed meanwhile counts too.
    /// </summary>
    protected override async Task ValidateDeletionAsync(IReadOnlyList<DeletionTarget> targets, CancellationToken cancellationToken)
    {
        // Bindings are taken before anything is awaited, so a refresh during the check cannot change what is checked.
        var bindings = targets
            .Where(target => target.SubQueue == ServiceBusSubQueue.DeadLetter)
            .Select(target => BindingOf(target))
            .Distinct()
            .ToArray();
        foreach (var binding in bindings)
        {
            var (source, topic) = await ObserveAsync(binding, cancellationToken).ConfigureAwait(false);
            if (Refusal(binding, source, topic) is { } own)
            {
                throw new InvalidOperationException(own);
            }
        }
        foreach (var binding in bindings)
        {
            if (DeadLetterRefusal(binding.Source, binding.Topic, binding.Reader) is { } recorded)
            {
                throw new InvalidOperationException(recorded);
            }
        }
    }

    private DeadLetterBinding BindingOf(DeletionTarget target)
    {
        if (target.Channel is PubSubChannel { DeadLetterTopic: { } openTopic } channel)
        {
            return new DeadLetterBinding(target.Source.Name, openTopic, channel.ReaderSubscription);
        }
        if (target.Source.TopicName is not null &&
            _subscriptions.TryGetValue((target.Source.TopicName, target.Source.Name), out var subscription) &&
            subscription.DeadLetterPolicy?.DeadLetterTopic is { Length: > 0 } topic &&
            _deadLetterReaders.TryGetValue(topic, out var reader))
        {
            return new DeadLetterBinding(target.Source.Name, topic, reader.SubscriptionName);
        }
        throw new InvalidOperationException(
            $"QueueLoom has not read where the dead letters of '{target.Source.Name}' are yet. Refresh and try again; nothing was deleted or moved.");
    }

    /// <summary>
    /// Reads the source's subscription and, when its policy still names the binding's topic, lists that topic's
    /// readers; records both observations and returns them (the topic one is null when it was not listed).
    /// </summary>
    private async Task<(SourceObservation Source, TopicObservation? Topic)> ObserveAsync(DeadLetterBinding binding, CancellationToken cancellationToken)
    {
        // Each read is ordered by when it was issued: a read issued later reflects at least as recent a state.
        var started = Interlocked.Increment(ref _observationClock);
        SourceObservation source;
        try
        {
            var live = await Subscriber.GetSubscriptionAsync(SubscriptionName.FromProjectSubscription(_projectId, binding.Source), cancellationToken)
                .ConfigureAwait(false);
            source = new SourceObservation(started, null, live.DeadLetterPolicy?.DeadLetterTopic is { Length: > 0 } policy ? policy : null);
        }
        catch (RpcException exception)
        {
            source = new SourceObservation(started,
                $"QueueLoom could not read subscription '{binding.Source}' ({exception.Status.StatusCode}), so it cannot check where its " +
                "dead letters are. Nothing was deleted or moved.", null);
        }
        Observe(_sourceObservations, binding.Source, source);
        if (source.Problem is not null || !string.Equals(source.Policy, binding.Topic, StringComparison.Ordinal))
        {
            return (source, null);
        }

        TopicObservation topic;
        started = Interlocked.Increment(ref _observationClock);
        try
        {
            var readers = new List<string>();
            await foreach (var name in Publisher.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = binding.Topic })
                               .WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                readers.Add(name);
            }
            topic = new TopicObservation(started, null, [.. readers.Order(StringComparer.Ordinal)]);
        }
        catch (RpcException exception)
        {
            topic = new TopicObservation(started,
                $"QueueLoom could not list the subscriptions of dead-letter topic '{GooglePubSubTopology.TopicIdOf(binding.Topic)}' " +
                $"({exception.Status.StatusCode}), so it cannot tell whether another consumer reads it. Nothing was deleted or moved.", []);
        }
        Observe(_topicObservations, binding.Topic, topic);
        return (source, topic);
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
        if (QueueLoom.Core.Validation.MessageDraftValidator.PubSubPublishProblem(pubsubMessage.Data.IsEmpty, pubsubMessage.Attributes.Keys) is { } refused)
        {
            throw new DeliveryRejectedException(refused + " Nothing was sent.");
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
    private async Task<IReadOnlyDictionary<string, (long Count, DateTimeOffset At)>?> ReadUndeliveredCountsAsync(CancellationToken cancellationToken)
    {
        if (_metrics is null)
        {
            return null;
        }

        try
        {
            var now = TimeProvider.GetUtcNow();
            var counts = new Dictionary<string, (long Count, DateTimeOffset At)>(StringComparer.Ordinal);
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
                    // Points come newest first. The point's own time is kept: it can be minutes older than this read.
                    var point = item.Points[0];
                    counts[subscriptionId] = (Math.Max(0, point.Value.Int64Value),
                        point.Interval?.EndTime?.ToDateTimeOffset() ?? now);
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

        /// <summary>Why messages read here must not be acknowledged, when they must not; they are released instead.</summary>
        public string? SettleRefusal => DeadLetterTopic is null
            ? null
            : owner.DeadLetterRefusal(source.Name, DeadLetterTopic, subscription);

        /// <summary>The dead-letter topic read here, when this channel reads one.</summary>
        public string? DeadLetterTopic { get; init; }

        /// <summary>The subscription this channel reads from.</summary>
        public SubscriptionName ReaderSubscription => subscription;

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
            if (SettleRefusal is not null)
            {
                await ReleaseAsync(messages, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException(SettleRefusal);
            }

            // Chunks acknowledged before a failure are gone; only the rest are reported as not settled.
            var acknowledged = 0;
            foreach (var chunk in messages.Chunk(1_000))
            {
                try
                {
                    await owner.Subscriber.AcknowledgeAsync(subscription, chunk.Select(message => message.LeaseHandle), cancellationToken)
                        .ConfigureAwait(false);
                    acknowledged += chunk.Length;
                }
                catch (RpcException exception)
                {
                    // An exactly-once subscription names the ack IDs that failed; the rest of that request were acknowledged.
                    // Without such details the whole chunk counts as not settled.
                    var failedIds = FailedAckIds(exception);
                    var unsettled = failedIds is null ? chunk : chunk.Where(message => failedIds.Contains(message.LeaseHandle));
                    return unsettled.Concat(messages.Skip(acknowledged + chunk.Length)).ToList();
                }
            }
            return [];
        }

        /// <summary>
        /// The ack IDs an exactly-once Acknowledge reports as failed (TRANSIENT_FAILURE_* or PERMANENT_FAILURE_* in its
        /// ErrorInfo metadata, as Google.Cloud.PubSub.V1's own AckError reads them), or null when it reports none.
        /// </summary>
        private static HashSet<string>? FailedAckIds(RpcException exception)
        {
            var failed = exception.GetErrorInfo()?.Metadata
                .Where(entry => entry.Value.StartsWith("TRANSIENT_FAILURE_", StringComparison.Ordinal) ||
                                entry.Value.StartsWith("PERMANENT_FAILURE_", StringComparison.Ordinal))
                .Select(entry => entry.Key)
                .ToHashSet(StringComparer.Ordinal);
            return failed is { Count: > 0 } ? failed : null;
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
    /// <summary>Dead-letter topics read by more than one subscription, with those subscriptions' names.</summary>
    public IReadOnlyDictionary<string, string[]> SharedDeadLetterTopics { get; init; } = new Dictionary<string, string[]>();

    public static GooglePubSubTopology Build(
        string projectId,
        IEnumerable<string> topicIds,
        IReadOnlyCollection<Subscription> subscriptions,
        DateTimeOffset fetchedAt,
        IReadOnlyDictionary<string, long>? undelivered = null,
        IReadOnlyDictionary<string, DateTimeOffset>? measuredAt = null)
    {
        // Detached subscriptions receive nothing and hold no backlog: they are shown, but never read or counted as readers.
        var readersByTopic = subscriptions
            .Where(subscription => !subscription.Detached)
            .GroupBy(subscription => subscription.Topic, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.OrderBy(item => item.Name, StringComparer.Ordinal).ToArray(),
                StringComparer.Ordinal);
        var byTopic = subscriptions
            .GroupBy(subscription => subscription.Topic, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.OrderBy(item => item.Name, StringComparer.Ordinal).ToArray(),
                StringComparer.Ordinal);
        // The subscription QueueLoom reads a dead-letter topic through: the first one by name.
        var deadLetterReaders = subscriptions
            .Select(subscription => subscription.DeadLetterPolicy?.DeadLetterTopic)
            .Where(topic => !string.IsNullOrEmpty(topic) && readersByTopic.ContainsKey(topic))
            .Distinct(StringComparer.Ordinal)
            .ToDictionary(topic => topic!, topic => readersByTopic[topic!][0], StringComparer.Ordinal);
        var sharedDeadLetterTopics = deadLetterReaders.Keys
            .Where(topic => readersByTopic[topic].Length > 1)
            .ToDictionary(topic => topic, topic => readersByTopic[topic].Select(item => item.SubscriptionName.SubscriptionId).ToArray(),
                StringComparer.Ordinal);
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
                        ? sharedDeadLetterTopics.ContainsKey(deadLetterTopic)
                            ? $"Dead letters go to {deadLetterTopicId}, which several subscriptions read: read-only here"
                            : $"Dead letters go to {deadLetterTopicId}"
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

                var runtime = Runtime(subscription, hasDeadLetters ? deadLetterReaders[deadLetterTopic!] : null);
                if (!string.IsNullOrEmpty(deadLetterTopic) && !hasDeadLetters)
                {
                    // Configured, but nothing reads the dead-letter topic: its count is unknown, never zero.
                    runtime = runtime with { DeadLetterCountError = "Its dead-letter topic has no subscription to read or count it from" };
                }
                return new ServiceBusSubscription(
                    topicId,
                    subscriptionId,
                    runtime,
                    subscription.State == Subscription.Types.State.Active
                        ? ServiceBusEntityStatus.Active
                        : ServiceBusEntityStatus.Unknown)
                {
                    HasDeadLetterQueue = !string.IsNullOrEmpty(deadLetterTopic),
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
        return new GooglePubSubTopology(topology, index, deadLetterReaders) { SharedDeadLetterTopics = sharedDeadLetterTopics };

        // Active: the subscription's undelivered messages. Dead letters: those of the subscription reading its
        // dead-letter topic, which covers every subscription that shares the topic.
        ServiceBusEntityRuntime Runtime(Subscription subscription, Subscription? deadLetterReader)
        {
            // Dead letters are counted through the reader subscription; a different reader is a different measurement.
            var reader = deadLetterReader?.SubscriptionName.SubscriptionId;
            // Project-qualified: the same subscription ID in another project is another reader.
            var readerName = deadLetterReader?.SubscriptionName.ToString();
            if (undelivered is null)
            {
                return new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty)
                {
                    CountsUnavailable = true,
                    HasTransferDeadLetterCount = false,
                    DeadLetterCountSource = readerName
                };
            }

            // A dead-letter reader that Cloud Monitoring reported no series for has no count yet (a new subscription, or
            // a gap in the metric): it is counted by reading, like a project without Monitoring, never taken as zero.
            if (reader is not null && !undelivered.ContainsKey(reader))
            {
                return new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty)
                {
                    CountsUnavailable = true,
                    HasTransferDeadLetterCount = false,
                    DeadLetterCountSource = readerName
                };
            }

            return new ServiceBusEntityRuntime(new ServiceBusMessageCounts(
                active: undelivered.GetValueOrDefault(subscription.SubscriptionName.SubscriptionId),
                deadLetter: deadLetterReader is null ? 0 : undelivered[deadLetterReader.SubscriptionName.SubscriptionId]))
            {
                HasTransferDeadLetterCount = false,
                // Cloud Monitoring's num_undelivered_messages is sampled and delayed: an estimate, as of its point's time.
                CountsAreEstimates = true,
                DeadLetterCountSource = readerName,
                DeadLetterCountMeasuredAt = reader is not null && measuredAt?.TryGetValue(reader, out var at) == true ? at : null
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
