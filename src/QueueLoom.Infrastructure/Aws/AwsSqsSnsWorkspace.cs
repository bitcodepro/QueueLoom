using System.Globalization;
using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Infrastructure.Aws;

/// <summary>
/// Amazon SQS queues and Amazon SNS topics of one account and region, shown the way QueueLoom shows
/// Service Bus: SQS queues are queues, SNS topics are topics and SNS subscriptions are subscriptions.
/// The dead-letter queue of a queue or subscription is the SQS queue named in its redrive policy.
/// </summary>
public sealed partial class AwsSqsSnsWorkspace : LeasedMessagingWorkspace
{
    /// <summary>How long a received message stays invisible while QueueLoom looks at it.</summary>
    internal const int HoldSeconds = 180;
    private const int MaximumBatch = 10;

    private readonly ISecretVault _secretVault;
    private AmazonSQSClient? _sqs;
    private AmazonSimpleNotificationServiceClient? _sns;
    private AwsTopologyIndex _index = AwsTopologyIndex.Empty;

    public AwsSqsSnsWorkspace(
        ISecretVault secretVault,
        TimeProvider? timeProvider = null,
        DeadLetterJsonBackupStore? backupStore = null)
        : base(backupStore, timeProvider)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
    }

    public override MessagingProvider Provider => MessagingProvider.AmazonSqsSns;

    protected override async Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        var settings = profile.Aws ?? throw new InvalidOperationException("The AWS region is missing.");
        var credentials = await ResolveCredentialsAsync(profile, cancellationToken).ConfigureAwait(false);
        var region = RegionEndpoint.GetBySystemName(settings.Region);

        var sqsConfig = new AmazonSQSConfig { RegionEndpoint = region };
        var snsConfig = new AmazonSimpleNotificationServiceConfig { RegionEndpoint = region };
        if (!string.IsNullOrWhiteSpace(settings.ServiceUrl))
        {
            sqsConfig.ServiceURL = settings.ServiceUrl;
            sqsConfig.AuthenticationRegion = settings.Region;
            snsConfig.ServiceURL = settings.ServiceUrl;
            snsConfig.AuthenticationRegion = settings.Region;
        }
        if (credentials is null && !string.IsNullOrWhiteSpace(settings.ProfileName))
        {
            sqsConfig.Profile = new Profile(settings.ProfileName.Trim());
            snsConfig.Profile = new Profile(settings.ProfileName.Trim());
        }

        _sqs = credentials is null ? new AmazonSQSClient(sqsConfig) : new AmazonSQSClient(credentials, sqsConfig);
        _sns = credentials is null
            ? new AmazonSimpleNotificationServiceClient(snsConfig)
            : new AmazonSimpleNotificationServiceClient(credentials, snsConfig);

        // Proves the endpoint, the credentials and the sqs:ListQueues permission without touching messages.
        await _sqs.ListQueuesAsync(new ListQueuesRequest { MaxResults = 1 }, cancellationToken).ConfigureAwait(false);
    }

    protected override ValueTask CloseAsync()
    {
        _sqs?.Dispose();
        _sns?.Dispose();
        _sqs = null;
        _sns = null;
        _index = AwsTopologyIndex.Empty;
        _shownSubscriptions.Clear();
        return ValueTask.CompletedTask;
    }

    protected override async Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken)
    {
        var sqs = Sqs;
        var queueUrls = new List<string>();
        string? nextToken = null;
        do
        {
            var page = await sqs.ListQueuesAsync(
                    new ListQueuesRequest { MaxResults = 1000, NextToken = nextToken },
                    cancellationToken)
                .ConfigureAwait(false);
            queueUrls.AddRange(page.QueueUrls ?? []);
            nextToken = page.NextToken;
        }
        while (!string.IsNullOrEmpty(nextToken));

        // A queue deleted between ListQueues and its GetQueueAttributes (by another tool, a test, a cleanup job) is
        // simply gone: it is left out, and the other queues are listed. Any other failure still fails the refresh.
        var queues = (await Task.WhenAll(queueUrls.Select(url => ReadQueueAsync(url, cancellationToken)))
                .ConfigureAwait(false))
            .OfType<AwsQueueInfo>()
            .ToArray();
        var topics = await ReadTopicsAsync(cancellationToken).ConfigureAwait(false);
        _index = new AwsTopologyIndex(queues, topics);
        return _index.ToTopology(TimeProvider.GetUtcNow());
    }

    protected override ILeasedMessageChannel OpenChannel(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        if (subQueue == ServiceBusSubQueue.TransferDeadLetter)
        {
            throw new InvalidOperationException("Amazon SQS has no transfer dead-letter queues.");
        }

        var index = _index;
        if (source.Kind == ServiceBusEntityKind.Queue)
        {
            var queue = index.FindQueue(source.Name)
                ?? throw new InvalidOperationException($"Queue '{source.Name}' was not found. Refresh and try again.");
            if (subQueue == ServiceBusSubQueue.Active)
            {
                return new SqsChannel(this, source, subQueue, queue, belongsTo: null);
            }

            var deadLetterQueue = index.FindQueueByArn(queue.DeadLetterTargetArn)
                ?? throw new InvalidOperationException(
                    $"Queue '{queue.Name}' has no dead-letter queue. Add a redrive policy to it in AWS first.");
            // A dead-letter queue can serve several queues; SQS stamps each moved message with its source.
            return new SqsChannel(this, source, subQueue, deadLetterQueue, belongsTo: queue.Arn);
        }

        if (source.Kind == ServiceBusEntityKind.Subscription)
        {
            var subscription = index.FindSubscription(source.TopicName!, source.Name)
                ?? throw new InvalidOperationException($"Subscription '{source.DisplayName}' was not found. Refresh and try again.");
            if (subQueue == ServiceBusSubQueue.Active)
            {
                var endpointQueue = subscription.Protocol == "sqs" ? index.FindQueueByArn(subscription.Endpoint) : null;
                return endpointQueue is null
                    ? throw new InvalidOperationException(
                        $"SNS does not store messages for {subscription.Protocol} endpoints, so there is nothing to browse. " +
                        "Only SQS subscriptions in this account and region can be browsed.")
                    : new SqsChannel(this, source, subQueue, endpointQueue, belongsTo: null, snsEnvelope: !subscription.RawMessageDelivery);
            }

            var deadLetterQueue = index.FindQueueByArn(subscription.DeadLetterTargetArn)
                ?? throw new InvalidOperationException(
                    $"Subscription '{source.DisplayName}' has no dead-letter queue. Add a redrive policy to it in AWS first.");
            return new SqsChannel(this, source, subQueue, deadLetterQueue, belongsTo: subscription.Arn,
                snsEnvelope: !subscription.RawMessageDelivery);
        }

        throw new ArgumentException("Only queues and subscriptions hold messages.", nameof(source));
    }

    protected override async Task SendCoreAsync(
        ServiceBusTopology topology,
        ServiceBusEntityReference destination,
        MessageDraft message,
        CancellationToken cancellationToken)
    {
        var body = AwsMessageMapper.BodyText(message);
        if (destination.Kind == ServiceBusEntityKind.Queue)
        {
            var queue = _index.FindQueue(destination.Name)
                ?? throw new InvalidOperationException($"Queue '{destination.Name}' was not found. Refresh and try again.");
            var request = new Amazon.SQS.Model.SendMessageRequest
            {
                QueueUrl = queue.Url,
                MessageBody = body,
                MessageAttributes = AwsMessageMapper.ToSqsAttributes(message)
            };
            AwsMessageMapper.EnsureAttributesAccepted(request.MessageAttributes.Keys, sqs: true);
            MessageSizeLimits.EnsureWithin(MessageSizeLimits.AwsSize(message), queue.MaximumMessageSize, $"Amazon SQS queue '{queue.Name}'");
            if (queue.IsFifo)
            {
                RejectFutureScheduling(message, "Amazon SQS FIFO queues");
                request.MessageGroupId = AwsMessageMapper.GroupId(message);
                request.MessageDeduplicationId = AwsMessageMapper.DeduplicationId(message);
                AwsMessageMapper.EnsureFifoIdentifiers(request.MessageGroupId, request.MessageDeduplicationId);
            }
            else if (AwsMessageMapper.DelaySeconds(message, TimeProvider.GetUtcNow()) is { } delay)
            {
                request.DelaySeconds = delay;
            }

            await Sqs.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (destination.Kind == ServiceBusEntityKind.Topic)
        {
            RejectFutureScheduling(message, "Amazon SNS topics");
            var topic = _index.FindTopic(destination.Name)
                ?? throw new InvalidOperationException($"Topic '{destination.Name}' was not found. Refresh and try again.");
            var request = new Sns.PublishRequest
            {
                TopicArn = topic.Arn,
                Message = body,
                MessageAttributes = AwsMessageMapper.ToSnsAttributes(message),
                // The subject line e-mail subscribers see, also in the JSON envelope SQS subscribers get; a copy read
                // from that envelope publishes it again.
                Subject = AwsMessageMapper.SnsSubject(message)
            };
            AwsMessageMapper.EnsureAttributesAccepted(request.MessageAttributes.Keys, sqs: false);
            // The topic's MaximumMessageSize (256 KiB unless raised, up to 1 MiB); when it could not be read, only the
            // 1 MiB ceiling is certain.
            MessageSizeLimits.EnsureWithin(MessageSizeLimits.AwsSize(message),
                topic.MaximumMessageSize ?? MessageSizeLimits.AmazonMaximumBytes, $"Amazon SNS topic '{topic.Name}'");
            if (topic.IsFifo)
            {
                request.MessageGroupId = AwsMessageMapper.GroupId(message);
                request.MessageDeduplicationId = AwsMessageMapper.DeduplicationId(message);
                AwsMessageMapper.EnsureFifoIdentifiers(request.MessageGroupId, request.MessageDeduplicationId);
            }

            await Sns.PublishAsync(request, cancellationToken).ConfigureAwait(false);
            return;
        }

        throw new ArgumentException("Messages can only be sent to queues or topics.", nameof(destination));
    }

    private void RejectFutureScheduling(MessageDraft message, string destination)
    {
        if (message.Properties.ScheduledEnqueueTime > TimeProvider.GetUtcNow())
            throw new InvalidOperationException($"{destination} do not support scheduling individual messages. Clear the scheduled time to send immediately.");
    }

    private AmazonSQSClient Sqs => _sqs ?? throw new InvalidOperationException("Connect to an environment first.");

    private AmazonSimpleNotificationServiceClient Sns =>
        _sns ?? throw new InvalidOperationException("Connect to an environment first.");

    private async Task<AWSCredentials?> ResolveCredentialsAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        if (profile.Authentication.Kind != AuthenticationKind.AwsAccessKey)
        {
            if (!string.IsNullOrWhiteSpace(profile.Aws?.ProfileName) &&
                !new CredentialProfileStoreChain().TryGetProfile(profile.Aws.ProfileName.Trim(), out _))
            {
                throw new InvalidOperationException(
                    $"AWS profile '{profile.Aws.ProfileName}' was not found in the shared credentials or config file.");
            }
            return null;
        }

        var secret = await _secretVault.RetrieveForProfileAsync(profile, ProfileSecretKind.ConnectionString, cancellationToken)
            .ConfigureAwait(false);
        var accessKey = AwsAccessKey.Parse(secret);
        return string.IsNullOrEmpty(accessKey.SessionToken)
            ? new BasicAWSCredentials(accessKey.AccessKeyId, accessKey.SecretAccessKey)
            : new SessionAWSCredentials(accessKey.AccessKeyId, accessKey.SecretAccessKey, accessKey.SessionToken);
    }

    /// <summary>The queue's settings and counts; null when the queue no longer exists.</summary>
    private async Task<AwsQueueInfo?> ReadQueueAsync(string url, CancellationToken cancellationToken)
    {
        try
        {
            var response = await Sqs.GetQueueAttributesAsync(
                    new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["All"] },
                    cancellationToken)
                .ConfigureAwait(false);
            return AwsQueueInfo.From(url, response.Attributes ?? []);
        }
        catch (QueueDoesNotExistException)
        {
            return null;
        }
        catch (AmazonSQSException exception) when (exception.ErrorCode is "AWS.SimpleQueueService.NonExistentQueue" or "QueueDoesNotExist")
        {
            // Older endpoints and emulators report it with this code instead of the modelled exception.
            return null;
        }
    }

    private async Task<IReadOnlyList<AwsTopicInfo>> ReadTopicsAsync(CancellationToken cancellationToken)
    {
        var topicArns = new List<string>();
        string? nextToken = null;
        try
        {
            do
            {
                var page = await Sns.ListTopicsAsync(new Sns.ListTopicsRequest { NextToken = nextToken }, cancellationToken)
                    .ConfigureAwait(false);
                topicArns.AddRange((page.Topics ?? []).Select(topic => topic.TopicArn));
                nextToken = page.NextToken;
            }
            while (!string.IsNullOrEmpty(nextToken));
        }
        catch (AmazonSimpleNotificationServiceException exception) when (
            exception.ErrorCode is "AuthorizationError" or "AccessDenied" or "AccessDeniedException")
        {
            // Queue-only credentials are common; show the queues rather than failing the whole environment.
            return [];
        }

        return await Task.WhenAll(topicArns.Select(arn => ReadTopicAsync(arn, cancellationToken))).ConfigureAwait(false);
    }

    private async Task<AwsTopicInfo> ReadTopicAsync(string topicArn, CancellationToken cancellationToken)
    {
        var subscriptions = new List<Sns.Subscription>();
        string? nextToken = null;
        do
        {
            var page = await Sns.ListSubscriptionsByTopicAsync(
                    new Sns.ListSubscriptionsByTopicRequest { TopicArn = topicArn, NextToken = nextToken },
                    cancellationToken)
                .ConfigureAwait(false);
            subscriptions.AddRange(page.Subscriptions ?? []);
            nextToken = page.NextToken;
        }
        while (!string.IsNullOrEmpty(nextToken));

        var details = await Task.WhenAll(subscriptions.Select(async subscription =>
        {
            Dictionary<string, string>? attributes = null;
            if (subscription.SubscriptionArn?.StartsWith("arn:", StringComparison.Ordinal) == true)
            {
                attributes = (await Sns.GetSubscriptionAttributesAsync(
                        new Sns.GetSubscriptionAttributesRequest { SubscriptionArn = subscription.SubscriptionArn },
                        cancellationToken)
                    .ConfigureAwait(false)).Attributes;
            }

            return (Subscription: subscription, Attributes: attributes ?? []);
        })).ConfigureAwait(false);

        return AwsTopicInfo.From(topicArn, details.Select(item => new AwsSubscriptionInfo(
            item.Subscription.SubscriptionArn ?? string.Empty,
            item.Subscription.Protocol ?? "unknown",
            item.Subscription.Endpoint ?? string.Empty,
            AwsQueueInfo.ReadDeadLetterTargetArn(item.Attributes.GetValueOrDefault("RedrivePolicy")))
        {
            FilterPolicy = string.IsNullOrWhiteSpace(item.Attributes.GetValueOrDefault("FilterPolicy")) ||
                           item.Attributes.GetValueOrDefault("FilterPolicy")!.Trim() == "{}"
                ? null
                : item.Attributes["FilterPolicy"],
            FilterPolicyOnBody = item.Attributes.GetValueOrDefault("FilterPolicyScope") == "MessageBody",
            RawMessageDelivery = string.Equals(item.Attributes.GetValueOrDefault("RawMessageDelivery"), "true", StringComparison.OrdinalIgnoreCase)
        })) with
        {
            MaximumMessageSize = await ReadTopicMaximumMessageSizeAsync(topicArn, cancellationToken).ConfigureAwait(false)
        };
    }

    /// <summary>
    /// The topic's MaximumMessageSize, so an oversized publish is refused before it is sent: the attribute when the
    /// topic has one, else SNS's documented default of 262,144 bytes. Null when sns:GetTopicAttributes is not allowed;
    /// then only the 1 MiB ceiling is certain.
    /// </summary>
    private async Task<int?> ReadTopicMaximumMessageSizeAsync(string topicArn, CancellationToken cancellationToken)
    {
        try
        {
            var attributes = (await Sns.GetTopicAttributesAsync(
                    new Sns.GetTopicAttributesRequest { TopicArn = topicArn }, cancellationToken)
                .ConfigureAwait(false)).Attributes ?? [];
            return attributes.TryGetValue("MaximumMessageSize", out var value)
                ? AwsQueueInfo.ReadMaximumMessageSize(value)
                : MessageSizeLimits.AmazonSnsDefaultMaximumBytes;
        }
        catch (AmazonSimpleNotificationServiceException exception) when (
            exception.ErrorCode is "AuthorizationError" or "AccessDenied" or "AccessDeniedException")
        {
            return null;
        }
    }

    private sealed class SqsChannel(
        AwsSqsSnsWorkspace owner,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        AwsQueueInfo queue,
        string? belongsTo,
        bool snsEnvelope = false) : ILeasedMessageChannel
    {
        public string PhysicalName => queue.Name;

        public int MaximumBatchSize => MaximumBatch;

        public bool ReadsOneBatchPerMessageGroup => queue.IsFifo;

        public async Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            // Every receive counts, also one released at once: ApproximateReceiveCount is "the number of times a message
            // has been received across all queues but not deleted" (SQS API Reference, ReceiveMessage), and a redrive
            // policy moves a message after maxReceiveCount receives (SQS Developer Guide, "Using dead-letter queues").
            var response = await owner.Sqs.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = queue.Url,
                        MaxNumberOfMessages = Math.Clamp(maxMessages, 1, MaximumBatch),
                        VisibilityTimeout = HoldSeconds,
                        // A short long-poll asks every SQS server, so an empty answer really means empty.
                        WaitTimeSeconds = 1,
                        MessageSystemAttributeNames = ["All"],
                        MessageAttributeNames = ["All"]
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            return (response.Messages ?? [])
                .Select(message => new LeasedMessage(
                    AwsMessageMapper.FromSqs(message, source, subQueue, snsEnvelope),
                    message.ReceiptHandle,
                    BelongsToSource(message)))
                .ToArray();
        }

        /// <summary>
        /// Makes the messages visible again. A batch answers per entry: entries it reports failed are retried (only
        /// those), and every later batch is still released. A receipt handle that is no longer valid, or a message no
        /// longer in flight, needs nothing more (it is visible again or gone). What still fails is reported after all
        /// batches were tried: those messages only reappear when their visibility timeout ends.
        /// </summary>
        public async Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            var unreleased = new List<(LeasedMessage Message, string Reason)>();
            foreach (var chunk in messages.Chunk(MaximumBatch))
            {
                var pending = chunk.ToList();
                for (var attempt = 1; pending.Count > 0; attempt++)
                {
                    ChangeMessageVisibilityBatchResponse response;
                    try
                    {
                        response = await owner.Sqs.ChangeMessageVisibilityBatchAsync(
                            new ChangeMessageVisibilityBatchRequest
                            {
                                QueueUrl = queue.Url,
                                Entries = pending.Select((message, index) => new ChangeMessageVisibilityBatchRequestEntry
                                {
                                    Id = index.ToString(CultureInfo.InvariantCulture),
                                    ReceiptHandle = message.LeaseHandle,
                                    VisibilityTimeout = 0
                                }).ToList()
                            },
                            cancellationToken)
                        .ConfigureAwait(false);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        // The SDK's own retries are spent: these stay hidden for now; later batches are still released.
                        unreleased.AddRange(pending.Select(message => (message, exception.GetBaseException().Message)));
                        break;
                    }
                    var retry = new List<LeasedMessage>();
                    foreach (var failure in response.Failed ?? [])
                    {
                        if (!int.TryParse(failure.Id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index) ||
                            index < 0 || index >= pending.Count || failure.Code is "ReceiptHandleIsInvalid" or "MessageNotInflight"
                            || failure.Code?.EndsWith(".ReceiptHandleIsInvalid", StringComparison.Ordinal) == true
                            || failure.Code?.EndsWith(".MessageNotInflight", StringComparison.Ordinal) == true)
                        {
                            continue;
                        }
                        if (failure.SenderFault != true && attempt < ReleaseAttempts)
                        {
                            retry.Add(pending[index]);
                        }
                        else
                        {
                            unreleased.Add((pending[index], failure.Code ?? failure.Message ?? "failed"));
                        }
                    }
                    pending = retry;
                    if (pending.Count > 0)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt), cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            if (unreleased.Count > 0)
            {
                throw new IOException(
                    $"{unreleased.Count:N0} message(s) read from '{queue.Name}' could not be made visible again " +
                    $"({string.Join(", ", unreleased.Select(item => item.Reason).Distinct().Take(3))}); they reappear when " +
                    "their visibility timeout ends. Nothing was lost.");
            }
        }

        private const int ReleaseAttempts = 3;

        public async Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(
            IReadOnlyCollection<LeasedMessage> messages,
            CancellationToken cancellationToken)
        {
            var failed = new List<LeasedMessage>();
            foreach (var chunk in messages.Chunk(MaximumBatch))
            {
                var response = await owner.Sqs.DeleteMessageBatchAsync(
                        new DeleteMessageBatchRequest
                        {
                            QueueUrl = queue.Url,
                            Entries = chunk.Select((message, index) => new DeleteMessageBatchRequestEntry
                            {
                                Id = index.ToString(CultureInfo.InvariantCulture),
                                ReceiptHandle = message.LeaseHandle
                            }).ToList()
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
                failed.AddRange((response.Failed ?? [])
                    .Select(entry => chunk[int.Parse(entry.Id, CultureInfo.InvariantCulture)]));
            }

            return failed;
        }

        public string? SourceAttributionError => source.Kind == ServiceBusEntityKind.Subscription && subQueue == ServiceBusSubQueue.DeadLetter
            ? "SNS dead letters do not identify their originating subscription. Source-scoped deletion is blocked. Browse the physical SQS dead-letter queue to review its contents."
            : null;

        private bool BelongsToSource(Message message) =>
            SourceAttributionError is null && (belongsTo is null ||
            (message.Attributes?.GetValueOrDefault("DeadLetterQueueSourceArn") is { Length: > 0 } sourceArn &&
            string.Equals(sourceArn, belongsTo, StringComparison.Ordinal)));
    }
}

/// <summary>An AWS access key as stored in the secret vault.</summary>
public sealed record AwsAccessKey(string AccessKeyId, string SecretAccessKey, string? SessionToken = null)
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string ToSecret() => JsonSerializer.Serialize(this, Options);

    public static AwsAccessKey Parse(string? secret)
    {
        if (string.IsNullOrWhiteSpace(secret))
        {
            throw new InvalidOperationException("This environment has no saved AWS access key.");
        }

        var key = JsonSerializer.Deserialize<AwsAccessKey>(secret, Options);
        if (key is null || string.IsNullOrWhiteSpace(key.AccessKeyId) || string.IsNullOrWhiteSpace(key.SecretAccessKey))
        {
            throw new InvalidOperationException("The saved AWS access key is incomplete. Edit the environment and enter it again.");
        }

        return key;
    }
}
