using System.Globalization;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Infrastructure.Aws;

internal sealed record AwsQueueInfo(
    string Name,
    string Url,
    string Arn,
    bool IsFifo,
    long Visible,
    long InFlight,
    long Delayed,
    string? DeadLetterTargetArn,
    DateTimeOffset? CreatedAt,
    DateTimeOffset? UpdatedAt)
{
    public static AwsQueueInfo From(string url, IReadOnlyDictionary<string, string> attributes)
    {
        var arn = attributes.GetValueOrDefault("QueueArn") ?? string.Empty;
        var name = arn.Length > 0 ? LastSegment(arn) : url.TrimEnd('/').Split('/')[^1];
        return new AwsQueueInfo(
            name,
            url,
            arn,
            string.Equals(attributes.GetValueOrDefault("FifoQueue"), "true", StringComparison.OrdinalIgnoreCase) ||
            name.EndsWith(".fifo", StringComparison.Ordinal),
            ReadLong(attributes, "ApproximateNumberOfMessages"),
            ReadLong(attributes, "ApproximateNumberOfMessagesNotVisible"),
            ReadLong(attributes, "ApproximateNumberOfMessagesDelayed"),
            ReadDeadLetterTargetArn(attributes.GetValueOrDefault("RedrivePolicy")),
            ReadEpochSeconds(attributes, "CreatedTimestamp"),
            ReadEpochSeconds(attributes, "LastModifiedTimestamp"))
        {
            MaximumMessageSize = ReadMaximumMessageSize(attributes.GetValueOrDefault("MaximumMessageSize"))
                                 ?? QueueLoom.Core.Validation.MessageSizeLimits.AmazonMaximumBytes
        };
    }

    /// <summary>
    /// The queue's MaximumMessageSize: "An integer from 1,024 bytes (1 KiB) up to 1,048,576 bytes (1 MiB). Default:
    /// 1,048,576 bytes (1 MiB)."
    /// </summary>
    public int MaximumMessageSize { get; init; } = QueueLoom.Core.Validation.MessageSizeLimits.AmazonMaximumBytes;

    /// <summary>A MaximumMessageSize attribute within SQS/SNS's 1,024 to 1,048,576 bytes, else null (unknown).</summary>
    internal static int? ReadMaximumMessageSize(string? value) =>
        int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bytes) &&
        bytes is >= 1_024 and <= QueueLoom.Core.Validation.MessageSizeLimits.AmazonMaximumBytes
            ? bytes
            : null;

    public static string? ReadDeadLetterTargetArn(string? redrivePolicy)
    {
        if (string.IsNullOrWhiteSpace(redrivePolicy))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(redrivePolicy);
            return document.RootElement.TryGetProperty("deadLetterTargetArn", out var target) &&
                   target.ValueKind == JsonValueKind.String
                ? target.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    internal static string LastSegment(string arn) => arn.Split(':')[^1];

    private static long ReadLong(IReadOnlyDictionary<string, string> attributes, string name) =>
        long.TryParse(attributes.GetValueOrDefault(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value > 0
            ? value
            : 0;

    private static DateTimeOffset? ReadEpochSeconds(IReadOnlyDictionary<string, string> attributes, string name) =>
        long.TryParse(attributes.GetValueOrDefault(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? BrokerClock.FromUnixSeconds(value)
            : null;
}

internal sealed record AwsSubscriptionInfo(
    string Arn,
    string Protocol,
    string Endpoint,
    string? DeadLetterTargetArn)
{
    /// <summary>Filled in by <see cref="AwsTopicInfo.From"/>: a readable, unique name within the topic.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>The subscription's filter policy (JSON), or null when it takes every message.</summary>
    public string? FilterPolicy { get; init; }

    /// <summary>True when the policy looks at the JSON body (FilterPolicyScope MessageBody) instead of the attributes.</summary>
    public bool FilterPolicyOnBody { get; init; }

    /// <summary>SNS delivers the published body as it is (RawMessageDelivery); otherwise it wraps it in its JSON envelope.</summary>
    public bool RawMessageDelivery { get; init; }

    public bool IsConfirmed => Arn.StartsWith("arn:", StringComparison.Ordinal);

    /// <summary>"sqs:orders-billing", "lambda:resize-image", "https:hooks.example.com" and so on.</summary>
    public string ReadableName()
    {
        var endpoint = Protocol switch
        {
            "sqs" or "sns" or "firehose" => AwsQueueInfo.LastSegment(Endpoint),
            "lambda" => Endpoint.Split(':').SkipWhile(part => part != "function").Skip(1).FirstOrDefault()
                        ?? AwsQueueInfo.LastSegment(Endpoint),
            // Only the host: MCP clients address subscriptions as 'topic/subscription', so names avoid '/'.
            "http" or "https" => Uri.TryCreate(Endpoint, UriKind.Absolute, out var uri) ? uri.Host : Endpoint.Replace('/', '_'),
            _ => Endpoint.Replace('/', '_')
        };
        return $"{Protocol}:{(string.IsNullOrWhiteSpace(endpoint) ? AwsQueueInfo.LastSegment(Arn) : endpoint)}";
    }
}

internal sealed record AwsTopicInfo(string Name, string Arn, bool IsFifo, IReadOnlyList<AwsSubscriptionInfo> Subscriptions)
{
    /// <summary>
    /// The topic's MaximumMessageSize ("Valid values are 1024 to 1048576 (1 MiB). The default is 262144 (256 KiB)"),
    /// or null when the topic's attributes could not be read.
    /// </summary>
    public int? MaximumMessageSize { get; init; }

    public static AwsTopicInfo From(string arn, IEnumerable<AwsSubscriptionInfo> subscriptions)
    {
        var name = AwsQueueInfo.LastSegment(arn);
        var used = new HashSet<string>(StringComparer.Ordinal);
        var named = subscriptions
            .OrderBy(subscription => subscription.ReadableName(), StringComparer.Ordinal)
            .Select(subscription =>
            {
                var baseName = subscription.ReadableName();
                var candidate = baseName;
                for (var suffix = 2; !used.Add(candidate); suffix++)
                {
                    candidate = $"{baseName} ({suffix})";
                }
                return subscription with { Name = candidate };
            })
            .ToArray();
        return new AwsTopicInfo(name, arn, name.EndsWith(".fifo", StringComparison.Ordinal), named);
    }
}

/// <summary>Name and ARN lookups for the last topology read, plus its conversion to QueueLoom's model.</summary>
internal sealed class AwsTopologyIndex
{
    private readonly Dictionary<string, AwsQueueInfo> _queuesByName;
    private readonly Dictionary<string, AwsQueueInfo> _queuesByArn;
    private readonly Dictionary<string, AwsTopicInfo> _topicsByName;

    public AwsTopologyIndex(IEnumerable<AwsQueueInfo> queues, IEnumerable<AwsTopicInfo> topics)
    {
        Queues = queues.OrderBy(queue => queue.Name, StringComparer.Ordinal).ToArray();
        Topics = topics.OrderBy(topic => topic.Name, StringComparer.Ordinal).ToArray();
        _queuesByName = Queues.DistinctBy(queue => queue.Name).ToDictionary(queue => queue.Name, StringComparer.Ordinal);
        _queuesByArn = Queues.Where(queue => queue.Arn.Length > 0).DistinctBy(queue => queue.Arn)
            .ToDictionary(queue => queue.Arn, StringComparer.Ordinal);
        _topicsByName = Topics.DistinctBy(topic => topic.Name).ToDictionary(topic => topic.Name, StringComparer.Ordinal);
    }

    public static AwsTopologyIndex Empty { get; } = new([], []);

    public IReadOnlyList<AwsQueueInfo> Queues { get; }

    public IReadOnlyList<AwsTopicInfo> Topics { get; }

    public AwsQueueInfo? FindQueue(string name) => _queuesByName.GetValueOrDefault(name);

    public AwsQueueInfo? FindQueueByArn(string? arn) => arn is null ? null : _queuesByArn.GetValueOrDefault(arn);

    public AwsTopicInfo? FindTopic(string name) => _topicsByName.GetValueOrDefault(name);

    public AwsSubscriptionInfo? FindSubscription(string topicName, string subscriptionName) =>
        FindTopic(topicName)?.Subscriptions.FirstOrDefault(subscription =>
            string.Equals(subscription.Name, subscriptionName, StringComparison.Ordinal));

    public ServiceBusTopology ToTopology(DateTimeOffset fetchedAt)
    {
        // Which queues serve as a dead-letter queue, and for whom: shown as a note in the explorer.
        var deadLetterUsers = Queues
            .Where(queue => queue.DeadLetterTargetArn is not null)
            .Select(queue => (Target: queue.DeadLetterTargetArn!, User: queue.Name))
            .Concat(Topics.SelectMany(topic => topic.Subscriptions
                .Where(subscription => subscription.DeadLetterTargetArn is not null)
                .Select(subscription => (Target: subscription.DeadLetterTargetArn!, User: $"{topic.Name}/{subscription.Name}"))))
            .GroupBy(item => item.Target, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Select(item => item.User).ToArray(), StringComparer.Ordinal);

        var queues = Queues.Select(queue =>
        {
            var deadLetterQueue = FindQueueByArn(queue.DeadLetterTargetArn);
            var runtime = new ServiceBusEntityRuntime(
                new ServiceBusMessageCounts(
                    active: queue.Visible,
                    deadLetter: deadLetterQueue?.Visible ?? 0,
                    scheduled: queue.Delayed),
                createdAt: queue.CreatedAt,
                updatedAt: queue.UpdatedAt)
            { HasTransferDeadLetterCount = false };

            string? note = null;
            if (deadLetterUsers.TryGetValue(queue.Arn, out var users))
            {
                note = $"Dead-letter queue of {string.Join(", ", users)}";
            }
            else if (queue.DeadLetterTargetArn is not null && deadLetterQueue is null)
            {
                note = "Its dead-letter queue is in another account or region";
            }
            else if (deadLetterQueue is not null &&
                     deadLetterUsers.TryGetValue(deadLetterQueue.Arn, out var sharing) && sharing.Length > 1)
            {
                // SQS counts per queue, not per source, so the DLQ column shows the whole shared queue.
                note = $"Shares dead-letter queue {deadLetterQueue.Name} with {sharing.Length - 1} other source(s); its count covers all of them";
            }
            if (queue.IsFifo)
            {
                // While QueueLoom holds messages of a group, SQS returns no further messages of that group,
                // so a read sees at most one receive batch (10 messages) per group.
                const string fifo = "FIFO: reading shows up to 10 messages per message group";
                note = note is null ? fifo : $"{note} · {fifo}";
            }
            if (queue.InFlight > 0)
            {
                var inFlight = $"{queue.InFlight.ToString("N0", CultureInfo.CurrentCulture)} in flight";
                note = note is null ? inFlight : $"{note} · {inFlight}";
            }

            return new ServiceBusQueue(queue.Name, runtime, ServiceBusEntityStatus.Active)
            {
                HasDeadLetterQueue = deadLetterQueue is not null,
                Note = note
            };
        });

        var topics = Topics.Select(topic => new ServiceBusTopic(
            topic.Name,
            new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty) { HasTransferDeadLetterCount = false },
            topic.Subscriptions.Select(subscription =>
            {
                var endpointQueue = subscription.Protocol == "sqs" ? FindQueueByArn(subscription.Endpoint) : null;
                var deadLetterQueue = FindQueueByArn(subscription.DeadLetterTargetArn);
                var runtime = new ServiceBusEntityRuntime(new ServiceBusMessageCounts(
                    active: endpointQueue?.Visible ?? 0,
                    deadLetter: deadLetterQueue?.Visible ?? 0))
                { HasTransferDeadLetterCount = false };
                return new ServiceBusSubscription(
                    topic.Name,
                    subscription.Name,
                    runtime,
                    subscription.IsConfirmed ? ServiceBusEntityStatus.Active : ServiceBusEntityStatus.Creating)
                {
                    HasDeadLetterQueue = deadLetterQueue is not null,
                    Note = subscription.IsConfirmed
                        ? endpointQueue is null ? $"Delivers to {subscription.Protocol}: {subscription.Endpoint}" : null
                        : "Pending confirmation"
                };
            }),
            ServiceBusEntityStatus.Active));

        return new ServiceBusTopology(fetchedAt, queues, topics) { SupportsTransferDeadLetter = false };
    }
}
