using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Kafka;

/// <param name="Retained">Messages the cluster still keeps: the sum of high minus low watermarks of all partitions.</param>
internal sealed record KafkaTopicInfo(string Name, IReadOnlyList<int> Partitions, long Retained)
{
    /// <summary>Why <see cref="Retained"/> is not known, for example while a new topic has no partition leader yet.</summary>
    public string? CountError { get; init; }

    /// <summary>The high watermark of each partition, for consumer lag.</summary>
    public IReadOnlyDictionary<int, long> Ends { get; init; } = new Dictionary<int, long>();

    /// <summary>Consumer groups with committed offsets on this topic.</summary>
    public IReadOnlyList<ConsumerGroupLag> Groups { get; init; } = [];
}

/// <summary>Kafka topics and which of them are dead-letter topics, by name.</summary>
internal sealed record KafkaTopologyIndex(IReadOnlyList<KafkaTopicInfo> Topics, IReadOnlyList<string> Suffixes)
{
    public static KafkaTopologyIndex Empty { get; } = new([], Core.Profiles.KafkaSettings.DefaultDeadLetterSuffixes);

    public KafkaTopicInfo? Find(string name) => Topics.FirstOrDefault(topic => topic.Name == name);

    /// <summary>"orders.DLT" for "orders" when that topic exists; the first ending in the configured order wins.</summary>
    public string? DeadLetterTopicOf(string topic) =>
        IsDeadLetterTopic(topic) ? null : Suffixes.Select(suffix => topic + suffix).FirstOrDefault(name => Find(name) is not null);

    /// <summary>The topic a dead-letter topic belongs to, when that topic exists.</summary>
    public string? SourceOf(string deadLetterTopic) =>
        Suffixes.Where(suffix => deadLetterTopic.EndsWith(suffix, StringComparison.Ordinal) && deadLetterTopic.Length > suffix.Length)
            .Select(suffix => deadLetterTopic[..^suffix.Length])
            .FirstOrDefault(name => Find(name) is not null);

    public bool IsDeadLetterTopic(string topic) => SourceOf(topic) is not null;

    /// <summary>Kafka's own topics (offsets, transactions) and Confluent's (schemas, metrics) are not shown.</summary>
    public static bool IsInternal(string topic) =>
        topic.StartsWith("__", StringComparison.Ordinal) || topic.StartsWith("_confluent", StringComparison.Ordinal) || topic == "_schemas";

    public ServiceBusTopology ToTopology(DateTimeOffset fetchedAt)
    {
        var queues = Topics.OrderBy(topic => topic.Name, StringComparer.Ordinal).Select(topic =>
        {
            var deadLetter = DeadLetterTopicOf(topic.Name) is { } name ? Find(name) : null;
            var partitions = topic.Partitions.Count == 1 ? "1 partition" : $"{topic.Partitions.Count.ToString(CultureInfo.CurrentCulture)} partitions";
            var note = SourceOf(topic.Name) is { } source ? $"Dead-letter topic of {source} · {partitions}" : partitions;
            if (topic.CountError is not null || deadLetter?.CountError is not null)
            {
                note += $" · {topic.CountError ?? deadLetter!.CountError}";
            }
            return new ServiceBusQueue(topic.Name,
                new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: topic.Retained, deadLetter: deadLetter?.Retained ?? 0))
                {
                    HasTransferDeadLetterCount = false,
                    CountsUnavailable = deadLetter?.CountError is not null,
                    DeadLetterCountError = deadLetter?.CountError,
                    // A dead-letter topic only grows at its end: a moved end offset is a new message, whatever the count.
                    DeadLetterContentMarkers = deadLetter is { CountError: null, Ends.Count: > 0 }
                        ? deadLetter.Ends.OrderBy(pair => pair.Key).Select(pair => $"{pair.Key}@{pair.Value}").ToArray()
                        : null
                },
                ServiceBusEntityStatus.Active)
            {
                HasDeadLetterQueue = deadLetter is not null,
                Note = note,
                Consumers = topic.Groups.Count == 0 ? null : new ConsumerActivity(null, topic.Groups)
            };
        });
        return new ServiceBusTopology(fetchedAt, queues, [])
        {
            SupportsTransferDeadLetter = false,
            CanDeleteSelectedMessages = false,
            QueueKindName = "topic"
        };
    }
}
