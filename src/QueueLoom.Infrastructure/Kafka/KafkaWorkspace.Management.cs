using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>Creating, changing and deleting topics: retention and partitions; a ".DLT" topic can be created alongside.</summary>
public sealed partial class KafkaWorkspace
{
    private const QueueSettingFlags KafkaSettings = QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions;

    public override QueueManagementCapabilities? QueueManagement => new("topic", KafkaSettings, KafkaSettings, CanCreateDeadLetterQueue: true,
        UpdateNote: "Time to live is the topic's retention (retention.ms). Partitions can be added but never removed.");

    public override async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        var configs = await Admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = queue }]).ConfigureAwait(false);
        var retention = configs[0].Entries.TryGetValue("retention.ms", out var entry) &&
                        long.TryParse(entry.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var milliseconds) && milliseconds > 0
            ? BrokerClock.FromMilliseconds(milliseconds)
            : (TimeSpan?)null;
        var partitions = await Task.Run(() => Admin.GetMetadata(queue, RequestTimeout).Topics.Single().Partitions.Count, cancellationToken)
            .ConfigureAwait(false);
        return new QueueSettings(retention, Partitions: partitions);
    }

    public override Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(definition);
            var (topic, deadLetterTopic) = TopicSpecifications(definition);
            // CreateTopics creates each topic of a request on its own, so one request for both could create one and
            // refuse the other. The topic goes first: when it is refused, nothing has been created.
            await Administer(() => Admin.CreateTopicsAsync([topic])).ConfigureAwait(false);
            var created = new List<string> { definition.Name };
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetter = deadLetterTopic.Name;
                try
                {
                    await Admin.CreateTopicsAsync([deadLetterTopic]).ConfigureAwait(false);
                    created.Add(deadLetter);
                }
                catch (CreateTopicsException exception) when (exception.Results.All(result =>
                                                                  !result.Error.IsError || result.Error.Code == ErrorCode.TopicAlreadyExists))
                {
                    // A dead-letter topic of that name already exists (Spring Kafka's recoverer creates "<topic>.DLT"
                    // too); it is the one dead letters of the new topic go to.
                }
                catch (KafkaException exception)
                {
                    var reason = (exception as CreateTopicsException)?.Results.FirstOrDefault(result => result.Error.IsError)?.Error.Reason
                                 ?? exception.Error.Reason;
                    throw new InvalidOperationException(
                        $"Topic '{definition.Name}' was created, but Kafka refused its dead-letter topic '{deadLetter}': {reason}", exception);
                }
            }
            await WaitUntilVisibleAsync(created, token).ConfigureAwait(false);
        }, cancellationToken);

    /// <summary>
    /// The topic and its ".DLT" topic. Spring's DeadLetterPublishingRecoverer writes a failed record to the same partition
    /// number of the dead-letter topic, so that topic gets as many partitions as its topic.
    /// </summary>
    internal static (TopicSpecification Topic, TopicSpecification DeadLetter) TopicSpecifications(QueueDefinition definition)
    {
        var configs = new Dictionary<string, string>(StringComparer.Ordinal);
        if (definition.Settings.MessageTimeToLive is { } retention)
        {
            configs["retention.ms"] = ((long)retention.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        }
        var partitions = definition.Settings.Partitions ?? 1;
        // -1 lets the cluster pick its default replication factor.
        return (new TopicSpecification { Name = definition.Name, NumPartitions = partitions, ReplicationFactor = -1, Configs = configs },
            new TopicSpecification { Name = definition.Name + ".DLT", NumPartitions = partitions, ReplicationFactor = -1 });
    }

    /// <summary>
    /// A new topic reaches every broker's metadata a moment after it is created; waiting here keeps the refresh that
    /// follows from missing it.
    /// </summary>
    private async Task WaitUntilVisibleAsync(IReadOnlyCollection<string> names, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 40; attempt++)
        {
            var metadata = await Task.Run(() => Admin.GetMetadata(RequestTimeout), cancellationToken).ConfigureAwait(false);
            if (names.All(name => metadata.Topics.Any(topic => topic.Topic == name && topic.Error.Code == ErrorCode.NoError &&
                                                               topic.Partitions.Count > 0 && topic.Partitions.All(partition => partition.Leader >= 0))))
            {
                return;
            }
            await Task.Delay(250, cancellationToken).ConfigureAwait(false);
        }
    }

    public override Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            if (settings.MessageTimeToLive is { } retention)
            {
                await Administer(() => Admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    [new ConfigResource { Type = ResourceType.Topic, Name = queue }] =
                    [
                        new ConfigEntry
                        {
                            Name = "retention.ms",
                            Value = ((long)retention.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
                            IncrementalOperation = AlterConfigOpType.Set
                        }
                    ]
                })).ConfigureAwait(false);
            }
            if (settings.Partitions is { } partitions)
            {
                await GrowPartitionsAsync(new AdminPartitions(Admin), queue, partitions, _index.Suffixes, RequestTimeout, token).ConfigureAwait(false);
            }
        }, cancellationToken);

    /// <summary>The two admin calls a partition change needs, so it can be tested without a cluster.</summary>
    internal interface IPartitionAdmin
    {
        Metadata GetMetadata(TimeSpan timeout);

        Task CreatePartitionsAsync(string topic, int increaseTo);
    }

    private sealed class AdminPartitions(IAdminClient admin) : IPartitionAdmin
    {
        public Metadata GetMetadata(TimeSpan timeout) => admin.GetMetadata(timeout);

        public Task CreatePartitionsAsync(string topic, int increaseTo) =>
            admin.CreatePartitionsAsync([new PartitionsSpecification { Topic = topic, IncreaseTo = increaseTo }]);
    }

    internal static async Task GrowPartitionsAsync(IPartitionAdmin admin, string queue, int partitions, IReadOnlyList<string> suffixes,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        var metadata = await Task.Run(() => admin.GetMetadata(timeout), cancellationToken).ConfigureAwait(false);
        var source = metadata.Topics.FirstOrDefault(topic => topic.Topic == queue);
        if (source is null || source.Error.IsError)
        {
            throw new InvalidOperationException($"Topic '{queue}' could not be read ({source?.Error.Reason ?? "not found"}).");
        }
        var current = source.Partitions.Count;
        if (partitions < current)
        {
            throw new InvalidOperationException($"Topic '{queue}' has {current} partitions; Kafka cannot remove partitions.");
        }
        if (partitions > current)
        {
            await Administer(() => admin.CreatePartitionsAsync(queue, partitions)).ConfigureAwait(false);
        }
        // Also when the topic already has them: a retry after a dead-letter topic failed to grow repairs it.
        await GrowDeadLetterTopicsAsync(admin, metadata, queue, partitions, suffixes).ConfigureAwait(false);
    }

    /// <summary>
    /// Spring's recoverer writes a failed record to the same partition number of the dead-letter topic, so a topic's
    /// dead-letter topics (by the profile's endings) grow with it. They only grow: one that already has as many
    /// partitions or more is left alone, and a dead-letter topic of another topic is never touched: with endings
    /// "-dlt" and "-retry-dlt", "orders-retry-dlt" belongs to "orders-retry" when that topic exists, as the topology
    /// shows it. A dead-letter topic whose metadata cannot be read is reported, not taken for absent or changed.
    /// </summary>
    private static async Task GrowDeadLetterTopicsAsync(IPartitionAdmin admin, Metadata metadata, string queue, int partitions,
        IReadOnlyList<string> suffixes)
    {
        var index = new KafkaTopologyIndex(metadata.Topics.Select(topic => new KafkaTopicInfo(topic.Topic, [], 0)).ToList(), suffixes);
        var owned = suffixes.Select(suffix => queue + suffix).Distinct(StringComparer.Ordinal)
            .Select(name => metadata.Topics.FirstOrDefault(topic => topic.Topic == name))
            .Where(topic => topic is not null && topic.Error.Code != ErrorCode.UnknownTopicOrPart && index.SourceOf(topic.Topic) == queue)
            .Select(topic => topic!)
            .ToList();
        var unreadable = owned.Where(topic => topic.Error.IsError).ToList();
        var smaller = owned.Where(topic => !topic.Error.IsError && topic.Partitions.Count < partitions).Select(topic => topic.Topic).ToList();
        foreach (var deadLetter in smaller)
        {
            try
            {
                await admin.CreatePartitionsAsync(deadLetter, partitions).ConfigureAwait(false);
            }
            catch (KafkaException exception)
            {
                var reason = (exception as CreatePartitionsException)?.Results.FirstOrDefault(result => result.Error.IsError)?.Error.Reason
                             ?? exception.Error.Reason;
                throw new InvalidOperationException(
                    $"Topic '{queue}' now has {partitions} partitions, but its dead-letter topic '{deadLetter}' could not grow to match " +
                    $"({reason}); records failing on the new partitions cannot be dead-lettered until it does.", exception);
            }
        }
        if (unreadable.Count > 0)
        {
            throw new InvalidOperationException(
                $"Topic '{queue}' now has {partitions} partitions, but the partitions of its dead-letter topic " +
                string.Join(", ", unreadable.Select(topic => $"'{topic.Topic}' ({topic.Error.Reason})")) +
                " could not be read, so it was left unchanged; refresh and save again to make it match.");
        }
    }

    public override Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        ManageAsync(_ => Administer(() => Admin.DeleteTopicsAsync([queue])), cancellationToken);

    private static async Task Administer(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (KafkaException exception)
        {
            var reason = exception switch
            {
                CreateTopicsException create => create.Results.FirstOrDefault(result => result.Error.IsError)?.Error.Reason,
                DeleteTopicsException delete => delete.Results.FirstOrDefault(result => result.Error.IsError)?.Error.Reason,
                CreatePartitionsException partitions => partitions.Results.FirstOrDefault(result => result.Error.IsError)?.Error.Reason,
                _ => null
            } ?? exception.Error.Reason;
            throw new InvalidOperationException($"Kafka refused the change: {reason}", exception);
        }
    }
}
