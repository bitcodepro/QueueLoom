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
            var configs = new Dictionary<string, string>(StringComparer.Ordinal);
            if (definition.Settings.MessageTimeToLive is { } retention)
            {
                configs["retention.ms"] = ((long)retention.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
            }
            // CreateTopics creates each topic of a request on its own, so one request for both could create one and
            // refuse the other. The topic goes first: when it is refused, nothing has been created.
            // -1 lets the cluster pick its default replication factor.
            await Administer(() => Admin.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = definition.Name, NumPartitions = definition.Settings.Partitions ?? 1, ReplicationFactor = -1, Configs = configs
                }
            ])).ConfigureAwait(false);
            var created = new List<string> { definition.Name };
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetter = definition.Name + ".DLT";
                try
                {
                    await Admin.CreateTopicsAsync([new TopicSpecification { Name = deadLetter, NumPartitions = 1, ReplicationFactor = -1 }])
                        .ConfigureAwait(false);
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
                var current = await Task.Run(() => Admin.GetMetadata(queue, RequestTimeout).Topics.Single().Partitions.Count, token)
                    .ConfigureAwait(false);
                if (partitions < current)
                {
                    throw new InvalidOperationException($"Topic '{queue}' has {current} partitions; Kafka cannot remove partitions.");
                }
                if (partitions > current)
                {
                    await Administer(() => Admin.CreatePartitionsAsync([new PartitionsSpecification { Topic = queue, IncreaseTo = partitions }]))
                        .ConfigureAwait(false);
                }
            }
        }, cancellationToken);

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
