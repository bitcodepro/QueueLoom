using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>Creating, changing and deleting topics: retention and partitions; a ".DLT" topic can be created alongside.</summary>
public sealed partial class KafkaWorkspace
{
    private const QueueSettingFlags KafkaSettings = QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions;

    public override QueueManagementCapabilities? QueueManagement => new("topic", KafkaSettings, KafkaSettings, CanCreateDeadLetterQueue: true,
        UpdateNote: "Time to live is the topic's retention (retention.ms). Partitions can be added but never removed.");

    public override async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        var configs = await Admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = queue }]).ConfigureAwait(false);
        var retention = configs[0].Entries.TryGetValue("retention.ms", out var entry) &&
                        long.TryParse(entry.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var milliseconds) && milliseconds > 0
            ? TimeSpan.FromMilliseconds(milliseconds)
            : (TimeSpan?)null;
        var partitions = await Task.Run(() => Admin.GetMetadata(queue, RequestTimeout).Topics.Single().Partitions.Count, cancellationToken)
            .ConfigureAwait(false);
        return new QueueSettings(retention, Partitions: partitions);
    }

    public override Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        ManageAsync(async _ =>
        {
            ArgumentNullException.ThrowIfNull(definition);
            var configs = new Dictionary<string, string>(StringComparer.Ordinal);
            if (definition.Settings.MessageTimeToLive is { } retention)
            {
                configs["retention.ms"] = ((long)retention.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
            }
            var topics = new List<TopicSpecification>
            {
                // -1 lets the cluster pick its default replication factor.
                new() { Name = definition.Name, NumPartitions = definition.Settings.Partitions ?? 1, ReplicationFactor = -1, Configs = configs }
            };
            if (definition.CreateDeadLetterQueue)
            {
                topics.Add(new TopicSpecification { Name = definition.Name + ".DLT", NumPartitions = 1, ReplicationFactor = -1 });
            }
            await Administer(() => Admin.CreateTopicsAsync(topics)).ConfigureAwait(false);
        }, cancellationToken);

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
