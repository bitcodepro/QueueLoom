using System.Buffers.Binary;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// Runs the Kafka workspace against a real broker (apache/kafka): "orders" (3 partitions) with the dead-letter topic
/// "orders.DLT" written the way Spring Kafka's dead-letter recoverer writes it, and "payments" without one.
/// </summary>
public sealed class KafkaWorkspaceTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly string _orders = Emulators.Unique("orders");
    private readonly string _payments = Emulators.Unique("payments");
    private IAdminClient _admin = null!;
    private IProducer<string?, string> _producer = null!;
    private KafkaWorkspace _workspace = null!;

    private string DeadLetters => _orders + ".DLT";

    public async Task InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.Kafka)))
        {
            return;
        }

        _admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Emulators.KafkaServers }).Build();
        await _admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = _orders, NumPartitions = 3, ReplicationFactor = 1 },
            new TopicSpecification { Name = DeadLetters, NumPartitions = 1, ReplicationFactor = 1 },
            new TopicSpecification { Name = _payments, NumPartitions = 1, ReplicationFactor = 1 }
        ]);
        _producer = new ProducerBuilder<string?, string>(new ProducerConfig { BootstrapServers = Emulators.KafkaServers }).Build();

        var profile = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.KafkaNone), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings(Emulators.KafkaServers),
            AllowQueueManagement = true
        };
        _workspace = new KafkaWorkspace(new InMemorySecretVault(), backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
    }

    public async Task DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        if (_admin is not null)
        {
            await _admin.DeleteTopicsAsync([_orders, DeadLetters, _payments]);
            _admin.Dispose();
            _producer.Dispose();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Topology_pairs_topics_with_their_dead_letter_topics_and_counts_retained_messages()
    {
        await ProduceAsync(_orders, "o-1", "o-2");
        await DeadLetterAsync("o-3", "o-4");

        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);

        var orders = topology.Queues.Single(queue => queue.Name == _orders);
        Assert.True(orders.HasDeadLetterQueue);
        Assert.Equal(2, orders.Runtime.MessageCounts.Active);
        Assert.Equal(2, orders.Runtime.MessageCounts.DeadLetter);
        Assert.Equal("3 partitions", orders.Note);
        Assert.Equal($"Dead-letter topic of {_orders} · 1 partition", topology.Queues.Single(queue => queue.Name == DeadLetters).Note);
        Assert.False(topology.Queues.Single(queue => queue.Name == _payments).HasDeadLetterQueue);
        Assert.False(topology.CanDeleteSelectedMessages);
        Assert.Equal("topic", topology.QueueKindName);
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Dead_letters_are_read_by_offset_with_their_reason_and_nothing_changes()
    {
        await DeadLetterAsync("o-3", "o-4");
        var orders = ServiceBusEntityReference.Queue(_orders);

        var first = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        var second = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));

        Assert.Equal(["o-3", "o-4"], first.Select(message => message.Properties.MessageId));
        Assert.Equal(2, second.Count);
        var message = first[0];
        Assert.Equal("ValidationException", message.DeadLetterReason);
        Assert.Equal($"Total must be positive · from {_orders}, partition 1, offset 17", message.DeadLetterErrorDescription);
        Assert.Equal("customer-7", message.Properties.PartitionKey);
        Assert.Equal("0", message.ApplicationProperties.Single(property => property.Name == "kafka.offset").Value);
        var marks = await WatermarksAsync(DeadLetters);
        Assert.Equal((0L, 2L), marks);
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Resending_a_dead_letter_sends_a_clean_copy_with_the_same_key()
    {
        await DeadLetterAsync("o-3");
        var orders = ServiceBusEntityReference.Queue(_orders);
        var dead = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter)));

        var result = await DeadLetterResender.ResendAsync(_workspace, [new ResendItem(dead, orders, dead.CreateDraft())], ResendMode.Copy);

        Assert.Equal(1, result.SentCount);
        var copy = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders)));
        Assert.Equal("o-3", copy.Properties.MessageId);
        Assert.Equal("customer-7", copy.Properties.PartitionKey);
        Assert.Null(copy.DeadLetterReason);
        Assert.DoesNotContain(copy.ApplicationProperties, property => property.Name.StartsWith("kafka_dlt-", StringComparison.Ordinal));
        Assert.Equal("{\"order\":\"o-3\"}", Encoding.UTF8.GetString(copy.Body.Span));
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Emptying_a_dead_letter_topic_backs_up_and_deletes_its_records()
    {
        await DeadLetterAsync("o-3", "o-4", "o-5");
        var orders = ServiceBusEntityReference.Queue(_orders);

        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(orders, ServiceBusSubQueue.DeadLetter)], batchSize: 20, maximumMessagesPerSubQueue: 100));

        Assert.Equal(3, purge.DeletedCount);
        Assert.Equal((3L, 3L), await WatermarksAsync(DeadLetters));
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter)));
        Assert.Equal(3, Directory.EnumerateFiles(purge.BackupDirectory, "*.json", SearchOption.AllDirectories)
            .Count(file => File.ReadAllText(file).Contains("\"o-", StringComparison.Ordinal)));
        var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);
        Assert.Equal(0, snapshot.Entities.Single(entity => entity.Entity.Name == _orders).Count);
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Dead_letters_are_found_by_text()
    {
        await DeadLetterAsync("o-3", "o-4");
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);

        var result = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("o-4", DeadLetterSearchTargets.ForTopology(topology)));

        Assert.Equal(["o-4"], result.Matches.Select(message => message.Properties.MessageId));
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Topics_are_created_with_a_dead_letter_topic_grown_and_deleted()
    {
        var name = Emulators.Unique("returns");
        await _workspace.CreateQueueAsync(new QueueDefinition(name, new QueueSettings(TimeSpan.FromDays(3), Partitions: 2)));

        Assert.True((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues.Single(queue => queue.Name == name).HasDeadLetterQueue);
        Assert.Equal(new QueueSettings(TimeSpan.FromDays(3), Partitions: 2), await _workspace.GetQueueSettingsAsync(name));

        await _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(TimeSpan.FromHours(12), Partitions: 4));
        Assert.Equal(new QueueSettings(TimeSpan.FromHours(12), Partitions: 4), await _workspace.GetQueueSettingsAsync(name));
        await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(Partitions: 1)));

        await _workspace.DeleteQueueAsync(name);
        await _workspace.DeleteQueueAsync(name + ".DLT");
        for (var attempt = 0; attempt < 20 && (await _workspace.GetTopologyAsync(forceRefresh: true)).Queues.Any(queue => queue.Name.StartsWith(name, StringComparison.Ordinal)); attempt++)
        {
            await Task.Delay(250);
        }
        Assert.DoesNotContain((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues, queue => queue.Name.StartsWith(name, StringComparison.Ordinal));
    }

    private async Task ProduceAsync(string topic, params string[] ids)
    {
        foreach (var id in ids)
        {
            await _producer.ProduceAsync(topic, new Message<string?, string>
            {
                Key = "customer-7",
                Value = $$"""{"order":"{{id}}"}""",
                Headers = new Headers { { "MessageId", Encoding.UTF8.GetBytes(id) } }
            });
        }
    }

    /// <summary>Writes dead letters like Spring Kafka's DeadLetterPublishingRecoverer.</summary>
    private async Task DeadLetterAsync(params string[] ids)
    {
        foreach (var id in ids)
        {
            var partition = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(partition, 1);
            var offset = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(offset, 17);
            await _producer.ProduceAsync(DeadLetters, new Message<string?, string>
            {
                Key = "customer-7",
                Value = $$"""{"order":"{{id}}"}""",
                Headers = new Headers
                {
                    { "MessageId", Encoding.UTF8.GetBytes(id) },
                    { "kafka_dlt-exception-fqcn", Encoding.UTF8.GetBytes("com.example.orders.ValidationException") },
                    { "kafka_dlt-exception-message", Encoding.UTF8.GetBytes("Total must be positive") },
                    { "kafka_dlt-original-topic", Encoding.UTF8.GetBytes(_orders) },
                    { "kafka_dlt-original-partition", partition },
                    { "kafka_dlt-original-offset", offset }
                }
            });
        }
    }

    private async Task<(long Low, long High)> WatermarksAsync(string topic)
    {
        await Task.Yield();
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = Emulators.KafkaServers, GroupId = "watermarks" }).Build();
        var marks = consumer.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(10));
        return (marks.Low.Value, marks.High.Value);
    }
}
