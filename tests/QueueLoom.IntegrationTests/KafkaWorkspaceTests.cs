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

    public async ValueTask InitializeAsync()
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

    public async ValueTask DisposeAsync()
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
    public async Task CycleTwoKafka_NewestPagesIncludeRecordsSeparatedByTransactionMarkers()
    {
        using var producer = new ProducerBuilder<string?, string>(new ProducerConfig
        { BootstrapServers = Emulators.KafkaServers, TransactionalId = Emulators.Unique("paging"), EnableIdempotence = true }).Build();
        producer.InitTransactions(TimeSpan.FromSeconds(30));
        for (var index = 0; index < 101; index++)
        {
            producer.BeginTransaction();
            await producer.ProduceAsync(new TopicPartition(_payments, 0), new Message<string?, string> { Value = "record-" + index });
            producer.CommitTransaction(TimeSpan.FromSeconds(30));
        }
        Assert.True((await WatermarksAsync(_payments)).High > 101);
        var source = ServiceBusEntityReference.Queue(_payments);
        var first = await _workspace.BrowseMessagesAsync(new(source, maxMessages: 100) { Start = new(BrowseStartKind.Newest) });
        Assert.Equal(100, first.Count);
        var nextOffset = first.Min(m => m.Position!.Value.Offset);
        var second = await _workspace.BrowseMessagesAsync(new(source, maxMessages: 100)
        { Start = new(BrowseStartKind.Newest) { Positions = new Dictionary<int, long> { [0] = nextOffset } } });
        Assert.Single(second);
        Assert.Equal("record-0", Encoding.UTF8.GetString(second[0].Body.Span));
        Assert.Equal(101, first.Concat(second).Select(m => m.Position).Distinct().Count());
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Records_that_share_a_MessageId_are_each_shown_and_purged()
    {
        await DeadLetterAsync("o-3", "o-3");
        var orders = ServiceBusEntityReference.Queue(_orders);

        Assert.Equal(2, (await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter))).Count);
        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(orders, ServiceBusSubQueue.DeadLetter)], batchSize: 20, maximumMessagesPerSubQueue: 100));

        Assert.Equal(2, purge.DeletedCount);
        Assert.False(purge.HasFailures);
        Assert.Equal((2L, 2L), await WatermarksAsync(DeadLetters));
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task A_dead_letter_topic_written_in_transactions_can_be_purged()
    {
        using (var producer = new ProducerBuilder<string?, string>(new ProducerConfig
               { BootstrapServers = Emulators.KafkaServers, TransactionalId = Emulators.Unique("dlt"), EnableIdempotence = true }).Build())
        {
            producer.InitTransactions(TimeSpan.FromSeconds(30));
            for (var index = 0; index < 5; index++)
            {
                producer.BeginTransaction();
                await producer.ProduceAsync(new TopicPartition(DeadLetters, 0), new Message<string?, string>
                {
                    Value = "dead-" + index,
                    Headers = new Headers { { "kafka_dlt-original-topic", Encoding.UTF8.GetBytes(_orders) } }
                });
                producer.CommitTransaction(TimeSpan.FromSeconds(30));
            }
        }
        var orders = ServiceBusEntityReference.Queue(_orders);
        Assert.True((await WatermarksAsync(DeadLetters)).High > 5);

        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(orders, ServiceBusSubQueue.DeadLetter)], batchSize: 20, maximumMessagesPerSubQueue: 100));

        Assert.Equal(5, purge.DeletedCount);
        Assert.False(purge.HasFailures);
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter)));
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
        Assert.Equal(new LogPosition(0, 0), message.Position);
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

    [EmulatorFact(Emulators.Kafka)]
    public async Task Reading_starts_at_the_newest_a_time_or_an_offset_and_pages_on()
    {
        var start = new DateTimeOffset(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);
        for (var index = 0; index < 5; index++)
        {
            await _producer.ProduceAsync(_payments, new Message<string?, string>
            {
                Value = $"p-{index}",
                Timestamp = new Timestamp(start.AddMinutes(index).UtcDateTime),
                Headers = new Headers { { "MessageId", Encoding.UTF8.GetBytes($"p-{index}") } }
            });
        }
        var payments = ServiceBusEntityReference.Queue(_payments);
        async Task<string[]> ReadAsync(BrowseStart from, int count = 100) =>
            (await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(payments, maxMessages: count) { Start = from }))
            .Select(message => message.Properties.MessageId!).ToArray();

        Assert.Equal(["p-4", "p-3"], await ReadAsync(new BrowseStart(BrowseStartKind.Newest), 2));
        Assert.Equal(["p-2", "p-1"], await ReadAsync(new BrowseStart(BrowseStartKind.Newest) { Positions = new Dictionary<int, long> { [0] = 3 } }, 2));
        Assert.Equal(["p-3", "p-4"], await ReadAsync(new BrowseStart(BrowseStartKind.FromOffset, Offset: 3)));
        Assert.Empty(await ReadAsync(new BrowseStart(BrowseStartKind.FromOffset, Offset: 3, Partition: 1)));
        Assert.Equal(["p-2", "p-3", "p-4"], await ReadAsync(new BrowseStart(BrowseStartKind.FromTime, Time: start.AddMinutes(1.5))));
        Assert.Equal(["p-1", "p-2"], await ReadAsync(BrowseStart.Oldest with { Positions = new Dictionary<int, long> { [0] = 1 } }, 2));
        var positioned = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(payments, maxMessages: 1));
        Assert.Equal(new LogPosition(0, 0), positioned[0].Position);
    }

    [EmulatorFact(Emulators.Kafka)]
    public async Task Consumer_group_lag_is_shown_on_the_topic()
    {
        await ProduceAsync(_payments, "p-1", "p-2", "p-3");
        var group = Emulators.Unique("billing");
        using (var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
               {
                   BootstrapServers = Emulators.KafkaServers, GroupId = group, EnableAutoCommit = false
               }).Build())
        {
            // A fresh broker creates its offsets topic on the first commit and answers "not coordinator" meanwhile.
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    consumer.Commit([new TopicPartitionOffset(_payments, 0, 1)]);
                    break;
                }
                catch (KafkaException exception) when (attempt < 60 && exception.Error.Code is ErrorCode.NotCoordinatorForGroup
                                                           or ErrorCode.GroupLoadInProgress or ErrorCode.GroupCoordinatorNotAvailable)
                {
                    await Task.Delay(500);
                }
            }
        }

        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);

        var payments = topology.Queues.Single(queue => queue.Name == _payments);
        var lag = Assert.Single(payments.Consumers!.Groups, item => item.Group == group);
        Assert.Equal(2, lag.Lag);
        Assert.Null(topology.Queues.Single(queue => queue.Name == _orders).Consumers);
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
