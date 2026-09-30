using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>
/// Topics of a Kafka cluster, shown as queues. A topic named like another one plus a dead-letter ending
/// ("orders.DLT") is its dead-letter queue. Kafka keeps messages after they are read, so reading is by offset without
/// a consumer group and changes nothing. Single messages cannot be removed; emptying a dead-letter topic deletes its
/// records from the oldest on.
/// </summary>
public sealed partial class KafkaWorkspace : LeasedMessagingWorkspace
{
    private const int MaximumBatch = 100;
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(15);

    private readonly ISecretVault _secretVault;
    private readonly List<KafkaChannel> _channels = [];
    private ClientConfig? _config;
    private IAdminClient? _admin;
    private IProducer<byte[]?, byte[]>? _producer;
    private SchemaRegistryClient? _schemaRegistry;
    private KafkaTopologyIndex _index = KafkaTopologyIndex.Empty;

    public KafkaWorkspace(ISecretVault secretVault, TimeProvider? timeProvider = null, DeadLetterJsonBackupStore? backupStore = null)
        : base(backupStore, timeProvider)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
    }

    public override MessagingProvider Provider => MessagingProvider.Kafka;

    private IAdminClient Admin => _admin ?? throw new InvalidOperationException("Connect to the environment first.");

    private ClientConfig Config => _config ?? throw new InvalidOperationException("Connect to the environment first.");

    protected override async Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        var settings = profile.Kafka ?? throw new InvalidOperationException("The Kafka settings are missing.");
        var config = new ClientConfig
        {
            BootstrapServers = settings.BootstrapServers,
            ClientId = "QueueLoom",
            SocketTimeoutMs = (int)RequestTimeout.TotalMilliseconds
        };
        var sasl = profile.Authentication.Kind == AuthenticationKind.KafkaSaslPassword;
        config.SecurityProtocol = (settings.UseTls, sasl) switch
        {
            (true, true) => SecurityProtocol.SaslSsl,
            (true, false) => SecurityProtocol.Ssl,
            (false, true) => SecurityProtocol.SaslPlaintext,
            _ => SecurityProtocol.Plaintext
        };
        if (sasl)
        {
            config.SaslMechanism = settings.SaslMechanism switch
            {
                KafkaSaslMechanism.Plain => SaslMechanism.Plain,
                KafkaSaslMechanism.ScramSha256 => SaslMechanism.ScramSha256,
                _ => SaslMechanism.ScramSha512
            };
            config.SaslUsername = settings.UserName;
            config.SaslPassword = await _secretVault.RetrieveAsync(ProfileSecretKey.ConnectionString(profile.Id), cancellationToken)
                                      .ConfigureAwait(false)
                                  ?? throw new InvalidOperationException("The Kafka password is missing. Edit the environment and enter it again.");
        }

        if (settings.SchemaRegistryUrl is { } registryUrl)
        {
            var registryPassword = settings.SchemaRegistryUserName is null
                ? null
                : await _secretVault.RetrieveAsync(ProfileSecretKey.SchemaRegistryPassword(profile.Id), cancellationToken).ConfigureAwait(false);
            _schemaRegistry = new SchemaRegistryClient(registryUrl, settings.SchemaRegistryUserName, registryPassword);
        }

        _config = config;
        _index = KafkaTopologyIndex.Empty with { Suffixes = settings.EffectiveDeadLetterSuffixes };
        _admin = new AdminClientBuilder(new AdminClientConfig(config)).Build();
        try
        {
            // Proves the servers and the credentials without touching any topic.
            var metadata = await Task.Run(() => Admin.GetMetadata(RequestTimeout), cancellationToken).ConfigureAwait(false);
            if (metadata.Brokers.Count == 0)
            {
                throw new InvalidOperationException($"No Kafka broker answered at {settings.BootstrapServers}.");
            }
            _producer = new ProducerBuilder<byte[]?, byte[]>(new ProducerConfig(config) { Acks = Acks.All, MessageTimeoutMs = 30_000 }).Build();
        }
        catch (KafkaException exception)
        {
            await CloseAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"Kafka at {settings.BootstrapServers} could not be reached: {exception.Error.Reason}", exception);
        }
        catch
        {
            await CloseAsync().ConfigureAwait(false);
            throw;
        }
    }

    protected override ValueTask CloseAsync()
    {
        CloseChannels();
        _producer?.Dispose();
        _producer = null;
        _admin?.Dispose();
        _admin = null;
        _schemaRegistry?.Dispose();
        _schemaRegistry = null;
        _config = null;
        return ValueTask.CompletedTask;
    }

    protected override async Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken)
    {
        var suffixes = _index.Suffixes;
        _index = await Task.Run(() =>
        {
            var metadata = Admin.GetMetadata(RequestTimeout);
            using var consumer = CreateConsumer();
            var topics = metadata.Topics
                .Where(topic => topic.Error.Code == ErrorCode.NoError && !KafkaTopologyIndex.IsInternal(topic.Topic))
                .Select(topic =>
                {
                    long retained = 0;
                    string? countError = null;
                    foreach (var partition in topic.Partitions)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var marks = QueryWatermarks(consumer, new TopicPartition(topic.Topic, partition.PartitionId), cancellationToken);
                        if (marks is null)
                        {
                            countError = "counts unavailable: a partition has no leader right now";
                            retained = 0;
                            break;
                        }
                        retained += Math.Max(0, marks.High.Value - marks.Low.Value);
                    }
                    return new KafkaTopicInfo(topic.Topic, topic.Partitions.Select(partition => partition.PartitionId).ToArray(), retained)
                    {
                        CountError = countError
                    };
                })
                .ToArray();
            return new KafkaTopologyIndex(topics, suffixes);
        }, cancellationToken).ConfigureAwait(false);
        return _index.ToTopology(TimeProvider.GetUtcNow());
    }

    protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        if (source.Kind != ServiceBusEntityKind.Queue)
        {
            throw new InvalidOperationException("Kafka keeps messages in topics. Pick a topic.");
        }
        if (subQueue == ServiceBusSubQueue.TransferDeadLetter)
        {
            throw new InvalidOperationException("Kafka has no transfer dead-letter queues.");
        }

        var topic = _index.Find(source.Name) ?? throw new InvalidOperationException($"Topic '{source.Name}' was not found. Refresh and try again.");
        if (subQueue == ServiceBusSubQueue.DeadLetter)
        {
            topic = _index.DeadLetterTopicOf(topic.Name) is { } name ? _index.Find(name)! : throw new InvalidOperationException(
                $"Topic '{source.Name}' has no dead-letter topic. QueueLoom looks for a topic named {source.Name} plus one of: " +
                string.Join(", ", _index.Suffixes) + ".");
        }

        // Operations run one at a time, so a new channel means the previous operation is over.
        CloseChannels();
        var channel = new KafkaChannel(this, topic, source, subQueue, belongsTo: subQueue == ServiceBusSubQueue.DeadLetter ? source.Name : null);
        _channels.Add(channel);
        return channel;
    }

    protected override ILeasedMessageChannel OpenBrowseChannel(ServiceBusTopology topology, BrowseMessagesRequest request)
    {
        var channel = (KafkaChannel)OpenChannel(topology, request.Source, request.SubQueue);
        channel.StartAt(request.Start, request.LoadAll ? LoadAllLimit : request.MaxMessages);
        return channel;
    }

    protected override async Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message,
        CancellationToken cancellationToken)
    {
        if (message.Properties.ScheduledEnqueueTime is not null)
        {
            throw new InvalidOperationException("Kafka cannot schedule messages. Clear the scheduled time.");
        }
        var producer = _producer ?? throw new InvalidOperationException("Connect to the environment first.");
        try
        {
            await producer.ProduceAsync(destination.Name, KafkaMessageMapper.ToKafka(message), cancellationToken).ConfigureAwait(false);
        }
        catch (ProduceException<byte[]?, byte[]> exception)
        {
            throw new InvalidOperationException($"Kafka did not accept the message: {exception.Error.Reason}", exception);
        }
    }

    /// <summary>
    /// Watermarks of one partition. A topic that was just created, or is being deleted or moved, can briefly have no
    /// leader; that is retried a few times and then reported as unknown rather than failing the whole topology.
    /// </summary>
    private static WatermarkOffsets? QueryWatermarks(IConsumer<byte[]?, byte[]?> consumer, TopicPartition partition, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return consumer.QueryWatermarkOffsets(partition, RequestTimeout);
            }
            catch (KafkaException exception) when (IsTransient(exception.Error.Code))
            {
                if (attempt == 4)
                {
                    return null;
                }
                cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(250 * (attempt + 1)));
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
    }

    private static bool IsTransient(ErrorCode code) => code is ErrorCode.NotLeaderForPartition or ErrorCode.LeaderNotAvailable
        or ErrorCode.UnknownTopicOrPart or ErrorCode.Local_UnknownPartition or ErrorCode.Local_UnknownTopic;

    private IConsumer<byte[]?, byte[]?> CreateConsumer() =>
        new ConsumerBuilder<byte[]?, byte[]?>(new ConsumerConfig(Config)
        {
            // Partitions are assigned by hand and nothing is committed, so the group never appears on the cluster.
            GroupId = $"queueloom-reader-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        }).Build();

    private void CloseChannels()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
        _channels.Clear();
    }

    /// <summary>
    /// Reads a topic up to the end it had when reading started: by default from the oldest retained message, or from a
    /// time, an offset, or only the newest messages. "Releasing" does nothing, since reading changes nothing;
    /// "settling" deletes records, which Kafka allows only from the oldest on.
    /// </summary>
    private sealed class KafkaChannel(
        KafkaWorkspace owner,
        KafkaTopicInfo topic,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        string? belongsTo) : ILeasedMessageChannel, IDisposable
    {
        private readonly Dictionary<int, long> _end = [];
        private readonly Dictionary<int, long> _deletableFrom = [];
        private readonly HashSet<int> _finished = [];
        private IConsumer<byte[]?, byte[]?>? _consumer;
        private BrowseStart _start = BrowseStart.Oldest;
        private int _newestCount;
        private Queue<LeasedMessage>? _newest;

        /// <summary>Sets where browsing starts; <paramref name="count"/> is how many of the newest messages to keep.</summary>
        public void StartAt(BrowseStart start, int count)
        {
            _start = start;
            _newestCount = count;
        }

        public string PhysicalName => topic.Name;

        public int MaximumBatchSize => MaximumBatch;

        public async Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            var messages = await Task.Run(() => Receive(maxMessages, cancellationToken), cancellationToken).ConfigureAwait(false);
            if (owner._schemaRegistry is not { } registry)
            {
                return messages;
            }

            for (var index = 0; index < messages.Count; index++)
            {
                var message = messages[index].Message;
                if (BodyDecoder.TryReadSchemaId(message.Body.Span, out var schemaId) &&
                    await registry.GetAsync(schemaId, cancellationToken).ConfigureAwait(false) is { } schema)
                {
                    messages[index] = messages[index] with { Message = message with { Schema = schema } };
                }
            }
            return messages;
        }

        private List<LeasedMessage> Receive(int maxMessages, CancellationToken cancellationToken)
        {
            if (_start.Kind != BrowseStartKind.Newest)
            {
                return ReceiveRange(Math.Clamp(maxMessages, 1, MaximumBatch), cancellationToken);
            }

            // The newest messages: each partition's tail is read whole, then the latest ones by time are kept.
            if (_newest is null)
            {
                var tail = ReceiveRange(int.MaxValue, cancellationToken);
                _newest = new Queue<LeasedMessage>(tail
                    .OrderByDescending(message => message.Message.EnqueuedAt ?? DateTimeOffset.MinValue)
                    .ThenByDescending(message => message.Message.Position?.Offset ?? 0)
                    .Take(Math.Max(1, _newestCount)));
            }
            var batch = new List<LeasedMessage>();
            while (batch.Count < Math.Clamp(maxMessages, 1, MaximumBatch) && _newest.TryDequeue(out var next))
            {
                batch.Add(next);
            }
            return batch;
        }

        private List<LeasedMessage> ReceiveRange(int limit, CancellationToken cancellationToken)
        {
            var consumer = Open();
            var messages = new List<LeasedMessage>();
            while (messages.Count < limit && _finished.Count < _end.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is null)
                {
                    break;
                }
                var partition = result.Partition.Value;
                if (_finished.Contains(partition))
                {
                    // Written after reading started; this read stops at the end it saw.
                    continue;
                }
                if (result.IsPartitionEOF || result.Offset.Value >= _end[partition])
                {
                    _finished.Add(partition);
                    continue;
                }
                messages.Add(ToLeased(result));
                if (result.Offset.Value >= _end[partition] - 1)
                {
                    _finished.Add(partition);
                }
            }
            return messages;
        }

        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            Dispose();
            return Task.CompletedTask;
        }

        public async Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            var failed = new List<LeasedMessage>();
            var deletions = new List<TopicPartitionOffset>();
            foreach (var partition in messages.GroupBy(message => Position(message).Partition))
            {
                var next = _deletableFrom[partition.Key];
                foreach (var message in partition.OrderBy(message => Position(message).Offset))
                {
                    if (Position(message).Offset == next)
                    {
                        next++;
                    }
                    else
                    {
                        failed.Add(message);
                    }
                }
                if (next > _deletableFrom[partition.Key])
                {
                    deletions.Add(new TopicPartitionOffset(topic.Name, partition.Key, next));
                }
            }

            if (deletions.Count > 0)
            {
                try
                {
                    await owner.Admin.DeleteRecordsAsync(deletions, new DeleteRecordsOptions { RequestTimeout = RequestTimeout }).ConfigureAwait(false);
                    foreach (var deletion in deletions)
                    {
                        _deletableFrom[deletion.Partition.Value] = deletion.Offset.Value;
                    }
                }
                catch (DeleteRecordsException)
                {
                    return messages;
                }
            }
            return failed;
        }

        public void Dispose()
        {
            _consumer?.Close();
            _consumer?.Dispose();
            _consumer = null;
        }

        private IConsumer<byte[]?, byte[]?> Open()
        {
            if (_consumer is not null)
            {
                return _consumer;
            }

            _consumer = owner.CreateConsumer();
            var byTime = _start is { Kind: BrowseStartKind.FromTime, Time: { } time }
                ? _consumer.OffsetsForTimes(
                        topic.Partitions.Select(partition => new TopicPartitionTimestamp(topic.Name, partition, new Timestamp(time.UtcDateTime))),
                        RequestTimeout)
                    .ToDictionary(offset => offset.Partition.Value, offset => offset.Offset.Value)
                : null;
            var assignments = new List<TopicPartitionOffset>();
            foreach (var partition in topic.Partitions)
            {
                var marks = _consumer.QueryWatermarkOffsets(new TopicPartition(topic.Name, partition), RequestTimeout);
                var (from, to) = Range(partition, marks.Low.Value, marks.High.Value, byTime);
                _end[partition] = to;
                _deletableFrom[partition] = marks.Low.Value;
                if (to > from)
                {
                    assignments.Add(new TopicPartitionOffset(topic.Name, partition, new Offset(from)));
                }
                else
                {
                    _finished.Add(partition);
                }
            }
            _consumer.Assign(assignments);
            return _consumer;
        }

        /// <summary>The offsets [from, to) to read in one partition.</summary>
        private (long From, long To) Range(int partition, long low, long high, IReadOnlyDictionary<int, long>? byTime)
        {
            long? position = _start.Positions?.TryGetValue(partition, out var saved) == true ? saved : null;
            var (from, to) = _start.Kind switch
            {
                BrowseStartKind.Newest => (0L, Math.Min(position ?? high, high)),
                BrowseStartKind.FromTime => (position ?? (byTime?.TryGetValue(partition, out var at) == true && at >= 0 ? at : high), high),
                BrowseStartKind.FromOffset when _start.Partition is { } only && only != partition => (high, high),
                BrowseStartKind.FromOffset => (position ?? _start.Offset ?? low, high),
                _ => (position ?? low, high)
            };
            if (_start.Kind == BrowseStartKind.Newest)
            {
                from = to - Math.Max(1, _newestCount);
            }
            return (Math.Clamp(from, low, high), to);
        }

        private LeasedMessage ToLeased(ConsumeResult<byte[]?, byte[]?> result)
        {
            var message = KafkaMessageMapper.FromKafka(result, source, subQueue);
            var original = KafkaMessageMapper.OriginalTopic(result.Message.Headers);
            return new LeasedMessage(message, $"{result.Partition.Value}:{result.Offset.Value}",
                BelongsToSource: belongsTo is null || original is null || original == belongsTo);
        }

        private static (int Partition, long Offset) Position(LeasedMessage message)
        {
            var parts = message.LeaseHandle.Split(':');
            return (int.Parse(parts[0], CultureInfo.InvariantCulture), long.Parse(parts[1], CultureInfo.InvariantCulture));
        }
    }
}
