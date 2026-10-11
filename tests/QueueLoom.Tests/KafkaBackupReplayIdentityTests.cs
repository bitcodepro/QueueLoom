using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static ServiceBusProfile KafkaReplayProfile(string endpoint = "broker.invalid:9092") =>
        CreateProfile("Kafka", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.Kafka,
            Authentication = new AuthenticationSettings(AuthenticationKind.KafkaNone),
            Kafka = new KafkaSettings(endpoint)
        };

    private static BrowsedMessage KafkaBackupRecord(string topic = "orders.DLT", int partition = 0, long offset = 0,
        string? messageId = null, ServiceBusEntityReference? source = null,
        ServiceBusSubQueue subQueue = ServiceBusSubQueue.DeadLetter,
        Message<byte[]?, byte[]?>? wireMessage = null)
    {
        var headers = new Headers();
        if (messageId is not null) headers.Add("MessageId", System.Text.Encoding.UTF8.GetBytes(messageId));
        // The displayed source is deliberately the same even when the physical DLT topic changes.
        return KafkaMessageMapper.FromKafka(new ConsumeResult<byte[]?, byte[]?>
        {
            Topic = topic, Partition = partition, Offset = offset,
            Message = wireMessage ?? new() { Value = "record"u8.ToArray(), Headers = headers }
        }, source ?? ServiceBusEntityReference.Queue("orders"), subQueue);
    }

    private static async Task AssertKafkaBackupReplayCount(QueueLoomPaths paths, ServiceBusProfile[] profiles,
        int backups, int copies)
    {
        var repository = new JsonDeadLetterBackupRepository(paths);
        var summaries = await repository.ListAsync();
        Assert.Equal(backups, summaries.Count);
        Assert.All(summaries, summary => Assert.True(summary.IsReadable, summary.Error));
        // Replay uses the cached summaries after this initial projection.
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UnixEpoch,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        await using var vm = CreateViewModel(new FakeProfileRepository(profiles, profiles[0].Id), workspace,
            new FakeDialogService { ConfirmResult = true }, repository,
            replayStore: new BatchReplayStore(paths.RootDirectory));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        vm.ReplayRate = 50;
        await vm.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(backups, vm.FilteredBackupMessages.Count);
        await vm.RestoreFilteredBackupsCommand.ExecuteAsync();
        Assert.True(workspace.SentMessages.Count > 0, vm.StatusText + " / " + vm.ReplayStatus);
        Assert.Equal(copies, workspace.SentMessages.Count);
        Assert.Equal(backups, (await repository.ListAsync()).Count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("producer-id")]
    public async Task KafkaBackupReplay_GenuineRepeatedBackupsRemainSeparateWithoutIncarnationEvidence(string? messageId)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var store = new DeadLetterJsonBackupStore(paths);
        var first = await store.CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var retry = await store.CreateSessionAsync(profile, DateTimeOffset.UnixEpoch.AddMinutes(1), default);
        var record = KafkaBackupRecord(messageId: messageId);
        var a = await first.BackupAsync(record, default);
        var b = await retry.BackupAsync(record, default);
        Assert.NotEqual(a, b);
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    [Fact]
    public async Task KafkaBackupReplay_FailedPurgeSettlementThenRetryRetainsBothCaptures()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var record = KafkaBackupRecord();
        await using var workspace = new KafkaReplayPurgeWorkspace(paths, record);
        await workspace.ConnectAsync(profile);
        var request = new DeadLetterPurgeRequest([record.Source], [ServiceBusSubQueue.DeadLetter]);

        var failed = await workspace.PurgeDeadLettersAsync(request);
        Assert.True(failed.HasFailures);
        Assert.Equal(0, failed.DeletedCount);
        Assert.Single(await new JsonDeadLetterBackupRepository(paths).ListAsync());

        workspace.FailSettlement = false;
        var retry = await workspace.PurgeDeadLettersAsync(request);
        Assert.False(retry.HasFailures);
        Assert.Equal(1, retry.DeletedCount);
        Assert.Equal(2, workspace.SettlementAttempts);
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    // Runs the production backup-before-settlement flow with an in-memory broker channel.
    private sealed class KafkaReplayPurgeWorkspace(QueueLoomPaths paths, BrowsedMessage record)
        : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public override MessagingProvider Provider => MessagingProvider.Kafka;
        public bool FailSettlement { get; set; } = true;
        public int SettlementAttempts { get; private set; }
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) =>
            Task.FromResult(new ServiceBusTopology(DateTimeOffset.UnixEpoch,
                [new ServiceBusQueue(record.Source.Name, ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology,
            ServiceBusEntityReference source, ServiceBusSubQueue subQueue) => new Channel(this, paths, record);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target,
            MessageDraft draft, CancellationToken token) => Task.CompletedTask;

        private sealed class Channel(KafkaReplayPurgeWorkspace owner, QueueLoomPaths paths, BrowsedMessage record) : ILeasedMessageChannel
        {
            private bool _received;
            public string PhysicalName => "orders.DLT";
            public int MaximumBatchSize => 1;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int count, CancellationToken token)
            {
                if (_received) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
                _received = true;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(
                    [new LeasedMessage(record, "0:0") { DeliveryIdentity = "0:0" }]);
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token) => Task.CompletedTask;
            public async Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(
                IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                Assert.False(token.CanBeCanceled);
                owner.SettlementAttempts++;
                // Each settlement is reached only after the real writer has persisted a durable capture.
                var backups = await new JsonDeadLetterBackupRepository(paths).ListAsync();
                Assert.Equal(owner.SettlementAttempts, backups.Count);
                Assert.All(backups, backup =>
                {
                    Assert.Equal(MessagingProvider.Kafka, backup.Provider);
                });
                return owner.FailSettlement ? messages : [];
            }
        }
    }

    [Fact]
    public async Task KafkaBackupReplay_SerializationLoadingAndCachingPreserveProviderAndSnapshot()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(KafkaReplayProfile(), DateTimeOffset.UnixEpoch, default);
        var headers = new Headers();
        headers.Add("opaque", [0xff]); headers.Add("opaque", null);
        var record = KafkaBackupRecord("orders.DLT", 3, long.MaxValue, wireMessage: new()
        {
            Key = [0xfe], Value = "record"u8.ToArray(), Headers = headers,
            Timestamp = new Timestamp(DateTime.UnixEpoch, TimestampType.CreateTime)
        });
        var file = await session.BackupAsync(record, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var summary = Assert.Single(await repository.ListAsync());
        var loaded = await repository.LoadAsync(summary);
        Assert.Equal(record.Body.ToArray(), loaded.Body.ToArray());
        Assert.Equal(record.EnqueuedAt, loaded.EnqueuedAt);
        Assert.Equal(record.KafkaEnvelope!.Key, loaded.KafkaEnvelope!.Key);
        Assert.Equal(record.KafkaEnvelope.IsTombstone, loaded.KafkaEnvelope.IsTombstone);
        Assert.Equal(2, loaded.KafkaEnvelope.Headers.Count);
        Assert.Equal(new byte[] { 0xff }, loaded.KafkaEnvelope.Headers[0].Value);
        Assert.Null(loaded.KafkaEnvelope.Headers[1].Value);
        Assert.Equal(MessagingProvider.Kafka, summary.Provider);
        Assert.Equal(summary, Assert.Single(await repository.ListAsync()));
        using var json = JsonDocument.Parse(await File.ReadAllTextAsync(file));
        Assert.Equal("Kafka", json.RootElement.GetProperty("provider").GetString());
    }

    [Theory]
    [InlineData("orders.dlt", 0, 0L, null)]
    [InlineData("orders.DLT", 1, 0L, null)]
    [InlineData("orders.DLT", 0, 1L, null)]
    [InlineData("orders.DLT", 0, long.MaxValue, null)]
    [InlineData("orders.dlt", 0, 0L, "shared")]
    [InlineData("orders.DLT", 1, 0L, "shared")]
    [InlineData("orders.DLT", 0, 1L, "shared")]
    [InlineData("orders.DLT", 0, long.MaxValue, "shared")]
    public async Task KafkaBackupReplay_DistinctPhysicalRecordsSurviveEvenWithEqualHashes(string topic, int partition, long offset, string? messageId)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        // Force the persisted UI hash to collide; it must never decide whether two Kafka records are one.
        foreach (var record in new[] { KafkaBackupRecord(messageId: messageId), KafkaBackupRecord(topic, partition, offset, messageId) })
        {
            var file = await session.BackupAsync(record, default);
            var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!;
            json["sequenceNumber"] = 7;
            await File.WriteAllTextAsync(file, json.ToJsonString());
        }
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    [Theory]
    [InlineData("body")]
    [InlineData("timestamp")]
    [InlineData("key")]
    [InlineData("raw-headers")]
    [InlineData("producer-id")]
    [InlineData("tombstone")]
    public async Task KafkaBackupReplay_ReusedCoordinatesWithDifferentSnapshotsKeepBothRecords(string difference)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var first = new Message<byte[]?, byte[]?>
        {
            Value = [], Timestamp = new Timestamp(DateTime.UnixEpoch, TimestampType.CreateTime), Headers = new Headers()
        };
        var second = new Message<byte[]?, byte[]?>
        {
            Value = [], Timestamp = first.Timestamp, Headers = new Headers()
        };
        switch (difference)
        {
            case "body": second.Value = "new incarnation"u8.ToArray(); break;
            case "timestamp": second.Timestamp = new Timestamp(DateTime.UnixEpoch.AddSeconds(1), TimestampType.CreateTime); break;
            case "key": second.Key = [0xff]; break;
            case "raw-headers":
                // Same editable last-header projection; the earlier raw header differs.
                first.Headers.Add("opaque", [0xff]); first.Headers.Add("opaque", [1]);
                second.Headers.Add("opaque", [0xfe]); second.Headers.Add("opaque", [1]);
                break;
            case "producer-id":
                first.Headers.Add("MessageId", "old-id"u8.ToArray());
                second.Headers.Add("MessageId", "new-id"u8.ToArray());
                break;
            case "tombstone": second.Value = null; break;
        }
        var a = KafkaBackupRecord(wireMessage: first);
        var b = KafkaBackupRecord(wireMessage: second);
        Assert.Equal(a.Position, b.Position);
        Assert.Equal(a.SequenceNumber, b.SequenceNumber);
        await session.BackupAsync(a, default);
        await session.BackupAsync(b, default);
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    [Fact]
    public async Task KafkaBackupReplay_EqualSnapshotsFromSeparateReadsDoNotProveTopicIncarnation()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        // A recreated topic may contain even the exact same bytes and timestamp at the same coordinates.
        await session.BackupAsync(KafkaBackupRecord(), default);
        await session.BackupAsync(KafkaBackupRecord(), default);
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    [Fact]
    public async Task KafkaBackupReplay_LogicalAliasesRemainSeparateWithoutIncarnationEvidence()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared",
            source: ServiceBusEntityReference.Queue("other-logical-source"), subQueue: ServiceBusSubQueue.Active), default);
        await AssertKafkaBackupReplayCount(paths, [profile], 2, 2);
    }

    [Theory]
    [InlineData("profile")]
    [InlineData("endpoint")]
    public async Task KafkaBackupReplay_DifferentEnvironmentsOrEndpointsRemainSeparate(string difference)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var other = difference == "profile" ? profile with { Id = Guid.NewGuid(), Name = "Other" }
            : profile with { Kafka = new KafkaSettings("other.invalid:9092") };
        var store = new DeadLetterJsonBackupStore(paths);
        foreach (var environment in new[] { profile, other })
        {
            var session = await store.CreateSessionAsync(environment, DateTimeOffset.UnixEpoch, default);
            await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        }
        await AssertKafkaBackupReplayCount(paths, difference == "profile" ? [profile, other] : [profile], 2, 2);
    }

    [Theory]
    [InlineData("legacy", null)]
    [InlineData("legacy", "shared")]
    [InlineData("provider", "shared")]
    [InlineData("endpoint", "shared")]
    [InlineData("unknown-provider", "shared")]
    [InlineData("blank-endpoint", "shared")]
    [InlineData("rabbit", "shared")]
    [InlineData("unknown-profile", "shared")]
    public async Task KafkaBackupReplay_AmbiguousAndRabbitBackupsRemainSeparate(string ambiguity, string? messageId)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var target = KafkaReplayProfile();
        var original = ambiguity == "rabbit" ? target with
        {
            Provider = MessagingProvider.RabbitMq, RabbitMq = new RabbitMqSettings("broker.invalid", "guest")
        } : target;
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(original, DateTimeOffset.UnixEpoch, default);
        for (var i = 0; i < 2; i++)
        {
            var file = await session.BackupAsync(KafkaBackupRecord(messageId: messageId), default);
            var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
            switch (ambiguity)
            {
                case "legacy": json.Remove("kafkaEnvelope"); json["schemaVersion"] = 1; break;
                case "provider": json.Remove("provider"); break;
                case "endpoint": json.Remove("fullyQualifiedNamespace"); break;
                case "unknown-provider": json["provider"] = "unknown"; break;
                case "blank-endpoint": json["fullyQualifiedNamespace"] = " "; break;
                case "unknown-profile": json["profileId"] = Guid.NewGuid(); break;
            }
            await File.WriteAllTextAsync(file, json.ToJsonString());
        }
        await AssertKafkaBackupReplayCount(paths, [ambiguity == "rabbit" ? original : target], 2, 2);
    }

    [Theory]
    [InlineData(MessagingProvider.RabbitMq, MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.Kafka, MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AzureServiceBus, MessagingProvider.Kafka)]
    public async Task KafkaBackupReplay_ChangedProviderDoesNotReinterpretPersistedIdentity(MessagingProvider before, MessagingProvider after)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var original = profile with { Provider = before, RabbitMq = new RabbitMqSettings("broker.invalid", "guest") };
        var current = profile with { Provider = after };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(original, DateTimeOffset.UnixEpoch, default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        await AssertKafkaBackupReplayCount(paths, [current], 2, 2);
    }

    [Fact]
    public async Task KafkaBackupReplay_OldMetadataCacheIsRebuiltBeforeReinterpretingAChangedProvider()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        await session.BackupAsync(KafkaBackupRecord(messageId: "shared"), default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        Assert.Equal(2, (await repository.ListAsync()).Count);
        foreach (var file in Directory.GetFiles(Path.Combine(directory.Path, "backup-metadata-cache")))
        {
            var cache = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
            cache.Remove("Version");
            var summary = cache["Summary"]!.AsObject();
            summary.Remove("Provider");
            await File.WriteAllTextAsync(file, cache.ToJsonString());
        }
        Assert.All(await repository.ListAsync(), summary => Assert.Equal(MessagingProvider.Kafka, summary.Provider));
        await AssertKafkaBackupReplayCount(paths, [profile with { Provider = MessagingProvider.AzureServiceBus }], 2, 2);
    }
}
