using Confluent.Kafka;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class KafkaAuditRegressionTests
{
    private static ServiceBusProfile KafkaProfile(string url) =>
        ServiceBusProfile.CreateNew("Events", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings("broker.invalid:9092") { SchemaRegistryUrl = url } };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void KafkaReplay_PreservesNullVersusEmptyValue(bool tombstone)
    {
        var original = new Message<byte[]?, byte[]?> { Value = tombstone ? null : [], Headers = new Headers() };
        var replay = KafkaMessageMapper.ToKafka(Browse(original).CreateDraft());
        Assert.Equal(original.Value, replay.Value);
    }

    [Fact]
    public void KafkaReplay_PreservesBinaryKey()
    {
        var original = new Message<byte[]?, byte[]?> { Key = [0xff], Value = [], Headers = new Headers() };
        Assert.Equal(original.Key, KafkaMessageMapper.ToKafka(Browse(original).CreateDraft()).Key);
    }

    [Fact]
    public void KafkaReplay_PreservesBinaryHeader()
    {
        var headers = new Headers();
        headers.Add("opaque", [0xff]);
        var replay = KafkaMessageMapper.ToKafka(Browse(new() { Value = [], Headers = headers }).CreateDraft());
        Assert.Equal(new byte[] { 0xff }, Assert.Single(replay.Headers).GetValueBytes());
    }

    [Fact]
    public void KafkaReplay_PreservesDuplicateHeaders()
    {
        var headers = new Headers();
        headers.Add("repeated", [1]);
        headers.Add("repeated", [2]);
        var replay = KafkaMessageMapper.ToKafka(Browse(new() { Value = [], Headers = headers }).CreateDraft());
        Assert.Equal(2, replay.Headers.Count);
        Assert.Equal(new byte[] { 1 }, replay.Headers[0].GetValueBytes());
        Assert.Equal(new byte[] { 2 }, replay.Headers[1].GetValueBytes());
    }

    [Fact]
    public async Task KafkaBackup_ReloadDraftReplayPreservesRawEnvelope()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var headers = new Headers();
        headers.Add("opaque", [0xff]);
        headers.Add("opaque", null);
        var original = new Message<byte[]?, byte[]?> { Key = [0xff], Value = null, Headers = headers };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(KafkaProfile("https://registry.invalid"), DateTimeOffset.UtcNow, CancellationToken.None);
        await session.BackupAsync(Browse(original), CancellationToken.None);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        var replay = KafkaMessageMapper.ToKafka(restored.CreateDraft());
        Assert.Null(replay.Value);
        Assert.Equal(original.Key, replay.Key);
        Assert.Equal(2, replay.Headers.Count);
        Assert.Equal(new byte[] { 0xff }, replay.Headers[0].GetValueBytes());
        Assert.Null(replay.Headers[1].GetValueBytes());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaBatchReplay_RetainsRawEnvelopeInDurablePayload(bool tombstone)
    {
        using var directory = new TemporaryDirectory();
        var profile = KafkaProfile("https://registry.invalid");
        var headers = new Headers();
        headers.Add("opaque", [0xff]);
        headers.Add("repeated", [1]);
        headers.Add("repeated", [2]);
        var original = new Message<byte[]?, byte[]?> { Key = [0xff], Value = tombstone ? null : [], Headers = headers };
        var store = new BatchReplayStore(directory.Path);
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("isolated"),
            [(Browse(original).CreateDraft(), "isolated fixture")], true, 50, CancellationToken.None);
        await using var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await new BatchReplayStore(directory.Path).RunAsync(plan, workspace, () => true, null, CancellationToken.None);
        var replay = KafkaMessageMapper.ToKafka(Assert.Single(workspace.SentMessages).Message);
        Assert.Equal(original.Value, replay.Value);
        Assert.Equal(original.Key, replay.Key);
        var retained = replay.Headers.Where(header => header.Key is "opaque" or "repeated").ToArray();
        Assert.Equal(3, retained.Length);
        Assert.Equal(new byte[] { 0xff }, retained[0].GetValueBytes());
        Assert.Equal(new byte[] { 1 }, retained[1].GetValueBytes());
        Assert.Equal(new byte[] { 2 }, retained[2].GetValueBytes());
    }
    private static BrowsedMessage Browse(Message<byte[]?, byte[]?> message) => KafkaMessageMapper.FromKafka(
        new ConsumeResult<byte[]?, byte[]?> { Topic = "isolated", Partition = new Partition(0), Offset = new Offset(1), Message = message },
        ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active);

    [Fact]
    public void KafkaReplay_HeaderEditsPreserveUneditedRawMetadata()
    {
        var headers = new Headers();
        headers.Add("opaque", [0xff]);
        headers.Add("editable", "old"u8.ToArray());
        var draft = Browse(new() { Key = [0xff], Value = "old"u8.ToArray(), Headers = headers }).CreateDraft();
        var changed = new MessageRewrite("old", "new", InProperties: true).Apply(draft);
        var replay = KafkaMessageMapper.ToKafka(changed);
        Assert.Equal(new byte[] { 0xff }, replay.Key);
        Assert.Equal(new byte[] { 0xff }, replay.Headers.Single(header => header.Key == "opaque").GetValueBytes());
        Assert.Equal("new"u8.ToArray(), replay.Headers.Single(header => header.Key == "editable").GetValueBytes());
        Assert.Equal("new"u8.ToArray(), replay.Value);
    }

    [Fact]
    public void ScheduledStore_PreservesConfigurationIdentityAndKafkaEnvelope()
    {
        using var directory = new TemporaryDirectory();
        var profile = KafkaProfile("https://registry.invalid");
        var message = Browse(new() { Key = [0xff], Value = null, Headers = new Headers() });
        var job = new ScheduledResend(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
            ResendMode.Copy, 0, "isolated", [ScheduledResendItem.From(new ResendItem(message, message.Source, message.CreateDraft()))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        store.Save([job]);
        var loaded = Assert.Single(store.Load());
        Assert.Equal(job.ConfigurationIdentity, loaded.ConfigurationIdentity);
        var replay = KafkaMessageMapper.ToKafka(Assert.Single(loaded.Items).Message);
        Assert.Null(replay.Value);
        Assert.Equal(new byte[] { 0xff }, replay.Key);
    }
}
