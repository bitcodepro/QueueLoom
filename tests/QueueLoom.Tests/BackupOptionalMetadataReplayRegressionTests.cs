using System.Text.Json.Nodes;
using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("hasSequenceNumber", "\"false\"", false)]
    [InlineData("hasSequenceNumber", "\"false\"", true)]
    [InlineData("position", "{\"partition\":0}", false)]
    [InlineData("position", "{\"partition\":0}", true)]
    public async Task BackupOptionalMetadata_FilteredRestoreKeepsValidAndEditedKafkaCaptures(
        string field, string value, bool warmMetadataCache)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = KafkaReplayProfile();
        var headers = new Headers();
        headers.Add("opaque", [0xff]);
        headers.Add("opaque", null);
        var original = KafkaBackupRecord(wireMessage: new()
        {
            Key = [0xfe], Value = "record"u8.ToArray(), Headers = headers
        });
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        await session.BackupAsync(original, default);
        var editedFile = await session.BackupAsync(original, default); // Same capture still represents a separate durable file.
        await session.BackupAsync(KafkaBackupRecord(source: ServiceBusEntityReference.Queue("unselected")), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(editedFile))!.AsObject();
        json[field] = JsonNode.Parse(value);
        await File.WriteAllTextAsync(editedFile, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        if (warmMetadataCache)
        {
            var summaries = await repository.ListAsync();
            Assert.Equal(3, summaries.Count);
            Assert.All(summaries, summary => Assert.True(summary.IsReadable, summary.Error));
            repository = new JsonDeadLetterBackupRepository(paths);
        }
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UnixEpoch,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true }, repository,
            replayStore: new BatchReplayStore(paths.RootDirectory));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        vm.ReplayRate = 50;
        vm.PreserveReplayMessageIds = true;
        await vm.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(3, vm.BackupMessages.Count);
        vm.BackupFilterText = "orders";
        Assert.Equal(2, vm.FilteredBackupMessages.Count);
        Assert.All(vm.FilteredBackupMessages, item => Assert.True(item.Summary.IsReadable, item.Summary.Error));

        await vm.RestoreFilteredBackupsCommand.ExecuteAsync();

        Assert.True(workspace.SentMessages.Count == 2, vm.StatusText + " / " + vm.ReplayStatus);
        foreach (var send in workspace.SentMessages)
        {
            var replay = KafkaMessageMapper.ToKafka(send.Message);
            Assert.Equal(original.Body.ToArray(), replay.Value);
            Assert.Equal(original.KafkaEnvelope!.Key, replay.Key);
            var opaque = replay.Headers.Where(header => header.Key == "opaque").ToArray();
            Assert.Equal(2, opaque.Length);
            Assert.Equal(new byte[] { 0xff }, opaque[0].GetValueBytes());
            Assert.Null(opaque[1].GetValueBytes());
        }
        var retained = await new JsonDeadLetterBackupRepository(paths).ListAsync();
        Assert.Equal(3, retained.Count);
        Assert.All(retained, summary => Assert.True(summary.IsReadable, summary.Error));
    }
}
