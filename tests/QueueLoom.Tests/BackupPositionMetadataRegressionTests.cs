using System.Text.Json;
using System.Text.Json.Nodes;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class BackupPositionMetadataRegressionTests
{
    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    [InlineData(MessagingProvider.GooglePubSub)]
    [InlineData(MessagingProvider.RabbitMq)]
    [InlineData(MessagingProvider.Kafka)]
    public async Task BackupPosition_SyntheticSequenceDoesNotBecomeABrokerSequence(MessagingProvider provider)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Synthetic", EnvironmentKind.Test) with { Provider = provider };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var original = Message() with { HasSequenceNumber = false };
        await session.BackupAsync(original, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var summary = Assert.Single(await repository.ListAsync());
        Assert.Equal(summary, Assert.Single(await repository.ListAsync())); // Exercise the ordinary metadata cache too.
        var loaded = await repository.LoadAsync(summary);

        Assert.False(loaded.HasSequenceNumber);
        Assert.Equal(original.SequenceNumber, loaded.SequenceNumber); // Keep the existing synthetic key.
        Assert.Equal("—", new MessageItemViewModel(loaded, profile.Id, profile.Name).SequenceDisplay);
        using var exported = new MemoryStream();
        await MessageExport.WriteJsonAsync(exported, [new(profile.Name, loaded)], default);
        using var json = JsonDocument.Parse(exported.ToArray());
        Assert.False(json.RootElement[0].TryGetProperty("sequenceNumber", out _));
    }

    [Theory]
    [InlineData(0, 0L)]
    [InlineData(3, long.MaxValue)]
    public async Task BackupPosition_RecordedKafkaPartitionAndOffsetSurviveLoadAndExport(int partition, long offset)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Synthetic Kafka", EnvironmentKind.Test) with { Provider = MessagingProvider.Kafka };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var original = Message() with { HasSequenceNumber = false, Position = new LogPosition(partition, offset) };
        await session.BackupAsync(original, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var loaded = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

        Assert.Equal(original.Position, loaded.Position);
        Assert.False(loaded.HasSequenceNumber);
        Assert.Equal($"partition {partition} · offset {offset}", new MessageItemViewModel(loaded, profile.Id, profile.Name).MessageId);
        using var exported = new MemoryStream();
        await MessageExport.WriteJsonAsync(exported, [new(profile.Name, loaded)], default);
        using var json = JsonDocument.Parse(exported.ToArray());
        Assert.Equal(partition, json.RootElement[0].GetProperty("partition").GetInt32());
        Assert.Equal(offset, json.RootElement[0].GetProperty("offset").GetInt64());
        Assert.False(json.RootElement[0].TryGetProperty("sequenceNumber", out _));
        using var csv = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        await MessageExport.WriteCsvAsync(csv, [new(profile.Name, loaded)], default);
        Assert.Contains($",{partition},{offset},", csv.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus, true)]
    [InlineData(MessagingProvider.AmazonSqsSns, false)]
    [InlineData(MessagingProvider.GooglePubSub, false)]
    [InlineData(MessagingProvider.RabbitMq, false)]
    [InlineData(MessagingProvider.Kafka, false)]
    public async Task BackupPosition_LegacyFilesUseRecordedProviderWithoutInventingPosition(MessagingProvider provider, bool hasSequenceNumber)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Legacy", EnvironmentKind.Test) with { Provider = provider };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var file = await session.BackupAsync(Message(), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json.Remove("hasSequenceNumber");
        json.Remove("position");
        await File.WriteAllTextAsync(file, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        var loaded = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

        Assert.Equal(hasSequenceNumber, loaded.HasSequenceNumber);
        Assert.Null(loaded.Position); // A UI hash is not evidence of a historical partition/offset.
    }

    [Fact]
    public async Task BackupPosition_LegacyAzureWithoutProviderRetainsItsSequenceNumber()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Legacy Azure", EnvironmentKind.Test);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var file = await session.BackupAsync(Message(), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json.Remove("hasSequenceNumber"); json.Remove("position"); json.Remove("provider");
        await File.WriteAllTextAsync(file, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        var loaded = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        Assert.True(loaded.HasSequenceNumber);
        Assert.Equal(7, loaded.SequenceNumber);
        Assert.Null(loaded.Position);
    }

    public static IEnumerable<object?[]> SequenceFlagCases()
    {
        foreach (var provider in new MessagingProvider?[] { MessagingProvider.AzureServiceBus, MessagingProvider.Kafka, null })
        {
            foreach (var value in new[] { "null", "\"false\"", "0", "{}", "[]", "true", "false" })
            {
                var expected = value is "true" or "false" ? value == "true" : provider is null or MessagingProvider.AzureServiceBus;
                yield return [value, provider, expected];
            }
        }
    }

    [Theory]
    [MemberData(nameof(SequenceFlagCases))]
    public async Task BackupPosition_OptionalFlagUsesOnlyBooleansAndOtherwiseLegacyDefault(
        string value, MessagingProvider? provider, bool expected)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Edited flag", EnvironmentKind.Test)
            with { Provider = provider ?? MessagingProvider.AzureServiceBus };
        var original = MessageWithEnvelope();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var file = await session.BackupAsync(original, default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json["hasSequenceNumber"] = JsonNode.Parse(value);
        if (provider is null) json.Remove("provider");
        await File.WriteAllTextAsync(file, json.ToJsonString());
        // First List reads the durable file; the next repository uses its ordinarily written metadata cache.
        for (var read = 0; read < 2; read++)
        {
            var repository = new JsonDeadLetterBackupRepository(paths);
            var summary = Assert.Single(await repository.ListAsync());
            Assert.True(summary.IsReadable);
            var loaded = await repository.LoadAsync(summary);

            Assert.Equal(expected, loaded.HasSequenceNumber);
            AssertPayloadAndEnvelope(original, loaded);
        }
    }

    [Theory]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("0")]
    [InlineData("\"bad\"")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("{\"partition\":0}")]
    [InlineData("{\"offset\":0}")]
    [InlineData("{\"partition\":\"0\",\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":\"0\"}")]
    [InlineData("{\"partition\":null,\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":null}")]
    [InlineData("{\"partition\":[],\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":{}}")]
    [InlineData("{\"partition\":true,\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":false}")]
    [InlineData("{\"partition\":2147483648,\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":9223372036854775808}")]
    [InlineData("{\"partition\":1.5,\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":0.5}")]
    [InlineData("{\"partition\":-1,\"offset\":0}")]
    [InlineData("{\"partition\":0,\"offset\":-1}")]
    public async Task BackupPosition_InvalidOptionalPositionLeavesTheBodyLoadable(string value)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Edited position", EnvironmentKind.Test) with { Provider = MessagingProvider.Kafka };
        var original = MessageWithEnvelope() with { HasSequenceNumber = false, Position = new LogPosition(0, 0) };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        var file = await session.BackupAsync(original, default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json["position"] = JsonNode.Parse(value);
        await File.WriteAllTextAsync(file, json.ToJsonString());
        for (var read = 0; read < 2; read++)
        {
            var repository = new JsonDeadLetterBackupRepository(paths);
            var summary = Assert.Single(await repository.ListAsync());
            Assert.True(summary.IsReadable);
            var loaded = await repository.LoadAsync(summary);

            Assert.Null(loaded.Position);
            Assert.False(loaded.HasSequenceNumber);
            AssertPayloadAndEnvelope(original, loaded);
        }
    }

    private static BrowsedMessage MessageWithEnvelope()
    {
        var message = Message();
        return message with
        {
            KafkaEnvelope = new KafkaEnvelope([0xfe, 0x00], false,
                [new KafkaRawHeader("opaque", [0xff]), new KafkaRawHeader("opaque", null)], message.Properties, [])
        };
    }

    private static void AssertPayloadAndEnvelope(BrowsedMessage original, BrowsedMessage loaded)
    {
        Assert.Equal(original.Body.ToArray(), loaded.Body.ToArray());
        Assert.Equal(original.SequenceNumber, loaded.SequenceNumber);
        Assert.Equal(original.Properties, loaded.Properties);
        Assert.NotNull(loaded.KafkaEnvelope);
        Assert.Equal(original.KafkaEnvelope!.Key, loaded.KafkaEnvelope.Key);
        Assert.Equal(original.KafkaEnvelope.IsTombstone, loaded.KafkaEnvelope.IsTombstone);
        Assert.Equal(original.KafkaEnvelope.OriginalProperties, loaded.KafkaEnvelope.OriginalProperties);
        Assert.Equal(original.KafkaEnvelope.OriginalApplicationProperties, loaded.KafkaEnvelope.OriginalApplicationProperties);
        Assert.Equal(original.KafkaEnvelope.Headers.Count, loaded.KafkaEnvelope.Headers.Count);
        for (var index = 0; index < original.KafkaEnvelope.Headers.Count; index++)
        {
            Assert.Equal(original.KafkaEnvelope.Headers[index].Name, loaded.KafkaEnvelope.Headers[index].Name);
            Assert.Equal(original.KafkaEnvelope.Headers[index].Value, loaded.KafkaEnvelope.Headers[index].Value);
        }
    }

    private static BrowsedMessage Message() => new(ServiceBusEntityReference.Queue("synthetic"),
        ServiceBusSubQueue.DeadLetter, 7, "synthetic body"u8.ToArray(), new EditableMessageProperties());
}
