using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class BackupRequiredEnumValidationRegressionTests
{
    [Theory]
    [InlineData(1, "999")]
    [InlineData(1, "-1")]
    [InlineData(2, "999")]
    [InlineData(2, "-1")]
    [InlineData(3, "999")]
    [InlineData(3, "-1")]
    public async Task UndefinedSubQueueIsUnreadableWithoutHidingOrChangingOtherBackups(int schema, string value)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var (good, bad) = await CreateBackupsAsync(paths);
        var root = JsonNode.Parse(await File.ReadAllTextAsync(bad))!.AsObject();
        root["schemaVersion"] = schema;
        root["subQueue"] = value;
        await File.WriteAllTextAsync(bad, root.ToJsonString());
        var damagedBytes = await File.ReadAllBytesAsync(bad);
        var goodBytes = await File.ReadAllBytesAsync(good);
        var repository = new JsonDeadLetterBackupRepository(paths);

        var listed = await repository.ListAsync();

        Assert.Equal(2, listed.Count);
        var damaged = Assert.Single(listed, item => item.FilePath == bad);
        Assert.False(damaged.IsReadable);
        Assert.Contains("subQueue", damaged.Error, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidDataException>(() => repository.LoadAsync(damaged));
        var intact = Assert.Single(listed, item => item.FilePath == good);
        Assert.True(intact.IsReadable, intact.Error);
        var restored = await repository.LoadAsync(intact);
        Assert.Equal("good", restored.Properties.MessageId);
        Assert.Equal("synthetic body"u8.ToArray(), restored.Body.ToArray());
        Assert.Equal(goodBytes, await File.ReadAllBytesAsync(good));
        Assert.Equal(damagedBytes, await File.ReadAllBytesAsync(bad));
    }

    [Theory]
    [InlineData("999")]
    [InlineData("-1")]
    public async Task UndefinedPropertyTypeIsRefusedWhenTheBodyIsLoaded(string value)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var (_, bad) = await CreateBackupsAsync(paths);
        var root = JsonNode.Parse(await File.ReadAllTextAsync(bad))!.AsObject();
        root["applicationProperties"]![0]!["type"] = value;
        await File.WriteAllTextAsync(bad, root.ToJsonString());
        var original = await File.ReadAllBytesAsync(bad);
        var repository = new JsonDeadLetterBackupRepository(paths);
        // Listing intentionally scans root metadata; full message validation happens on load.
        var summary = Assert.Single(await repository.ListAsync(), item => item.FilePath == bad);

        await Assert.ThrowsAsync<InvalidDataException>(() => repository.LoadAsync(summary));

        Assert.Equal(original, await File.ReadAllBytesAsync(bad));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task DefinedNumericEnumsRemainCompatibleWithExistingBackups(int schema)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var (good, _) = await CreateBackupsAsync(paths);
        var root = JsonNode.Parse(await File.ReadAllTextAsync(good))!.AsObject();
        root["schemaVersion"] = schema;
        root["subQueue"] = ((int)ServiceBusSubQueue.DeadLetter).ToString(System.Globalization.CultureInfo.InvariantCulture);
        root["applicationProperties"]![0]!["type"] = ((int)ApplicationPropertyType.Int64).ToString(System.Globalization.CultureInfo.InvariantCulture);
        await File.WriteAllTextAsync(good, root.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        var summary = Assert.Single(await repository.ListAsync(), item => item.FilePath == good);

        var restored = await repository.LoadAsync(summary);

        Assert.True(summary.IsReadable, summary.Error);
        Assert.Equal(ServiceBusSubQueue.DeadLetter, restored.SubQueue);
        Assert.Equal(new MessageApplicationProperty("number", ApplicationPropertyType.Int64, "42"),
            Assert.Single(restored.ApplicationProperties));
        Assert.Equal("good", restored.Properties.MessageId);
    }

    private static async Task<(string Good, string Bad)> CreateBackupsAsync(QueueLoomPaths paths)
    {
        var profile = ServiceBusProfile.CreateNew("Synthetic backup", EnvironmentKind.Test,
            AuthenticationSettings.Entra(), "synthetic.servicebus.windows.net");
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UnixEpoch, default);
        BrowsedMessage Message(long sequence, string id) => new(
            ServiceBusEntityReference.Queue("synthetic-orders"), ServiceBusSubQueue.DeadLetter, sequence,
            "synthetic body"u8.ToArray(), new EditableMessageProperties(MessageId: id),
            [new MessageApplicationProperty("number", ApplicationPropertyType.Int64, "42")]);
        return (await session.BackupAsync(Message(1, "good"), default),
            await session.BackupAsync(Message(2, "damaged"), default));
    }
}
