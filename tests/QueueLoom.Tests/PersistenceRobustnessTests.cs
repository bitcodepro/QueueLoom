using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class ScheduledResendFileShapeTests
{
    [Theory]
    [InlineData("[null]")]
    [InlineData("""[{"id":"7b0f2d4e-1c1a-4c51-9a55-3f1d1e2a4b6c"}]""")]
    [InlineData("""[{"id":"7b0f2d4e-1c1a-4c51-9a55-3f1d1e2a4b6c","environmentName":"Dev","destinationDisplay":"q","items":[null]}]""")]
    [InlineData("""[{"id":"7b0f2d4e-1c1a-4c51-9a55-3f1d1e2a4b6c","environmentName":"Dev","destinationDisplay":"q","items":[{"source":{"kind":"Queue","queueName":"a"},"destination":{"kind":"Queue","queueName":"b"},"body":null}]}]""")]
    public void IncompleteJobs_AreKeptAsideAndTheAppStarts(string content)
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        Directory.CreateDirectory(directory.Path);
        File.WriteAllText(store.FilePath, content);

        Assert.Empty(store.Load());

        Assert.False(File.Exists(store.FilePath));
        Assert.Single(Directory.GetFiles(directory.Path, "scheduled-resends.v2.json.damaged-*"));
    }
}

[Collection("MetadataAllocation")]
public sealed class BackupMetadataCacheCleanupTests
{
    private static async Task<string> Backup(QueueLoomPaths paths, long number)
    {
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.DeadLetter, number,
            new byte[8], new EditableMessageProperties(MessageId: $"m-{number}"));
        return await session.BackupAsync(message, default);
    }

    private static string[] Caches(TemporaryDirectory directory)
    {
        var folder = Path.Combine(directory.Path, "backup-metadata-cache");
        return Directory.Exists(folder) ? Directory.GetFiles(folder) : [];
    }

    [Fact]
    public async Task DeletingABackup_RemovesItsCachedMetadata()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await Backup(paths, 1);
        await Backup(paths, 2);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var listed = await repository.ListAsync();
        Assert.Equal(2, Caches(directory).Length);

        await repository.DeleteAsync(listed[0]);

        Assert.Single(Caches(directory));
    }

    [Fact]
    public async Task ListingPrunesCachesOfBackupsRemovedElsewhere()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var first = await Backup(paths, 1);
        await Backup(paths, 2);
        var repository = new JsonDeadLetterBackupRepository(paths);
        await repository.ListAsync();
        File.Delete(first);

        Assert.Single(await repository.ListAsync());

        Assert.Single(Caches(directory));
    }
}
