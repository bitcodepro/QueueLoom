using System.Text;
using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

[CollectionDefinition("MetadataAllocation", DisableParallelization = true)]
public sealed class MetadataAllocationCollection;

[Collection("MetadataAllocation")]
public sealed class ImprovementBackupTests
{
    private static async Task<string> Backup(QueueLoomPaths paths, int size)
    {
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.DeadLetter, 7,
            new byte[size], new EditableMessageProperties(MessageId: "large-backup"));
        return await session.BackupAsync(message, default);
    }

    [Fact]
    public async Task LargeLegacyBodyBeforeMetadataIsSkippedAndCacheCanBeRebuilt()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var path = await Backup(paths, 8 * 1024 * 1024);
        var text = await File.ReadAllTextAsync(path);
        var bodyStart = text.IndexOf("\"bodyBase64\":", StringComparison.Ordinal);
        var bodyEnd = text.IndexOf('"', text.IndexOf('"', bodyStart + 13) + 1) + 1;
        var body = text[bodyStart..bodyEnd];
        var beforeBody = text[..bodyStart].TrimEnd();
        if (beforeBody.EndsWith(',')) beforeBody = beforeBody[..^1];
        await File.WriteAllTextAsync(path, "{" + body + "," + beforeBody[1..] + text[bodyEnd..]);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var before = GC.GetTotalAllocatedBytes(true);
        var summary = Assert.Single(await repository.ListAsync());
        var allocated = GC.GetTotalAllocatedBytes(true) - before;
        Assert.True(summary.IsReadable, summary.Error);
        Assert.Equal(8 * 1024 * 1024, summary.BodySize);
        Assert.True(allocated < 4 * 1024 * 1024, $"Listing allocated {allocated} bytes; body must not be materialized.");
        var cache = Assert.Single(Directory.GetFiles(Path.Combine(directory.Path, "backup-metadata-cache")));
        File.WriteAllText(cache, "damaged cache");
        Assert.Equal(summary, Assert.Single(await repository.ListAsync()));
        Assert.True(File.Exists(path));
        Assert.Equal(8 * 1024 * 1024, (await repository.LoadAsync(summary)).Body.Length);
    }

    [Fact]
    public async Task CacheStorageFailureCannotInvalidateDurableBackup()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var path = await Backup(paths, 40);
        File.WriteAllText(Path.Combine(directory.Path, "backup-metadata-cache"), "block cache directory");
        var repository = new JsonDeadLetterBackupRepository(paths);
        var summary = Assert.Single(await repository.ListAsync());
        Assert.True(summary.IsReadable, summary.Error);
        Assert.Equal(40, (await repository.LoadAsync(summary)).Body.Length);
        Assert.True(File.Exists(path));
    }

    [Theory]
    [InlineData("nullSummary")]
    [InlineData("missingSummary")]
    [InlineData("invalidSourceName")]
    [InlineData("nullSource")]
    [InlineData("missingProfileName")]
    [InlineData("cachedError")]
    public async Task MalformedMatchingCacheIsRebuiltFromIntactBackup(string corruption)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var path = await Backup(paths, 40);
        var durableBytes = await File.ReadAllBytesAsync(path);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var original = Assert.Single(await repository.ListAsync());
        var cachePath = Assert.Single(Directory.GetFiles(Path.Combine(directory.Path, "backup-metadata-cache")));
        var cache = JsonNode.Parse(await File.ReadAllTextAsync(cachePath))!.AsObject();
        var summary = cache["Summary"]!.AsObject();
        switch (corruption)
        {
            case "nullSummary": cache["Summary"] = null; break;
            case "missingSummary": cache.Remove("Summary"); break;
            case "invalidSourceName": summary["Source"]!["Name"] = " "; break;
            case "nullSource": summary["Source"] = null; break;
            case "missingProfileName": summary.Remove("ProfileName"); break;
            case "cachedError": summary["Error"] = "An optional cache cannot make a durable backup unreadable."; break;
        }
        // Keep the real file's length, timestamp and path, so the cache fingerprint still matches.
        await File.WriteAllTextAsync(cachePath, cache.ToJsonString());

        var rebuilt = Assert.Single(await repository.ListAsync());
        Assert.True(rebuilt.IsReadable, rebuilt.Error);
        Assert.Equal(original, rebuilt);
        Assert.Equal(40, (await repository.LoadAsync(rebuilt)).Body.Length);
        Assert.Equal(durableBytes, await File.ReadAllBytesAsync(path));
        Assert.Equal(original, Assert.Single(await new JsonDeadLetterBackupRepository(paths).ListAsync()));
    }

    [Fact]
    public async Task ChangedBackupInvalidatesCacheAndFalseBodySizeCannotAuthorizeReplay()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var path = await Backup(paths, 40);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var original = Assert.Single(await repository.ListAsync());
        await File.WriteAllTextAsync(path, (await File.ReadAllTextAsync(path)).Replace("\"bodySize\": 40", "\"bodySize\": 1", StringComparison.Ordinal));
        var changed = Assert.Single(await repository.ListAsync());
        Assert.Equal(1, changed.BodySize);
        await Assert.ThrowsAsync<InvalidDataException>(() => repository.LoadAsync(changed));
        Assert.True(File.Exists(original.FilePath));
    }
}
