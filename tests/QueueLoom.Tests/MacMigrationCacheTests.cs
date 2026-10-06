using System.Diagnostics;
using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MacMigrationCacheTests
{
    private static (string Bundle, string Source, string Destination, string File) Layout(TemporaryDirectory directory)
    {
        var bundle = Path.Combine(directory.Path, "QueueLoom.app");
        var source = Directory.CreateDirectory(Path.Combine(bundle, "Contents", "MacOS", "backups")).FullName;
        var destination = Path.Combine(directory.Path, "backups");
        var file = Path.Combine(source, "orders.json");
        File.WriteAllText(file, "{\"backup\":1}");
        return (bundle, source, destination, file);
    }

    // A cached hint names a destination outside the backup folder (the backup folder changed, or the cache file was
    // edited). It is not trusted: the file is verified and copied again, and the hint replaced, instead of the whole
    // migration failing at every start.
    [Fact]
    public void AHintOutsideTheBackupFolderIsVerifiedAgainNotFatal()
    {
        using var directory = new TemporaryDirectory();
        var (bundle, source, destination, file) = Layout(directory);
        var outside = Path.Combine(directory.Path, "elsewhere", "orders.json");
        var stamp = MacBackupMigration.FileStamp.Read(file)!;
        var key = Path.GetFullPath(file) + "\n" + Path.GetFullPath(Path.Combine(destination, "orders.json"));
        var cache = new MacBackupMigration.CopyCache(new() { [key] = new(stamp, outside, stamp) });

        var copied = MacBackupMigration.CopyLegacyDirectory(source, destination, bundle, CancellationToken.None, cache);

        Assert.Equal(1, copied);
        Assert.Equal("{\"backup\":1}", File.ReadAllText(Path.Combine(destination, "orders.json")));
        Assert.True(cache.Changed);
        Assert.Equal(Path.GetFullPath(Path.Combine(destination, "orders.json")), cache.Entries[key].Destination);
    }

    // Cancelled while a large backup is being hashed: the migration stops promptly instead of reading the whole file.
    [Fact]
    public void ALargeBackupStopsPromptlyWhenCancelled()
    {
        using var directory = new TemporaryDirectory();
        var (bundle, source, destination, file) = Layout(directory);
        using (var large = File.Create(Path.Combine(source, "large.json")))
        {
            large.SetLength(1024L * 1024 * 1024);
        }
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        var watch = Stopwatch.StartNew();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            MacBackupMigration.CopyLegacyDirectory(source, destination, bundle, cancellation.Token, new MacBackupMigration.CopyCache([])));
        watch.Stop();

        Assert.True(watch.Elapsed < TimeSpan.FromMilliseconds(800), $"Stopping took {watch.Elapsed}.");
        Assert.False(File.Exists(Path.Combine(destination, "large.json")));
    }
}
