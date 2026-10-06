using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class OutageRollbackTests
{
    [Fact]
    public void Outage_CopyFailureAfterMovingTheCurrentFileCanBeRetried()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing-violation copy barrier."); return; }
        using var directory = new TemporaryDirectory();
        var id = Guid.NewGuid().ToString("N");
        var current = Path.Combine(directory.Path, "QueueLoom.exe");
        var backup = current + "." + id + ".old";
        File.WriteAllText(current, "new");
        File.WriteAllText(backup, "old");
        var receipt = new UpdateRestart.Receipt(id, new("synthetic", directory.Path, current, null), "", [new(current, backup)]);
        using (var barrier = new FileStream(backup, FileMode.Open, FileAccess.Read, FileShare.None))
            Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
        Assert.False(File.Exists(current));
        Assert.Equal("new", File.ReadAllText(current + "." + id + ".failed"));
        Assert.Equal("old", File.ReadAllText(backup));
        UpdateRestart.Restore(receipt);
        UpdateRestart.Restore(receipt);
        Assert.Equal("old", File.ReadAllText(current));
        Assert.True(File.Exists(backup));
    }

    [Fact]
    public async Task Outage_PartialBundleRecoveryRetainsBothVersionsMessageBackups()
    {
        using var directory = new TemporaryDirectory();
        var id = Guid.NewGuid().ToString("N");
        var bundle = Path.Combine(directory.Path, "QueueLoom.app");
        var backup = bundle + "." + id + ".old";
        var executable = Path.Combine(bundle, "Contents", "MacOS", "QueueLoom");
        var oldExecutable = Path.Combine(backup, "Contents", "MacOS", "QueueLoom");
        Directory.CreateDirectory(Path.GetDirectoryName(executable)!);
        Directory.CreateDirectory(Path.GetDirectoryName(oldExecutable)!);
        File.WriteAllText(executable, "new");
        File.WriteAllText(oldExecutable, "old");
        Directory.CreateDirectory(Path.Combine(Path.GetDirectoryName(executable)!, "backups"));
        Directory.CreateDirectory(Path.Combine(Path.GetDirectoryName(oldExecutable)!, "backups"));
        File.WriteAllText(Path.Combine(Path.GetDirectoryName(executable)!, "backups", "new.json"), "new message backup");
        File.WriteAllText(Path.Combine(Path.GetDirectoryName(oldExecutable)!, "backups", "old.json"), "old message backup");
        var other = Path.Combine(directory.Path, "other.dat");
        File.WriteAllText(other, "new-other");
        File.WriteAllText(other + "." + id + ".old", "old-other");
        var obstacle = other + "." + id + ".failed";
        Directory.CreateDirectory(obstacle);
        var target = new UpdateTarget("osx-arm64", directory.Path, executable, bundle);
        var receipt = new UpdateRestart.Receipt(id, target, "", [new(other, other + "." + id + ".old"), new(bundle, backup)]);
        Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
        Assert.Equal("old", File.ReadAllText(executable));
        Assert.True(Directory.Exists(backup));
        Directory.Delete(obstacle);
        UpdateRestart.Restore(receipt);
        UpdateRestart.Restore(receipt);
        await UpdateRestart.CleanAsync(receipt with { Recovered = true }, UpdateRestart.ReceiptPath(target), TimeSpan.Zero);
        Assert.Equal("old", File.ReadAllText(executable));
        Assert.Equal("old message backup", File.ReadAllText(Path.Combine(Path.GetDirectoryName(executable)!, "backups", "old.json")));
        Assert.Equal("new message backup", File.ReadAllText(Path.Combine(directory.Path, "backups", "new.json")));
    }

    [Fact]
    public void Outage_UnixBundleRecoveryPreservesLinksAndExecutablePermissions()
    {
        if (OperatingSystem.IsWindows()) { Assert.Skip("Unix bundle permissions and links require Linux or macOS."); return; }
        using var directory = new TemporaryDirectory();
        var id = Guid.NewGuid().ToString("N");
        var bundle = Path.Combine(directory.Path, "QueueLoom.app");
        var backup = bundle + "." + id + ".old";
        var relative = Path.Combine("Contents", "MacOS", "QueueLoom");
        Directory.CreateDirectory(Path.Combine(bundle, "Contents", "MacOS"));
        Directory.CreateDirectory(Path.Combine(backup, "Contents", "MacOS"));
        File.WriteAllText(Path.Combine(bundle, relative), "new");
        File.WriteAllText(Path.Combine(backup, relative), "old");
        var mode = UnixFileMode.UserRead | UnixFileMode.UserExecute;
        File.SetUnixFileMode(Path.Combine(backup, relative), mode);
        var external = Path.Combine(directory.Path, "external");
        Directory.CreateDirectory(external);
        File.WriteAllText(Path.Combine(external, "message.json"), "keep");
        Directory.CreateSymbolicLink(Path.Combine(backup, "linked"), "../external");
        var receipt = new UpdateRestart.Receipt(id, new("osx-arm64", directory.Path, Path.Combine(bundle, relative), bundle), "", [new(bundle, backup)]);
        UpdateRestart.Restore(receipt);
        UpdateRestart.Restore(receipt);
        Assert.Equal(mode, File.GetUnixFileMode(receipt.Target.Executable));
        Assert.Equal("../external", new DirectoryInfo(Path.Combine(bundle, "linked")).LinkTarget);
        Assert.Equal("keep", File.ReadAllText(Path.Combine(external, "message.json")));
    }

    [Fact]
    public void Outage_PartialTwoEntryRecoveryCanBeRetriedWithoutConsumingBackups()
    {
        using var directory = new TemporaryDirectory();
        var (receipt, first, second, obstacle) = Prepare(directory.Path);
        Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
        Assert.Equal("old-first", File.ReadAllText(first));
        Directory.Delete(obstacle);
        UpdateRestart.Restore(receipt);
        UpdateRestart.Restore(receipt); // A completed retry is idempotent too.
        Assert.Equal("old-first", File.ReadAllText(first));
        Assert.Equal("old-second", File.ReadAllText(second));
        foreach (var entry in receipt.Entries) Assert.True(File.Exists(entry.Backup));
        Assert.Equal("new-first", File.ReadAllText(first + "." + receipt.Id + ".failed"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Outage_PartialRecoveryDoesNotTrustAMissingBackupOrTamperedRestoredFile(bool tamper)
    {
        using var directory = new TemporaryDirectory();
        var (receipt, first, _, obstacle) = Prepare(directory.Path);
        Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
        Directory.Delete(obstacle);
        if (tamper) File.WriteAllText(first, "tampered");
        else File.Delete(receipt.Entries[1].Backup!);
        Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
        Assert.Equal(tamper ? "tampered" : "old-first", File.ReadAllText(first));
    }

    private static (UpdateRestart.Receipt Receipt, string First, string Second, string Obstacle) Prepare(string root)
    {
        var id = Guid.NewGuid().ToString("N");
        var first = Path.Combine(root, "QueueLoom.exe");
        var second = Path.Combine(root, "other.dat");
        File.WriteAllText(first, "new-first");
        File.WriteAllText(second, "new-second");
        File.WriteAllText(first + "." + id + ".old", "old-first");
        File.WriteAllText(second + "." + id + ".old", "old-second");
        var receipt = new UpdateRestart.Receipt(id, new UpdateTarget("synthetic", root, first, null), "",
            [new(second, second + "." + id + ".old"), new(first, first + "." + id + ".old")]);
        var obstacle = second + "." + id + ".failed";
        Directory.CreateDirectory(obstacle);
        return (receipt, first, second, obstacle);
    }
}
