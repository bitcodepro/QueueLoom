using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    [Fact]
    public async Task Audit_MacCleanupPreservesLateWritesAndConflictingExistingBackups()
    {
        var (target, legacy, staging) = MacBackupFixture();
        var destination = Path.Combine(target.InstallDirectory, "backups");
        Directory.CreateDirectory(destination);
        File.WriteAllText(Path.Combine(destination, "sentinel.json"), "existing backup");
        AppUpdater.Install(target, staging);
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var old = Assert.Single(receipt.Entries).Backup!;
        File.WriteAllText(Path.Combine(old, "Contents", "MacOS", "backups", "late.json"), "late backup");
        await UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero);
        Assert.False(Directory.Exists(old));
        Assert.Equal("existing backup", File.ReadAllText(Path.Combine(destination, "sentinel.json")));
        Assert.Equal("message backup", File.ReadAllText(Assert.Single(Directory.GetFiles(destination, "sentinel.recovered-*.json"))));
        Assert.Equal("late backup", File.ReadAllText(Path.Combine(destination, "late.json")));
    }

    [Fact]
    public void Audit_MacBackupPreservationFailureLeavesOldInstallationAndBackupsUntouched()
    {
        var (target, legacy, staging) = MacBackupFixture();
        File.WriteAllText(Path.Combine(target.InstallDirectory, "backups"), "blocked destination");
        Assert.ThrowsAny<IOException>(() => AppUpdater.Install(target, staging));
        Assert.Equal("old program", File.ReadAllText(target.Executable));
        Assert.Equal("message backup", File.ReadAllText(Path.Combine(legacy, "sentinel.json")));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
    }

    [Fact]
    public async Task Audit_MacCleanupFailureKeepsReceiptAndOldBackupsForRetry()
    {
        var (target, _, staging) = MacBackupFixture();
        AppUpdater.Install(target, staging);
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var old = Assert.Single(receipt.Entries).Backup!;
        var legacy = Path.Combine(old, "Contents", "MacOS", "backups");
        Directory.CreateDirectory(Path.Combine(legacy, "blocked"));
        File.WriteAllText(Path.Combine(legacy, "blocked", "late.json"), "late backup");
        var blocker = Path.Combine(target.InstallDirectory, "backups", "blocked");
        File.WriteAllText(blocker, "blocker");
        await Assert.ThrowsAnyAsync<IOException>(() => UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero));
        Assert.True(File.Exists(path));
        Assert.Equal("late backup", File.ReadAllText(Path.Combine(legacy, "blocked", "late.json")));
        File.Move(blocker, blocker + ".kept");
        await UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero);
        Assert.False(Directory.Exists(old));
        Assert.Equal("late backup", File.ReadAllText(Path.Combine(blocker, "late.json")));
    }

    [Fact]
    public async Task Audit_MacRollbackPreservesBackupsCreatedByBothVersions()
    {
        var (target, legacy, staging) = MacBackupFixture();
        AppUpdater.Install(target, staging);
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        Directory.CreateDirectory(legacy);
        File.WriteAllText(Path.Combine(legacy, "new-version.json"), "new version backup");
        UpdateRestart.Restore(receipt);
        await UpdateRestart.CleanAsync(receipt with { Recovered = true }, path, TimeSpan.Zero);
        Assert.Equal("old program", File.ReadAllText(target.Executable));
        Assert.Equal("message backup", File.ReadAllText(Path.Combine(legacy, "sentinel.json")));
        Assert.Equal("new version backup", File.ReadAllText(Path.Combine(target.InstallDirectory, "backups", "new-version.json")));
    }

    [Fact]
    public void Audit_MacPathsAndCustomBackupDirectoriesStayOutsideReplacedBundle()
    {
        var (target, _, _) = MacBackupFixture();
        var executableDirectory = Path.GetDirectoryName(target.Executable)!;
        Assert.Equal(Path.Combine(target.InstallDirectory, "backups"), QueueLoomPaths.ProgramBackupsDirectoryFor(executableDirectory));
        var external = Path.Combine(_root, "custom-backups");
        Assert.Equal(external, QueueLoomPaths.OutsideApplicationBundle(external, executableDirectory));
        Assert.Single(MacBackupMigration.CaptureDirectories(target, external)!);
        var custom = Path.Combine(target.Bundle!, "Contents", "saved-messages");
        Directory.CreateDirectory(custom);
        File.WriteAllText(Path.Combine(custom, "custom.json"), "custom backup");
        var receipt = new UpdateRestart.Receipt(Guid.NewGuid().ToString("N"), target, "", [])
        { BundleBackupDirectories = MacBackupMigration.CaptureDirectories(target, custom) };
        var remapped = QueueLoomPaths.OutsideApplicationBundle(custom, executableDirectory);
        Assert.StartsWith(Path.Combine(target.InstallDirectory, "backups") + Path.DirectorySeparatorChar, remapped, StringComparison.Ordinal);
        MacBackupMigration.Preserve(receipt, target.Bundle!);
        Assert.Equal("custom backup", File.ReadAllText(Path.Combine(remapped, "custom.json")));
        Assert.Equal("custom backup", File.ReadAllText(Path.Combine(custom, "custom.json")));
    }

    private (UpdateTarget Target, string Legacy, string Staging) MacBackupFixture()
    {
        var bundle = Path.Combine(_root, "Applications", "QueueLoom.app");
        var executable = Path.Combine(bundle, "Contents", "MacOS", "QueueLoom");
        Directory.CreateDirectory(Path.GetDirectoryName(executable)!);
        File.WriteAllText(executable, "old program");
        var legacy = Path.Combine(Path.GetDirectoryName(executable)!, "backups");
        Directory.CreateDirectory(legacy);
        File.WriteAllText(Path.Combine(legacy, "sentinel.json"), "message backup");
        return (AppUpdater.TargetFor("osx-arm64", executable), legacy,
            Staging(("QueueLoom.app/Contents/MacOS/QueueLoom", "new program")));
    }

    [Fact]
    public void Audit_MacOlderReceiptPreservesConfiguredCustomBackups()
    {
        var (target, _, _) = MacBackupFixture();
        var custom = Path.Combine(target.Bundle!, "Contents", "custom-backups");
        Directory.CreateDirectory(custom);
        File.WriteAllText(Path.Combine(custom, "custom.json"), "old custom backup");
        // Receipts from released versions have no BundleBackupDirectories field. The helper inherits the override.
        var oldReceipt = new UpdateRestart.Receipt(Guid.NewGuid().ToString("N"), target, "", []);

        MacBackupMigration.Preserve(oldReceipt, target.Bundle!, custom);

        var remapped = QueueLoomPaths.OutsideApplicationBundle(custom, Path.GetDirectoryName(target.Executable)!);
        Assert.Equal("old custom backup", File.ReadAllText(Path.Combine(remapped, "custom.json")));
    }
}
