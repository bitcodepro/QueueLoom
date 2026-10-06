using QueueLoom.App.Mcp;
using QueueLoom.App.Services;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Core.IO;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class AuditLegacyMigrationPerformanceTests
{
    [Fact]
    public async Task Review2_UnchangedBackupIsNotOpenedOnAnotherStartup()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        Assert.Equal(1, await new LegacyBackupMigration(paths, executable).RunAsync());
        using var blockedSource = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.None);
        using var blockedTarget = new FileStream(Path.Combine(paths.BackupsDirectory, "sentinel.json"), FileMode.Open, FileAccess.Read, FileShare.None);
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
    }

    [Fact]
    public async Task Review2_LateBackupIsCopiedWithoutReopeningUnchangedBackup()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        await new LegacyBackupMigration(paths, executable).RunAsync();
        using var blockedSource = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.None);
        var late = Path.Combine(Path.GetDirectoryName(source)!, "late.json");
        File.WriteAllText(late, "late backup");
        Assert.Equal(1, await new LegacyBackupMigration(paths, executable).RunAsync());
        Assert.Equal("late backup", File.ReadAllText(Path.Combine(paths.BackupsDirectory, "late.json")));
        Assert.True(File.Exists(late));
    }

    [Fact]
    public async Task Review2_SameSizeLateRewriteRetainsBothBackupVersions()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        await new LegacyBackupMigration(paths, executable).RunAsync();
        var originalTime = File.GetLastWriteTimeUtc(source);
        File.WriteAllText(source, "modified"); // Same length as "original"; file count and total size are unchanged.
        File.SetLastWriteTimeUtc(source, originalTime.AddSeconds(10));
        Assert.Equal(1, await new LegacyBackupMigration(paths, executable).RunAsync());
        Assert.Equal("original", File.ReadAllText(Path.Combine(paths.BackupsDirectory, "sentinel.json")));
        Assert.Equal("modified", File.ReadAllText(Assert.Single(Directory.GetFiles(paths.BackupsDirectory, "sentinel.recovered-*.json"))));
        using var blockedSource = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.None);
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Review2_MissingOrChangedDestinationInvalidatesVerifiedCopy(bool changed)
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        await new LegacyBackupMigration(paths, executable).RunAsync();
        var target = Path.Combine(paths.BackupsDirectory, "sentinel.json");
        if (changed)
        {
            var timestamp = File.GetLastWriteTimeUtc(target);
            File.WriteAllText(target, "modified");
            File.SetLastWriteTimeUtc(target, timestamp.AddSeconds(10));
        }
        else File.Move(target, target + ".kept");
        Assert.Equal(1, await new LegacyBackupMigration(paths, executable).RunAsync());
        var recovered = changed ? Assert.Single(Directory.GetFiles(paths.BackupsDirectory, "sentinel.recovered-*.json")) : target;
        Assert.Equal("original", File.ReadAllText(recovered));
        Assert.Equal("original", File.ReadAllText(source));
    }

    [Fact]
    public async Task Review2_McpServesWhileMigrationWaitsForDesktopLock()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, _) = Fixture(directory.Path);
        paths.EnsureCreated();
        using var logs = new FileLoggerProvider(Path.Combine(paths.RootDirectory, "logs"));
        using var held = new FileStream(Path.Combine(paths.RootDirectory, ".legacy-backups.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        var serving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<int>? migration = null;
        var server = McpMode.RunServerAsync(new McpServerSettings(ReadOnly: true), paths, logs, new ElicitationApprover(),
            token => migration = new LegacyBackupMigration(paths, executable).RunAsync(token),
            () => { serving.TrySetResult(); return disconnect.Task; });
        try
        {
            await serving.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(migration);
            Assert.False(migration.IsCompleted);
            held.Dispose();
            Assert.Equal(1, await migration.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal("original", File.ReadAllText(Path.Combine(paths.BackupsDirectory, "sentinel.json")));
        }
        finally { held.Dispose(); disconnect.TrySetResult(); await server; }
    }

    [Fact]
    public async Task Review2_DamagedHintIsRebuiltFromVerifiedCopies()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        await new LegacyBackupMigration(paths, executable).RunAsync();
        File.WriteAllText(Path.Combine(paths.RootDirectory, "legacy-backup-migration.v1.json"), "{broken hint");
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
        using var blockedSource = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.None);
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
    }

    [Fact]
    public async Task Review2_FailedHintPublicationRetainsBackupsAndCanBeRetried()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        var previous = SafeFileWriter.BeforePublish.Value;
        SafeFileWriter.BeforePublish.Value = (_, _) => throw new IOException("Synthetic hint publication failure");
        try { await Assert.ThrowsAsync<IOException>(() => new LegacyBackupMigration(paths, executable).RunAsync()); }
        finally { SafeFileWriter.BeforePublish.Value = previous; }
        Assert.False(File.Exists(Path.Combine(paths.RootDirectory, "legacy-backup-migration.v1.json")));
        Assert.Equal("original", File.ReadAllText(source));
        Assert.Equal("original", File.ReadAllText(Path.Combine(paths.BackupsDirectory, "sentinel.json")));
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
        using var blockedSource = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.None);
        Assert.Equal(0, await new LegacyBackupMigration(paths, executable).RunAsync());
    }

    [Fact]
    public async Task Review2_McpDisconnectCancelsMigrationWaitWithoutCompletingItsHint()
    {
        using var directory = new TemporaryDirectory();
        var (paths, executable, source) = Fixture(directory.Path);
        paths.EnsureCreated();
        using var logs = new FileLoggerProvider(Path.Combine(paths.RootDirectory, "logs"));
        using var held = new FileStream(Path.Combine(paths.RootDirectory, ".legacy-backups.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        Task<int>? migration = null;
        await McpMode.RunServerAsync(new McpServerSettings(ReadOnly: true), paths, logs, new ElicitationApprover(),
            token => migration = new LegacyBackupMigration(paths, executable).RunAsync(token), () => Task.CompletedTask);
        Assert.NotNull(migration);
        Assert.True(migration.IsCanceled);
        Assert.False(File.Exists(Path.Combine(paths.RootDirectory, "legacy-backup-migration.v1.json")));
        Assert.False(Directory.Exists(paths.BackupsDirectory));
        Assert.Equal("original", File.ReadAllText(source));
    }

    private static (QueueLoomPaths Paths, string Executable, string Source) Fixture(string root)
    {
        var executable = Path.Combine(root, "Applications", "QueueLoom.app", "Contents", "MacOS");
        var legacy = Path.Combine(executable, "backups");
        Directory.CreateDirectory(legacy);
        var source = Path.Combine(legacy, "sentinel.json");
        File.WriteAllText(source, "original");
        var paths = QueueLoomPaths.ForRoot(Path.Combine(root, "data")) with { BackupsDirectory = Path.Combine(root, "backups") };
        return (paths, executable, source);
    }
}
