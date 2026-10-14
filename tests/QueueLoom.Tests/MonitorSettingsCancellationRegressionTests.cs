using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MonitorSettingsCancellationRegressionTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FailureAfterRecoveryPreservesBothPrimaryAndRecovery(bool cancel, bool truncated)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        var json = truncated
            ? "{\"SchemaVersion\":1,\"MonitorIntervalSeconds\":300,\"Marker\":\"synthetic-truncated"
            : """{"SchemaVersion":1,"MonitorIntervalSeconds":"invalid","Marker":"synthetic-damaged"}""";
        var original = System.Text.Encoding.UTF8.GetBytes(json);
        await File.WriteAllBytesAsync(paths.SettingsFile, original);
        using var store = new JsonAppSettingsStore(paths);
        using var cancellation = new CancellationTokenSource();
        var reachedWriter = false;
        var previous = AtomicFile.ContentWriting.Value;
        AtomicFile.ContentWriting.Value = _ =>
        {
            reachedWriter = true;
            var recovery = Assert.Single(RecoveryFiles(paths));
            Assert.Equal(original, File.ReadAllBytes(recovery));
            // Existing flow-local seam: recovery has finished, but new primary content has not been written.
            if (cancel) cancellation.Cancel();
            else throw new IOException("Synthetic primary write failure after recovery.");
        };
        try
        {
            if (cancel)
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.SaveMonitorIntervalSecondsAsync(120, cancellation.Token));
            else
                await Assert.ThrowsAsync<IOException>(() => store.SaveMonitorIntervalSecondsAsync(120, cancellation.Token));
        }
        finally { AtomicFile.ContentWriting.Value = previous; }

        Assert.True(reachedWriter);
        Assert.True(File.Exists(paths.SettingsFile), "A failed settings write removed the damaged primary file.");
        Assert.Equal(original, await File.ReadAllBytesAsync(paths.SettingsFile));
        var recoveryPath = Assert.Single(RecoveryFiles(paths));
        Assert.Equal(original, await File.ReadAllBytesAsync(recoveryPath));
        if (!OperatingSystem.IsWindows())
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(recoveryPath));
        Assert.Empty(Directory.GetFiles(paths.RootDirectory, ".*.tmp"));

        // The failed transaction releases its ownership and an ordinary retry still commits a recoverable update.
        await store.SaveMonitorIntervalSecondsAsync(180);
        Assert.Equal(180, await store.LoadMonitorIntervalSecondsAsync());
        Assert.All(RecoveryFiles(paths), file => Assert.Equal(original, File.ReadAllBytes(file)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadablePrimaryAlsoSurvivesWriterFailure(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var store = new JsonAppSettingsStore(paths);
        await store.UpdateAsync(settings => settings with { MonitorIntervalSeconds = 300, KeepInTray = true, SystemNotifications = false });
        var original = await File.ReadAllBytesAsync(paths.SettingsFile);
        using var cancellation = new CancellationTokenSource();
        var previous = AtomicFile.ContentWriting.Value;
        AtomicFile.ContentWriting.Value = _ =>
        {
            if (cancel) cancellation.Cancel();
            else throw new IOException("Synthetic readable settings write failure.");
        };
        try
        {
            if (cancel)
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.SaveMonitorIntervalSecondsAsync(120, cancellation.Token));
            else
                await Assert.ThrowsAsync<IOException>(() => store.SaveMonitorIntervalSecondsAsync(120, cancellation.Token));
        }
        finally { AtomicFile.ContentWriting.Value = previous; }

        Assert.Equal(original, await File.ReadAllBytesAsync(paths.SettingsFile));
        Assert.Empty(RecoveryFiles(paths));
        var loaded = await store.LoadAsync();
        Assert.Equal(300, loaded.MonitorIntervalSeconds);
        Assert.True(loaded.KeepInTray);
        Assert.False(loaded.SystemNotifications);
    }

    [Fact]
    public async Task FailedWriterDoesNotRollBackOverANewerPrimary()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        var original = """{"SchemaVersion":1,"MonitorIntervalSeconds":"invalid","Marker":"synthetic-original"}""";
        var newer = """{"SchemaVersion":1,"MonitorIntervalSeconds":450,"KeepInTray":true,"Marker":"synthetic-newer"}""";
        await File.WriteAllTextAsync(paths.SettingsFile, original);
        using var store = new JsonAppSettingsStore(paths);
        var previous = AtomicFile.ContentWriting.Value;
        AtomicFile.ContentWriting.Value = _ =>
        {
            Assert.Equal(original, File.ReadAllText(Assert.Single(RecoveryFiles(paths))));
            // A non-cooperating writer bypasses the sidecar lock. Failure must not restore old bytes over its data.
            File.WriteAllText(paths.SettingsFile, newer);
            throw new IOException("Synthetic failure with a newer primary already present.");
        };
        try { await Assert.ThrowsAsync<IOException>(() => store.SaveMonitorIntervalSecondsAsync(120)); }
        finally { AtomicFile.ContentWriting.Value = previous; }

        Assert.Equal(newer, await File.ReadAllTextAsync(paths.SettingsFile));
        Assert.Equal(original, await File.ReadAllTextAsync(Assert.Single(RecoveryFiles(paths))));
        Assert.Equal(450, await store.LoadMonitorIntervalSecondsAsync());
    }

    private static string[] RecoveryFiles(QueueLoomPaths paths) =>
        Directory.GetFiles(paths.RootDirectory, Path.GetFileName(paths.SettingsFile) + ".damaged-*");

    [Theory]
    [InlineData("""{"SchemaVersion":1,"MonitorIntervalSeconds":"invalid","KeepInTray":true,"Marker":"synthetic-monitor"}""")]
    [InlineData("{\"SchemaVersion\":1,\"MonitorIntervalSeconds\":300,\"Marker\":\"synthetic-truncated-monitor")]
    public async Task CancelledMonitorUpdateLeavesDamagedPrimaryFileInPlace(string json)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        var original = System.Text.Encoding.UTF8.GetBytes(json);
        await File.WriteAllBytesAsync(paths.SettingsFile, original);
        using var store = new JsonAppSettingsStore(paths);
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.UpdateAsync(settings =>
        {
            // The public update callback is a deterministic boundary after reading and before publishing.
            cancellation.Cancel();
            return settings with { MonitorIntervalSeconds = 120 };
        }, cancellation.Token));

        Assert.True(File.Exists(paths.SettingsFile), "A cancelled monitor save displaced the original settings file.");
        Assert.Equal(original, await File.ReadAllBytesAsync(paths.SettingsFile));
        Assert.Empty(Directory.GetFiles(paths.RootDirectory, Path.GetFileName(paths.SettingsFile) + ".damaged-*"));
    }

    [Fact]
    public async Task CancelledMonitorUpdateAlsoPreservesReadableSettingsAndOtherPreferences()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var store = new JsonAppSettingsStore(paths);
        await store.UpdateAsync(settings => settings with
        {
            MonitorIntervalSeconds = 300,
            SystemNotifications = false,
            KeepInTray = true
        });
        var original = await File.ReadAllBytesAsync(paths.SettingsFile);
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.UpdateAsync(settings =>
        {
            cancellation.Cancel();
            return settings with { MonitorIntervalSeconds = 120 };
        }, cancellation.Token));

        Assert.Equal(original, await File.ReadAllBytesAsync(paths.SettingsFile));
        var loaded = await store.LoadAsync();
        Assert.Equal(300, loaded.MonitorIntervalSeconds);
        Assert.False(loaded.SystemNotifications);
        Assert.True(loaded.KeepInTray);
    }

    [Fact]
    public async Task SuccessfulMonitorUpdateStillRetainsTheDamagedInputForRecovery()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        var original = """{"SchemaVersion":1,"MonitorIntervalSeconds":"invalid","Marker":"synthetic-recoverable"}""";
        await File.WriteAllTextAsync(paths.SettingsFile, original);
        using var store = new JsonAppSettingsStore(paths);

        await store.SaveMonitorIntervalSecondsAsync(120);

        using var reopened = new JsonAppSettingsStore(paths);
        Assert.Equal(120, await reopened.LoadMonitorIntervalSecondsAsync());
        Assert.Equal(original, await File.ReadAllTextAsync(
            Assert.Single(Directory.GetFiles(paths.RootDirectory, Path.GetFileName(paths.SettingsFile) + ".damaged-*"))));
    }
}
