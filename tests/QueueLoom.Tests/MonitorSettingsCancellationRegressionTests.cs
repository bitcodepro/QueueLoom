using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MonitorSettingsCancellationRegressionTests
{
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
