using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class JsonAppSettingsStoreTests
{
    [Fact]
    public async Task MonitorInterval_RoundTripsAcrossStoreInstances()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);

        using (var store = new JsonAppSettingsStore(paths))
        {
            Assert.Equal(60, await store.LoadMonitorIntervalSecondsAsync());
            await store.SaveMonitorIntervalSecondsAsync(135);
        }

        using var reopened = new JsonAppSettingsStore(paths);
        Assert.Equal(135, await reopened.LoadMonitorIntervalSecondsAsync());
    }

    [Fact]
    public async Task DamagedSettings_FallBackToDefault()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        await File.WriteAllTextAsync(paths.SettingsFile, "{not-json");

        using var store = new JsonAppSettingsStore(paths);

        Assert.Equal(JsonAppSettingsStore.DefaultMonitorIntervalSeconds,
            await store.LoadMonitorIntervalSecondsAsync());
    }
}
