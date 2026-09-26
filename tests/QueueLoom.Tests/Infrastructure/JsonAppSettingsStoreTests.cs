using QueueLoom.Core.Settings;
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

    [Fact]
    public async Task Theme_RoundTripsAndSurvivesMonitorIntervalUpdates()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);

        using (var store = new JsonAppSettingsStore(paths))
        {
            Assert.Equal(AppThemePreference.Dark, (await store.LoadAsync()).Theme);
            await store.SaveThemeAsync(AppThemePreference.Light);
            await store.SaveMonitorIntervalSecondsAsync(90);
        }

        using var reopened = new JsonAppSettingsStore(paths);
        Assert.Equal(new AppSettings(90, AppThemePreference.Light), await reopened.LoadAsync());
    }

    [Fact]
    public async Task SettingsWithoutTheme_KeepTheOriginalDarkTheme()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        await File.WriteAllTextAsync(paths.SettingsFile, """{"SchemaVersion":1,"MonitorIntervalSeconds":120}""");

        using var store = new JsonAppSettingsStore(paths);

        Assert.Equal(new AppSettings(120, AppThemePreference.Dark), await store.LoadAsync());
    }
}
