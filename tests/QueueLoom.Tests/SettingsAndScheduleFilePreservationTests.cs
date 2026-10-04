using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class SettingsAndScheduleFilePreservationTests
{
    [Fact]
    public async Task UpdatingSettings_DoesNotWipeAFileWithANewerSchema()
    {
        using var dir = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(dir.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        await File.WriteAllTextAsync(paths.SettingsFile,
            """{"SchemaVersion":2,"MonitorIntervalSeconds":300,"SavedSearches":[{"Name":"Prod errors","Query":"error"}],"AlertWebhookUrl":"https://hooks.example.com/x"}""");

        using var store = new JsonAppSettingsStore(paths);
        await Assert.ThrowsAsync<InvalidDataException>(() => store.SaveThemeAsync(AppThemePreference.Light));

        var text = await File.ReadAllTextAsync(paths.SettingsFile);
        Assert.Contains("Prod errors", text);
    }

    [Fact]
    public async Task UpdatingSettings_DoesNotWipeAFileWithOneMalformedField()
    {
        using var dir = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(dir.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        await File.WriteAllTextAsync(paths.SettingsFile,
            """{"SchemaVersion":1,"MonitorIntervalSeconds":"300","SavedSearches":[{"Name":"Prod errors","Query":"error"}]}""");

        using var store = new JsonAppSettingsStore(paths);
        await store.SaveThemeAsync(AppThemePreference.Light);

        Assert.True(Directory.GetFiles(paths.RootDirectory).Any(f => File.ReadAllText(f).Contains("Prod errors")),
            "The saved searches were silently destroyed.");
    }

    [Fact]
    public void DamagedScheduledResends_EachDamagedCopyIsKept()
    {
        using var dir = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(dir.Path);
        Directory.CreateDirectory(paths.RootDirectory);
        var store = new JsonScheduledResendStore(paths);

        File.WriteAllText(store.FilePath, "{first-damaged");
        Assert.Empty(store.Load());
        File.WriteAllText(store.FilePath, "{second-damaged");
        Assert.Empty(store.Load());

        var kept = Directory.GetFiles(paths.RootDirectory, "scheduled-resends.v2.json.damaged-*").Select(File.ReadAllText).ToArray();
        Assert.Contains("{first-damaged", kept);
        Assert.Contains("{second-damaged", kept);
    }
}
