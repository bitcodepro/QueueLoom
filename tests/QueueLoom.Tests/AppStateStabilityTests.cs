using System.Text.Json.Nodes;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AppStability_ComposerSendAttemptsStayBounded()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var profileId = Guid.NewGuid();
        for (var index = 0; index < MainWindowViewModel.MaximumComposerSendAttempts * 4; index++)
        {
            vm.RecordComposerSendAttempt((profileId, $"message-{index}"), "fingerprint", isMove: false);
        }

        Assert.Equal(MainWindowViewModel.MaximumComposerSendAttempts, vm.ComposerSendAttemptCount);
    }

    [Theory]
    [InlineData(0.0004)]
    [InlineData(1.23456)]
    public void AppStability_ComposerTimeToLiveFormatKeepsSubMillisecondValues(double seconds)
    {
        var timeToLive = TimeSpan.FromSeconds(seconds);
        var text = MainWindowViewModel.FormatTimeToLiveSeconds(timeToLive);

        var parsed = double.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(parsed > 0);
        Assert.Equal(timeToLive, TimeSpan.FromSeconds(parsed));
    }

    [Fact]
    public void AppStability_SavedSearchesFromTwoWindowsAreMerged()
    {
        var shared = new SavedSearch("shared", "a");
        var removedHere = new SavedSearch("removed", "b");
        var savedElsewhere = new SavedSearch("other window", "c");
        var savedHere = new SavedSearch("this window", "d");

        var merged = MainWindowViewModel.MergeSavedSearches(
            baseline: [shared, removedHere],
            local: [savedHere, shared],
            stored: [savedElsewhere, shared, removedHere]);

        Assert.Equal([savedHere, shared, savedElsewhere], merged);
    }

    [Fact]
    public async Task AppStability_SettingsRewriteKeepsUnknownFields()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        await File.WriteAllTextAsync(paths.SettingsFile,
            """{ "SchemaVersion": 1, "MonitorIntervalSeconds": 60, "FutureOption": { "Enabled": true } }""");

        using (var store = new JsonAppSettingsStore(paths))
        {
            await store.SaveThemeAsync(AppThemePreference.Light);
        }

        var json = JsonNode.Parse(await File.ReadAllTextAsync(paths.SettingsFile))!;
        Assert.True(json["FutureOption"]!["Enabled"]!.GetValue<bool>());
        Assert.Equal("Light", json["Theme"]!.GetValue<string>());
    }

    [Fact]
    public async Task AppStability_ProfileRewriteKeepsUnknownTopLevelFields()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        await File.WriteAllTextAsync(paths.ProfilesFile,
            """{ "schemaVersion": 1, "profiles": [], "futureFolders": ["ops"] }""");

        using (var repository = new JsonProfileRepository(paths))
        {
            await repository.UpsertAsync(CreateProfile("dev", EnvironmentKind.Development));
        }

        var json = JsonNode.Parse(await File.ReadAllTextAsync(paths.ProfilesFile))!;
        Assert.Equal("ops", json["futureFolders"]![0]!.GetValue<string>());
        Assert.Single(json["profiles"]!.AsArray());
    }
}
