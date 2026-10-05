using System.Text.Json.Nodes;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Attempted MessageIds are safety evidence for broker duplicate detection, not a cache: many later sends must not
    // make the composer forget one, or an edited move could reuse it and delete the original after suppression.
    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    public async Task AppStability_ManySendsDoNotEvictAnAttemptedMessageId(MessagingProvider provider)
    {
        var (vm, broker, _) = await CreateAzureComposerAsync(provider);
        await using var owner = vm;
        vm.DraftMessageId = "copy-B";
        await vm.SendDraftCommand.ExecuteAsync();
        var profileId = vm.SelectedProfile!.Id;
        for (var index = 0; index < 1_000; index++)
        {
            vm.RecordComposerSendAttempt((profileId, $"later-{index}"), "fingerprint", isMove: false);
        }

        vm.DraftBody = "edited replacement";
        vm.DraftMovesOriginal = true;
        await vm.SendDraftCommand.ExecuteAsync();

        Assert.Contains("already attempted", vm.ErrorText, StringComparison.Ordinal);
        Assert.Empty(broker.DeleteRequests);
        Assert.Equal(1_001, vm.ComposerSendAttemptCount);
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

        Assert.Equal([savedHere, savedElsewhere, shared], merged);
    }

    // Another window deleted A while this window, which still shows A unchanged, added B: A stays deleted.
    [Fact]
    public void AppStability_SavedSearchMergeKeepsARemoteDeletion()
    {
        var a = new SavedSearch("A", "a");
        var b = new SavedSearch("B", "b");

        var merged = MainWindowViewModel.MergeSavedSearches(baseline: [a], local: [b, a], stored: []);

        Assert.Equal([b], merged);
    }

    // Another window replaced A with an edited A (same name) while this window added B: the edit is kept.
    [Fact]
    public void AppStability_SavedSearchMergeKeepsARemoteEdit()
    {
        var a = new SavedSearch("A", "a");
        var editedA = new SavedSearch("A", "a edited");
        var b = new SavedSearch("B", "b");

        var merged = MainWindowViewModel.MergeSavedSearches(baseline: [a], local: [b, a], stored: [editedA]);

        Assert.Equal([b, editedA], merged);
    }

    // Deleting A fails to write; after storage recovers, adding B must still delete A rather than resurrect it.
    [Fact]
    public async Task AppStability_FailedSavedSearchWriteIsCarriedByTheNextSave()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var b = new SavedSearch("B", "b");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [a] });
        IReadOnlyList<SavedSearch> stored = [a];

        vm.SavedSearches.Remove(a);
        _ = vm.CaptureSavedSearchChanges(); // the write fails: stored stays [A], nothing is acknowledged

        vm.SavedSearches.Insert(0, b);
        var save = vm.CaptureSavedSearchChanges();
        stored = save.Merge(stored);
        save.Acknowledge();

        Assert.Equal([b], stored);

        // Once acknowledged, the next save starts from what was written.
        var next = vm.CaptureSavedSearchChanges();
        Assert.Equal([b], next.Merge(stored));
    }

    // Saves that finish out of order: the older acknowledgement must not move the baseline back.
    [Fact]
    public async Task AppStability_OutOfOrderSavedSearchAcknowledgementsKeepTheNewest()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var b = new SavedSearch("B", "b");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });

        vm.SavedSearches.Insert(0, a);
        var first = vm.CaptureSavedSearchChanges();
        vm.SavedSearches.Insert(0, b);
        var second = vm.CaptureSavedSearchChanges();
        IReadOnlyList<SavedSearch> stored = second.Merge(first.Merge([]));
        second.Acknowledge();
        first.Acknowledge();

        vm.SavedSearches.Remove(a);
        Assert.Equal([b], vm.CaptureSavedSearchChanges().Merge(stored));
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
