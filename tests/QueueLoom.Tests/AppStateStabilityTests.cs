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

    // Add A then remove A, both captured before the first write completes, written in order: A ends up removed.
    [Fact]
    public async Task AppStability_QueuedAddThenRemoveEndsRemoved()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });

        vm.SavedSearches.Insert(0, a);
        var first = vm.CaptureSavedSearchChanges();
        vm.SavedSearches.Remove(a);
        var second = vm.CaptureSavedSearchChanges();

        IReadOnlyList<SavedSearch> stored = first.Merge([]);
        Assert.Equal([a], stored);
        stored = second.Merge(stored);
        first.Acknowledge();
        second.Acknowledge();

        Assert.Empty(stored);
    }

    // Re-saving an existing search removes and reinserts it synchronously (two captures before any write): it stays.
    [Fact]
    public async Task AppStability_ResavingAnExistingSearchKeepsItStored()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        IReadOnlyList<SavedSearch> stored = [];
        var saves = new List<SavedSearchSave>();
        vm.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName == nameof(MainWindowViewModel.SavedSearches) && !vm.IsLoadingSavedSearches)
                saves.Add(vm.CaptureSavedSearchChanges());
        };
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });
        vm.DeadLetterSearchQuery = "orders";
        vm.SaveSearchCommand.Execute(null);
        foreach (var save in saves) { stored = save.Merge(stored); save.Acknowledge(); }
        saves.Clear();
        Assert.Single(stored);

        vm.SaveSearchCommand.Execute(null); // the same query again: removed, then inserted at the top
        Assert.True(saves.Count >= 2);
        var merges = saves.Select(save => save.Merge).ToArray();
        foreach (var merge in merges) stored = merge(stored); // all captured before any write, written in order
        foreach (var save in saves) save.Acknowledge();

        Assert.Single(stored);
        Assert.Equal("orders", stored[0].Query);
    }

    // c1 (+A) and c2 (+B) are both captured before c1 is written; c1 is written, another window then deletes or edits
    // A, and only then c2 runs: it must not replay +A over the remote change.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AppStability_QueuedSaveDoesNotReplayAWrittenChangeOverARemoteOne(bool remoteEdit)
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var editedA = new SavedSearch("A", "a edited");
        var b = new SavedSearch("B", "b");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });

        vm.SavedSearches.Insert(0, a);
        var first = vm.CaptureSavedSearchChanges();
        vm.SavedSearches.Insert(0, b);
        var second = vm.CaptureSavedSearchChanges();

        IReadOnlyList<SavedSearch> stored = first.Merge([]);
        first.Acknowledge();
        Assert.Equal([a], stored);
        stored = remoteEdit ? [editedA] : [];
        stored = second.Merge(stored);
        second.Acknowledge();

        Assert.Equal(remoteEdit ? [b, editedA] : [b], stored);
    }

    // Same queue, but c2 runs after c1 was written and before c1's acknowledgement arrived: still no replay.
    [Fact]
    public async Task AppStability_QueuedSaveSkipsAChangeAnotherSaveIsWriting()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var b = new SavedSearch("B", "b");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });

        vm.SavedSearches.Insert(0, a);
        var first = vm.CaptureSavedSearchChanges();
        vm.SavedSearches.Insert(0, b);
        var second = vm.CaptureSavedSearchChanges();

        _ = first.Merge([]);
        IReadOnlyList<SavedSearch> stored = second.Merge([]); // A was deleted remotely after c1's write
        first.Acknowledge();
        second.Acknowledge();

        Assert.Equal([b], stored);
    }

    // c1's write fails after c2 already ran without it: c1's change goes with the next save instead of being lost.
    [Fact]
    public async Task AppStability_AFailedQueuedSaveIsCarriedByTheNextSave()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var b = new SavedSearch("B", "b");
        var c = new SavedSearch("C", "c");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });

        vm.SavedSearches.Insert(0, a);
        var first = vm.CaptureSavedSearchChanges();
        vm.SavedSearches.Insert(0, b);
        var second = vm.CaptureSavedSearchChanges();
        _ = first.Merge([]);
        IReadOnlyList<SavedSearch> stored = second.Merge([]);
        second.Acknowledge();
        first.Fail();

        vm.SavedSearches.Insert(0, c);
        stored = vm.CaptureSavedSearchChanges().Merge(stored);

        Assert.Equal([c, a, b], stored);
    }

    // c1 (+A) fails to write and c2 (-A) is queued behind it: c2 must not run until c1's failure is settled, so the
    // removal is not lost and a later +C leaves only C.
    [Fact]
    public async Task AppStability_FailedAddThenQueuedRemoveDoesNotResurrect()
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var a = new SavedSearch("A", "a");
        var c = new SavedSearch("C", "c");
        vm.ApplyPreferences(new AppSettings { SavedSearches = [] });
        IReadOnlyList<SavedSearch> stored = [];
        var firstWriting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstFails = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        vm.SavedSearches.Insert(0, a);
        var first = vm.PersistSavedSearchesAsync(vm.CaptureSavedSearchChanges(), async merge =>
        {
            _ = merge(stored); // claimed, but the disk write fails
            firstWriting.SetResult();
            await firstFails.Task;
            throw new IOException("disk full");
        });
        await firstWriting.Task;
        vm.SavedSearches.Remove(a);
        var second = vm.PersistSavedSearchesAsync(vm.CaptureSavedSearchChanges(), merge =>
        {
            stored = merge(stored);
            return Task.CompletedTask;
        });
        Assert.False(second.IsCompleted); // waits for the first save to settle
        firstFails.SetResult();
        await Assert.ThrowsAsync<IOException>(() => first);
        await second;

        vm.SavedSearches.Insert(0, c);
        await vm.PersistSavedSearchesAsync(vm.CaptureSavedSearchChanges(), merge =>
        {
            stored = merge(stored);
            return Task.CompletedTask;
        });

        Assert.Equal([c], stored);
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
