using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class LocalHistoryRetentionTests
{
    private static readonly TimeSpan Expired = LocalHistoryRetention.Period + TimeSpan.FromHours(1);

    [Fact]
    public void RetentionPeriodIsThreeDays()
    {
        Assert.Equal(3, LocalHistoryRetention.RetentionDays);
        Assert.Equal(TimeSpan.FromDays(3), LocalHistoryRetention.Period);
    }

    [Fact]
    public async Task OldFinishedOperationsAreDeletedAndRecentOnesKept()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var oldCopy = await Prepare(store, ResendMode.Copy, "Sent", "Sent");
        var oldMove = await Prepare(store, ResendMode.Move, "Moved", "SentOriginalKept");
        var recent = await Prepare(store, ResendMode.Copy, "Sent", "Sent");
        Age(store, oldCopy, Expired);
        Age(store, oldMove, Expired);
        Age(store, recent, LocalHistoryRetention.Period - TimeSpan.FromHours(1));

        var result = new LocalHistoryRetention(null, store, clock: new ManualClock(DateTimeOffset.UtcNow)).RunNow();

        Assert.Equal(new RetentionResult(0, 2), result);
        Assert.False(Directory.Exists(FolderOf(store, oldCopy)));
        Assert.False(Directory.Exists(FolderOf(store, oldMove)));
        // A window still showing the removed operation must not read its missing item states as Pending.
        Assert.Throws<InvalidDataException>(() => store.ReadHistory(oldCopy));
        Assert.Equal(recent, Assert.Single(store.List()));
        Assert.All(store.ReadHistory(recent).Items, item => Assert.Equal("Sent", item.State));
    }

    [Theory]
    [InlineData(ResendMode.Copy, "Pending")]
    [InlineData(ResendMode.Copy, "Sending")]
    [InlineData(ResendMode.Copy, "Uncertain")]
    [InlineData(ResendMode.Copy, "Rejected")]
    [InlineData(ResendMode.Copy, "AwaitingScheduleClaim")]
    [InlineData(ResendMode.Copy, "Corrupt")]
    [InlineData(ResendMode.Copy, "SomethingNew")]
    [InlineData(ResendMode.Move, "Pending")]
    [InlineData(ResendMode.Move, "Sent")] // the copy is out but the original was never removed
    [InlineData(ResendMode.Move, "Deleting")]
    [InlineData(ResendMode.Move, "DeleteUncertain")]
    [InlineData(ResendMode.Move, "Uncertain")]
    [InlineData(ResendMode.Move, "Rejected")]
    public async Task OldOperationWithAnItemThatCanStillMatterIsKept(ResendMode mode, string state)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await Prepare(store, mode, mode == ResendMode.Move ? "Moved" : "Sent", state);
        Age(store, plan, TimeSpan.FromDays(30));
        var files = Directory.GetFileSystemEntries(FolderOf(store, plan)).Order().ToArray();

        var result = new LocalHistoryRetention(null, store, clock: new ManualClock(DateTimeOffset.UtcNow)).RunNow();

        Assert.Equal(0, result!.OperationsDeleted);
        Assert.Equal(plan, Assert.Single(store.List()));
        Assert.Equal(files, Directory.GetFileSystemEntries(FolderOf(store, plan)).Where(f => Path.GetFileName(f) != ".lock").Order());
    }

    [Fact]
    public async Task LastActivityNotCreationDecidesAge()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await Prepare(store, ResendMode.Copy, "Sent", "Sent");
        Age(store, plan, TimeSpan.FromDays(10));
        // The last item was resolved yesterday, long after the operation was prepared.
        File.SetLastWriteTimeUtc(Path.Combine(FolderOf(store, plan), "000001.state"), DateTime.UtcNow.AddDays(-1));

        Assert.Equal(0, store.DeleteExpired(DateTimeOffset.UtcNow - LocalHistoryRetention.Period));
        Assert.Single(store.List());
    }

    [Fact]
    public async Task OperationInUseByAnotherProcessIsKept()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await Prepare(store, ResendMode.Copy, "Sent", "Sent");
        Age(store, plan, Expired);
        using (new FileStream(Path.Combine(FolderOf(store, plan), ".lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            Assert.Equal(0, store.DeleteExpired(DateTimeOffset.UtcNow - LocalHistoryRetention.Period));
            Assert.Single(store.List());
        }
        Assert.Equal(1, store.DeleteExpired(DateTimeOffset.UtcNow - LocalHistoryRetention.Period));
        Assert.Empty(store.List());
    }

    [Fact]
    public async Task ScheduledSnapshotsAreKeptUntilTheirJobIsConsumed()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var schedules = new JsonScheduledResendStore(paths);
        var profile = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        var job = new ScheduledResend(Guid.NewGuid(), profile, "Test", now, now.AddHours(1), ResendMode.Copy, 1, "target",
            [ScheduledResendItem.From(Item(0))]) { ConfigurationIdentity = "identity" };
        schedules.Add(job);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "replay"));
        // A crash between preparing the snapshot and claiming the job leaves it blocked: kept for inspection.
        var blocked = await store.CreateResendAsync(profile, [Item(0)], ResendMode.Copy, 1, "ns", "identity", "Scheduled resend",
            default, deferActivation: true);
        var activated = await store.CreateResendAsync(profile, [Item(0)], ResendMode.Copy, 1, "ns", "identity", "Scheduled resend",
            default, deferActivation: true);
        await store.ActivateScheduledAsync(activated, default);
        File.WriteAllText(Path.Combine(FolderOf(store, activated), "000000.state"), "Sent");
        Age(store, blocked, Expired);
        Age(store, activated, Expired);
        var retention = new LocalHistoryRetention(null, store, schedules, new ManualClock(now));

        Assert.Equal(0, retention.RunNow()!.OperationsDeleted);
        Assert.Equal("AwaitingScheduleClaim", Assert.Single(store.ReadHistory(blocked).Items).State);

        Assert.True(schedules.TryRemove(job));
        Assert.Equal(1, retention.RunNow()!.OperationsDeleted);
        Assert.False(Directory.Exists(FolderOf(store, activated)));
        Assert.Equal(blocked, Assert.Single(store.List()));
        Assert.Equal("AwaitingScheduleClaim", Assert.Single(store.ReadHistory(blocked).Items).State);
    }

    [Fact]
    public async Task UnreadableScheduleListKeepsScheduledSnapshotsButNotOthers()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var scheduled = await store.CreateResendAsync(Guid.NewGuid(), [Item(0)], ResendMode.Copy, 1, "ns", "identity",
            "Scheduled resend", default, deferActivation: true);
        await store.ActivateScheduledAsync(scheduled, default);
        File.WriteAllText(Path.Combine(FolderOf(store, scheduled), "000000.state"), "Sent");
        var immediate = await Prepare(store, ResendMode.Copy, "Sent", "Sent");
        Age(store, scheduled, Expired);
        Age(store, immediate, Expired);

        var result = new LocalHistoryRetention(null, store, new BrokenSchedules(), new ManualClock(DateTimeOffset.UtcNow)).RunNow();

        Assert.Equal(1, result!.OperationsDeleted);
        Assert.Equal(scheduled, Assert.Single(store.List()));
    }

    [Fact]
    public void OldUnpublishedFolderIsDeletedAndNothingElseIsTouched()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var old = Unpublished(store, Expired);
        var fresh = Unpublished(store, TimeSpan.FromHours(1));
        var foreignFolder = Path.Combine(store.RootDirectory, "not-an-operation");
        Directory.CreateDirectory(foreignFolder);
        File.WriteAllText(Path.Combine(foreignFolder, "plan.json"), "{}");
        var foreignFile = Path.Combine(store.RootDirectory, "notes.txt");
        File.WriteAllText(foreignFile, "keep");
        var withSubfolder = Unpublished(store, Expired);
        Directory.CreateDirectory(Path.Combine(withSubfolder, "nested"));
        File.SetLastWriteTimeUtc(foreignFile, DateTime.UtcNow - Expired);
        File.SetLastWriteTimeUtc(Path.Combine(foreignFolder, "plan.json"), DateTime.UtcNow - Expired);

        Assert.Equal(1, store.DeleteExpired(DateTimeOffset.UtcNow - LocalHistoryRetention.Period));

        Assert.False(Directory.Exists(old));
        Assert.True(File.Exists(Path.Combine(fresh, "000000.message.json")));
        Assert.True(File.Exists(Path.Combine(foreignFolder, "plan.json")));
        Assert.True(File.Exists(foreignFile));
        Assert.True(Directory.Exists(Path.Combine(withSubfolder, "nested")));
    }

    [Fact]
    public void ActivityRecordsOlderThanThreeDaysAreDeletedAndNewerKept()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var now = DateTimeOffset.UtcNow;
        var veryOld = Record(now.AddDays(-20), "very old");
        var justExpired = Record(now - LocalHistoryRetention.Period - TimeSpan.FromMinutes(1), "just expired");
        var justKept = Record(now - LocalHistoryRetention.Period + TimeSpan.FromMinutes(1), "just kept");
        var current = Record(now, "current");
        foreach (var record in new[] { veryOld, justExpired, justKept, current }) journal.Append(record);
        journal.SetClearViewCutoff(now.AddDays(-30));

        var result = new LocalHistoryRetention(journal, null, clock: new ManualClock(now)).RunNow();

        Assert.Equal(new RetentionResult(2, 0), result);
        Assert.Equal(new[] { current, justKept }, journal.ReadRecent());
        Assert.False(Directory.Exists(DayFolder(directory.Path, veryOld.Timestamp)));
        Assert.True(File.Exists(Path.Combine(directory.Path, ".view-cutoff")));
    }

    [Fact]
    public void DamagedActivityRecordDoesNotStopCleanup()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var now = DateTimeOffset.UtcNow;
        var old = Record(now.AddDays(-5), "old");
        var kept = Record(now.AddDays(-1), "kept");
        journal.Append(old);
        journal.Append(kept);
        var oldDay = DayFolder(directory.Path, old.Timestamp);
        // Sorted before the valid record, so it is met first.
        var damagedOld = Path.Combine(oldDay, "0-damaged.json");
        File.WriteAllText(damagedOld, "{ not json");
        File.SetLastWriteTimeUtc(damagedOld, DateTime.UtcNow.AddDays(-5));
        var refused = Path.Combine(oldDay, "1-refused.json");
        // Valid JSON the model refuses (an entity without a name), with a timestamp that cannot be trusted.
        File.WriteAllText(refused, """{"OperationId":"00000000-0000-0000-0000-000000000000","Timestamp":"2020-01-01T00:00:00Z","Source":{"Kind":0,"Name":""}}""");
        File.SetLastWriteTimeUtc(refused, DateTime.UtcNow.AddDays(-5));
        // Damaged and without a trustworthy timestamp, but written recently: kept until its file time expires.
        var damagedRecent = Path.Combine(oldDay, "2-damaged.json");
        File.WriteAllText(damagedRecent, "garbage");
        var unrelated = Path.Combine(oldDay, "notes.txt");
        File.WriteAllText(unrelated, "keep");
        File.SetLastWriteTimeUtc(unrelated, DateTime.UtcNow.AddDays(-5));

        var result = new LocalHistoryRetention(journal, null, clock: new ManualClock(now)).RunNow();

        Assert.Equal(3, result!.ActivityRecordsDeleted);
        Assert.False(File.Exists(damagedOld));
        Assert.False(File.Exists(refused));
        Assert.True(File.Exists(damagedRecent));
        Assert.True(File.Exists(unrelated));
        Assert.Equal(kept, Assert.Single(journal.ReadRecent()));
    }

    [Fact]
    public void RetentionRunsAgainWhenTheDayChanges()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var start = new DateTimeOffset(2026, 10, 5, 1, 0, 0, TimeSpan.Zero);
        var clock = new ManualClock(start);
        var record = Record(start - TimeSpan.FromDays(2.5), "ages out tomorrow");
        journal.Append(record);
        using var retention = new LocalHistoryRetention(journal, null, clock: clock);

        Assert.True(retention.RunIfNewDay());
        Assert.Single(journal.ReadRecent());
        clock.Now = start.AddHours(20);
        Assert.False(retention.RunIfNewDay()); // once a day
        Assert.Single(journal.ReadRecent());
        clock.Now = start.AddDays(1);
        Assert.True(retention.RunIfNewDay());
        Assert.Empty(journal.ReadRecent());
    }

    [Fact]
    public async Task StartRunsAtOnceAndThePeriodicCheckRunsOnANewDay()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var start = new DateTimeOffset(2026, 10, 5, 1, 0, 0, TimeSpan.Zero);
        var clock = new ManualClock(start);
        journal.Append(Record(start.AddDays(-4), "expired at start-up"));
        var tomorrow = Record(start - TimeSpan.FromDays(2.5), "expires tomorrow");
        journal.Append(tomorrow);
        using var retention = new LocalHistoryRetention(journal, null, clock: clock);

        await retention.Start();

        Assert.Equal(tomorrow, Assert.Single(journal.ReadRecent()));
        var timer = Assert.Single(clock.Timers);
        Assert.Equal(LocalHistoryRetention.CheckInterval, timer.Period);
        timer.Fire();
        Assert.Single(journal.ReadRecent());
        clock.Now = start.AddDays(1);
        timer.Fire();
        Assert.Empty(journal.ReadRecent());
        retention.Dispose();
        Assert.True(timer.Disposed);
    }

    [Fact]
    public void MissingDirectoriesAndFailuresNeverThrow()
    {
        using var directory = new TemporaryDirectory();
        var retention = new LocalHistoryRetention(new FileActivityJournal(Path.Combine(directory.Path, "none")),
            new BatchReplayStore(Path.Combine(directory.Path, "missing")), new BrokenSchedules(), new ManualClock(DateTimeOffset.UtcNow));

        Assert.Equal(new RetentionResult(0, 0), retention.RunNow());
    }

    private static ResendItem Item(int index) => new(
        new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.DeadLetter, index,
            ReadOnlyMemory<byte>.Empty, new EditableMessageProperties(MessageId: $"original-{index}")),
        ServiceBusEntityReference.Queue("target"),
        new MessageDraft(new EditableMessageBody("hello", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: $"copy-{index}")));

    /// <summary>A real operation whose items ended in <paramref name="states"/> ("Pending" leaves no state file,
    /// "Corrupt" is a state path that is a folder).</summary>
    private static async Task<ReplayPlan> Prepare(BatchReplayStore store, ResendMode mode, params string[] states)
    {
        var plan = await store.CreateResendAsync(Guid.NewGuid(), states.Select((_, index) => Item(index)).ToArray(), mode, 50, "ns",
            "identity", "Immediate resend", default);
        for (var index = 0; index < states.Length; index++)
        {
            var path = Path.Combine(FolderOf(store, plan), $"{index:D6}.state");
            if (states[index] == "Corrupt") Directory.CreateDirectory(path);
            else if (states[index] != "Pending") File.WriteAllText(path, states[index]);
        }
        return plan;
    }

    private static string Unpublished(BatchReplayStore store, TimeSpan age)
    {
        var folder = Path.Combine(store.RootDirectory, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        File.WriteAllText(Path.Combine(folder, "000000.message.json"), JsonSerializer.Serialize(new { Body = "secret" }));
        File.WriteAllText(Path.Combine(folder, "000000.metadata.json"), "{}");
        foreach (var file in Directory.GetFiles(folder)) File.SetLastWriteTimeUtc(file, DateTime.UtcNow - age);
        return folder;
    }

    private static string FolderOf(BatchReplayStore store, ReplayPlan plan) => Path.Combine(store.RootDirectory, plan.Id.ToString("N"));

    private static void Age(BatchReplayStore store, ReplayPlan plan, TimeSpan age)
    {
        var folder = FolderOf(store, plan);
        foreach (var file in Directory.GetFiles(folder)) File.SetLastWriteTimeUtc(file, DateTime.UtcNow - age);
        Directory.SetLastWriteTimeUtc(folder, DateTime.UtcNow - age);
    }

    private static ActivityRecord Record(DateTimeOffset timestamp, string action) =>
        new(Guid.NewGuid(), timestamp, "Info", action, "details", null, null, null);

    private static string DayFolder(string journal, DateTimeOffset timestamp) =>
        Path.Combine(journal, timestamp.UtcDateTime.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture));

    private sealed class BrokenSchedules : IScheduledResendStore
    {
        public IReadOnlyList<ScheduledResend> Load() => throw new IOException("Scheduled resends are locked.");
        public void Save(IReadOnlyList<ScheduledResend> resends) => throw new NotSupportedException();
        public void Add(ScheduledResend resend) => throw new NotSupportedException();
        public bool TryRemove(ScheduledResend expected) => throw new NotSupportedException();
    }

    private sealed class ManualClock(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;
        public List<ManualTimer> Timers { get; } = [];

        public override DateTimeOffset GetUtcNow() => Now;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(() => callback(state), period);
            Timers.Add(timer);
            return timer;
        }
    }

    private sealed class ManualTimer(Action callback, TimeSpan period) : ITimer
    {
        public TimeSpan Period { get; } = period;
        public bool Disposed { get; private set; }
        public void Fire() => callback();
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;
        public void Dispose() => Disposed = true;
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
