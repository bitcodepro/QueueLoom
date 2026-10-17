using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task FailedScheduledSnapshotPersistenceDoesNotConsumeJobOrSend()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var schedules = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1), PreserveMessageIds = false } };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs,
            replayStore: store, scheduledResends: schedules);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.Messages.Add(new MessageItemViewModel(OperationItem(7).Original, profile.Id) { IsMarked = true });
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        vm.Clock = new ManualClock(DateTimeOffset.UtcNow.AddHours(2));
        store.BeforeStateWrite = (_, state) => { if (state == "AwaitingScheduleClaim") throw new IOException("Injected snapshot failure"); };
        await vm.RunDueScheduledResendsAsync();
        Assert.Single(schedules.Load()); Assert.Empty(workspace.SentMessages); Assert.Empty(store.List());
        store.BeforeStateWrite = null;
        await vm.RunDueScheduledResendsAsync();
        Assert.Empty(schedules.Load()); Assert.Single(workspace.SentMessages);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ImmediateAndScheduledResendsUseDurableHistory(bool scheduled)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var schedules = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            ResendChoice = dialog => dialog.ToOptions() with
            { Mode = ResendMode.Move, SendAt = scheduled ? DateTimeOffset.UtcNow.AddHours(1) : null, PreserveMessageIds = false }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs,
            replayStore: store, scheduledResends: schedules);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.Messages.Add(new MessageItemViewModel(OperationItem(7).Original, profile.Id) { IsMarked = true });
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        if (scheduled)
        {
            Assert.Single(vm.ScheduledResends); Assert.Empty(store.List());
            vm.Clock = new ManualClock(DateTimeOffset.UtcNow.AddHours(2));
            await vm.RunDueScheduledResendsAsync();
            Assert.Empty(schedules.Load());
        }
        var plan = Assert.Single(store.List());
        Assert.Equal(scheduled ? "Scheduled resend" : "Immediate resend", plan.Kind);
        Assert.Equal("Moved", Assert.Single(store.ReadHistory(plan).Items).State);
        Assert.Single(workspace.SentMessages); Assert.Single(workspace.DeleteRequests);
        await vm.OperationHistoryRefresh;
        Assert.Single(vm.OperationHistory);
        vm.OperationItems[0].IsMarked = true;
        await vm.ContinueOperationCommand.ExecuteAsync();
        Assert.Single(workspace.SentMessages);
        Assert.Contains("cannot be retried", vm.ErrorText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ClearActivityDoesNotChangeOperationsOrBackupsAndRestoreSurvivesReopen()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var journal = new FileActivityJournal(Path.Combine(directory.Path, "activity"));
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await PrepareOperation(store, profile);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, replayStore: store, activityJournal: journal);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        Assert.NotEmpty(vm.Activity);
        await vm.ClearActivityViewCommand.ExecuteAsync();
        Assert.Empty(vm.Activity);
        Assert.Equal(plan, Assert.Single(store.List()));
        // Ordinary entries are written by the journal's background writer: check the disk once it is done.
        await journal.WaitForPendingEntriesAsync().WaitAsync(TimeSpan.FromSeconds(30));
        // Closed before the folder is read and reopened, as the app closes it on shutdown; a later entry is written directly.
        await journal.DisposeAsync();
        Assert.NotEmpty(Directory.GetFiles(Path.Combine(directory.Path, "activity"), "*.json", SearchOption.AllDirectories));
        await using var reopened = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            replayStore: store, activityJournal: new FileActivityJournal(Path.Combine(directory.Path, "activity")));
        await reopened.InitializeAsync();
        Assert.Empty(reopened.Activity);
        await reopened.RestoreActivityViewCommand.ExecuteAsync();
        Assert.NotEmpty(reopened.Activity);
        Assert.Equal(plan, Assert.Single(store.List()));
    }

    // Clear view and Restore view took a cross-process lock and wrote a file, and the history read up to 500 files,
    // all on the window's thread: another window holding the lock froze this one. Each now returns to its caller
    // while the journal is still busy.
    [Fact]
    public async Task ActivityJournalReadsAndViewChangesDoNotHoldTheCaller()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var journal = new GatedJournal();
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, activityJournal: journal);

        async Task ReturnsWhileTheJournalIsBusy(Func<Task> start, string call)
        {
            journal.Gate.Reset();
            var running = start();
            // Synchronous journal work would have held the caller until the gate's own timeout.
            Assert.True(await journal.Entered.WaitAsync(TimeSpan.FromSeconds(30)), call + " never reached the journal");
            Assert.False(running.IsCompleted, call + " finished while the journal was still busy");
            journal.Gate.Set();
            await running.WaitAsync(TimeSpan.FromSeconds(30));
        }

        await ReturnsWhileTheJournalIsBusy(() => vm.InitializeAsync(), "loading the history");
        await ReturnsWhileTheJournalIsBusy(() => vm.ClearActivityViewCommand.ExecuteAsync(), "clear view");
        await ReturnsWhileTheJournalIsBusy(() => vm.RestoreActivityViewCommand.ExecuteAsync(), "restore view");

        Assert.Equal(["read", "cutoff", "cutoff null", "read"], journal.Calls);
        Assert.Contains(vm.Activity, item => item.Action == "Earlier entry");
        Assert.Null(journal.ClearViewCutoff);
    }

    private sealed class GatedJournal : QueueLoom.Core.Abstractions.IActivityViewJournal
    {
        public ManualResetEventSlim Gate { get; } = new(true);
        public SemaphoreSlim Entered { get; } = new(0);
        public List<string> Calls { get; } = [];
        public DateTimeOffset? ClearViewCutoff { get; private set; }
        public void Append(QueueLoom.Core.Abstractions.ActivityRecord record) { }
        private void Enter(string call)
        {
            lock (Calls) Calls.Add(call);
            Entered.Release();
            Gate.Wait(TimeSpan.FromSeconds(10));
        }
        public void SetClearViewCutoff(DateTimeOffset? cutoff)
        {
            Enter(cutoff is null ? "cutoff null" : "cutoff");
            ClearViewCutoff = cutoff;
        }
        public IReadOnlyList<QueueLoom.Core.Abstractions.ActivityRecord> ReadRecent(int maximum = 500)
        {
            Enter("read");
            return [new(Guid.NewGuid(), DateTimeOffset.UtcNow.AddDays(-1), "Info", "Earlier entry", "d", null, null, null)];
        }
    }
}
