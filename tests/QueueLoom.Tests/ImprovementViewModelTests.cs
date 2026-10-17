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

    // An older read finishing after a newer Clear view put back the rows that Clear had hidden and the "not hidden"
    // notice, although the journal kept the newer cutoff. Restore, Clear and the start-up read now run one at a time.
    // The interleaving is set by signals, not by time: the older read is held after it took its rows; Clear is
    // started; the read is released only once Clear waits for the gate. Without the gate Clear changes the journal
    // while the read is held, which the test reports at once.
    [Theory]
    [InlineData("restore")]
    [InlineData("startup")]
    public async Task AClearViewThatOverlapsAnOlderReadIsNotUndoneByIt(string older)
    {
        var safety = TimeSpan.FromSeconds(30);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var journal = new SignalledJournal(older == "restore" ? DateTimeOffset.UtcNow.AddHours(-1) : null);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, activityJournal: journal);
        if (older == "restore") await vm.InitializeAsync().WaitAsync(safety);

        var held = journal.HoldNextRead();
        var reading = older == "restore" ? vm.RestoreActivityViewCommand.ExecuteAsync() : vm.InitializeAsync();
        await held.Entered.WaitAsync(safety);
        var cutoffsBefore = journal.CutoffsSet;
        var clearing = vm.ClearActivityViewCommand.ExecuteAsync();
        var deadline = DateTime.UtcNow + safety;
        while (vm.ActivityViewWaiters == 0 && journal.CutoffsSet == cutoffsBefore)
        {
            Assert.True(DateTime.UtcNow < deadline, "Clear neither waited for the older read nor changed the journal.");
            await Task.Delay(1);
        }
        Assert.Equal(cutoffsBefore, journal.CutoffsSet); // Clear did not change the journal while the older read was held.
        held.Release();
        await Task.WhenAll(reading, clearing).WaitAsync(safety);

        Assert.NotNull(journal.ClearViewCutoff);
        Assert.StartsWith("Activity through", vm.ActivityViewNotice, StringComparison.Ordinal);
        Assert.DoesNotContain(vm.Activity, item => item.Action == "Earlier entry");
        Assert.True(vm.RestoreActivityViewCommand.CanExecute(null));
    }

    private sealed class SignalledJournal(DateTimeOffset? cutoff) : QueueLoom.Core.Abstractions.IActivityViewJournal
    {
        public sealed class HeldRead
        {
            private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task Entered => _entered.Task;
            public void Release() => _released.TrySetResult();
            internal void Hold()
            {
                _entered.TrySetResult();
                if (!_released.Task.Wait(TimeSpan.FromSeconds(30))) throw new TimeoutException("The held read was never released.");
            }
        }

        private readonly QueueLoom.Core.Abstractions.ActivityRecord _earlier =
            new(Guid.NewGuid(), DateTimeOffset.UtcNow.AddDays(-1), "Info", "Earlier entry", "d", null, null, null);
        private HeldRead? _next;
        private int _cutoffsSet;
        public DateTimeOffset? ClearViewCutoff { get; private set; } = cutoff;
        /// <summary>How many times a cutoff (not a restore) was written.</summary>
        public int CutoffsSet => Volatile.Read(ref _cutoffsSet);
        public HeldRead HoldNextRead() => _next = new HeldRead();
        public void Append(QueueLoom.Core.Abstractions.ActivityRecord record) { }
        public void SetClearViewCutoff(DateTimeOffset? value)
        {
            ClearViewCutoff = value;
            if (value is not null) Interlocked.Increment(ref _cutoffsSet);
        }
        public IReadOnlyList<QueueLoom.Core.Abstractions.ActivityRecord> ReadRecent(int maximum = 500)
        {
            // Like the file journal: the rows are chosen against the cutoff of the moment, then returned.
            QueueLoom.Core.Abstractions.ActivityRecord[] rows = _earlier.Timestamp <= ClearViewCutoff ? [] : [_earlier];
            Interlocked.Exchange(ref _next, null)?.Hold();
            return rows;
        }
    }

    private sealed class GatedJournal : QueueLoom.Core.Abstractions.IActivityViewJournal
    {
        private readonly QueueLoom.Core.Abstractions.ActivityRecord _earlier =
            new(Guid.NewGuid(), DateTimeOffset.UtcNow.AddDays(-1), "Info", "Earlier entry", "d", null, null, null);
        public ManualResetEventSlim Gate { get; } = new(true);
        public SemaphoreSlim Entered { get; } = new(0);
        public List<string> Calls { get; } = [];
        public DateTimeOffset? ClearViewCutoff { get; private set; }
        public void Append(QueueLoom.Core.Abstractions.ActivityRecord record) { }
        private void Enter(string call)
        {
            lock (Calls) Calls.Add(call);
            Entered.Release();
            if (!Gate.Wait(TimeSpan.FromSeconds(30))) throw new TimeoutException("The gate was never opened.");
        }
        public void SetClearViewCutoff(DateTimeOffset? cutoff)
        {
            Enter(cutoff is null ? "cutoff null" : "cutoff");
            ClearViewCutoff = cutoff;
        }
        public IReadOnlyList<QueueLoom.Core.Abstractions.ActivityRecord> ReadRecent(int maximum = 500)
        {
            // Like the file journal: the rows are chosen against the cutoff of the moment, then returned.
            QueueLoom.Core.Abstractions.ActivityRecord[] rows = _earlier.Timestamp <= ClearViewCutoff ? [] : [_earlier];
            Enter("read");
            return rows;
        }
    }
}
