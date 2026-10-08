using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Scheduled resends are shared by every QueueLoom window through one file and its cross-process lock. While another
// window held that lock (up to 30 seconds), this window waited for it on its own thread: the background check, cancel,
// schedule and run froze the window. These tests synchronize on explicit signals, never on elapsed time: the store
// reports when an operation has started waiting, and every synchronous store call once the window is up fails the test.
public sealed partial class ViewModelStateTests
{
    private static readonly TimeSpan DeadlockGuard = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData("check", "load")]
    [InlineData("cancel", "remove")]
    [InlineData("schedule", "add")]
    [InlineData("run", "remove")]
    public async Task ScheduledResends_AnotherWindowHoldingTheListDoesNotFreezeThisWindow(string action, string waitsIn)
    {
        await using var fixture = await ScheduleFixture.CreateAsync(withJob: action != "schedule");
        var vm = fixture.ViewModel;

        Task pending;
        await using (fixture.HoldListAsOtherWindow())
        {
            pending = action switch
            {
                "check" => vm.RunDueScheduledResendsAsync(),
                "cancel" => CancelThroughCommand(vm),
                "schedule" => vm.ResendMarkedMessagesCommand.ExecuteAsync(),
                _ => vm.RunScheduledNowAsync(Assert.Single(vm.ScheduledResends))
            };
            // The call came back to this thread, and the store is waiting for the other window: nothing held this one.
            await fixture.Store.Entered(waitsIn).Task.WaitAsync(DeadlockGuard);
            Assert.False(pending.IsCompleted, "The action finished while the list was still held.");
        }

        // Once the other window lets go, the action completes as before.
        await pending.WaitAsync(DeadlockGuard);
        var saved = fixture.Store.Inner.Load();
        switch (action)
        {
            case "check":
            case "schedule":
                Assert.Single(saved);
                Assert.Single(vm.ScheduledResends);
                break;
            case "cancel":
                Assert.Empty(saved);
                Assert.Empty(vm.ScheduledResends);
                break;
            default:
                Assert.Empty(saved);
                Assert.Single(fixture.Workspace.SentMessages);
                break;
        }
    }

    // Cancelled while waiting for the list: scheduling saves nothing (so nothing can be sent later), and claiming a job
    // to run leaves it pending. The cancellation completes while the other window still holds the list.
    [Theory]
    [InlineData("schedule")]
    [InlineData("run")]
    public async Task ScheduledResends_CancellingWhileWaitingForTheListChangesNothing(string action)
    {
        await using var fixture = await ScheduleFixture.CreateAsync(withJob: action == "run");
        var vm = fixture.ViewModel;

        await using (fixture.HoldListAsOtherWindow())
        {
            var pending = action == "schedule"
                ? vm.ResendMarkedMessagesCommand.ExecuteAsync()
                : vm.RunScheduledNowAsync(Assert.Single(vm.ScheduledResends));
            await fixture.Store.Entered(action == "schedule" ? "add" : "remove").Task.WaitAsync(DeadlockGuard);

            vm.CancelCurrentOperationCommand.Execute(null);
            await pending.WaitAsync(DeadlockGuard);
        }

        var saved = fixture.Store.Inner.Load();
        if (action == "schedule")
        {
            Assert.Empty(saved);
            Assert.Empty(vm.ScheduledResends);
            Assert.DoesNotContain("Scheduled for", vm.StatusText, StringComparison.Ordinal);
            Assert.True(Assert.Single(vm.Messages).IsMarked);
        }
        else
        {
            Assert.Single(saved);
            Assert.Single(vm.ScheduledResends);
            Assert.Empty(fixture.Workspace.SentMessages);
        }
    }

    // The background check can list a job that scheduling has saved but not yet shown. It is shown once, and stays once
    // through later checks.
    [Fact]
    public async Task ScheduledResends_AJobListedByTheCheckWhileBeingScheduledShowsOnce()
    {
        await using var fixture = await ScheduleFixture.CreateAsync(withJob: false);
        var vm = fixture.ViewModel;
        var persisted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Store.AfterAddPersisted = async () => { persisted.TrySetResult(); await release.Task; };

        var scheduling = vm.ResendMarkedMessagesCommand.ExecuteAsync();
        await persisted.Task.WaitAsync(DeadlockGuard);
        await vm.RunDueScheduledResendsAsync().WaitAsync(DeadlockGuard);
        Assert.Single(vm.ScheduledResends);
        release.TrySetResult();
        await scheduling.WaitAsync(DeadlockGuard);

        Assert.Single(vm.ScheduledResends);
        Assert.Single(fixture.Store.Inner.Load());
        await vm.RunDueScheduledResendsAsync().WaitAsync(DeadlockGuard);
        Assert.Single(vm.ScheduledResends);
    }

    // A damaged list found by another read (the history cleanup) can be reported late in the check. That report reads
    // the list again, and does so without holding the window's thread too.
    [Fact]
    public async Task ScheduledResends_ALateSetAsideReportDoesNotReadOnTheWindowThread()
    {
        await using var fixture = await ScheduleFixture.CreateAsync(withJob: true);
        var vm = fixture.ViewModel;
        // The first report check of the check finds nothing; the set-aside arrives before the last one.
        fixture.Store.SetAsideOnSecondTake = Path.Combine(fixture.Directory, "scheduled-resends.v2.json.damaged-test");

        await vm.RunDueScheduledResendsAsync().WaitAsync(DeadlockGuard);

        Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
    }

    private static Task CancelThroughCommand(MainWindowViewModel vm)
    {
        vm.CancelScheduledResendCommand.Execute(Assert.Single(vm.ScheduledResends));
        return vm.PendingScheduledCancellation;
    }

    private sealed class ScheduleFixture(TemporaryDirectory directory, ObservedScheduleStore store, FakeWorkspace workspace,
        MainWindowViewModel viewModel) : IAsyncDisposable
    {
        public ObservedScheduleStore Store => store;
        public FakeWorkspace Workspace => workspace;
        public MainWindowViewModel ViewModel => viewModel;
        public string Directory => directory.Path;

        public static async Task<ScheduleFixture> CreateAsync(bool withJob)
        {
            var directory = new TemporaryDirectory();
            var profile = CreateProfile("Contended", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
            var store = new ObservedScheduleStore(new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path)));
            if (withJob) store.Inner.Add(DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) });
            var workspace = new FakeWorkspace();
            var dialogs = new FakeDialogService
            {
                ConfirmResult = true,
                ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1), PreserveMessageIds = false }
            };
            var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, scheduledResends: store);
            await vm.InitializeAsync();
            await vm.ConnectCommand.ExecuteAsync();
            if (!withJob) vm.Messages.Add(new MessageItemViewModel(OperationItem(7).Original, profile.Id) { IsMarked = true });
            // From here on the window is up: a synchronous store call would run on its thread.
            store.WindowShown = true;
            return new ScheduleFixture(directory, store, workspace, vm);
        }

        /// <summary>Holds the list's cross-process lock the way another window does, until disposed.</summary>
        public IAsyncDisposable HoldListAsOtherWindow() =>
            new FileStream(store.Inner.FilePath + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        public async ValueTask DisposeAsync()
        {
            await viewModel.DisposeAsync();
            directory.Dispose();
        }
    }

    /// <summary>
    /// The JSON store, observed: each asynchronous operation reports when it starts waiting for the list, and every
    /// synchronous call after <see cref="WindowShown"/> fails, since it would run on the window's thread.
    /// </summary>
    private sealed class ObservedScheduleStore(JsonScheduledResendStore inner) : IScheduledResendStore
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, TaskCompletionSource> _entered = new();
        private int _takes;

        public JsonScheduledResendStore Inner => inner;
        public bool WindowShown { get; set; }
        public Func<Task>? AfterAddPersisted { get; set; }
        public string? SetAsideOnSecondTake { get; set; }

        public TaskCompletionSource Entered(string operation) =>
            _entered.GetOrAdd(operation, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        public IReadOnlyList<ScheduledResend> Load() => Synchronous(inner.Load);
        public void Save(IReadOnlyList<ScheduledResend> resends) => Synchronous(() => { inner.Save(resends); return true; });
        public void Add(ScheduledResend resend) => Synchronous(() => { inner.Add(resend); return true; });
        public bool TryRemove(ScheduledResend expected) => Synchronous(() => inner.TryRemove(expected));

        public string? TakeSetAsideFile()
        {
            if (WindowShown && Interlocked.Increment(ref _takes) == 2 && SetAsideOnSecondTake is { } aside) return aside;
            return inner.TakeSetAsideFile();
        }

        public Task<IReadOnlyList<ScheduledResend>> LoadAsync(CancellationToken cancellationToken = default)
        {
            Entered("load").TrySetResult();
            return inner.LoadAsync(cancellationToken);
        }

        public async Task AddAsync(ScheduledResend resend, CancellationToken cancellationToken = default)
        {
            Entered("add").TrySetResult();
            await inner.AddAsync(resend, cancellationToken);
            if (AfterAddPersisted is { } after) await after();
        }

        public Task<bool> TryRemoveAsync(ScheduledResend expected, CancellationToken cancellationToken = default)
        {
            Entered("remove").TrySetResult();
            return inner.TryRemoveAsync(expected, cancellationToken);
        }

        private T Synchronous<T>(Func<T> call) => WindowShown
            ? throw new InvalidOperationException("A synchronous scheduled-resend store call once the window is up.")
            : call();
    }
}
