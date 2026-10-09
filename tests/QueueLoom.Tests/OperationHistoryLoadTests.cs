using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The operation history was read on the window's thread: listing every saved operation of the retention period, and
// selecting one read up to 1,000 items with several small files each. These tests hold the store's reads and check that
// the window's thread is not held with them, and that ticked items always belong to the operation shown.
public sealed partial class ViewModelStateTests
{
    private static readonly TimeSpan OperationLoadGuard = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task OperationHistory_SelectingAnOperationReadsItsItemsOffTheWindowsThread()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        var first = await PrepareReplayRegression(store.Inner, profile);
        var second = await PrepareReplayRegression(store.Inner, profile);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        await vm.OperationHistoryRefresh.WaitAsync(OperationLoadGuard);
        var other = vm.OperationHistory.Single(operation => operation.Plan.Id != vm.SelectedOperation!.Plan.Id);
        Assert.Equal(2, vm.OperationItems.Count);
        vm.OperationItems[0].IsMarked = true;

        store.HoldReads();
        try
        {
            // The selection came back while the read was held: nothing of the previous operation is left to tick.
            vm.SelectedOperation = other;
            await store.ReadEntered.Task.WaitAsync(OperationLoadGuard);
            Assert.Empty(vm.OperationItems);
            Assert.Null(vm.SelectedOperationItem);
            Assert.False(vm.OperationItemsLoad.IsCompleted);
        }
        finally
        {
            store.ReleaseReads();
        }

        await vm.OperationItemsLoad.WaitAsync(OperationLoadGuard);
        Assert.Equal(2, vm.OperationItems.Count);
        Assert.All(vm.OperationItems, item => Assert.False(item.IsMarked));
        Assert.Contains(vm.SelectedOperation!.Plan.Id, new[] { first.Id, second.Id });
    }

    // Reads finish in any order; the items shown are those of the operation selected last.
    [Fact]
    public async Task OperationHistory_AnOlderSelectionsReadFinishingLastIsDropped()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        await PrepareReplayRegression(store.Inner, profile);
        await PrepareReplayRegression(store.Inner, profile);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        await vm.OperationHistoryRefresh.WaitAsync(OperationLoadGuard);
        var initial = vm.SelectedOperation!;
        var other = vm.OperationHistory.Single(operation => operation != initial);

        store.HoldReads();
        vm.SelectedOperation = other;
        var older = vm.OperationItemsLoad;
        await store.ReadEntered.Task.WaitAsync(OperationLoadGuard);
        store.ReleaseReadsAfterNext();
        vm.SelectedOperation = initial;
        await vm.OperationItemsLoad.WaitAsync(OperationLoadGuard);
        await older.WaitAsync(OperationLoadGuard);

        Assert.Same(initial, vm.SelectedOperation);
        Assert.Equal(2, vm.OperationItems.Count);
        Assert.Equal(store.Inner.ReadHistory(initial.Plan).Items.Select(item => item.Origin),
            vm.OperationItems.Select(item => item.Item.Origin));
    }

    [Fact]
    public async Task OperationHistory_RefreshingListsTheOperationsOffTheWindowsThread()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        var plan = await PrepareReplayRegression(store.Inner, profile);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        await vm.OperationHistoryRefresh.WaitAsync(OperationLoadGuard);
        vm.SelectedOperationItem = vm.OperationItems[1];

        Task refresh;
        store.HoldLists();
        try
        {
            refresh = vm.RefreshOperationHistoryCommand.ExecuteAsync();
            await store.ListEntered.Task.WaitAsync(OperationLoadGuard);
            Assert.False(refresh.IsCompleted);
        }
        finally
        {
            store.ReleaseLists();
        }

        await refresh.WaitAsync(OperationLoadGuard);
        Assert.Equal(plan.Id, vm.SelectedOperation!.Plan.Id);
        Assert.Equal(1, vm.SelectedOperationItem?.Item.Index);
    }

    // The window lists the saved operations from its construction on. Initialization ends only after that read, so a
    // refresh asked for afterwards never overlaps it: without a UI thread to serialize them (as in these tests), two
    // overlapping reads both added their items, and the operation showed every item twice.
    [Fact]
    public async Task OperationHistory_InitializationWaitsForTheFirstReadSoALaterRefreshNeverOverlapsIt()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        await PrepareReplayRegression(store.Inner, profile);
        store.HoldLists();
        Task initialized;
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        try
        {
            await store.ListEntered.Task.WaitAsync(OperationLoadGuard);
            initialized = vm.InitializeAsync();
            // Everything else initialization does completes at once with these fakes; only the held list is left.
            Assert.NotSame(initialized, await Task.WhenAny(initialized, Task.Delay(TimeSpan.FromSeconds(2))));
        }
        finally
        {
            store.ReleaseLists();
        }

        await initialized.WaitAsync(OperationLoadGuard);
        Assert.Equal(2, vm.OperationItems.Count);
        await vm.RefreshOperationHistoryCommand.ExecuteAsync();

        Assert.Equal([0, 1], vm.OperationItems.Select(item => item.Item.Index));
    }

    // A refresh started after a resend or recovery (fire and forget) reads plan.json files off the window's thread.
    // Closing the window did not wait for it, so the read went on after shutdown; a test removing its folder then hit
    // "plan.json is being used by another process". Closing now waits for the outstanding list and items reads.
    [Fact]
    public async Task OperationHistory_ClosingTheWindowWaitsForAnOutstandingRead()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        await PrepareReplayRegression(store.Inner, profile);
        await PrepareReplayRegression(store.Inner, profile);
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        await vm.InitializeAsync();

        Task closed;
        store.HoldReads();
        try
        {
            // Selecting another operation reads its items without any command awaiting it.
            vm.SelectedOperation = vm.OperationHistory.Single(operation => operation != vm.SelectedOperation);
            await store.ReadEntered.Task.WaitAsync(OperationLoadGuard);
            closed = vm.DisposeAsync().AsTask();
            // The items are still being read: closing has not finished, whatever else it had to do.
            Assert.NotSame(closed, await Task.WhenAny(closed, Task.Delay(TimeSpan.FromSeconds(2))));
        }
        finally
        {
            store.ReleaseReads();
        }

        await closed.WaitAsync(OperationLoadGuard);
        Assert.True(vm.OperationHistoryRefresh.IsCompleted);
        Assert.True(vm.OperationItemsLoad.IsCompleted);
    }

    // A read superseded by another selection is still reading: closing waits for it too, not only for the latest one.
    [Fact]
    public async Task OperationHistory_ClosingWaitsForASupersededRead()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        await PrepareReplayRegression(store.Inner, profile);
        await PrepareReplayRegression(store.Inner, profile);
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), replayStore: store);
        await vm.InitializeAsync();

        Task closed;
        Task superseded;
        store.HoldReads();
        try
        {
            vm.SelectedOperation = vm.OperationHistory.Single(operation => operation != vm.SelectedOperation);
            await store.ReadEntered.Task.WaitAsync(OperationLoadGuard);
            superseded = vm.OperationItemsLoad;
            vm.SelectedOperation = null;
            Assert.NotSame(superseded, vm.OperationItemsLoad);
            closed = vm.DisposeAsync().AsTask();
            // The superseded read is still held: closing has not finished.
            Assert.NotSame(closed, await Task.WhenAny(closed, Task.Delay(TimeSpan.FromSeconds(2))));
            Assert.False(superseded.IsCompleted);
        }
        finally
        {
            store.ReleaseReads();
        }

        await closed.WaitAsync(OperationLoadGuard);
        Assert.True(superseded.IsCompleted);
    }

    // A recovery still running when closing starts refreshes the history in its finally. That refresh is refused before
    // it reads any plan file, and closing still waits for the recovery itself.
    [Fact]
    public async Task OperationHistory_ARefreshAfterClosingStartedReadsNothing()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new HeldReplayStore(new BatchReplayStore(directory.Path));
        await PrepareReplayRegression(store.Inner, profile);
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, replayStore: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.RefreshOperationHistoryCommand.ExecuteAsync();
        foreach (var item in vm.OperationItems) item.IsMarked = true;

        Task closed;
        int listsBeforeClosing;
        store.HoldRuns();
        try
        {
            _ = vm.ContinueOperationCommand.ExecuteAsync();
            await store.RunEntered.Task.WaitAsync(OperationLoadGuard);
            listsBeforeClosing = store.Lists;
            closed = vm.DisposeAsync().AsTask();
        }
        finally
        {
            store.ReleaseRuns();
        }

        await closed.WaitAsync(OperationLoadGuard);
        Assert.Equal(listsBeforeClosing, store.Lists);
    }

    /// <summary>The file store, with reads that can be held until released.</summary>
    private sealed class HeldReplayStore(BatchReplayStore inner) : IBatchReplayStore
    {
        private readonly ManualResetEventSlim _reads = new(true);
        private readonly ManualResetEventSlim _lists = new(true);
        private readonly ManualResetEventSlim _runs = new(true);
        private int _listCount;
        private int _releaseAfterNext;

        public BatchReplayStore Inner => inner;
        public TaskCompletionSource ReadEntered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ListEntered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void HoldReads() { ReadEntered = new(TaskCreationOptions.RunContinuationsAsynchronously); _reads.Reset(); }
        public void ReleaseReads() => _reads.Set();
        /// <summary>The next read completes at once, and then releases the held one, so the held one finishes last.</summary>
        public void ReleaseReadsAfterNext() => Interlocked.Exchange(ref _releaseAfterNext, 1);
        public void HoldLists() { ListEntered = new(TaskCreationOptions.RunContinuationsAsynchronously); _lists.Reset(); }
        public TaskCompletionSource RunEntered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void HoldRuns() { RunEntered = new(TaskCreationOptions.RunContinuationsAsynchronously); _runs.Reset(); }
        public void ReleaseRuns() => _runs.Set();
        /// <summary>How many times the operation list was read.</summary>
        public int Lists => Volatile.Read(ref _listCount);
        public void ReleaseLists() => _lists.Set();

        public string RootDirectory => inner.RootDirectory;

        public OperationHistory ReadHistory(ReplayPlan plan)
        {
            if (Interlocked.Exchange(ref _releaseAfterNext, 0) == 1)
            {
                var history = inner.ReadHistory(plan);
                _reads.Set();
                return history;
            }
            ReadEntered.TrySetResult();
            if (!_reads.Wait(OperationLoadGuard)) throw new TimeoutException("The held read was never released.");
            return inner.ReadHistory(plan);
        }

        public IReadOnlyList<ReplayPlan> List()
        {
            Interlocked.Increment(ref _listCount);
            ListEntered.TrySetResult();
            if (!_lists.Wait(OperationLoadGuard)) throw new TimeoutException("The held list was never released.");
            return inner.List();
        }

        public Task<ReplayPlan> CreateAsync(Guid profileId, ServiceBusEntityReference destination,
            IEnumerable<(MessageDraft Draft, string Origin)> drafts, bool preserveIds, int rate, CancellationToken token,
            string? fullyQualifiedNamespace = null, string? configurationIdentity = null, MessagingProvider? provider = null) =>
            inner.CreateAsync(profileId, destination, drafts, preserveIds, rate, token, fullyQualifiedNamespace, configurationIdentity, provider);

        public ReplayPlan? Latest(Guid profileId) => inner.Latest(profileId);

        public Task<ReplayPlan> CreateResendAsync(Guid profileId, IReadOnlyList<ResendItem> items, ResendMode mode, int rate, string? ns,
            string configurationIdentity, string kind, CancellationToken token, bool deferActivation = false, MessagingProvider? provider = null) =>
            inner.CreateResendAsync(profileId, items, mode, rate, ns, configurationIdentity, kind, token, deferActivation, provider);

        public Task ActivateScheduledAsync(ReplayPlan plan, CancellationToken token) => inner.ActivateScheduledAsync(plan, token);

        public Task<ResendResult> RunItemsAsync(ReplayPlan plan, IReadOnlyList<int> indexes, bool retryRejected,
            IServiceBusWorkspace workspace, Func<bool> canWrite, IProgress<ResendProgress>? progress, CancellationToken token)
        {
            RunEntered.TrySetResult();
            return Task.Run(async () =>
            {
                if (!_runs.Wait(OperationLoadGuard)) throw new TimeoutException("The held run was never released.");
                return await inner.RunItemsAsync(plan, indexes, retryRejected, workspace, canWrite, progress, CancellationToken.None);
            });
        }

        public Task<ReplayProgress> RunAsync(ReplayPlan plan, IServiceBusWorkspace workspace, Func<bool> canWrite,
            IProgress<ReplayProgress>? progress, CancellationToken token) => inner.RunAsync(plan, workspace, canWrite, progress, token);
    }
}
