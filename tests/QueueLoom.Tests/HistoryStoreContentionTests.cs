using QueueLoom.App.Models;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

// The dead-letter history is one file shared by every QueueLoom window and the MCP server, under a cross-process
// lock. The window recorded a sample after every scan and monitor check, and read the history for the Monitors page,
// by waiting for that lock on its own thread: while another process held it (up to 30 seconds), the window froze.
// These tests hold the lock as another process would and never wait on elapsed time.
public sealed partial class ViewModelStateTests
{
    private static readonly TimeSpan HistoryDeadlockGuard = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task History_AnotherProcessHoldingTheFileDoesNotFreezeAScan()
    {
        using var directory = new Infrastructure.TemporaryDirectory();
        var (profile, workspace) = HistoryEnvironment();
        var file = Path.Combine(directory.Path, "dlq-history.jsonl");
        var store = new JsonLinesDeadLetterHistoryStore(file);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        Task pending;
        await using (new FileStream(file + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            // The scan came back to this thread while the history file was still held: nothing waited on it here.
            pending = viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
            Assert.False(pending.IsCompleted, "The scan finished while the history file was still held.");
        }

        await pending.WaitAsync(HistoryDeadlockGuard);
        Assert.Equal(4, Assert.Single(await store.ReadAsync(profile.Id, DateTimeOffset.MinValue)).Total);
    }

    [Fact]
    public async Task History_AnotherProcessHoldingTheFileDoesNotFreezeTheMonitorsPage()
    {
        using var directory = new Infrastructure.TemporaryDirectory();
        var (profile, workspace) = HistoryEnvironment();
        var file = Path.Combine(directory.Path, "dlq-history.jsonl");
        var store = new JsonLinesDeadLetterHistoryStore(file);
        await store.AppendAsync(new DeadLetterHistorySample(DateTimeOffset.UtcNow.AddMinutes(-5), profile.Id, profile.Name, 7,
            new Dictionary<string, long> { ["orders"] = 7 }));
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
        await viewModel.InitializeAsync();

        await using (new FileStream(file + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
            Assert.False(viewModel.HistoryRefresh.IsCompleted, "The history was read while its file was still held.");
            Assert.False(viewModel.HasHistory);
        }

        await viewModel.HistoryRefresh.WaitAsync(HistoryDeadlockGuard);
        Assert.True(viewModel.HasHistory);
        Assert.Equal("7", viewModel.HistoryNowText);
    }

    // Reads finish in any order; the page shows the one asked for last, never an older one that finished after it.
    [Fact]
    public async Task History_AnOlderReadFinishingLastDoesNotReplaceTheNewerOne()
    {
        var (profile, workspace) = HistoryEnvironment();
        var store = new GatedHistoryStore();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
        await viewModel.InitializeAsync();

        viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
        var first = viewModel.HistoryRefresh;
        viewModel.HistoryRange = "Last 6 hours";
        var second = viewModel.HistoryRefresh;
        Assert.Equal(2, store.Reads.Count);

        store.Reads[1].SetResult([Sample(profile, minutesAgo: 1, total: 2)]);
        await second.WaitAsync(HistoryDeadlockGuard);
        Assert.Equal("2", viewModel.HistoryNowText);

        store.Reads[0].SetResult([Sample(profile, minutesAgo: 1, total: 9)]);
        await first.WaitAsync(HistoryDeadlockGuard);
        Assert.Equal("2", viewModel.HistoryNowText);
    }

    // While the new selection's history is read, nothing of the previous selection is shown under it: not its count, graph
    // or queues. The page says it is reading, then shows the new result, an empty one included.
    [Theory]
    [InlineData("environment")]
    [InlineData("range")]
    public async Task History_ANewSelectionNeverShowsThePreviousSelectionsSummary(string change)
    {
        var (development, workspace) = HistoryEnvironment();
        var production = CreateProfile("Production orders", EnvironmentKind.Production);
        var store = new SerializedGatedHistoryStore();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, production], development.Id), workspace,
            history: store);
        await viewModel.InitializeAsync();
        viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
        var shown = viewModel.HistoryProfile!;
        store.Reads[0].Gate.SetResult([Sample(shown.Profile, minutesAgo: 1, total: 7, source: "only-in-the-first")]);
        await viewModel.HistoryRefresh.WaitAsync(HistoryDeadlockGuard);
        Assert.Equal("7", viewModel.HistoryNowText);
        Assert.Equal("only-in-the-first", Assert.Single(viewModel.HistorySources).Name);

        if (change == "environment") viewModel.HistoryProfile = viewModel.Profiles.Single(profile => profile.Id != shown.Id);
        else viewModel.HistoryRange = "Last 6 hours";

        Assert.True(viewModel.IsHistoryLoading);
        Assert.False(viewModel.HasHistory);
        Assert.Empty(viewModel.HistoryPoints);
        Assert.Empty(viewModel.HistorySources);
        Assert.Equal("—", viewModel.HistoryNowText);
        Assert.Equal("—", viewModel.HistoryPeakText);
        Assert.StartsWith("Reading the dead-letter history of " + viewModel.HistoryProfile!.Name, viewModel.HistoryEmptyText,
            StringComparison.Ordinal);

        if (change == "environment")
        {
            store.Reads[1].Gate.SetResult([]);
            await viewModel.HistoryRefresh.WaitAsync(HistoryDeadlockGuard);
            Assert.False(viewModel.HasHistory);
            Assert.StartsWith($"No checks of {viewModel.HistoryProfile.Name} in this period.", viewModel.HistoryEmptyText, StringComparison.Ordinal);
        }
        else
        {
            store.Reads[1].Gate.SetResult([Sample(shown.Profile, minutesAgo: 1, total: 3)]);
            await viewModel.HistoryRefresh.WaitAsync(HistoryDeadlockGuard);
            Assert.Equal("3", viewModel.HistoryNowText);
        }
        Assert.False(viewModel.IsHistoryLoading);
    }

    // The store serializes reads and appends, as the file store does. A read superseded while it waits is cancelled: it no
    // longer holds up the latest read, nor the next sample a scan or monitor check appends.
    [Fact]
    public async Task History_ASupersededReadIsCancelledAndHoldsUpNeitherTheLatestReadNorAnAppend()
    {
        var (profile, workspace) = HistoryEnvironment();
        var store = new SerializedGatedHistoryStore();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
        await viewModel.InitializeAsync();

        viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
        await store.Reads[0].Entered.Task.WaitAsync(HistoryDeadlockGuard);
        viewModel.HistoryRange = "Last 6 hours";
        viewModel.HistoryRange = "Last 7 days";
        Assert.Equal(3, store.Reads.Count);

        // The first held the store and the second waited for it. Both were given up, so the latest now has the store.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Reads[0].Task.WaitAsync(HistoryDeadlockGuard));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Reads[1].Task.WaitAsync(HistoryDeadlockGuard));
        await store.Reads[2].Entered.Task.WaitAsync(HistoryDeadlockGuard);

        store.Reads[2].Gate.SetResult([Sample(profile, minutesAgo: 1, total: 5)]);
        await viewModel.HistoryRefresh.WaitAsync(HistoryDeadlockGuard);
        Assert.Equal("5", viewModel.HistoryNowText);
        await store.AppendAsync(Sample(profile, minutesAgo: 0, total: 6)).WaitAsync(HistoryDeadlockGuard);
    }

    [Fact]
    public async Task History_ClosingTheWindowCancelsAnOutstandingRead()
    {
        var (profile, workspace) = HistoryEnvironment();
        var store = new SerializedGatedHistoryStore();
        var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
        await viewModel.InitializeAsync();
        viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
        await store.Reads[0].Entered.Task.WaitAsync(HistoryDeadlockGuard);

        await viewModel.DisposeAsync().AsTask().WaitAsync(HistoryDeadlockGuard);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.Reads[0].Task.WaitAsync(HistoryDeadlockGuard));
        Assert.True(viewModel.HistoryRefresh.IsCompletedSuccessfully);
    }

    private static (ServiceBusProfile Profile, FakeWorkspace Workspace) HistoryEnvironment()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development);
        var orders = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 4)))])
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(orders, 4)]);
        return (profile, workspace);
    }

    private static DeadLetterHistorySample Sample(ServiceBusProfile profile, int minutesAgo, long total, string source = "orders") =>
        new(DateTimeOffset.UtcNow.AddMinutes(-minutesAgo), profile.Id, profile.Name, total, new Dictionary<string, long> { [source] = total });

    /// <summary>Serializes reads and appends like the file store; each read waits for its gate and honours cancellation.</summary>
    private sealed class SerializedGatedHistoryStore : IDeadLetterHistoryStore
    {
        private readonly SemaphoreSlim _gate = new(1, 1);

        public List<GatedRead> Reads { get; } = [];

        public async Task AppendAsync(DeadLetterHistorySample sample, CancellationToken cancellationToken = default)
        {
            await _gate.WaitAsync(cancellationToken);
            _gate.Release();
        }

        public Task<IReadOnlyList<DeadLetterHistorySample>> ReadAsync(Guid profileId, DateTimeOffset since,
            CancellationToken cancellationToken = default)
        {
            var read = new GatedRead();
            lock (Reads) Reads.Add(read);
            read.Task = ReadCoreAsync(read, cancellationToken);
            return read.Task;
        }

        private async Task<IReadOnlyList<DeadLetterHistorySample>> ReadCoreAsync(GatedRead read, CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken);
            try
            {
                read.Entered.TrySetResult();
                return await read.Gate.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                _gate.Release();
            }
        }
    }

    private sealed class GatedRead
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<IReadOnlyList<DeadLetterHistorySample>> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<IReadOnlyList<DeadLetterHistorySample>> Task { get; set; } = null!;
    }

    private sealed class GatedHistoryStore : IDeadLetterHistoryStore
    {
        public List<TaskCompletionSource<IReadOnlyList<DeadLetterHistorySample>>> Reads { get; } = [];

        public Task AppendAsync(DeadLetterHistorySample sample, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<IReadOnlyList<DeadLetterHistorySample>> ReadAsync(Guid profileId, DateTimeOffset since,
            CancellationToken cancellationToken = default)
        {
            var read = new TaskCompletionSource<IReadOnlyList<DeadLetterHistorySample>>(TaskCreationOptions.RunContinuationsAsynchronously);
            Reads.Add(read);
            return read.Task;
        }
    }
}
