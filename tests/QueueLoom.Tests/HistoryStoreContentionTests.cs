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

    private static DeadLetterHistorySample Sample(ServiceBusProfile profile, int minutesAgo, long total) =>
        new(DateTimeOffset.UtcNow.AddMinutes(-minutesAgo), profile.Id, profile.Name, total, new Dictionary<string, long> { ["orders"] = total });

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
