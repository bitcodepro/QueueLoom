using System.Reflection;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("queue")]
    [InlineData("load-more")]
    [InlineData("routing")]
    [InlineData("startup")]
    public async Task StaticShutdown_WaitsForRealOperationCleanupAndRepeatedDisposal(string action)
    {
        var profile = CreateProfile("Fake", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = action == "routing" ? RoutingWorkspace() : new FakeWorkspace
        {
            QueueManagement = SqsLike,
            Topology = new(DateTimeOffset.UtcNow, [new("q", ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active)]),
            BrowseMessages = Enumerable.Range(1, 200).Select(i => new BrowsedMessage(ServiceBusEntityReference.Queue("q"),
                ServiceBusSubQueue.Active, i, new byte[] { 1 }, EditableMessageProperties.Empty)).ToArray()
        };
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task Block(CancellationToken token)
        {
            started.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
            finally { cleanup.TrySetResult(); await release.Task; }
        }
        var repository = new FakeProfileRepository([profile], profile.Id);
        var dialogs = new FakeDialogService { FillQueueDialog = d => d.Name = "new-queue" };
        var vm = CreateViewModel(repository, workspace, dialogs);
        Task operation;
        if (action == "startup") { repository.ListGate = Block; operation = vm.InitializeAsync(); }
        else
        {
            await vm.InitializeAsync();
            await vm.ConnectCommand.ExecuteAsync();
            if (action == "queue") { workspace.CleanupOperationGate = Block; operation = vm.CreateQueueCommand.ExecuteAsync(); }
            else if (action == "load-more")
            {
                vm.SelectedEntity = vm.Entities.First(e => e.IsQueue);
                await vm.BrowseSelectedActiveCommand.ExecuteAsync();
                Assert.True(vm.LoadMoreMessagesCommand.CanExecute(null));
                workspace.CleanupOperationGate = Block;
                operation = vm.LoadMoreMessagesCommand.ExecuteAsync();
            }
            else
            {
                vm.SelectedEntity = vm.Entities.First(e => e.IsTopic);
                dialogs.RoutingGate = Block;
                operation = vm.OpenTopicRoutingCommand.ExecuteAsync();
            }
        }
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var disposal = vm.DisposeAsync().AsTask();
        var again = vm.DisposeAsync().AsTask();
        bool earlyDisposal;
        int closed;
        try
        {
            await cleanup.Task.WaitAsync(TimeSpan.FromSeconds(5));
            earlyDisposal = disposal.IsCompleted || again.IsCompleted;
            closed = workspace.DisposeCalls;
        }
        finally { release.TrySetResult(); }
        await Task.WhenAll(operation, disposal, again).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(earlyDisposal, "Shutdown returned while cancellation-resistant cleanup was blocked.");
        Assert.Equal(0, closed);
        Assert.Equal(1, workspace.DisposeCalls);
        Assert.DoesNotContain(vm.Activity, a => a.Details.Contains("disposed", StringComparison.OrdinalIgnoreCase));
    }
}

public sealed class StaticLeasedShutdownRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disposal_DrainsBrowseReleaseAndStartupBeforeClosingClients(bool startup)
    {
        using var directory = new TemporaryDirectory();
        var workspace = new BlockingWorkspace(new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = ViewModelStateTests.CreateProfile("Fake", EnvironmentKind.Test) with { Provider = MessagingProvider.AmazonSqsSns };
        Task operation;
        if (startup) { workspace.BlockOpen = true; operation = workspace.ConnectAsync(profile); }
        else
        {
            await workspace.ConnectAsync(profile);
            operation = workspace.BrowseMessagesAsync(new(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.Active, maxMessages: 1));
        }
        await workspace.Blocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var before = workspace.CloseCalls;
        var disposal = workspace.DisposeAsync().AsTask();
        var again = workspace.DisposeAsync().AsTask();
        var early = disposal.IsCompleted || again.IsCompleted;
        var closed = workspace.CloseCalls;
        workspace.Release.TrySetResult();
        await Task.WhenAll(operation, disposal, again).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(early, "DisposeAsync returned before open/release cleanup finished.");
        Assert.Equal(before, closed);
        Assert.Equal(before + 1, workspace.CloseCalls);
        Assert.False(workspace.ClientClosedDuringCleanup);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => workspace.GetTopologyAsync());
    }

    private sealed class BlockingWorkspace(DeadLetterJsonBackupStore store) : LeasedMessagingWorkspace(store, null), ILeasedMessageChannel
    {
        public override MessagingProvider Provider => MessagingProvider.AmazonSqsSns;
        public bool BlockOpen { get; set; }
        public int CloseCalls { get; private set; }
        public bool ClientClosedDuringCleanup { get; private set; }
        public TaskCompletionSource Blocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private async Task Block()
        {
            var before = CloseCalls;
            Blocked.TrySetResult(); await Release.Task;
            ClientClosedDuringCleanup = CloseCalls != before;
        }
        protected override async Task OpenAsync(ServiceBusProfile profile, CancellationToken token) { if (BlockOpen) await Block(); }
        protected override ValueTask CloseAsync() { CloseCalls++; return ValueTask.CompletedTask; }
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) => this;
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft draft, CancellationToken token) => Task.CompletedTask;
        public string PhysicalName => "q";
        public int MaximumBatchSize => 1;
        public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token) => Task.FromResult<IReadOnlyList<LeasedMessage>>(
            [new(new(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.Active, 1, new byte[] { 1 }, EditableMessageProperties.Empty), "lease")]);
        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token) => Block();
        public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token) => Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
    }
}
