using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using System.Reflection;
using Azure.Messaging.ServiceBus;
using ServiceBusMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(ServiceBusMessageState.Scheduled)]
    [InlineData(ServiceBusMessageState.Deferred)]
    public async Task Audit_PendingRemovalStopsAtWriteExpiryAndReportsCompletedResults(ServiceBusMessageState state)
    {
        var profile = CreateProfile("Temporary writes", EnvironmentKind.Development, ProfileAccessMode.ReadOnly);
        var source = ServiceBusEntityReference.Queue("orders");
        var messages = Enumerable.Range(1, 3).Select(i => new BrowsedMessage(source, ServiceBusSubQueue.Active, i,
            "pending"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{i}"), state: state)).ToArray();
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 3)))]),
            BrowseMessages = messages
        };
        var clock = new ExpiryClock();
        using var directory = new TemporaryDirectory();
        var client = new PendingExpiryClient(state, () => clock.Advance(TimeSpan.FromMinutes(11)));
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(), clock,
            new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure,
            profile with { AccessMode = ProfileAccessMode.ReadWrite });
        typeof(AzureServiceBusWorkspace).GetField("_cachedTopology", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, workspace.Topology);
        RemovePendingMessagesResult? reported = null;
        workspace.PendingRemoval = async (batch, token) => reported = await azure.RemovePendingMessagesAsync(batch, token);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true });
        vm.Clock = clock;
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        await vm.BrowseSelectedActiveCommand.ExecuteAsync();
        vm.AreAllMessagesMarked = true;

        await vm.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal([1L], client.Touched);
        Assert.NotNull(reported);
        Assert.Equal(1, reported.RemovedCount);
        Assert.Equal(2, reported.CancelledCount);
        Assert.Equal([2L, 3L], vm.Messages.Select(m => m.SequenceNumber));
        var report = Assert.Single(vm.Activity, a => a.Action == "Removal of scheduled or deferred messages cancelled");
        Assert.Contains("1 of 3", report.Details, StringComparison.Ordinal);
        Assert.Contains("2 not processed (cancelled)", report.Details, StringComparison.Ordinal);
        Assert.Single(Directory.GetFiles(reported.BackupDirectory, "*.json", SearchOption.AllDirectories),
            p => Path.GetFileName(p) != "session.json");
    }

    private sealed class PendingExpiryClient(ServiceBusMessageState state, Action expire) : ServiceBusClient
    {
        public List<long> Touched { get; } = [];
        public override ServiceBusReceiver CreateReceiver(string queueName) => new PendingExpiryReceiver(this, state, expire);
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => CreateReceiver(queueName);
        public override ServiceBusSender CreateSender(string queueOrTopicName) => new PendingExpirySender(this, expire);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class PendingExpiryReceiver(PendingExpiryClient client, ServiceBusMessageState state, Action expire) : ServiceBusReceiver
    {
        private ServiceBusReceivedMessage Message(long number) => ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("pending"), messageId: $"m-{number}", sequenceNumber: number,
            serviceBusMessageState: state == ServiceBusMessageState.Scheduled
                ? Azure.Messaging.ServiceBus.ServiceBusMessageState.Scheduled : Azure.Messaging.ServiceBus.ServiceBusMessageState.Deferred);
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Message(fromSequenceNumber!.Value));
        }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveDeferredMessagesAsync(IEnumerable<long> sequenceNumbers,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(sequenceNumbers.Select(Message).ToArray());
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            client.Touched.Add(message.SequenceNumber); expire(); return Task.CompletedTask;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class PendingExpirySender(PendingExpiryClient client, Action expire) : ServiceBusSender
    {
        public override Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            client.Touched.Add(sequenceNumber); expire(); return Task.CompletedTask;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ExpiryClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        private readonly List<ExpiryTimer> _timers = [];
        public override DateTimeOffset GetUtcNow() => _now;
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ExpiryTimer(this, callback, state);
            timer.Change(dueTime, period);
            _timers.Add(timer);
            return timer;
        }
        public void Advance(TimeSpan elapsed)
        {
            _now += elapsed;
            foreach (var timer in _timers.ToArray()) timer.Fire();
        }
        private sealed class ExpiryTimer(ExpiryClock clock, TimerCallback callback, object? state) : ITimer
        {
            private DateTimeOffset? _at;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                _at = dueTime == Timeout.InfiniteTimeSpan ? null : clock._now + dueTime;
                return true;
            }
            public void Fire()
            {
                if (_at is { } at && at <= clock._now) { _at = null; callback(state); }
            }
            public void Dispose() => _at = null;
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }
}
