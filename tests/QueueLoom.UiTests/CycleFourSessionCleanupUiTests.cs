using System.Reflection;
using Avalonia.Threading;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.UiTests;

public sealed class CycleFourSessionCleanupUiTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public Task SuccessfulBrowseWithCloseFailureShowsLockExpiryWarningWithoutSdkDetails(int closeFailures) => UiSession.RunAsync(async () =>
    {
        var workspace = new SessionWorkspace();
        await using var fixture = await WindowFixture.OpenWithAsync(workspace, new InMemorySecretVault(), DemoData.Development);
        var vm = fixture.ViewModel;
        await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        const string detail = "INTERNAL_RECEIVER_FAILURE; Password=cycle-four-synthetic-secret";
        var first = new Receiver(1, failsClose: true, closeDetail: detail);
        var second = new Receiver(2, failsClose: closeFailures == 2, closeDetail: detail);
        workspace.Client.Reset([first, second]);

        await vm.BrowseSelectedActiveCommand.ExecuteAsync();
        await fixture.SettleAsync();

        Assert.Equal(1, first.CloseAttempts);
        Assert.Equal(1, second.CloseAttempts);
        Assert.True(vm.HasError);
        Assert.False(vm.IsBusy);
        Assert.Contains("Some session locks may remain until they expire", vm.ErrorText, StringComparison.Ordinal);
        Assert.DoesNotContain("INTERNAL_RECEIVER_FAILURE", vm.ErrorText, StringComparison.Ordinal);
        Assert.DoesNotContain("cycle-four-synthetic-secret", vm.ErrorText, StringComparison.Ordinal);
    });

    [Fact]
    public Task CancelledSessionBrowseReleasesOtherSessionsAndCanBeRepeatedInTheWindow() => UiSession.RunAsync(async () =>
    {
        var workspace = new SessionWorkspace();
        await using var fixture = await WindowFixture.OpenWithAsync(workspace, new InMemorySecretVault(), DemoData.Development);
        var vm = fixture.ViewModel;
        await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        var dispatcherErrors = new List<Exception>();
        void Handle(object? sender, DispatcherUnhandledExceptionEventArgs e) { dispatcherErrors.Add(e.Exception); e.Handled = true; }
        Dispatcher.UIThread.UnhandledException += Handle;
        try
        {
            for (var cycle = 0; cycle < 2; cycle++)
            {
                var peekStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var first = new Receiver(1, failsClose: true);
                var interrupted = new Receiver(2, peekStarted: peekStarted);
                workspace.Client.Reset([first, interrupted]);
                vm.BrowseSelectedActiveCommand.Execute(null);
                await peekStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
                vm.CancelCurrentOperationCommand.Execute(null);
                await vm.BrowseSelectedActiveCommand.Completion.WaitAsync(TimeSpan.FromSeconds(10));
                await fixture.SettleAsync();
                Assert.Equal(1, first.CloseAttempts);
                Assert.Equal(1, interrupted.CloseAttempts);
                Assert.False(vm.IsBusy);
                Assert.False(vm.HasError);
                Assert.Contains("cancelled", vm.StatusText, StringComparison.OrdinalIgnoreCase);
                Assert.Empty(dispatcherErrors);
            }

            var retry = new Receiver(3);
            workspace.Client.Reset([retry]);
            await vm.BrowseSelectedActiveCommand.ExecuteAsync();
            await fixture.SettleAsync();
            Assert.Equal(3L, Assert.Single(vm.Messages).Message.SequenceNumber);
            Assert.Equal(1, retry.CloseAttempts);
            Assert.False(vm.HasError);
            Assert.Empty(dispatcherErrors);
        }
        finally { Dispatcher.UIThread.UnhandledException -= Handle; }
    });

    // Only connection/topology setup is synthetic. Browsing goes through the real Azure
    // workspace and virtual SDK receivers, without constructing a network transport.
    private sealed class SessionWorkspace : IServiceBusWorkspace
    {
        private readonly DemoWorkspace _connection = new();
        private readonly AzureServiceBusWorkspace _azure = new(new InMemorySecretVault());
        private readonly ServiceBusTopology _topology = new(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: true)]);
        public Client Client { get; } = new();
        public WorkspaceConnectionState ConnectionState => _connection.ConnectionState;
        public Guid? ConnectedProfileId => _connection.ConnectedProfileId;
        public string? ConnectedConfigurationIdentity => _connection.ConnectedConfigurationIdentity;
        public MessagingProvider? ConnectedProvider => MessagingProvider.AzureServiceBus;
        public async Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
            Set("_client", Client);
            Set("_cachedTopology", _topology);
            await _connection.ConnectAsync(profile, cancellationToken);
        }
        private void Set(string name, object value) => typeof(AzureServiceBusWorkspace)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(_azure, value);
        public Task DisconnectAsync(CancellationToken cancellationToken = default) => _connection.DisconnectAsync(cancellationToken);
        public Task SetAccessModeAsync(ProfileAccessMode mode, CancellationToken cancellationToken = default) => _connection.SetAccessModeAsync(mode, cancellationToken);
        public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) => Task.FromResult(_topology);
        public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(BrowseMessagesRequest request, CancellationToken cancellationToken = default) =>
            _azure.BrowseMessagesAsync(request, cancellationToken);
        public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(DeadLetterMonitorScope scope, CancellationToken cancellationToken = default) =>
            _connection.GetDeadLetterSnapshotAsync(scope, cancellationToken);
        public Task<DeadLetterSearchResult> SearchDeadLettersAsync(DeadLetterSearchRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(DeadLetterPurgeRequest request, CancellationToken cancellationToken = default,
            IProgress<DeadLetterPurgeProgress>? progress = null) => throw new NotSupportedException();
        public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(DeleteDeadLetterMessagesRequest request, CancellationToken cancellationToken = default,
            IProgress<DeadLetterMessageDeletionProgress>? progress = null) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => _azure.DisposeAsync();
    }

    private sealed class Client : ServiceBusClient
    {
        private Receiver[] _receivers = [];
        private int _next;
        public void Reset(Receiver[] receivers) { _receivers = receivers; _next = 0; }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_next == _receivers.Length)
                throw new ServiceBusException("No available sessions", ServiceBusFailureReason.ServiceTimeout);
            return Task.FromResult<ServiceBusSessionReceiver>(_receivers[_next++]);
        }
    }

    private sealed class Receiver(long sequence, bool failsClose = false, TaskCompletionSource? peekStarted = null,
        string closeDetail = "session close failed") : ServiceBusSessionReceiver
    {
        public int CloseAttempts { get; private set; }
        public override async Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default)
        {
            if (peekStarted is not null)
            {
                peekStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            return fromSequenceNumber > sequence ? [] :
                [ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("{}"), messageId: $"m-{sequence}", sequenceNumber: sequence)];
        }
        public override ValueTask DisposeAsync()
        {
            CloseAttempts++;
            return failsClose ? ValueTask.FromException(new IOException(closeDetail)) : ValueTask.CompletedTask;
        }
    }
}
