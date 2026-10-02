using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

public sealed class StaticProviderShutdownRegressionTests
{
    [Theory]
    [InlineData("startup")]
    [InlineData("queue-settings")]
    [InlineData("rules")]
    public async Task RouterDisposal_DrainsForwardedOperationsBeforeDisposingProviders(string action)
    {
        var provider = DispatchProxy.Create<IServiceBusWorkspace, BlockingProvider>();
        var fake = (BlockingProvider)(object)provider;
        var router = new MultiProviderWorkspace(_ => provider);
        var profile = ViewModelStateTests.CreateProfile("Fake", EnvironmentKind.Test);
        if (action != "startup") await router.ConnectAsync(profile);
        fake.Block = true;
        Task operation = action switch
        {
            "startup" => router.ConnectAsync(profile),
            "rules" => router.GetTopicRulesAsync("events"),
            _ => router.GetQueueSettingsAsync("orders")
        };
        await fake.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var disposal = router.DisposeAsync().AsTask();
        var again = router.DisposeAsync().AsTask();
        var early = disposal.IsCompleted || again.IsCompleted;
        var closed = fake.DisposeCalls;
        fake.Release.TrySetResult();
        await Task.WhenAll(operation, disposal, again).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(early);
        Assert.Equal(0, closed);
        Assert.Equal(1, fake.DisposeCalls);
        Assert.False(fake.ClosedDuringCleanup);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => router.GetQueueSettingsAsync("orders"));
    }

    [Fact]
    public async Task SqsReadOnlyManagement_DrainsSdkCallBeforeClientDisposal()
    {
        var client = new BlockingSqs();
        var workspace = new AwsSqsSnsWorkspace(DispatchProxy.Create<ISecretVault, NullVault>());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(workspace, client);
        var read = workspace.GetQueueSettingsAsync("orders");
        await client.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var disposal = workspace.DisposeAsync().AsTask();
        var early = disposal.IsCompleted;
        var closed = client.Closed;
        client.Release.TrySetResult();
        await Task.WhenAll(read, disposal).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(early);
        Assert.False(closed);
        Assert.True(client.Closed);
        Assert.False(client.ClosedDuringRead);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => workspace.GetQueueSettingsAsync("orders"));
    }

    public class BlockingProvider : DispatchProxy
    {
        public bool Block { get; set; }
        public int DisposeCalls { get; private set; }
        public bool ClosedDuringCleanup { get; private set; }
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private async Task Wait()
        {
            if (!Block) return;
            Started.TrySetResult();
            await Release.Task;
            ClosedDuringCleanup = DisposeCalls != 0;
        }
        private async Task<QueueSettings> Settings() { await Wait(); return new QueueSettings(); }
        private async Task<IReadOnlyList<SubscriptionRules>> Rules() { await Wait(); return []; }
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method!.Name switch
        {
            "ConnectAsync" => Wait(),
            "GetQueueSettingsAsync" => Settings(),
            "GetTopicRulesAsync" => Rules(),
            "DisposeAsync" => Close(),
            "get_ConnectionState" => WorkspaceConnectionState.Connected,
            _ => throw new NotSupportedException(method.Name)
        };
        private ValueTask Close() { DisposeCalls++; return ValueTask.CompletedTask; }
    }

    public class NullVault : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? method, object?[]? args) => throw new NotSupportedException();
    }

    private sealed class BlockingSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public bool Closed { get; private set; }
        public bool ClosedDuringRead { get; private set; }
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueUrlResponse { QueueUrl = "https://fake.invalid/orders" });
        public override async Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request, CancellationToken cancellationToken = default)
        {
            Started.TrySetResult(); await Release.Task;
            ClosedDuringRead = Closed;
            return new GetQueueAttributesResponse { Attributes = new() };
        }
        protected override void Dispose(bool disposing) { Closed = true; base.Dispose(disposing); }
    }
}
