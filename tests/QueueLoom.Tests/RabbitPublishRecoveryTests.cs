using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace QueueLoom.Tests;

// Exercises the actual adapter over a protocol-interface fake; broker behavior is covered by emulator tests.
public sealed class RabbitPublishRecoveryTests
{
    [Theory]
    [InlineData("return")]
    [InlineData("nack")]
    [InlineData("timeout")]
    [InlineData("cancel")]
    public async Task OnlyExplicitMandatoryReturnProvesNonDelivery(string failure)
    {
        Exception transport = failure switch
        {
            "return" => new PublishException(1, true),
            "nack" => new PublishException(1, false),
            "timeout" => new TimeoutException("Acknowledgement was lost"),
            _ => new OperationCanceledException("Cancelled while awaiting acknowledgement")
        };
        var publishCalls = 0;
        var channel = Proxy<IChannel>((method, args) =>
        {
            if (method.Name == "BasicPublishAsync")
            {
                publishCalls++;
                Assert.True((bool)args![2]!);
                return new ValueTask(Task.FromException(transport));
            }
            return Done(method.ReturnType);
        });
        var connection = Proxy<IConnection>((method, _) => method.Name == "CreateChannelAsync"
            ? Task.FromResult(channel) : Done(method.ReturnType));
        await using var owner = new RabbitMqWorkspace(new EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_connection", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, connection);
        var send = typeof(RabbitMqWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var error = await Record.ExceptionAsync(() => (Task)send.Invoke(owner,
            [new ServiceBusTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Topic("events"),
                new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json), new EditableMessageProperties(Subject: "unbound")), CancellationToken.None])!);
        Assert.Equal(1, publishCalls);
        if (failure == "return")
        {
            var rejected = Assert.IsType<DeliveryRejectedException>(error);
            Assert.Same(transport, rejected.InnerException);
        }
        else
        {
            Assert.Same(transport, error);
            Assert.IsNotType<DeliveryRejectedException>(error);
        }
        // Feed the actual adapter exception into the older durable replay executor.
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test) with { Provider = MessagingProvider.RabbitMq };
        var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(profile);
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Topic("events"),
            [(MessageDraft.Empty, "adapter replay")], false, 50, default,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        workspace.OnSend = () => throw error!;
        Assert.Same(error, await Record.ExceptionAsync(() => store.RunAsync(plan, workspace, () => true, null, default)));
        var reopened = new BatchReplayStore(directory.Path);
        Assert.Equal(failure == "return" ? "Rejected" : "Uncertain", Assert.Single(reopened.ReadHistory(plan).Items).State);
        workspace.OnSend = null;
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, workspace, () => true, null, default));
        if (failure == "return")
        {
            await reopened.RunItemsAsync(plan, [0], true, workspace, () => true, null, default);
            Assert.Equal(workspace.SentMessages[0].Message.Properties.MessageId, workspace.SentMessages[1].Message.Properties.MessageId);
        }
        else
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
            Assert.Single(workspace.SentMessages);
        }
    }

    public class ProtocolProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Call { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Call(method!, args);
    }
    private static T Proxy<T>(Func<MethodInfo, object?[]?, object?> call) where T : class
    {
        var proxy = DispatchProxy.Create<T, ProtocolProxy>();
        ((ProtocolProxy)(object)proxy).Call = call;
        return proxy;
    }
    private static object? Done(Type type) => type == typeof(void) ? null : type == typeof(Task) ? Task.CompletedTask :
        type == typeof(ValueTask) ? ValueTask.CompletedTask : type.IsValueType ? Activator.CreateInstance(type) : null;
    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
