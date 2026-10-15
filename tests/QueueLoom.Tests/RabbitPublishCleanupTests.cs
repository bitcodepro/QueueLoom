using System.Net;
using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace QueueLoom.Tests;

// Exercise public sends and resend accounting through the real adapter, with no network or timing synchronization.
public sealed class RabbitPublishCleanupTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CleanupFailureCannotReplaceAConfirmedPublish(bool failingWarningListener)
    {
        var broker = new Publisher { CleanupError = new TimeoutException("channel close timed out") };
        await using var workspace = broker.CreateWorkspace();
        var warnings = new List<string>();
        workspace.CleanupWarning += (_, warning) =>
        {
            warnings.Add(warning);
            if (failingWarningListener) throw new InvalidOperationException("listener failed");
        };
        var draft = new MessageDraft(new EditableMessageBody("AP8B", MessageBodyFormat.Base64),
            new EditableMessageProperties(MessageId: "m-1", Subject: "orders.created", AmqpContentEncoding: "gzip", AmqpPriority: 7),
            [new("binary", ApplicationPropertyType.Binary, "AP8=")]);

        var error = await Record.ExceptionAsync(() => workspace.SendMessageAsync(new(ServiceBusEntityReference.Topic("events"), draft)));

        Assert.Null(error);
        Assert.Contains("cleanup", Assert.Single(warnings), StringComparison.OrdinalIgnoreCase);
        var published = Assert.Single(broker.Published);
        Assert.Equal("events", published.Exchange);
        Assert.Equal("orders.created", published.RoutingKey);
        Assert.Equal(new byte[] { 0, 255, 1 }, published.Body);
        Assert.Equal("m-1", published.Properties.MessageId);
        Assert.Equal("gzip", published.Properties.ContentEncoding);
        Assert.Equal(7, published.Properties.Priority);
        Assert.Equal(new byte[] { 0, 255 }, Assert.IsType<byte[]>(published.Properties.Headers!["binary"]));
        Assert.Equal(1, broker.Disposals);
    }

    [Theory]
    [InlineData("return")]
    [InlineData("nack")]
    [InlineData("timeout")]
    [InlineData("cancel")]
    public async Task CleanupFailureCannotMaskTheOriginalPublishOutcome(string outcome)
    {
        Exception original = outcome switch
        {
            "return" => new PublishException(1, true),
            "nack" => new PublishException(1, false),
            "timeout" => new TimeoutException("publish acknowledgement lost"),
            _ => new OperationCanceledException("publish cancelled")
        };
        var broker = new Publisher { PublishError = _ => original, CleanupError = new IOException("channel cleanup failed") };
        await using var workspace = broker.CreateWorkspace();
        var warnings = new List<string>();
        workspace.CleanupWarning += (_, warning) => warnings.Add(warning);

        var error = await Record.ExceptionAsync(() => workspace.SendMessageAsync(new(ServiceBusEntityReference.Queue("orders"), MessageDraft.Empty)));

        if (outcome == "return") Assert.Same(original, Assert.IsType<DeliveryRejectedException>(error).InnerException);
        else Assert.Same(original, error);
        Assert.Single(warnings);
        Assert.Single(broker.Published);
        Assert.Equal(1, broker.Disposals);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CleanupWarningKeepsUsefulSanitizedDetailAndThePublishOutcome(bool rejected)
    {
        var secret = Guid.NewGuid().ToString("N");
        var refusal = new PublishException(1, true);
        var broker = new Publisher
        {
            PublishError = _ => rejected ? refusal : null,
            CleanupError = new IOException("cleanup wrapper",
                new TimeoutException($"channel close timed out; password={secret}\r\nConnection reset"))
        };
        await using var workspace = broker.CreateWorkspace();
        var warnings = new List<string>();
        workspace.CleanupWarning += (_, warning) => warnings.Add(warning);

        var error = await Record.ExceptionAsync(() => workspace.SendMessageAsync(new(ServiceBusEntityReference.Queue("orders"), MessageDraft.Empty)));

        // Boolean assertions keep synthetic credential material out of failure output as well.
        Assert.True(rejected ? error is DeliveryRejectedException && ReferenceEquals(error.InnerException, refusal) : error is null);
        Assert.Single(warnings.Select(_ => true));
        var warning = warnings[0];
        Assert.True(warning.Contains("channel close timed out", StringComparison.Ordinal));
        Assert.True(warning.Contains("Connection reset", StringComparison.Ordinal));
        Assert.True(warning.Contains("password=[REDACTED]", StringComparison.Ordinal));
        Assert.False(warning.Contains(secret, StringComparison.Ordinal));
        Assert.False(warning.Contains('\r') || warning.Contains('\n'));
        Assert.Equal(1, broker.Disposals);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PartialBatchKeepsConfirmedCountWhenCleanupFails(bool cancelSecondPublish)
    {
        using var cancellation = new CancellationTokenSource();
        var broker = new Publisher
        {
            CleanupError = new TimeoutException("channel cleanup timed out"),
            PublishError = index =>
            {
                if (index != 1) return null;
                if (!cancelSecondPublish) return new PublishException(2, true);
                cancellation.Cancel();
                return new OperationCanceledException(cancellation.Token);
            }
        };
        await using var workspace = broker.CreateWorkspace();
        var items = Enumerable.Range(0, 3).Select(index =>
        {
            var original = new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.Active,
                index, "body"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{index}"));
            return new ResendItem(original, ServiceBusEntityReference.Queue("orders"), original.CreateDraft());
        }).ToArray();

        var result = await DeadLetterResender.ResendAsync(workspace, items, ResendMode.Copy, cancellationToken: cancellation.Token);

        Assert.Equal(cancelSecondPublish ? 1 : 2, result.SentCount);
        Assert.Equal(1, result.FailedCount);
        Assert.Equal(cancelSecondPublish ? 1 : 0, result.CancelledCount);
        Assert.Equal(ResendOutcome.Sent, result.Items[0].Outcome);
        Assert.Equal(cancelSecondPublish ? 2 : 3, broker.Published.Count);
        Assert.Equal(broker.Published.Count, broker.Disposals);
        Assert.All(broker.Published, published => { Assert.Equal("", published.Exchange); Assert.Equal("orders", published.RoutingKey); });
    }

    private sealed class Publisher
    {
        public Func<int, Exception?> PublishError { get; init; } = _ => null;
        public Exception? CleanupError { get; init; }
        public List<(string Exchange, string RoutingKey, byte[] Body, IReadOnlyBasicProperties Properties)> Published { get; } = [];
        public int Disposals { get; private set; }

        public RabbitMqWorkspace CreateWorkspace()
        {
            var connection = Proxy<IConnection>((method, _) =>
            {
                if (method.Name != "CreateChannelAsync") return Done(method.ReturnType);
                var channel = Proxy<IChannel>((call, args) =>
                {
                    if (call.Name == "BasicPublishAsync")
                    {
                        Assert.True((bool)args![2]!);
                        var index = Published.Count;
                        Published.Add(((string)args[0]!, (string)args[1]!, ((ReadOnlyMemory<byte>)args[4]!).ToArray(), (IReadOnlyBasicProperties)args[3]!));
                        var error = PublishError(index);
                        return error is null ? ValueTask.CompletedTask : new ValueTask(Task.FromException(error));
                    }
                    if (call.Name == "DisposeAsync")
                    {
                        Disposals++;
                        return CleanupError is null ? ValueTask.CompletedTask : new ValueTask(Task.FromException(CleanupError));
                    }
                    return Done(call.ReturnType);
                });
                return Task.FromResult(channel);
            });
            var workspace = new RabbitMqWorkspace(new EmptyVault());
            typeof(RabbitMqWorkspace).GetField("_connection", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, connection);
            typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
                new HttpClient(new EmptyTopology()) { BaseAddress = new Uri("https://isolated.invalid/") });
            var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword),
                accessMode: ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.RabbitMq };
            typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, profile);
            return workspace;
        }
    }

    private sealed class EmptyTopology : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("[]") });
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
