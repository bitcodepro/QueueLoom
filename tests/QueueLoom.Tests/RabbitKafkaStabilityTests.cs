using System.Net;
using System.Reflection;
using System.Text;
using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class RabbitKafkaStabilityTests
{
    [Fact]
    public async Task SchemaRegistry_TransientFailure_IsNotCachedForever()
    {
        var handler = new SequenceHandler(
            new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"schema":"\"string\""}""", Encoding.UTF8, "application/json")
            });
        using var client = new SchemaRegistryClient("http://registry.test", null, null, handler);

        Assert.Null(await client.GetAsync(5, CancellationToken.None));
        var schema = await client.GetAsync(5, CancellationToken.None);

        Assert.NotNull(schema);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task SchemaRegistry_NotFound_IsCached()
    {
        var handler = new SequenceHandler(new HttpResponseMessage(HttpStatusCode.NotFound), new HttpResponseMessage(HttpStatusCode.NotFound));
        using var client = new SchemaRegistryClient("http://registry.test", null, null, handler);

        Assert.Null(await client.GetAsync(9, CancellationToken.None));
        Assert.Null(await client.GetAsync(9, CancellationToken.None));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task RabbitClose_UnexpectedExceptionFromBroker_IsSwallowedAndEverythingIsReleased()
    {
        var workspace = new RabbitMqWorkspace(new NullVault());
        var disposed = false;
        var connection = DispatchProxy.Create<IConnection, CallProxy>();
        ((CallProxy)(object)connection).Call = (method, _) =>
        {
            if (method.Name == "CloseAsync") throw new TimeoutException("close timed out");
            if (method.Name == "Dispose") disposed = true;
            return method.ReturnType == typeof(Task) ? Task.CompletedTask
                : method.ReturnType.IsValueType && method.ReturnType != typeof(void) ? Activator.CreateInstance(method.ReturnType) : null;
        };
        var flags = BindingFlags.Instance | BindingFlags.NonPublic;
        typeof(RabbitMqWorkspace).GetField("_connection", flags)!.SetValue(workspace, connection);
        var management = new HttpClient();
        typeof(RabbitMqWorkspace).GetField("_management", flags)!.SetValue(workspace, management);

        var close = (ValueTask)typeof(RabbitMqWorkspace).GetMethod("CloseAsync", flags)!.Invoke(workspace, null)!;
        await close;

        Assert.True(disposed);
        Assert.Null(typeof(RabbitMqWorkspace).GetField("_connection", flags)!.GetValue(workspace));
        Assert.Null(typeof(RabbitMqWorkspace).GetField("_management", flags)!.GetValue(workspace));
        Assert.Throws<ObjectDisposedException>(() => { management.GetAsync("http://x.test").GetAwaiter().GetResult(); });
    }

    public class CallProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Call { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Call(method!, args);
    }

    private sealed class SequenceHandler(params HttpResponseMessage[] responses) : HttpMessageHandler
    {
        public int Calls { get; private set; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(responses[Math.Min(Calls++, responses.Length - 1)]);
    }

    private sealed class NullVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
