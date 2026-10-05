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
        var time = new ManualTime();
        using var client = new SchemaRegistryClient("http://registry.test", null, null, handler, time);

        Assert.Null(await client.GetAsync(5, CancellationToken.None));
        time.Now += SchemaRegistryClient.FailureBackoff;
        var schema = await client.GetAsync(5, CancellationToken.None);

        Assert.NotNull(schema);
        Assert.Equal(2, handler.Calls);
    }

    // A sustained outage: a page of 100 records sharing one schema id asks the registry once, not 100 times.
    [Fact]
    public async Task SchemaRegistry_SustainedFailure_IsNotRetriedForEveryRecord()
    {
        var handler = new SequenceHandler(Enumerable.Range(0, 200).Select(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)).ToArray());
        using var client = new SchemaRegistryClient("http://registry.test", null, null, handler, new ManualTime());

        for (var record = 0; record < 100; record++)
        {
            Assert.Null(await client.GetAsync(5, CancellationToken.None));
        }

        Assert.Equal(1, handler.Calls);
    }

    // An unreachable registry (timeout, connection refused) is skipped for every id until the back-off ends, then recovers.
    [Fact]
    public async Task SchemaRegistry_UnreachableRegistry_IsSkippedForAllIdsThenRecovers()
    {
        var handler = new ThrowingThenOkHandler(failures: 1);
        var time = new ManualTime();
        using var client = new SchemaRegistryClient("http://registry.test", null, null, handler, time);

        for (var id = 1; id <= 50; id++)
        {
            Assert.Null(await client.GetAsync(id, CancellationToken.None));
        }
        Assert.Equal(1, handler.Calls);

        time.Now += SchemaRegistryClient.FailureBackoff;
        Assert.NotNull(await client.GetAsync(7, CancellationToken.None));
        Assert.Equal(2, handler.Calls);
    }

    private sealed class ManualTime : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private sealed class ThrowingThenOkHandler(int failures) : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            if (Calls <= failures) throw new HttpRequestException("connection refused");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""{"schema":"\"string\""}""", Encoding.UTF8, "application/json")
            });
        }
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
