using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

// Runs the actual RabbitChannel and shared purge workflow over an AMQP interface fake.
// This verifies application lifecycle logic, not broker protocol behavior.
public sealed class RabbitLifecycleAuditTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("duplicate-application-id")]
    public async Task Purge_DeletesDistinctDeliveriesWhenTagsRestartOrApplicationIdsRepeat(string? id)
    {
        using var directory = new TemporaryDirectory();
        var fake = new BrokerFake(id, messages: 2);
        var (owner, channel) = Create(fake);
        await using var lifetime = owner;
        var result = await Purge(owner, channel, directory.Path);
        Assert.Equal(2, result.DeletedCount);
        Assert.Equal(0, fake.Pending);
        Assert.Equal(0, fake.OpenChannels);
        Assert.Equal(0, fake.Unacked);
    }

    [Fact]
    public async Task EmptyRead_ClosesChannelAtEndOfOperation()
    {
        var fake = new BrokerFake(null, 0);
        var (owner, channel) = Create(fake);
        await using var lifetime = owner;
        var held = new List<LeasedMessage>();
        await LeasedMessagingWorkspace.ReceiveUpToAsync(channel, 1, held, CancellationToken.None);
        await Release(channel, held);
        Assert.Equal(0, fake.OpenChannels);
    }

    [Theory]
    [InlineData("backup")]
    [InlineData("mapping")]
    [InlineData("cancellation")]
    public async Task FailedOperation_ReturnsAllAcquiredUnackedDeliveries(string failure)
    {
        using var directory = new TemporaryDirectory();
        var fake = new BrokerFake(null, 1) { MappingFailure = failure == "mapping", CancelAfterAcquire = failure == "cancellation" };
        var (owner, channel) = Create(fake);
        await using var lifetime = owner;
        if (failure == "backup")
        {
            var blocked = Path.Combine(directory.Path, "blocked");
            File.WriteAllText(blocked, "fixture");
            Assert.False((await Purge(owner, channel, blocked)).IsSuccessful);
        }
        else
        {
            var held = new List<LeasedMessage>();
            try
            {
                var error = await Assert.ThrowsAnyAsync<Exception>(() => LeasedMessagingWorkspace.ReceiveUpToAsync(channel, 2, held, fake.Cancellation.Token));
                if (failure == "mapping") Assert.IsType<FormatException>(error);
                else Assert.IsAssignableFrom<OperationCanceledException>(error);
            }
            finally { await Release(channel, held); }
        }
        Assert.Equal(0, fake.Unacked);
        Assert.Equal(0, fake.OpenChannels);
        Assert.Equal(1, fake.Pending);
        Assert.Equal(1, fake.Acquired);
    }

    [Fact]
    public async Task Purge_CloseFailurePreservesAcknowledgedCountAndReportsCleanupFailure()
    {
        using var directory = new TemporaryDirectory();
        var fake = new BrokerFake(null, 1) { CloseFailure = true };
        var (owner, channel) = Create(fake);
        await using var lifetime = owner;
        DeadLetterPurgeSourceResult? result = null;
        var error = await Record.ExceptionAsync(async () => result = await Purge(owner, channel, directory.Path));
        Assert.Null(error);
        Assert.Equal(1, result!.DeletedCount);
        Assert.Contains("cleanup", result.Error, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, fake.OpenChannels);
        Assert.Equal(0, fake.Unacked);
        Assert.Equal(0, fake.Pending);
    }
    private static async Task<DeadLetterPurgeSourceResult> Purge(RabbitMqWorkspace owner, ILeasedMessageChannel channel, string root)
    {
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword));
        var session = new DeadLetterJsonBackupSession(root, profile, DateTimeOffset.UtcNow);
        return await (Task<DeadLetterPurgeSourceResult>)typeof(LeasedMessagingWorkspace)
            .GetMethod("PurgeTargetAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(owner,
            [new DeadLetterPurgeTarget(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter), channel, 1, 10, session, 1, 1, null, CancellationToken.None])!;
    }
    private static Task Release(ILeasedMessageChannel channel, List<LeasedMessage> held) =>
        (Task)typeof(LeasedMessagingWorkspace).GetMethod("ReleaseQuietlyAsync", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [channel, held])!;
    private static (RabbitMqWorkspace, ILeasedMessageChannel) Create(BrokerFake fake)
    {
        var owner = new RabbitMqWorkspace(new EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_connection", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner,
            Proxy<IConnection>(fake.Connection));
        var type = typeof(RabbitMqWorkspace).GetNestedType("RabbitChannel", BindingFlags.NonPublic)!;
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null, [owner, "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, null], null)!;
        return (owner, channel);
    }

    public class InterfaceProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Call { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Call(method!, args);
    }
    private static T Proxy<T>(Func<MethodInfo,object?[]?,object?> call) where T : class
    {
        var proxy = DispatchProxy.Create<T, InterfaceProxy>();
        ((InterfaceProxy)(object)proxy).Call = call;
        return proxy;
    }
    private static object? Done(Type type) => type == typeof(void) ? null : type == typeof(Task) ? Task.CompletedTask : type.IsValueType ? Activator.CreateInstance(type) : null;

    private sealed class BrokerFake(string? id, int messages)
    {
        public int Pending { get; private set; } = messages;
        public int Unacked { get; private set; }
        public int Acquired { get; private set; }
        public int OpenChannels { get; private set; }
        public bool CloseFailure { get; init; }
        public bool MappingFailure { get; init; }
        public bool CancelAfterAcquire { get; init; }
        public CancellationTokenSource Cancellation { get; } = new();
        public object? Connection(MethodInfo method, object?[]? args)
        {
            if (method.Name != "CreateChannelAsync") return Done(method.ReturnType);
            OpenChannels++;
            ulong tag = 0;
            var held = new HashSet<ulong>();
            bool closed = false;
            return Task.FromResult(Proxy<IChannel>((method, args) =>
            {
                if (method.Name == "BasicGetAsync")
                {
                    ((CancellationToken)args![2]!).ThrowIfCancellationRequested();
                    if (Pending == 0) return Task.FromResult<BasicGetResult?>(null);
                    Pending--; Unacked++; Acquired++; held.Add(++tag);
                    if (CancelAfterAcquire) Cancellation.Cancel();
                    IReadOnlyBasicProperties properties = new BasicProperties { MessageId = id };
                    if (MappingFailure) properties = Proxy<IReadOnlyBasicProperties>((m,a) => throw new FormatException("isolated mapping failure"));
                    return Task.FromResult<BasicGetResult?>(new BasicGetResult(tag, false, "", "", 0, properties, new byte[] { 1 }));
                }
                if (method.Name is "BasicAckAsync" or "BasicNackAsync")
                {
                    if (held.Remove((ulong)args![0]!)) { Unacked--; if (method.Name == "BasicNackAsync") Pending++; }
                }
                if (method.Name == "CloseAsync" && CloseFailure) throw new IOException("isolated close failure");
                if (method.Name is "CloseAsync" or "Dispose" or "DisposeAsync" && !closed)
                {
                    if (!closed) { closed = true; OpenChannels--; Pending += held.Count; Unacked -= held.Count; held.Clear(); }
                }
                return Done(method.ReturnType);
            }));
        }
    }
    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
