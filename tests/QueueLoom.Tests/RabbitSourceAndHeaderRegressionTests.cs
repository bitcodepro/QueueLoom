using System.Reflection;
using System.Text;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class RabbitSourceAndHeaderRegressionTests
{
    [Theory]
    [InlineData("deleted")]
    [InlineData("reconfigured")]
    [InlineData("shared")]
    public async Task SourcePurge_LeavesForeignAndUnattributedMessages(string topologyState)
    {
        using var directory = new TemporaryDirectory();
        var broker = new SourceBroker();
        var queues = new List<RabbitQueueInfo>
        {
            new("orders", "classic", 0, 0, "dlx", "failed", null, "running"),
            new("dead-letters", "classic", 5, 0, null, null, null, "running")
        };
        if (topologyState != "deleted")
            queues.Add(new("payments", "classic", 0, 0, topologyState == "shared" ? "dlx" : null, "failed", null, "running"));
        var index = new RabbitMqTopologyIndex(queues, [new("dlx", "direct")], [new("dlx", "dead-letters", "failed")]);
        await using var owner = new RabbitMqWorkspace(new EmptyVault());
        Set(owner, "_connection", Proxy<IConnection>(broker.Connection));
        Set(owner, "_index", index);
        var channel = (ILeasedMessageChannel)typeof(RabbitMqWorkspace)
            .GetMethod("OpenChannel", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(owner,
                [index.ToTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter])!;
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword));
        var session = new DeadLetterJsonBackupSession(directory.Path, profile, DateTimeOffset.UtcNow);
        var result = await (Task<DeadLetterPurgeSourceResult>)typeof(LeasedMessagingWorkspace)
            .GetMethod("PurgeTargetAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(owner,
                [new DeadLetterPurgeTarget(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter),
                    channel, 1, 20, session, 1, 1, null, CancellationToken.None])!;

        Assert.True(result.IsSuccessful, result.Error);
        Assert.Equal(1, result.DeletedCount);
        Assert.Equal(["own"], broker.Acknowledged);
        Assert.Equal(["foreign", "missing-queue", "older-own", "unattributed"], broker.Remaining.Order(StringComparer.Ordinal));
        Assert.Single(Directory.EnumerateFiles(directory.Path, "*.json", SearchOption.AllDirectories));
        Assert.Empty(broker.Held);
    }

    [Theory]
    [InlineData("/w==", ApplicationPropertyType.Binary)]
    [InlineData("wyg=", ApplicationPropertyType.Binary)]
    [InlineData("8ICA", ApplicationPropertyType.Binary)]
    [InlineData("aGVsbG8=", ApplicationPropertyType.String)]
    [InlineData("0J/RgNC40LLQtdGC", ApplicationPropertyType.String)]
    [InlineData("", ApplicationPropertyType.String)]
    public async Task Headers_RoundTripUnchangedThroughDraftAndBackup(string base64, ApplicationPropertyType expectedType)
    {
        using var directory = new TemporaryDirectory();
        var bytes = Convert.FromBase64String(base64);
        var message = RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
            new BasicProperties { Headers = new Dictionary<string, object?> { ["payload"] = bytes, ["plain"] = "ordinary text" } },
            "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);
        Assert.Equal(bytes, Assert.IsType<byte[]>(RabbitMqMessageMapper.ToAmqp(message.CreateDraft()).Headers!["payload"]));
        Assert.Equal(expectedType, message.ApplicationProperties.Single(property => property.Name == "payload").Type);
        Assert.Equal("ordinary text"u8.ToArray(), Assert.IsType<byte[]>(RabbitMqMessageMapper.ToAmqp(message.CreateDraft()).Headers!["plain"]));

        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword))
            with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, CancellationToken.None);
        await session.BackupAsync(message, CancellationToken.None);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        Assert.Equal(bytes, Assert.IsType<byte[]>(RabbitMqMessageMapper.ToAmqp(restored.CreateDraft()).Headers!["payload"]));
        Assert.Equal("ordinary text"u8.ToArray(), Assert.IsType<byte[]>(RabbitMqMessageMapper.ToAmqp(restored.CreateDraft()).Headers!["plain"]));
    }

    private static void Set(object owner, string field, object value) =>
        owner.GetType().GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);

    private static T Proxy<T>(Func<MethodInfo, object?[]?, object?> call) where T : class
    {
        var proxy = DispatchProxy.Create<T, RabbitLifecycleAuditTests.InterfaceProxy>();
        ((RabbitLifecycleAuditTests.InterfaceProxy)(object)proxy).Call = call;
        return proxy;
    }

    private static object? Done(Type type) => type == typeof(void) ? null : type == typeof(Task) ? Task.CompletedTask
        : type.IsValueType ? Activator.CreateInstance(type) : null;

    private sealed class SourceBroker
    {
        private readonly Queue<BasicProperties> _pending = new(new[]
        {
            Properties("own", "orders"), Properties("foreign", "payments"), Properties("unattributed", null),
            Properties("missing-queue", ""), Properties("older-own", "payments", "orders")
        });
        public Dictionary<ulong, BasicProperties> Held { get; } = [];
        public List<string> Acknowledged { get; } = [];
        public IEnumerable<string> Remaining => _pending.Select(properties => properties.MessageId!);

        private static BasicProperties Properties(string id, string? newest, string? older = null)
        {
            var properties = new BasicProperties { MessageId = id };
            if (newest is not null)
            {
                var deaths = new List<object?> { new Dictionary<string, object?> { ["queue"] = Encoding.UTF8.GetBytes(newest) } };
                if (older is not null) deaths.Add(new Dictionary<string, object?> { ["queue"] = Encoding.UTF8.GetBytes(older) });
                properties.Headers = new Dictionary<string, object?> { ["x-death"] = deaths };
            }
            return properties;
        }

        public object? Connection(MethodInfo method, object?[]? args)
        {
            if (method.Name != "CreateChannelAsync") return Done(method.ReturnType);
            ulong next = 0;
            return Task.FromResult(Proxy<IChannel>((method, args) =>
            {
                if (method.Name == "BasicGetAsync")
                {
                    if (!_pending.TryDequeue(out var properties)) return Task.FromResult<BasicGetResult?>(null);
                    Held.Add(++next, properties);
                    return Task.FromResult<BasicGetResult?>(new BasicGetResult(next, false, "dlx", "failed", 0, properties, "fixture"u8.ToArray()));
                }
                if (method.Name is "BasicAckAsync" or "BasicNackAsync" && Held.Remove((ulong)args![0]!, out var settled))
                {
                    if (method.Name == "BasicAckAsync") Acknowledged.Add(settled.MessageId!);
                    else _pending.Enqueue(settled);
                }
                if (method.Name is "CloseAsync" or "Dispose" or "DisposeAsync")
                {
                    foreach (var properties in Held.Values) _pending.Enqueue(properties);
                    Held.Clear();
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
