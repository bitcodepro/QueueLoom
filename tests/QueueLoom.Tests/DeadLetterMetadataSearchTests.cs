using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class DeadLetterMetadataSearchTests
{
    private const string Needle = "metadata-42";
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");

    public static IEnumerable<object[]> AzureCases() => Cases(
        nameof(EditableMessageProperties.PartitionKey),
        nameof(EditableMessageProperties.TransactionPartitionKey),
        nameof(EditableMessageProperties.ReplyToSessionId));

    public static IEnumerable<object[]> RabbitCases() => Cases(
        nameof(EditableMessageProperties.AmqpType),
        nameof(EditableMessageProperties.AmqpAppId),
        nameof(EditableMessageProperties.AmqpContentEncoding));

    private static IEnumerable<object[]> Cases(params string[] fields)
    {
        foreach (var field in fields)
        foreach (var regex in new[] { false, true })
        foreach (var expected in new[] { false, true })
            yield return [field, regex, expected];
    }

    private static string Query(bool regex, bool expected) => regex
        ? expected ? "/^METADATA-[0-9]+$/i" : "/^absent-[0-9]+$/i"
        : expected ? "METADATA-42" : "absent-93";

    [Theory]
    [MemberData(nameof(AzureCases))]
    public void AzureMappedMessagesSearchRetainedRoutingIdentifiers(string field, bool regex, bool expected)
    {
        var message = AzureMessageMapper.FromAzure(AzureMessage(field), Source, ServiceBusSubQueue.DeadLetter);
        Assert.Equal(Needle, typeof(EditableMessageProperties).GetProperty(field)!.GetValue(message.Properties));

        Assert.Equal(expected, DeadLetterSearchMatcher.IsMatch(message, Query(regex, expected)));
    }

    [Theory]
    [MemberData(nameof(AzureCases))]
    public async Task AzureFullBodySearchReturnsMessagesByTheirRoutingIdentifiers(string field, bool regex, bool expected)
    {
        var message = AzureMessage(field);
        var target = new DeadLetterSearchTarget(Source, ServiceBusSubQueue.DeadLetter, 1);
        var request = new DeadLetterSearchRequest(Query(regex, expected), [target]);

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(target, request,
            (_, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from is null ? [message] : []),
            received => AzureMessageMapper.FromAzure(received, Source, ServiceBusSubQueue.DeadLetter),
            shouldStop: () => false, CancellationToken.None);

        Assert.True(result.IsSuccessful);
        Assert.Equal(1, result.ScannedMessageCount);
        Assert.Equal(expected ? 1 : 0, result.Matches.Count);
    }

    [Theory]
    [MemberData(nameof(RabbitCases))]
    public async Task RabbitSearchReturnsMessagesByTheirBasicProperties(string field, bool regex, bool expected)
    {
        var properties = new BasicProperties
        {
            MessageId = "message-1",
            Type = field == nameof(EditableMessageProperties.AmqpType) ? Needle : null,
            AppId = field == nameof(EditableMessageProperties.AmqpAppId) ? Needle : null,
            ContentEncoding = field == nameof(EditableMessageProperties.AmqpContentEncoding) ? Needle : null
        };
        var message = RabbitMqMessageMapper.FromAmqp("{}"u8.ToArray(), properties, "failed", Source, ServiceBusSubQueue.DeadLetter);
        Assert.Equal(Needle, typeof(EditableMessageProperties).GetProperty(field)!.GetValue(message.Properties));
        using var directory = new TemporaryDirectory();
        await using var workspace = new RabbitSearchWorkspace(directory.Path, message);
        await workspace.ConnectAsync(ServiceBusProfile.CreateNew("Isolated Rabbit", EnvironmentKind.Development,
            new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq });

        var result = await workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest(Query(regex, expected),
            [new DeadLetterSearchTarget(Source, ServiceBusSubQueue.DeadLetter, 1)]));

        Assert.True(result.IsComplete);
        Assert.Equal(1, result.ScannedMessageCount);
        Assert.Equal(expected ? 1 : 0, result.Matches.Count);
        Assert.Equal(1, workspace.ReleasedCount);
    }

    [Fact]
    public void JsonConditionsStillCheckTheBodyRatherThanRoutingMetadata()
    {
        var received = AzureMessage(nameof(EditableMessageProperties.PartitionKey));
        var mapped = AzureMessageMapper.FromAzure(received, Source, ServiceBusSubQueue.DeadLetter);
        var query = MessageSearchQuery.Parse("$.partitionKey == 'metadata-42'");

        Assert.False(query.Matches(mapped));
        Assert.False(AzureServiceBusWorkspace.MatchesSearch(received, query));
    }

    private static ServiceBusReceivedMessage AzureMessage(string field) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("{}"), messageId: "message-1", sequenceNumber: 1,
        partitionKey: field == nameof(EditableMessageProperties.PartitionKey) ? Needle : null,
        viaPartitionKey: field == nameof(EditableMessageProperties.TransactionPartitionKey) ? Needle : null,
        replyToSessionId: field == nameof(EditableMessageProperties.ReplyToSessionId) ? Needle : null);

    private sealed class RabbitSearchWorkspace(string root, BrowsedMessage message)
        : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)), null)
    {
        public override MessagingProvider Provider => MessagingProvider.RabbitMq;
        public int ReleasedCount { get; private set; }
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(
            new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source,
            ServiceBusSubQueue subQueue) => new Channel(this, message);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft draft,
            CancellationToken token) => throw new NotSupportedException();

        private sealed class Channel(RabbitSearchWorkspace owner, BrowsedMessage message) : ILeasedMessageChannel
        {
            private bool _received;
            public string PhysicalName => "orders-dlq";
            public int MaximumBatchSize => 10;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
            {
                if (_received) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
                _received = true;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>([new(message, "receipt")]);
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                owner.ReleasedCount += messages.Count;
                return Task.CompletedTask;
            }
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages,
                CancellationToken token) => throw new InvalidOperationException("A search must release, never settle messages.");
        }
    }
}
