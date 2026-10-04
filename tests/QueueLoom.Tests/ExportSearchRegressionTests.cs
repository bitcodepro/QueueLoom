using System.Text;
using System.Text.RegularExpressions;
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

/// <summary>Export, search and decoding defects found in bug-hunt cycle 4.</summary>
public sealed class ExportSearchRegressionTests
{
    private static BrowsedMessage Message(string? correlationId = null, byte[]? body = null, long? originalBodySize = null) => new(
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, body ?? "body"u8.ToArray(),
        new EditableMessageProperties(MessageId: "m-1", CorrelationId: correlationId), originalBodySize: originalBodySize);

    // OWASP's CSV-injection list includes a leading TAB and CR: spreadsheets skip them and run the formula behind.
    [Theory]
    [InlineData("\t=HYPERLINK(\"http://evil\")")]
    [InlineData("\r=1+cmd|' /C calc'!A0")]
    public async Task Export_CsvDefusesFormulasBehindLeadingTabOrCarriageReturn(string value)
    {
        using var writer = new StringWriter();

        await MessageExport.WriteCsvAsync(writer, [new("Dev", Message(value))], CancellationToken.None);

        var expected = "\"'" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
        Assert.Contains(expected, writer.ToString(), StringComparison.Ordinal);
    }

    // A CSV row of a truncated body must not look like the complete message (the JSON export says bodyTruncated).
    [Fact]
    public async Task Export_CsvMarksTruncatedBodies()
    {
        using var writer = new StringWriter();
        var truncated = Message(body: "first part"u8.ToArray(), originalBodySize: 5_000_000);
        var complete = Message(body: "whole"u8.ToArray());

        await MessageExport.WriteCsvAsync(writer, [new("Dev", truncated), new("Dev", complete)], CancellationToken.None);

        var lines = writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        var header = lines[0].Split(',');
        var truncatedColumn = Array.IndexOf(header, "bodyTruncated");
        Assert.True(truncatedColumn >= 0, "The CSV has no bodyTruncated column: " + lines[0]);
        Assert.Equal("true", lines[1].Split(',')[truncatedColumn]);
        Assert.Equal("false", lines[2].Split(',')[truncatedColumn]);
        var sizeColumn = Array.IndexOf(header, "bodySize");
        Assert.True(sizeColumn >= 0, "The CSV has no bodySize column: " + lines[0]);
        Assert.Equal("5000000", lines[1].Split(',')[sizeColumn]);
    }

    // A catastrophic pattern that times out is not "no match": "needle" is in the body.
    private const string CatastrophicQuery = "/(a|aa)+$|needle/";
    private static readonly string CatastrophicBody = new string('a', 80) + "! needle";

    [Fact]
    public void Search_RegexTimeoutIsNotReportedAsNoMatch()
    {
        var query = MessageSearchQuery.Parse(CatastrophicQuery);
        var message = Message(body: Encoding.UTF8.GetBytes(CatastrophicBody));

        Assert.Throws<RegexMatchTimeoutException>(() => query.Matches(message));
    }

    // A value whose regular expression times out does not stop the next [*] value from matching, and a condition
    // that is certainly false decides its AND group even when another condition timed out.
    [Fact]
    public void Search_TimeoutOnOneWildcardValueStillChecksTheNextOne()
    {
        var query = MessageSearchQuery.Parse("$.items[*] =~ /(a|aa)+$|needle/");
        var body = System.Text.Json.JsonSerializer.Serialize(new { items = new[] { new string('a', 80) + "!", "needle" } });

        Assert.True(query.Matches(Message(body: Encoding.UTF8.GetBytes(body))));
    }

    [Fact]
    public void Search_ACertainlyFalseConditionDecidesItsGroupDespiteATimeout()
    {
        var query = MessageSearchQuery.Parse("$.text =~ /(a|aa)+$/ and $.region == 'EU'");
        var body = System.Text.Json.JsonSerializer.Serialize(new { text = new string('a', 80) + "!", region = "US" });

        Assert.False(query.Matches(Message(body: Encoding.UTF8.GetBytes(body))));
    }

    [Fact]
    public async Task Search_AzureSourceWithUndecidableMessagesIsIncomplete()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var target = new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 2);
        var request = new DeadLetterSearchRequest(CatastrophicQuery, [target], batchSize: 10, maximumMessagesPerTarget: 10);

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(
            target,
            request,
            (_, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from is null
                ? [Received(1, CatastrophicBody), Received(2, "a plain needle")]
                : []),
            message => AzureMessageMapper.FromAzure(message, source, ServiceBusSubQueue.DeadLetter),
            shouldStop: () => false,
            CancellationToken.None);

        // The scan goes on past the undecidable message, keeps what it found, and says it is not complete.
        Assert.Equal(2, result.ScannedMessageCount);
        Assert.Equal(2, Assert.Single(result.Matches).SequenceNumber);
        Assert.False(result.IsSuccessful, "A source whose search timed out on a message was reported as fully searched.");
        Assert.False(new DeadLetterSearchResult(Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, [result]).IsComplete);
    }

    [Fact]
    public async Task Search_LeasedSourceWithUndecidableMessagesIsIncomplete()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new SearchWorkspace(directory.Path, [CatastrophicBody, "a plain needle"]);
        await workspace.ConnectAsync(ServiceBusProfile.CreateNew("Isolated Rabbit", EnvironmentKind.Development,
            new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq });
        var target = new DeadLetterSearchTarget(SearchWorkspace.Source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 2);

        var result = await workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest(CatastrophicQuery, [target]));

        var source = Assert.Single(result.Sources);
        Assert.Equal(2, source.ScannedMessageCount);
        Assert.Equal("a plain needle", Encoding.UTF8.GetString(Assert.Single(result.Matches).Body.Span));
        Assert.False(source.IsSuccessful, "A source whose search timed out on a message was reported as fully searched.");
        Assert.False(result.IsComplete);
    }

    // The Azure search must look at the same property text the message list shows (ISO dates, base64 bytes).
    [Theory]
    [InlineData("2026-10-04T08:15", true)]
    [InlineData("/processedAt|2026-10-04T08/", true)]
    [InlineData("AQID", true)]
    [InlineData("System.Byte", false)]
    [InlineData("10/04/2026", false)]
    public async Task Search_AzurePropertiesAreSearchedAsShown(string query, bool expected)
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var target = new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 1);
        var request = new DeadLetterSearchRequest(query, [target], batchSize: 10, maximumMessagesPerTarget: 10);
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"),
            messageId: "m-1",
            sequenceNumber: 1,
            properties: new Dictionary<string, object>
            {
                ["when"] = new DateTime(2026, 10, 4, 8, 15, 0, DateTimeKind.Utc),
                ["bytes"] = new byte[] { 1, 2, 3 }
            });
        var shown = AzureMessageMapper.FromAzure(message, source, ServiceBusSubQueue.DeadLetter);
        Assert.Equal(expected, MessageSearchQuery.Parse(query).Matches(shown));

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(
            target,
            request,
            (_, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from is null ? [message] : []),
            received => AzureMessageMapper.FromAzure(received, source, ServiceBusSubQueue.DeadLetter),
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(expected, result.Matches.Count == 1);
    }

    private sealed class SearchWorkspace(string root, string[] bodies)
        : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)), null)
    {
        public static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");
        private readonly BrowsedMessage[] _messages = bodies.Select((body, index) => RabbitMqMessageMapper.FromAmqp(
            Encoding.UTF8.GetBytes(body), new BasicProperties { MessageId = $"m-{index}" }, "failed", Source,
            ServiceBusSubQueue.DeadLetter)).ToArray();
        public override MessagingProvider Provider => MessagingProvider.RabbitMq;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(
            new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source,
            ServiceBusSubQueue subQueue) => new Channel(_messages);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft message,
            CancellationToken token) => throw new NotSupportedException();

        private sealed class Channel(BrowsedMessage[] messages) : ILeasedMessageChannel
        {
            private int _next;
            public string PhysicalName => "orders-dlq";
            public int MaximumBatchSize => 10;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
            {
                var batch = messages.Skip(_next).Take(maxMessages).ToArray();
                _next += batch.Length;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(batch.Select(message =>
                    new LeasedMessage(message, message.Properties.MessageId!) { DeliveryIdentity = message.Properties.MessageId! }).ToArray());
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> released, CancellationToken token) => Task.CompletedTask;
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> settled, CancellationToken token) =>
                throw new NotSupportedException();
        }
    }

    private static ServiceBusReceivedMessage Received(long sequenceNumber, string body) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(body),
            messageId: $"message-{sequenceNumber}",
            sequenceNumber: sequenceNumber);
}
