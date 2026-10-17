using System.Text;
using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// An export could be read in a spreadsheet or another tool but not sent again: moving dead letters to another
// environment, or keeping a test set, meant retyping them. A JSON export can now be replayed to any destination.
public sealed class MessageImportTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    private static BrowsedMessage[] Samples() =>
    [
        new(Orders, ServiceBusSubQueue.DeadLetter, 3, Encoding.UTF8.GetBytes("{\"orderId\":42}"),
            new EditableMessageProperties(MessageId: "m-1", CorrelationId: "c-1", ContentType: "application/json", Subject: "order",
                To: "billing", ReplyTo: "replies", SessionId: "s-1", ReplyToSessionId: "rs-1", PartitionKey: "s-1",
                TimeToLive: TimeSpan.FromMinutes(90)),
            [
                new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "acme"),
                new MessageApplicationProperty("amount", ApplicationPropertyType.Int64, "42"),
                // A repeated name and a service type label survive only in the typed list.
                new MessageApplicationProperty("amount", ApplicationPropertyType.String, "42") { WireType = "Number.custom" }
            ],
            deadLetterReason: "MaxDeliveryCountExceeded"),
        new(Orders, ServiceBusSubQueue.DeadLetter, 4, new byte[] { 0x00, 0xff, 0x10 },
            new EditableMessageProperties(MessageId: "m-2", AmqpType: "binary", AmqpAppId: "app", AmqpContentEncoding: "gzip",
                AmqpPriority: 7)),
        new(ServiceBusEntityReference.Queue("events.DLT"), ServiceBusSubQueue.Active, 5, Encoding.UTF8.GetBytes("payload"),
            new EditableMessageProperties(MessageId: "k-1"))
        {
            KafkaEnvelope = new KafkaEnvelope(new byte[] { 1, 2 }, false,
                [new KafkaRawHeader("opaque", new byte[] { 0xff }), new KafkaRawHeader("opaque", null)],
                new EditableMessageProperties(MessageId: "k-1"), [])
        }
    ];

    private static async Task<MemoryStream> Export(IEnumerable<BrowsedMessage> messages)
    {
        var stream = new MemoryStream();
        await MessageExport.WriteJsonAsync(stream, messages.Select(message => new ExportedMessage("Staging", message)).ToArray(), default);
        stream.Position = 0;
        return stream;
    }

    [Fact]
    public async Task AJsonExportReadsBackToTheDraftsTheMessagesWouldHaveGiven()
    {
        var samples = Samples();
        using var file = await Export(samples);

        var imported = await MessageImport.ReadAsync(file);

        Assert.Equal(samples.Length, imported.Count);
        foreach (var (sample, message) in samples.Zip(imported))
        {
            var expected = sample.CreateDraft();
            Assert.Equal(expected.Body.GetBytes(), message.Draft.Body.GetBytes());
            Assert.Equal(expected.Body.Format, message.Draft.Body.Format);
            Assert.Equal(expected.Properties, message.Draft.Properties);
            Assert.Equal(expected.ApplicationProperties, message.Draft.ApplicationProperties);
            Assert.Equal(expected.ApplicationProperties.Select(property => property.WireType),
                message.Draft.ApplicationProperties.Select(property => property.WireType));
            Assert.Equal(expected.KafkaEnvelope?.Key, message.Draft.KafkaEnvelope?.Key);
            Assert.Equal(expected.KafkaEnvelope?.IsTombstone, message.Draft.KafkaEnvelope?.IsTombstone);
            Assert.Equal(expected.KafkaEnvelope?.Headers.Select(header => (header.Name, header.Value)),
                message.Draft.KafkaEnvelope?.Headers.Select(header => (header.Name, header.Value)));
            Assert.StartsWith("Staging / ", message.Origin, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("truncated", "was cut when it was exported")]
    [InlineData("newer", "newer QueueLoom")]
    [InlineData("base64", "Message 1")]
    [InlineData("encoding", "body encoding 'hex' is unknown")]
    [InlineData("object", "should be a list of messages")]
    [InlineData("empty", "holds no messages")]
    [InlineData("json", "not a QueueLoom JSON export")]
    public async Task AFileThatCannotBeSentAsItWasIsRefusedWithTheReason(string shape, string expected)
    {
        using var file = await Export([Samples()[0]]);
        var node = JsonNode.Parse(Encoding.UTF8.GetString(file.ToArray()))!;
        var first = node[0]!.AsObject();
        string text = shape switch
        {
            "truncated" => Set(first, "bodyTruncated", JsonValue.Create(true)),
            "newer" => Set(first, "exportVersion", JsonValue.Create(MessageExport.ExportVersion + 1)),
            "base64" => Set(Set2(first, "bodyEncoding", "base64"), "body", JsonValue.Create("not base64!")),
            "encoding" => Set(first, "bodyEncoding", JsonValue.Create("hex")),
            "object" => "{}",
            "empty" => "[]",
            _ => "{ not json"
        };
        string Set(JsonObject target, string name, JsonNode? value) { target[name] = value; return node.ToJsonString(); }
        JsonObject Set2(JsonObject target, string name, string value) { target[name] = value; return target; }

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            MessageImport.ReadAsync(new MemoryStream(Encoding.UTF8.GetBytes(text))));

        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MoreMessagesThanAReplaySendsAreRefused()
    {
        using var file = await Export(Enumerable.Range(0, MessageImport.MaximumMessages + 1).Select(index =>
            new BrowsedMessage(Orders, ServiceBusSubQueue.DeadLetter, index, "x"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{index}"))));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => MessageImport.ReadAsync(file));

        Assert.Contains($"at most {MessageImport.MaximumMessages:N0}", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACsvExportIsRefusedBecauseItLeavesOutTypes()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "export.csv");
        await File.WriteAllTextAsync(path, "environment,source\n");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => MessageImport.ReadAsync(path));

        Assert.Contains("JSON export", error.Message, StringComparison.Ordinal);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ReplayExport_SendsTheMessagesOfAJsonExportToTheDestination()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "export.json");
        var source = ServiceBusEntityReference.Queue("orders");
        await MessageExport.WriteAsync(path,
        [
            new ExportedMessage("Other", new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1, "{\"a\":1}"u8.ToArray(),
                new EditableMessageProperties(MessageId: "m-1", Subject: "first"))),
            new ExportedMessage("Other", new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 2, "{\"a\":2}"u8.ToArray(),
                new EditableMessageProperties(MessageId: "m-2", Subject: "second")))
        ]);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UnixEpoch,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true, OpenFilePath = path },
            replayStore: new BatchReplayStore(Path.Combine(directory.Path, "replay")));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        vm.ReplayRate = 50;
        vm.PreserveReplayMessageIds = true;

        await vm.ReplayExportFileCommand.ExecuteAsync();

        Assert.True(workspace.SentMessages.Count == 2, vm.StatusText + " / " + vm.ReplayStatus + " / " + vm.ErrorText);
        Assert.Equal(["first", "second"], workspace.SentMessages.Select(send => send.Message.Properties.Subject));
        Assert.All(workspace.SentMessages, send => Assert.Equal("target", send.Destination.Name));
        Assert.Contains("2/2 acknowledged", vm.ReplayStatus, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayExport_IsNotOfferedWithoutWriteAccess()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        using var directory = new TemporaryDirectory();
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UnixEpoch,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            replayStore: new BatchReplayStore(directory.Path));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);

        Assert.False(vm.ReplayExportFileCommand.CanExecute(null));
    }
}
