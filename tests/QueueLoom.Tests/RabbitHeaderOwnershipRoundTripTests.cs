using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class RabbitHeaderOwnershipRoundTripTests
{
    [Theory]
    [InlineData(false, "4.2.0", false)]
    [InlineData(false, "4.3.0", false)]
    [InlineData(true, "4.2.0", false)]
    [InlineData(true, "4.3.0", false)]
    [InlineData(false, "4.2.0", true)]
    [InlineData(false, "4.3.0", true)]
    [InlineData(true, "4.2.0", true)]
    [InlineData(true, "4.3.0", true)]
    public async Task CopyAndBackupRemoveOnlyEstablishedBrokerHeaders(bool quorum, string version, bool backup)
    {
        var owned = RabbitMqWorkspace.BrokerOwnedHeaders(quorum, Version.Parse(version));
        var headers = new Dictionary<string, object?>
        {
            ["x-death-note"] = "invoice"u8.ToArray(),
            ["x-delivery-counter"] = (short)7,
            ["x-first-death-custom"] = new BinaryTableValue([0xff, 0x00]),
            ["x-last-death-custom"] = new List<object?> { (byte)200, null },
            ["x-delivery-count"] = 3L,
            ["x-acquired-count"] = 4L,
            ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["queue"] = "orders"u8.ToArray(), ["reason"] = "rejected"u8.ToArray() } }
        };
        string[] deathNames = ["x-first-death-queue", "x-first-death-reason", "x-first-death-exchange",
            "x-last-death-queue", "x-last-death-reason", "x-last-death-exchange"];
        foreach (var name in deathNames) headers[name] = "broker"u8.ToArray();
        var message = RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
            new BasicProperties { MessageId = "ownership", Headers = headers }, "orders",
            ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, owned);

        using var directory = new TemporaryDirectory();
        if (backup)
        {
            var paths = QueueLoomPaths.ForRoot(directory.Path);
            var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development,
                new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq };
            var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
            await session.BackupAsync(message, default);
            var repository = new JsonDeadLetterBackupRepository(paths);
            message = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
            Assert.Equal(owned.Order(StringComparer.Ordinal), message.BrokerOwnedHeaders.Order(StringComparer.Ordinal));
        }

        // Browsing and backups keep the bookkeeping visible; only the outgoing draft strips owned counters.
        Assert.Equal(headers.Count, message.ApplicationProperties.Count);
        var draft = message.CreateDraft();
        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.RabbitMq).IsValid);
        var outgoing = RabbitMqMessageMapper.ToAmqp(draft).Headers!;
        var expected = headers.Where(pair => pair.Key != "x-death" && !deathNames.Contains(pair.Key) && !owned.Contains(pair.Key))
            .ToDictionary(pair => pair.Key, pair => pair.Value);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), outgoing.Keys.Order(StringComparer.Ordinal));
        foreach (var (name, value) in expected)
        {
            // Canonical typed AMQP checks field types, nested values and bytes, not just text representations.
            Assert.Equal(RabbitMqMessageMapper.ToTyped(value).ToJsonString(), RabbitMqMessageMapper.ToTyped(outgoing[name]).ToJsonString());
        }
        Assert.Equal("body"u8.ToArray(), draft.Body.GetBytes());
    }

    // An export replayed through MessageImport kept every property, so the broker's delivery counters of a quorum
    // queue went out as ordinary headers. The export now says which headers the broker owns; same-name headers of a
    // classic queue are a producer's and stay, with their types. An older export says nothing, so nothing is dropped.
    [Theory]
    [InlineData(false, "4.2.0", false)]
    [InlineData(false, "4.3.0", false)]
    [InlineData(true, "4.2.0", false)]
    [InlineData(true, "4.3.0", false)]
    [InlineData(true, "4.3.0", true)]
    public async Task AnExportReplayRemovesOnlyEstablishedBrokerHeaders(bool quorum, string version, bool olderExport)
    {
        var owned = RabbitMqWorkspace.BrokerOwnedHeaders(quorum, Version.Parse(version));
        var headers = new Dictionary<string, object?>
        {
            ["tenant"] = "acme"u8.ToArray(),
            ["x-delivery-counter"] = (short)7,
            ["x-delivery-count"] = 3L,
            ["x-acquired-count"] = 4L
        };
        var message = RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
            new BasicProperties { MessageId = "ownership", Headers = headers }, "orders",
            ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, owned);
        using var file = new MemoryStream();
        await MessageExport.WriteJsonAsync(file, [new ExportedMessage("Rabbit", message)], default);
        var json = System.Text.Json.Nodes.JsonNode.Parse(file.ToArray())!;
        if (olderExport)
        {
            json[0]!.AsObject().Remove("brokerOwnedHeaders");
            json[0]!["exportVersion"] = 2;
        }

        var imported = Assert.Single(await MessageImport.ReadAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json.ToJsonString()))));
        var outgoing = RabbitMqMessageMapper.ToAmqp(imported.Draft).Headers!;

        var expected = olderExport
            ? headers
            : headers.Where(pair => !owned.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), outgoing.Keys.Order(StringComparer.Ordinal));
        foreach (var (name, value) in expected)
        {
            Assert.Equal(RabbitMqMessageMapper.ToTyped(value).ToJsonString(), RabbitMqMessageMapper.ToTyped(outgoing[name]).ToJsonString());
        }
        // The same as a copy of the loaded message, for an export that carries the evidence.
        if (!olderExport)
        {
            Assert.Equal(RabbitMqMessageMapper.ToAmqp(message.CreateDraft()).Headers!.Keys.Order(StringComparer.Ordinal),
                outgoing.Keys.Order(StringComparer.Ordinal));
        }
    }
}
