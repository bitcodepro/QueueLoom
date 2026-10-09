using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class RabbitLegacyHeaderPersistenceTests
{
    private static readonly ServiceBusProfile Profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development,
        new(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with
        { Provider = MessagingProvider.RabbitMq, RabbitMq = new("broker.invalid", "fixture") };

    private static BrowsedMessage Browse() => RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
        new BasicProperties { MessageId = "fixture", Headers = new Dictionary<string, object?>
            { ["x-delivery-count"] = 3L, ["x-delivery-counter"] = (short)7, ["x-death-note"] = "invoice"u8.ToArray() } },
        "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BackupDistinguishesLegacyFromExplicitEmptyOwnershipAndKeepsPolicyAfterResave(bool legacy)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(Profile, DateTimeOffset.UtcNow, default);
        var file = await session.BackupAsync(Browse(), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        if (legacy) { json.Remove("brokerOwnedHeaders"); await File.WriteAllTextAsync(file, json.ToJsonString()); }
        else Assert.Empty(json["brokerOwnedHeaders"]!.AsArray());
        var repository = new JsonDeadLetterBackupRepository(paths);
        var message = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        Assert.Empty(message.BrokerOwnedHeaders); // Old formats supply no evidence of ownership.
        AssertWire(message.CreateDraft(), legacy);
        await session.BackupAsync(message, default);
        foreach (var summary in await repository.ListAsync()) AssertWire((await repository.LoadAsync(summary)).CreateDraft(), legacy);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledAndBothReplayWritersKeepLegacyPolicyAcrossSaveCloneAndRetry(bool legacy)
    {
        using var directory = new TemporaryDirectory();
        var message = Browse();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var schedules = new JsonScheduledResendStore(paths);
        schedules.Save([new ScheduledResend(Guid.NewGuid(), Profile.Id, Profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
            ResendMode.Copy, 0, "isolated", [ScheduledResendItem.From(new ResendItem(message, message.Source, message.CreateDraft()))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(Profile) }]);
        if (legacy) RemoveMarker(schedules.FilePath);
        var jobs = schedules.Load();
        schedules.Save(jobs);
        var draft = Assert.Single(Assert.Single(schedules.Load()).Items).Message;
        AssertWire(draft, legacy);
        draft = new ResendItem(message, message.Source, draft).WithNewMessageId().Message;
        AssertWire(draft, legacy);
        draft = new MessageRewrite("body", "edited").Apply(draft);
        Assert.Equal("edited", draft.Body.Content);
        AssertWire(draft, legacy);

        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await store.CreateAsync(Profile.Id, message.Source, [(message.CreateDraft(), "isolated")], true, 50, default,
            Profile.EndpointDisplay, ScheduledResend.IdentityFor(Profile));
        if (legacy) RemoveMarker(Path.Combine(store.RootDirectory, plan.Id.ToString("N"), "000000.message.json"));
        await using var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(Profile);
        await store.RunAsync(plan, workspace, () => true, null, default);
        var replayed = Assert.Single(workspace.SentMessages).Message;
        AssertWire(replayed, legacy);
        var retry = await store.CreateAsync(Profile.Id, message.Source, [(replayed, "retry")], true, 50, default,
            Profile.EndpointDisplay, ScheduledResend.IdentityFor(Profile));
        await store.RunAsync(retry, workspace, () => true, null, default);
        AssertWire(workspace.SentMessages[^1].Message, legacy);
        var copy = await store.CreateResendAsync(Profile.Id, [new ResendItem(message, message.Source, draft)], ResendMode.Copy, 50,
            Profile.EndpointDisplay, ScheduledResend.IdentityFor(Profile), "Resend", default);
        await store.RunItemsAsync(copy, [0], false, workspace, () => true, null, default);
        AssertWire(workspace.SentMessages[^1].Message, legacy);
    }

    [Fact]
    public async Task MissingOwnershipFieldDoesNotMigrateNonRabbitBackups()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = Profile with { Provider = MessagingProvider.AmazonSqsSns };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        var file = await session.BackupAsync(Browse(), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json.Remove("brokerOwnedHeaders");
        await File.WriteAllTextAsync(file, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        var message = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        Assert.False(message.LegacyAmqpBrokerHeaders);
        AssertWire(message.CreateDraft(), false);
    }

    private static void AssertWire(MessageDraft draft, bool legacy)
    {
        var headers = RabbitMqMessageMapper.ToAmqp(draft).Headers!;
        Assert.Equal(!legacy, headers.ContainsKey("x-delivery-count"));
        if (!legacy) Assert.Equal(3L, Assert.IsType<long>(headers["x-delivery-count"]));
        Assert.Equal((short)7, Assert.IsType<short>(headers["x-delivery-counter"]));
        Assert.Equal("invoice"u8.ToArray(), Assert.IsType<byte[]>(headers["x-death-note"]));
    }

    private static void RemoveMarker(string file)
    {
        var json = JsonNode.Parse(File.ReadAllText(file))!;
        Visit(json);
        File.WriteAllText(file, json.ToJsonString());
        static void Visit(JsonNode node)
        {
            if (node is JsonArray array) foreach (var child in array) { if (child is not null) Visit(child); }
            if (node is not JsonObject obj) return;
            foreach (var key in obj.Select(p => p.Key).Where(k => k.Equals("hasClassifiedAmqpHeaders", StringComparison.OrdinalIgnoreCase)).ToArray()) obj.Remove(key);
            // HasSeparatedAmqpMetadata already existed in main; retain it in these historical fixtures.
            foreach (var child in obj.Select(p => p.Value).ToArray()) if (child is not null) Visit(child);
        }
    }
}
