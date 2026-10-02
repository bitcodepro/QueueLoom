using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using QueueLoom.App.Serialization;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class CycleTwoRabbitRegressionTests
{
    internal static BrowsedMessage Browse(bool basic, bool header) => RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
        new BasicProperties { MessageId = "fixture", Type = basic ? "basic-type" : null, AppId = basic ? "basic-app" : null,
            Headers = header ? new Dictionary<string, object?> { ["amqp-type"] = "header-type"u8.ToArray(), ["amqp-app-id"] = "header-app"u8.ToArray() } : null },
        "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task CycleTwoRabbit_BasicMetadataAndUserHeadersSurviveDraftAndBackupSeparately(bool basic, bool header)
    {
        var message = Browse(basic, header);
        AssertWire(message.CreateDraft(), basic, header);
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        await session.BackupAsync(message, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        AssertWire(restored.CreateDraft(), basic, header);
        Assert.Equal(message.Body.ToArray(), restored.Body.ToArray());
    }

    [Fact]
    public async Task CycleTwoRabbit_CollidingHeaderNamesRemainUsableInComposerAndExports()
    {
        var message = Browse(true, true);
        var properties = ApplicationPropertiesJson.Deserialize(ApplicationPropertiesJson.Serialize(message.ApplicationProperties));
        Assert.Equal("header-type", Assert.Single(properties, p => p.Name == "amqp-type").Value);
        using var inspector = JsonDocument.Parse(new MessageItemViewModel(message).PropertiesJson);
        Assert.Equal("basic-type", inspector.RootElement.GetProperty("broker").GetProperty("AmqpType").GetString());
        using var csv = new StringWriter();
        await MessageExport.WriteCsvAsync(csv, [new("isolated", message)], default);
        Assert.Contains("header-type", csv.ToString(), StringComparison.Ordinal);
        Assert.Contains(",basic-type,basic-app,", csv.ToString(), StringComparison.Ordinal);
        using var output = new MemoryStream();
        await MessageExport.WriteJsonAsync(output, [new("isolated", message)], default);
        using var exported = JsonDocument.Parse(output.ToArray());
        Assert.Equal("header-app", exported.RootElement[0].GetProperty("applicationProperties").GetProperty("amqp-app-id").GetString());
        Assert.Equal("basic-type", exported.RootElement[0].GetProperty("amqpType").GetString());
    }

    internal static void AssertWire(MessageDraft draft, bool basic, bool header)
    {
        var wire = RabbitMqMessageMapper.ToAmqp(draft);
        Assert.Equal(basic ? "basic-type" : null, wire.Type);
        Assert.Equal(basic ? "basic-app" : null, wire.AppId);
        if (header)
        {
            Assert.Equal("header-type"u8.ToArray(), Assert.IsType<byte[]>(wire.Headers!["amqp-type"]));
            Assert.Equal("header-app"u8.ToArray(), Assert.IsType<byte[]>(wire.Headers!["amqp-app-id"]));
        }
        else { Assert.DoesNotContain("amqp-type", wire.Headers!.Keys); Assert.DoesNotContain("amqp-app-id", wire.Headers.Keys); }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CycleTwoRabbit_MetadataTravelsThroughDurableAndScheduledCopies(bool header)
    {
        using var directory = new TemporaryDirectory();
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite)
            with { Provider = MessagingProvider.RabbitMq, RabbitMq = new("broker.invalid", "fixture") };
        var message = Browse(true, header);
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var scheduled = new JsonScheduledResendStore(paths);
        var job = new ScheduledResend(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
            ResendMode.Copy, 0, "isolated", [ScheduledResendItem.From(new ResendItem(message, message.Source, message.CreateDraft()))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };
        scheduled.Save([job]);
        var draft = Assert.Single(Assert.Single(new JsonScheduledResendStore(paths).Load()).Items).Message;
        AssertWire(draft, true, header);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await store.CreateAsync(profile.Id, message.Source, [(draft, "isolated")], true, 50, default, profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        await using var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await new BatchReplayStore(store.RootDirectory).RunAsync(plan, workspace, () => true, null, default);
        AssertWire(Assert.Single(workspace.SentMessages).Message, true, header);
        var copyStore = new BatchReplayStore(Path.Combine(directory.Path, "copies"));
        var copyPlan = await copyStore.CreateResendAsync(profile.Id, [new ResendItem(message, message.Source, draft)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Resend", default);
        await copyStore.RunItemsAsync(copyPlan, [0], false, workspace, () => true, null, default);
        AssertWire(workspace.SentMessages[^1].Message, true, header);
    }

    [Fact]
    public async Task CycleTwoRabbit_LegacyDurableAndScheduledMetadataKeepsHistoricalWireMeaning()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite)
            with { Provider = MessagingProvider.RabbitMq, RabbitMq = new("broker.invalid", "fixture") };
        var message = Browse(true, false);
        var scheduled = new JsonScheduledResendStore(paths);
        scheduled.Save([new ScheduledResend(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
            ResendMode.Copy, 0, "isolated", [ScheduledResendItem.From(new ResendItem(message, message.Source, message.CreateDraft()))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) }]);
        MakeLegacy(scheduled.FilePath);
        var loaded = Assert.Single(Assert.Single(scheduled.Load()).Items).Message;
        AssertWire(loaded, true, false);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await store.CreateAsync(profile.Id, message.Source, [(message.CreateDraft(), "isolated")], true, 50, default,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        MakeLegacy(Path.Combine(store.RootDirectory, plan.Id.ToString("N"), "000000.message.json"));
        await using var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await store.RunAsync(plan, workspace, () => true, null, default);
        AssertWire(Assert.Single(workspace.SentMessages).Message, true, false);

        static void MakeLegacy(string file)
        {
            var json = JsonNode.Parse(File.ReadAllText(file))!;
            Visit(json);
            File.WriteAllText(file, json.ToJsonString());
            static void Visit(JsonNode node)
            {
                if (node is JsonArray array) { foreach (var child in array) if (child is not null) Visit(child); }
                if (node is not JsonObject obj) return;
                var app = obj.FirstOrDefault(p => p.Key.Equals("applicationProperties", StringComparison.OrdinalIgnoreCase)).Key;
                if (app is not null)
                {
                    foreach (var key in obj.Select(p => p.Key).Where(k => k.Equals("hasSeparatedAmqpMetadata", StringComparison.OrdinalIgnoreCase)).ToArray()) obj.Remove(key);
                    var properties = obj.First(p => p.Key.Equals("properties", StringComparison.OrdinalIgnoreCase)).Value!.AsObject();
                    foreach (var key in properties.Select(p => p.Key).Where(k => k.Equals("amqpType", StringComparison.OrdinalIgnoreCase) || k.Equals("amqpAppId", StringComparison.OrdinalIgnoreCase)).ToArray()) properties.Remove(key);
                    var lower = app[0] == 'a';
                    obj[app] = JsonNode.Parse(lower
                        ? """[{"name":"amqp-type","type":"String","value":"basic-type"},{"name":"amqp-app-id","type":"String","value":"basic-app"}]"""
                        : """[{"Name":"amqp-type","Type":0,"Value":"basic-type"},{"Name":"amqp-app-id","Type":0,"Value":"basic-app"}]""");
                }
                else foreach (var child in obj.Select(p => p.Value).ToArray()) if (child is not null) Visit(child);
            }
        }
    }

    [Fact]
    public async Task CycleTwoRabbit_LegacyBackupKeepsItsHistoricalBasicPropertyInterpretation()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        var file = await session.BackupAsync(Browse(true, false), default);
        var json = JsonNode.Parse(await File.ReadAllTextAsync(file))!.AsObject();
        json["schemaVersion"] = 1; json.Remove("amqpType"); json.Remove("amqpAppId");
        json["applicationProperties"] = JsonNode.Parse("""[{"name":"amqp-type","type":"String","value":"basic-type"},{"name":"amqp-app-id","type":"String","value":"basic-app"}]""");
        await File.WriteAllTextAsync(file, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);
        AssertWire((await repository.LoadAsync(Assert.Single(await repository.ListAsync()))).CreateDraft(), true, false);
    }
}

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CycleTwoRabbit_ActualComposerPreservesIndependentHeaderAndBasicValues(bool basic)
    {
        var profile = CreateProfile("isolated", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.RabbitMq };
        var workspace = new FakeWorkspace();
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, new FakeDialogService { ConfirmResult = true });
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        var message = CycleTwoRabbitRegressionTests.Browse(basic, true);
        vm.Destinations.Add(new DestinationItemViewModel(message.Source));
        vm.SelectedMessage = new MessageItemViewModel(message, profile.Id);
        vm.OpenMessageAsDraftCommand.Execute(null);
        Assert.False(vm.CanMoveDraftOriginal);
        await vm.SendDraftCommand.ExecuteAsync();
        CycleTwoRabbitRegressionTests.AssertWire(Assert.Single(workspace.SentMessages).Message, basic, true);
    }
}
