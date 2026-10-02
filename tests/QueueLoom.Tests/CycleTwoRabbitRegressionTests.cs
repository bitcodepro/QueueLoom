using System.Text;
using System.Text.Json;
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
        using var csv = new StringWriter();
        await MessageExport.WriteCsvAsync(csv, [new("isolated", message)], default);
        Assert.Contains("header-type", csv.ToString(), StringComparison.Ordinal);
        using var output = new MemoryStream();
        await MessageExport.WriteJsonAsync(output, [new("isolated", message)], default);
        using var exported = JsonDocument.Parse(output.ToArray());
        Assert.Equal("header-app", exported.RootElement[0].GetProperty("applicationProperties").GetProperty("amqp-app-id").GetString());
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
