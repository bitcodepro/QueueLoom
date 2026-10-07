using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A profile's configuration identity is a hash of its serialized settings. Scheduled resends and recoverable batch
// operations store it, and refuse to run when it changes. Adding the derived HostName to RabbitMqSettings must not
// change it: an unchanged environment after an upgrade still runs what it saved before. The identities and the JSON
// below were produced by the version before HostName existed.
public sealed partial class ViewModelStateTests
{
    public static TheoryData<string, string, string> PreviousRabbitMqIdentities => new()
    {
        {
            "rabbit.internal", "1E4CCF6C5EAB9DCDCC37E3915900344431E19184C3D0A009F0A7ED1F91126C9C",
            """{"Host":"rabbit.internal","UserName":"orders","VirtualHost":"billing","AmqpPort":5671,"ManagementPort":15671,"UseTls":true,"ManagementUri":"https://rabbit.internal:15671/"}"""
        },
        {
            "10.0.0.5", "E16F720828E6BD0AD7F047255BFA29905171F8D2149C04CF820D236F49F5C190",
            """{"Host":"10.0.0.5","UserName":"orders","VirtualHost":"billing","AmqpPort":5671,"ManagementPort":15671,"UseTls":true,"ManagementUri":"https://10.0.0.5:15671/"}"""
        }
    };

    private static ServiceBusProfile PreviousRabbitMqProfile(string host) =>
        new(Guid.Parse("6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b"), "Rabbit", EnvironmentKind.Production, null, null,
            new AuthenticationSettings(AuthenticationKind.RabbitMqPassword), ProfileAccessMode.ReadWrite)
        {
            Provider = MessagingProvider.RabbitMq,
            ConfigurationRevision = Guid.Parse("0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"),
            RabbitMq = new RabbitMqSettings(host, "orders", "billing", 5671, 15671, UseTls: true)
        };

    [Theory]
    [MemberData(nameof(PreviousRabbitMqIdentities))]
    public void RabbitMqUpgrade_AnUnchangedProfileKeepsItsSerializedShapeAndIdentity(string host, string identity, string json)
    {
        var profile = PreviousRabbitMqProfile(host);

        Assert.Equal(json, System.Text.Json.JsonSerializer.Serialize(profile.RabbitMq));
        Assert.Equal(identity, ScheduledResend.IdentityFor(profile));
    }

    [Theory]
    [MemberData(nameof(PreviousRabbitMqIdentities))]
    public async Task RabbitMqUpgrade_AResendScheduledBeforeTheUpgradeStillRuns(string host, string identity, string json)
    {
        _ = json;
        using var directory = new TemporaryDirectory();
        var profile = PreviousRabbitMqProfile(host) with { Environment = EnvironmentKind.Development };
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        // Saved by the previous version: its identity is the fixed one, not one computed now.
        store.Add(DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1), ConfigurationIdentity = identity });
        var workspace = new FakeWorkspace();
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), workspace,
            new FakeDialogService { ConfirmResult = true }, scheduledResends: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();

        await vm.RunScheduledNowAsync(Assert.Single(vm.ScheduledResends));

        Assert.DoesNotContain("configuration changed", vm.StatusText + vm.ErrorText, StringComparison.Ordinal);
        Assert.Equal("scheduled", Assert.Single(workspace.SentMessages).Message.Body.Content);
    }

    [Theory]
    [MemberData(nameof(PreviousRabbitMqIdentities))]
    public async Task RabbitMqUpgrade_ABatchOperationSavedBeforeTheUpgradeIsStillRecovered(string host, string identity, string json)
    {
        _ = json;
        using var directory = new TemporaryDirectory();
        var profile = PreviousRabbitMqProfile(host);
        var store = new BatchReplayStore(directory.Path);
        var draft = new MessageDraft(new EditableMessageBody("saved", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: "saved-0"));
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("orders"), [(draft, "saved")], false, 50, default,
            profile.EndpointDisplay, identity);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);

        await new BatchReplayStore(directory.Path).RunAsync(Assert.Single(new BatchReplayStore(directory.Path).List()), workspace, () => true, null, default);

        Assert.Equal("saved", Assert.Single(workspace.SentMessages).Message.Body.Content);
        Assert.NotEqual(Guid.Empty, plan.Id);
    }
}
