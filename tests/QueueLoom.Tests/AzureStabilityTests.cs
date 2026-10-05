using System.Collections.Concurrent;
using System.Reflection;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class AzureStabilityTests
{
    public static TheoryData<string> ManagementCalls => new() { "create", "update", "delete", "save-rule", "delete-rule" };

    [Theory]
    [MemberData(nameof(ManagementCalls))]
    public async Task Management_InFlight_HoldsOffDisconnect(string call)
    {
        var administration = new BlockingAdministration();
        await using var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Install(workspace, administration);

        var operation = call switch
        {
            "create" => workspace.CreateQueueAsync(new QueueDefinition("q", new QueueSettings())),
            "update" => workspace.UpdateQueueSettingsAsync("q", new QueueSettings(MaxDeliveryCount: 5)),
            "delete" => workspace.DeleteQueueAsync("q"),
            "save-rule" => workspace.SaveSubscriptionRuleAsync("t", "s", new SubscriptionRule("r", RuleFilterKind.Sql, "1=1"), false),
            _ => workspace.DeleteSubscriptionRuleAsync("t", "s", "r")
        };
        await administration.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var disconnect = workspace.DisconnectAsync();
        Assert.False(disconnect.IsCompleted, "Disconnect must wait for the management call that is still using the client.");

        administration.Release.SetException(new InvalidOperationException("broker failure"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => operation);
        await disconnect.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Disconnect_SenderDisposeFailure_StillDisposesClient()
    {
        var client = new TrackingClient();
        await using var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Install(workspace, new BlockingAdministration());
        Set(workspace, "_client", client);
        var senders = (ConcurrentDictionary<string, ServiceBusSender>)Field("_senders").GetValue(workspace)!;
        senders["q"] = new ThrowingSender();

        await workspace.DisconnectAsync();
        Assert.Equal(WorkspaceConnectionState.Disconnected, workspace.ConnectionState);

        Assert.True(client.Disposed);
        Assert.Null(Field("_client").GetValue(workspace));
        Assert.Empty(senders);
    }

    private static FieldInfo Field(string name) =>
        typeof(AzureServiceBusWorkspace).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static void Set(AzureServiceBusWorkspace workspace, string name, object? value) => Field(name).SetValue(workspace, value);

    private static void Install(AzureServiceBusWorkspace workspace, ServiceBusAdministrationClient administration)
    {
        var profile = ServiceBusProfile.CreateNew("Azure", EnvironmentKind.Development,
                AuthenticationSettings.ConnectionString(), "ns.servicebus.windows.net")
            with { AccessMode = ProfileAccessMode.ReadWrite, AllowQueueManagement = true };
        Set(workspace, "_administration", administration);
        Set(workspace, "_profile", profile);
    }

    private sealed class BlockingAdministration : ServiceBusAdministrationClient
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private async Task<T> Block<T>()
        {
            Entered.TrySetResult();
            await Release.Task;
            throw new InvalidOperationException("unreachable");
        }

        public override Task<Response<QueueProperties>> CreateQueueAsync(CreateQueueOptions options, CancellationToken cancellationToken = default) => Block<Response<QueueProperties>>();
        public override Task<Response<QueueProperties>> GetQueueAsync(string name, CancellationToken cancellationToken = default) => Block<Response<QueueProperties>>();
        public override Task<Response> DeleteQueueAsync(string name, CancellationToken cancellationToken = default) => Block<Response>();
        public override Task<Response<RuleProperties>> CreateRuleAsync(string topicName, string subscriptionName, CreateRuleOptions options, CancellationToken cancellationToken = default) => Block<Response<RuleProperties>>();
        public override Task<Response> DeleteRuleAsync(string topicName, string subscriptionName, string ruleName, CancellationToken cancellationToken = default) => Block<Response>();
    }

    private sealed class TrackingClient : ServiceBusClient
    {
        public bool Disposed { get; private set; }
        public override ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private sealed class ThrowingSender : ServiceBusSender
    {
        public override ValueTask DisposeAsync() => throw new InvalidOperationException("link already faulted");
    }
}
