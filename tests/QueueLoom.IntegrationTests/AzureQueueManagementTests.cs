using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

/// <summary>Creating, changing and deleting a queue in the Azure Service Bus emulator.</summary>
public sealed class AzureQueueManagementTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private AzureServiceBusWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.ServiceBus)))
        {
            return;
        }

        var vault = new InMemorySecretVault();
        var profile = ServiceBusProfile.CreateNew(
                "Emulator", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite)
            with { AllowQueueManagement = true };
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        _workspace = new AzureServiceBusWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task A_queue_is_created_changed_and_deleted()
    {
        var name = Emulators.Unique("refunds");

        await _workspace.CreateQueueAsync(new QueueDefinition(name,
            new QueueSettings(TimeSpan.FromMinutes(45), MaxDeliveryCount: 4, LockDuration: TimeSpan.FromSeconds(45), DeadLetterOnExpiration: true)));
        Assert.Contains((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues, queue => queue.Name == name);
        Assert.Equal(new QueueSettings(TimeSpan.FromMinutes(45), 4, TimeSpan.FromSeconds(45), true), await _workspace.GetQueueSettingsAsync(name));

        await _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(MaxDeliveryCount: 7));
        Assert.Equal(7, (await _workspace.GetQueueSettingsAsync(name)).MaxDeliveryCount);

        await _workspace.DeleteQueueAsync(name);
        Assert.DoesNotContain((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues, queue => queue.Name == name);
    }
}
