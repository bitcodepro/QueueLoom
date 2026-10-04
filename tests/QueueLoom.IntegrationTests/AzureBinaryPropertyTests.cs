using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

public sealed class AzureBinaryPropertyTests
{
    /// <summary>
    /// A Binary property from another service (a non-UTF-8 Kafka header restored from a backup, for example) is
    /// accepted by Service Bus and arrives as the Base64 text of the same bytes.
    /// </summary>
    [EmulatorFact(Emulators.ServiceBus)]
    public async Task BinaryPropertyFromAnotherServiceIsSentAsItsBase64Text()
    {
        using var directory = new TemporaryDirectory();
        var profile = ServiceBusProfile.CreateNew("Binary emulator", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        var vault = new InMemorySecretVault();
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        await using var workspace = new AzureServiceBusWorkspace(vault,
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        await workspace.ConnectAsync(profile);
        // A queue of its own, so no other test can take this message or have its own taken here.
        var administration = new ServiceBusAdministrationClient(
            EmulatorConnection.AdministrationConnectionString(Emulators.ServiceBusConnectionString, profile.EmulatorManagementPort));
        var queue = Emulators.Unique("binary");
        await administration.CreateQueueAsync(queue);
        try
        {
            await using var client = new ServiceBusClient(Emulators.ServiceBusConnectionString);
            await using var receiver = client.CreateReceiver(queue);

            var marker = Guid.NewGuid().ToString("N");
            var bytes = new byte[] { 0xff, 0x00, 0xfe };
            var draft = new MessageDraft(new EditableMessageBody("binary header", MessageBodyFormat.Text),
                new EditableMessageProperties(MessageId: marker),
                [new MessageApplicationProperty("opaque", ApplicationPropertyType.Binary, Convert.ToBase64String(bytes))]);
            await workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Queue(queue), draft));

            var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

            Assert.NotNull(received);
            Assert.Equal(marker, received.MessageId);
            Assert.Equal(bytes, Convert.FromBase64String(Assert.IsType<string>(received.ApplicationProperties["opaque"])));
        }
        finally
        {
            await administration.DeleteQueueAsync(queue);
        }
    }
}
