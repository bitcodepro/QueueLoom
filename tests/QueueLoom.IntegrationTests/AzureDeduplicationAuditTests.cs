using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

public sealed class AzureDeduplicationAuditTests
{
    [EmulatorFact(Emulators.ServiceBus)]
    public async Task AzureMoveRetainsOriginalForPreservedIdAndDeliversANewIdExactlyOnce()
    {
        using var directory = new TemporaryDirectory();
        await using var client = new ServiceBusClient(Emulators.ServiceBusConnectionString);
        await using var sender = client.CreateSender("audit-dedup");
        await using var receiver = client.CreateReceiver("audit-dedup");
        var id = Guid.NewGuid().ToString("N");
        await sender.SendMessageAsync(new Azure.Messaging.ServiceBus.ServiceBusMessage("audit") { MessageId = id });
        await sender.SendMessageAsync(new Azure.Messaging.ServiceBus.ServiceBusMessage("audit duplicate") { MessageId = id });
        var original = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(original);
        Assert.Equal(id, original.MessageId);
        await receiver.DeadLetterMessageAsync(original, "AuditTest");
        // This asserts that the emulator actually suppresses the accepted duplicate; no fake stands in for it.
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));

        var profile = ServiceBusProfile.CreateNew("Audit emulator", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        var vault = new InMemorySecretVault();
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        await using var workspace = new AzureServiceBusWorkspace(vault,
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        await workspace.ConnectAsync(profile);
        var source = ServiceBusEntityReference.Queue("audit-dedup");
        var dead = Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
        var item = new ResendItem(dead, source, dead.CreateDraft());
        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(workspace, [item], ResendMode.Move));
        Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));

        item = item.WithNewMessageId();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await store.CreateResendAsync(profile.Id, [item], ResendMode.Move, 50, workspace.ConnectedNamespace,
            ScheduledResend.IdentityFor(profile), "Emulator move", default);
        var result = await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal(1, result.MovedCount);
        Assert.Equal("Moved", Assert.Single(store.ReadHistory(plan).Items).State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        await workspace.SendMessageAsync(new SendMessageRequest(source, item.Message));
        var replacement = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(replacement);
        Assert.Equal(item.Message.Properties.MessageId, replacement.MessageId);
        Assert.NotEqual(id, replacement.MessageId);
        await receiver.CompleteMessageAsync(replacement);
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        Assert.Empty(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
    }
}
