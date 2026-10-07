using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

public sealed partial class RealCloudsUiTests
{
    [EmulatorFact(Emulators.ServiceBus)]
    public Task AzureComposerCopyThenEditedMoveRequiresANewIdentity() => UiSession.RunAsync(async () =>
    {
        var connection = Environment.GetEnvironmentVariable(Emulators.ServiceBus)!;
        var root = Path.Combine(Path.GetTempPath(), "queueloom-composer-audit", Guid.NewGuid().ToString("N"));
        var source = ServiceBusEntityReference.Queue("audit-composer-dedup");
        var originalId = Guid.NewGuid().ToString("N");
        await using var client = new ServiceBusClient(connection);
        await using var sender = client.CreateSender(source.Name);
        await using var receiver = client.CreateReceiver(source.Name);
        await sender.SendMessageAsync(new Azure.Messaging.ServiceBus.ServiceBusMessage("original") { MessageId = originalId });
        var original = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(original);
        await receiver.DeadLetterMessageAsync(original, "ComposerAudit");
        var profile = ServiceBusProfile.CreateNew("Azure emulator", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        var vault = new InMemorySecretVault();
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), connection);
        await using var workspace = new AzureServiceBusWorkspace(vault,
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)));
        var dialogs = DispatchProxy.Create<IUserDialogService, ComposerAuditDialogs>();
        var confirmations = ((ComposerAuditDialogs)dialogs).Confirmations;
        await using var vm = new MainWindowViewModel(new InMemoryProfileRepository(profile), vault, workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        Assert.Equal(WorkspaceConnectionState.Connected, workspace.ConnectionState);
        var dead = Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
        vm.SelectedMessage = new MessageItemViewModel(dead, profile.Id, profile.Name);
        vm.OpenMessageAsDraftCommand.Execute(null);
        vm.DraftMessageId = Guid.NewGuid().ToString("N");
        var copyId = vm.DraftMessageId;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Empty(vm.ErrorText);
        var copy = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(copy);
        Assert.Equal(copyId, copy.MessageId);
        await receiver.CompleteMessageAsync(copy);
        // Prove the actual emulator's duplicate detection is active for this ID.
        await sender.SendMessageAsync(new Azure.Messaging.ServiceBus.ServiceBusMessage("suppressed") { MessageId = copyId });
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        vm.DraftBody = "edited replacement";
        vm.DraftMovesOriginal = true;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Contains("already attempted", vm.ErrorText, StringComparison.Ordinal);
        Assert.Single(confirmations);
        Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
        vm.GenerateMessageIdCommand.Execute(null);
        var moveId = vm.DraftMessageId;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Empty(vm.ErrorText);
        Assert.Contains($"MessageId: {moveId}", confirmations.Last(), StringComparison.Ordinal);
        var replacement = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(replacement);
        Assert.Equal(moveId, replacement.MessageId);
        Assert.Equal("edited replacement", replacement.Body.ToString());
        await receiver.CompleteMessageAsync(replacement);
        Assert.Empty(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1)));
    });

    public class ComposerAuditDialogs : DispatchProxy
    {
        public List<string> Confirmations { get; } = [];
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "ConfirmAsync") { Confirmations.Add((string)args![1]!); return Task.FromResult(true); }
            if (method.Name == "ShowMessageAsync") return Task.CompletedTask;
            throw new NotSupportedException(method.Name);
        }
    }
}
