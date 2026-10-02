using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ComposerCopyThenEditedMoveCannotDeleteAfterDuplicateSuppression()
    {
        var (vm, broker, dialogs) = await CreateAzureComposerAsync();
        await using var owner = vm;
        var delivered = new List<SendMessageRequest>();
        var seen = new HashSet<string>(StringComparer.Ordinal) { "original-A" };
        broker.OnSend = () =>
        {
            var request = broker.SentMessages.Last();
            if (seen.Add(request.Message.Properties.MessageId!)) delivered.Add(request);
        };
        vm.DraftMessageId = "copy-B";
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Single(delivered);
        Assert.Empty(broker.DeleteRequests);
        vm.DraftBody = "edited replacement";
        vm.DraftMovesOriginal = true;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Empty(broker.DeleteRequests);
        Assert.Single(broker.SentMessages);
        Assert.Contains("already attempted", vm.ErrorText, StringComparison.Ordinal);
        Assert.Single(dialogs.Confirmations);
        vm.DraftMessageId = "move-C";
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Equal(2, delivered.Count);
        Assert.Equal("edited replacement", delivered.Last().Message.Body.Content);
        Assert.Single(broker.DeleteRequests);
        Assert.Contains("MessageId: move-C", dialogs.Confirmations.Last().Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ComposerUnchangedMoveRetryKeepsIdAfterAmbiguousSendFailure()
    {
        var (vm, broker, _) = await CreateAzureComposerAsync();
        await using var owner = vm;
        vm.DraftMessageId = "retry-B";
        vm.DraftMovesOriginal = true;
        var delivered = new HashSet<string>(StringComparer.Ordinal);
        broker.OnSend = () => delivered.Add(broker.SentMessages.Last().Message.Properties.MessageId!);
        broker.SendGate = () => throw new IOException("The broker accepted the send but its response was lost.");
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Empty(broker.DeleteRequests);
        broker.SendGate = null;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Equal(2, broker.SentMessages.Count);
        Assert.All(broker.SentMessages, send => Assert.Equal("retry-B", send.Message.Properties.MessageId));
        Assert.Single(delivered);
        Assert.Single(broker.DeleteRequests);
    }

    [Fact]
    public async Task ComposerEditedMoveAfterAmbiguousSendFailureRequiresNewId()
    {
        var (vm, broker, _) = await CreateAzureComposerAsync();
        await using var owner = vm;
        vm.DraftMessageId = "move-B";
        vm.DraftMovesOriginal = true;
        broker.SendGate = () => throw new IOException("Lost response");
        await vm.SendDraftCommand.ExecuteAsync();
        broker.SendGate = null;
        vm.DraftBody = "different operation";
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.Empty(broker.DeleteRequests);
        Assert.Single(broker.SentMessages);
        Assert.Contains("already attempted", vm.ErrorText, StringComparison.Ordinal);
    }

    private static async Task<(MainWindowViewModel Vm, FakeWorkspace Broker, FakeDialogService Dialogs)> CreateAzureComposerAsync()
    {
        var profile = CreateProfile("Azure fake", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AzureServiceBus };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var source = ServiceBusEntityReference.Queue("orders");
        vm.Destinations.Add(new DestinationItemViewModel(source));
        vm.SelectedMessage = new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1,
            "original body"u8.ToArray(), new EditableMessageProperties(MessageId: "original-A")), profile.Id, profile.Name);
        vm.OpenMessageAsDraftCommand.Execute(null);
        return (vm, workspace, dialogs);
    }
}
