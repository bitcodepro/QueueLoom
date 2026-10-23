using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    public async Task ComposerCopyThenEditedMoveCannotDeleteAfterDuplicateSuppression(MessagingProvider provider)
    {
        var (vm, broker, dialogs) = await CreateAzureComposerAsync(provider);
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

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    public async Task ComposerUnchangedMoveRetryKeepsIdAfterAmbiguousSendFailure(MessagingProvider provider)
    {
        var (vm, broker, _) = await CreateAzureComposerAsync(provider);
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

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    public async Task ComposerEditedMoveAfterAmbiguousSendFailureRequiresNewId(MessagingProvider provider)
    {
        var (vm, broker, _) = await CreateAzureComposerAsync(provider);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ComposerSendsReceivedAzureAnnotationsThatJsonCannotWrite(bool move)
    {
        var profile = CreateProfile("Azure fake", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var broker = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), broker, dialogs);
        await using var owner = vm;
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var source = ServiceBusEntityReference.Queue("orders");
        vm.Destinations.Add(new DestinationItemViewModel(source));
        var annotations = new Dictionary<string, object?>
        {
            ["nan"] = double.NaN, ["up"] = double.PositiveInfinity, ["down"] = float.NegativeInfinity,
            ["bytes"] = new byte[] { 1, 2 }, ["at"] = new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc),
            ["id"] = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff"), ["big"] = ulong.MaxValue
        };
        vm.SelectedMessage = new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1,
            "original body"u8.ToArray(), new EditableMessageProperties(MessageId: "original-A")) { AzureMessageAnnotations = annotations },
            profile.Id, profile.Name);
        vm.OpenMessageAsDraftCommand.Execute(null);
        vm.DraftMessageId = "copy-B";
        vm.DraftMovesOriginal = move;

        await vm.SendDraftCommand.ExecuteAsync();

        Assert.True(string.IsNullOrEmpty(vm.ErrorText), vm.ErrorText);
        Assert.Equal(annotations, Assert.Single(broker.SentMessages).Message.AzureMessageAnnotations);
        Assert.Equal(move ? 1 : 0, broker.DeleteRequests.Count);
    }

    private static async Task<(MainWindowViewModel Vm, FakeWorkspace Broker, FakeDialogService Dialogs)> CreateAzureComposerAsync(MessagingProvider provider)
    {
        var profile = CreateProfile("Azure fake", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = provider };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var source = ServiceBusEntityReference.Queue(provider == MessagingProvider.AmazonSqsSns ? "orders.fifo" : "orders");
        vm.Destinations.Add(new DestinationItemViewModel(source));
        vm.SelectedMessage = new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1,
            "original body"u8.ToArray(), new EditableMessageProperties(MessageId: "original-A")), profile.Id, profile.Name);
        vm.OpenMessageAsDraftCommand.Execute(null);
        return (vm, workspace, dialogs);
    }
}
