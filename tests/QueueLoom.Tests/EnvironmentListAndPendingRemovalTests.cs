using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static BrowsedMessage HuntPending(ServiceBusEntityReference source, long number) => new(
        source, ServiceBusSubQueue.Active, number, Encoding.UTF8.GetBytes("order"),
        new EditableMessageProperties(MessageId: $"m-{number}", ScheduledEnqueueTime: DateTimeOffset.Parse("2026-10-01T09:00:00Z")),
        state: ServiceBusMessageState.Scheduled);

    [Fact]
    public async Task PendingRemovalFailureDetailIsRedactedLikeDeadLetterDeletion()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        var inner = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1)))]),
            BrowseMessages = [HuntPending(orders, 7)]
        };
        var workspace = PendingFailureProxy.Wrap(inner,
            "Put token failed. Endpoint=sb://orders.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=TOP-SECRET-KEY");
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();
        viewModel.Messages[0].IsMarked = true;

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Contains("Sequence 7", viewModel.ErrorText, StringComparison.Ordinal);
        Assert.DoesNotContain("TOP-SECRET-KEY", viewModel.ErrorText, StringComparison.Ordinal);
    }

    // The limit is written in the operator's culture (1,000 / 1.000 / 1 000); the test expects exactly that.
    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("ru-RU")]
    public async Task PendingRemovalOverTheLimitIsRefusedBeforeAskingForConfirmation(string culture)
    {
        using var scope = new TestCulture(culture);
        var profile = CreateProfile("Orders", EnvironmentKind.Production, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1_001)))])
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.ReplaceMessages(Enumerable.Range(1, PendingMessages.MaximumMessages + 1)
            .Select(number => new MessageItemViewModel(HuntPending(orders, number), profile.Id, profile.Name)));
        viewModel.AreAllMessagesMarked = true;
        Assert.Equal(PendingMessages.MaximumMessages + 1, viewModel.MarkedMessageCount);

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        // The workspace refuses more than 1,000 only after the operator typed the production name to confirm.
        Assert.Empty(dialogs.Confirmations);
        Assert.Empty(workspace.PendingRemovals);
        Assert.Contains($"{PendingMessages.MaximumMessages:N0}", viewModel.ErrorText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EditingAnotherEnvironmentKeepsTheConnectedEnvironmentsMessages()
    {
        var connected = CreateProfile("Production", EnvironmentKind.Production);
        var other = CreateProfile("Development", EnvironmentKind.Development);
        var orders = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1, deadLetter: 2)))]),
            BrowseMessages = [SearchMessage(orders, 1, "2026-08-12T10:00:00Z"), SearchMessage(orders, 2, "2026-08-12T10:01:00Z")]
        };
        var dialogs = new FakeDialogService
        {
            EditResult = new ProfileEditorResult(other with { Name = "Development (renamed)" }, null, false)
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([connected, other], connected.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Equal(2, viewModel.Messages.Count);
        viewModel.Messages[0].IsMarked = true;

        viewModel.SelectedProfile = viewModel.Profiles.Single(item => item.Id == other.Id);
        await viewModel.EditEnvironmentCommand.ExecuteAsync();

        Assert.Equal(string.Empty, viewModel.ErrorText);
        Assert.Equal(connected.Id, viewModel.ConnectedProfileId);
        Assert.Equal(2, viewModel.Messages.Count);
        Assert.Equal(1, viewModel.MarkedMessageCount);
    }

    [Fact]
    public async Task AddingAnEnvironmentKeepsTheTickedMessagesOfTheConnectedEnvironment()
    {
        var connected = CreateProfile("Production", EnvironmentKind.Production);
        var orders = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1, deadLetter: 2)))]),
            BrowseMessages = [SearchMessage(orders, 1, "2026-08-12T10:00:00Z"), SearchMessage(orders, 2, "2026-08-12T10:01:00Z")]
        };
        var dialogs = new FakeDialogService
        {
            EditResult = new ProfileEditorResult(CreateProfile("Staging", EnvironmentKind.Test), null, false)
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([connected], connected.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        viewModel.Messages[0].IsMarked = true;

        await viewModel.AddEnvironmentCommand.ExecuteAsync();

        Assert.Equal(2, viewModel.Profiles.Count);
        Assert.Equal(connected.Id, viewModel.ConnectedProfileId);
        Assert.Equal(2, viewModel.Messages.Count);
        Assert.Equal(1, viewModel.MarkedMessageCount);
    }

    /// <summary>Forwards everything to the fake workspace, except that every pending removal fails with the given detail.</summary>
    public class PendingFailureProxy : DispatchProxy
    {
        private IServiceBusWorkspace _inner = null!;
        private string _detail = string.Empty;

        public static IServiceBusWorkspace Wrap(IServiceBusWorkspace inner, string detail)
        {
            var proxy = Create<IServiceBusWorkspace, PendingFailureProxy>();
            var self = (PendingFailureProxy)(object)proxy;
            self._inner = inner;
            self._detail = detail;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IServiceBusWorkspace.RemovePendingMessagesAsync))
            {
                var messages = (IReadOnlyList<BrowsedMessage>)args![0]!;
                return Task.FromResult(new RemovePendingMessagesResult(
                    messages.Select(message => new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Failed, _detail)).ToArray(),
                    Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup")));
            }
            try
            {
                return targetMethod.Invoke(_inner, args);
            }
            catch (TargetInvocationException exception) when (exception.InnerException is not null)
            {
                ExceptionDispatchInfo.Throw(exception.InnerException);
                throw;
            }
        }
    }
}
