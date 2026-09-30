using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Pending_ScheduledAndDeferredMessagesAreTickedByKindAndRemovedAfterConfirmation()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Active(long number, ServiceBusMessageState state) => new(
            orders, ServiceBusSubQueue.Active, number, Encoding.UTF8.GetBytes("order"),
            new EditableMessageProperties(MessageId: $"m-{number}",
                ScheduledEnqueueTime: state == ServiceBusMessageState.Scheduled ? DateTimeOffset.Parse("2026-10-01T09:00:00Z") : null),
            state: state);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 4)))]),
            BrowseMessages =
            [
                Active(1, ServiceBusMessageState.Active), Active(2, ServiceBusMessageState.Scheduled),
                Active(3, ServiceBusMessageState.Scheduled), Active(4, ServiceBusMessageState.Deferred)
            ]
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();

        Assert.Equal(["ACTIVE", "SCHEDULED", "SCHEDULED", "DEFERRED"], viewModel.Messages.Select(message => message.SubQueueLabel));
        Assert.StartsWith("Due ", viewModel.Messages[1].StatusDetail, StringComparison.Ordinal);
        Assert.False(viewModel.Messages[0].CanDelete);
        Assert.Equal("WAITING", viewModel.DeadLetterReasonsTitle);
        Assert.Equal(["Scheduled · 2", "Deferred · 1"], viewModel.DeadLetterReasons.Select(reason => reason.Label));
        Assert.False(viewModel.HasDeadLetterMessages);

        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons[0]);
        Assert.Equal("Cancel 2 scheduled…", viewModel.DeleteMarkedMessagesLabel);
        viewModel.Messages[3].IsMarked = true;
        Assert.Equal("Remove 3 pending…", viewModel.DeleteMarkedMessagesLabel);

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        var confirmation = Assert.Single(dialogs.Confirmations);
        Assert.Contains("2 scheduled message(s) are cancelled", confirmation.Message, StringComparison.Ordinal);
        Assert.Contains("1 deferred message(s) are removed", confirmation.Message, StringComparison.Ordinal);
        Assert.Equal([2L, 3L, 4L], Assert.Single(workspace.PendingRemovals).Select(message => message.SequenceNumber));
        Assert.Empty(workspace.DeleteRequests);
        Assert.Equal([1L], viewModel.Messages.Select(message => message.SequenceNumber));
    }
}
