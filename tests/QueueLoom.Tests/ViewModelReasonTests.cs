using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Reasons_SummariseTheListAndTickAllMessagesOfOneReason()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Dead(long number, string? reason) => new(
            orders, ServiceBusSubQueue.DeadLetter, number, Encoding.UTF8.GetBytes("order"),
            new EditableMessageProperties(MessageId: $"m-{number}"),
            enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z").AddMinutes(number),
            deadLetterReason: reason);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 4)))]),
            SearchMatches =
            {
                [profile.Id] =
                [
                    Dead(1, "MaxDeliveryCountExceeded"), Dead(2, "SchemaValidationFailed"),
                    Dead(3, "MaxDeliveryCountExceeded"), Dead(4, null)
                ]
            }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(orders, 4)]);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "order";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        Assert.Equal(["MaxDeliveryCountExceeded · 2", "(no reason) · 1", "SchemaValidationFailed · 1"],
            viewModel.DeadLetterReasons.Select(reason => reason.Label));
        Assert.Equal("3 reasons", viewModel.DeadLetterReasonsSummary);

        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons[0]);

        Assert.Equal([1L, 3L], viewModel.Messages.Where(message => message.IsMarked).Select(message => message.SequenceNumber));
        Assert.True(viewModel.DeadLetterReasons[0].IsSelected);
        Assert.Equal("Delete 2 messages…", viewModel.DeleteMarkedMessagesLabel);

        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons[0]);
        Assert.Equal(0, viewModel.MarkedMessageCount);
    }
}
