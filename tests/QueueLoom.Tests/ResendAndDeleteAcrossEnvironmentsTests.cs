using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task DeletingInOneEnvironmentKeepsTheSameSequenceFromAnotherEnvironmentListed()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        // An all-environments DLQ search lists a production message with the same queue name and sequence number.
        var production = Guid.NewGuid();
        var productionRow = new MessageItemViewModel(
            SearchMessage(ServiceBusEntityReference.Queue("orders"), 2, "2026-08-12T11:00:00Z"), production, "Production");
        viewModel.Messages.Add(productionRow);

        viewModel.Messages.Single(m => m.SequenceNumber == 2 && m.ProfileId != production).IsMarked = true;
        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal([2L], Assert.Single(workspace.DeleteRequests).Messages.Select(m => m.SequenceNumber));
        // Only the connected environment's message was deleted; the production message still exists.
        Assert.Contains(productionRow, viewModel.Messages);
    }

    [Fact]
    public async Task ScheduledResendThatCanNeverBePreparedIsNotRetriedForeverLeavingBodySnapshots()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var operations = Path.Combine(directory.Path, "operations");
        var store = new QueueLoom.Infrastructure.Persistence.BatchReplayStore(operations);
        var schedules = new QueueLoom.Infrastructure.Persistence.JsonScheduledResendStore(
            QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path));
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1), PreserveMessageIds = true }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs,
            replayStore: store, scheduledResends: schedules);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        var source = ServiceBusEntityReference.Queue("source");
        // A valid dead letter and one whose broker-assigned ID is longer than QueueLoom's 128-character draft limit
        // (RabbitMQ message-id, Kafka key...). Scheduling accepts both without validating the drafts.
        vm.Messages.Add(new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1,
            "secret body"u8.ToArray(), new EditableMessageProperties(MessageId: "ok")), profile.Id) { IsMarked = true });
        vm.Messages.Add(new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 2,
            "other"u8.ToArray(), new EditableMessageProperties(MessageId: new string('x', 200))), profile.Id) { IsMarked = true });
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var scheduled = schedules.Load().Count;

        vm.Clock = new ManualClock(DateTimeOffset.UtcNow.AddHours(2));
        for (var tick = 0; tick < 3; tick++) await vm.RunDueScheduledResendsAsync(); // three 20-second schedule ticks

        var orphans = Directory.Exists(operations)
            ? Directory.GetDirectories(operations).Where(d => !File.Exists(Path.Combine(d, "plan.json"))).ToArray()
            : [];
        Assert.Empty(workspace.SentMessages);
        // Either the job is rejected up front, or a failed preparation must not leave a full-body snapshot per tick.
        Assert.True(scheduled == 0 || orphans.Length == 0,
            $"job kept={schedules.Load().Count}, orphan snapshot folders={orphans.Length}, " +
            $"bodies={orphans.Sum(d => Directory.GetFiles(d, "*.message.json").Length)}");
    }
}
