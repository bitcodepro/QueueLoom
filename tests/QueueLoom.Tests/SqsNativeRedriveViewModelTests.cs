using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static FakeWorkspace RedriveWorkspace() => new()
    {
        Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]),
        RedrivePlan = new NativeRedrivePlan(ServiceBusEntityReference.Queue("orders"), "orders-dlq", ["orders"], 42)
        {
            DeadLetterArn = "arn:aws:sqs:us-east-1:0:orders-dlq"
        }
    };

    private static async Task<MainWindowViewModel> ConnectedForRedrive(FakeWorkspace workspace, FakeDialogService dialogs,
        IActivityJournal? journal = null)
    {
        var profile = CreateProfile("AWS", EnvironmentKind.Development);
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, activityJournal: journal);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        Assert.True(vm.CanWrite);
        return vm;
    }

    [Fact]
    public async Task Redrive_TheConfirmationSaysMessagesAlsoGoBackToQueuesThatUsedTheDeadLetterQueueBefore()
    {
        var workspace = RedriveWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = await ConnectedForRedrive(workspace, dialogs);

        await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();

        var (title, message, dangerous, _) = Assert.Single(dialogs.Confirmations, confirmation => confirmation.Title.StartsWith("Redrive", StringComparison.Ordinal));
        Assert.True(dangerous);
        Assert.Equal("Redrive dead-letter queue", title);
        Assert.Contains("every message in dead-letter queue 'orders-dlq'", message, StringComparison.Ordinal);
        Assert.Contains("Queues using it now: 'orders'", message, StringComparison.Ordinal);
        Assert.Contains("queues that used it before go back to those queues as well", message, StringComparison.Ordinal);
        Assert.Single(workspace.RedriveStarts);
    }

    // The dialog keeps the operation open: the ten-minute write grant can expire before the operator approves.
    [Fact]
    public async Task Redrive_WriteAccessThatExpiresWhileConfirmingMovesNothing()
    {
        var workspace = RedriveWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = await ConnectedForRedrive(workspace, dialogs);
        dialogs.BeforeConfirm = () => SetPrivate(vm, "_writeUnlockExpiresAt", DateTimeOffset.UtcNow.AddMinutes(-1));

        await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();

        Assert.Empty(workspace.RedriveStarts);
        Assert.False(vm.CanWrite);
        Assert.Contains("Write access expired", vm.ErrorText, StringComparison.Ordinal);
    }

    // A durable record comes before the move, which cannot be undone; without it, nothing is moved.
    [Fact]
    public async Task Redrive_IsRecordedDurablyBeforeItStartsAndNotStartedWhenTheRecordFails()
    {
        var journal = new RedriveJournal();
        var workspace = RedriveWorkspace();
        workspace.OnRedriveStart = () => Assert.Contains(journal.Durable, record => record.Action == "DLQ redrive started");
        await using (var vm = await ConnectedForRedrive(workspace, new FakeDialogService { ConfirmResult = true }, journal))
        {
            await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();
            Assert.Single(workspace.RedriveStarts);
            Assert.Contains("orders-dlq", Assert.Single(journal.Durable, record => record.Action == "DLQ redrive started").Details, StringComparison.Ordinal);
        }

        var failing = new RedriveJournal { FailDurable = true };
        var second = RedriveWorkspace();
        await using var other = await ConnectedForRedrive(second, new FakeDialogService { ConfirmResult = true }, failing);
        await other.RedriveSelectedDeadLettersCommand.ExecuteAsync();

        Assert.Empty(second.RedriveStarts);
        Assert.Contains("disk full", other.ErrorText, StringComparison.Ordinal);
    }

    private sealed class RedriveJournal : IActivityJournal
    {
        public bool FailDurable { get; init; }
        public List<ActivityRecord> Durable { get; } = [];
        public void Append(ActivityRecord record)
        {
            if (FailDurable) throw new IOException("disk full");
            lock (Durable) Durable.Add(record);
        }
        public void AppendEntry(ActivityRecord record) { }
        public IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500) => [];
    }
}
