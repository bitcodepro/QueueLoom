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

    // Approved in time, but the grant ran out while the provider read AWS again: the token passed to the start ends
    // with it, and nothing is submitted.
    [Fact]
    public async Task Redrive_WriteAccessThatExpiresBeforeSubmissionMovesNothing()
    {
        var clock = new RedriveClock();
        var workspace = RedriveWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var profile = CreateProfile("AWS", EnvironmentKind.Development);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        vm.Clock = clock;
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        workspace.RedriveStartGate = _ =>
        {
            clock.Advance(TimeSpan.FromMinutes(11));
            return Task.CompletedTask;
        };

        await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();

        Assert.Empty(workspace.RedriveStarts);
        Assert.Contains("Write access expired before the redrive was submitted", vm.ErrorText, StringComparison.Ordinal);
    }

    // Write access ending after AWS accepted the move does not undo it, and the window says the move was accepted.
    [Fact]
    public async Task Redrive_WriteAccessThatEndsAfterSubmissionStillReportsTheAcceptedMove()
    {
        var clock = new RedriveClock();
        var workspace = RedriveWorkspace();
        var profile = CreateProfile("AWS", EnvironmentKind.Development);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true });
        vm.Clock = clock;
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        workspace.AfterRedriveSubmitted = () => clock.Advance(TimeSpan.FromMinutes(11));

        await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();
        // The expired unlock relocks writes in the background (off any UI thread here) and records it in Activity:
        // wait for that before reading Activity, which it would otherwise change while the assertions enumerate it.
        await WaitUntilAsync(() =>
        {
            try { return vm.Activity.Any(item => item.Action == "Writes relocked"); }
            catch (InvalidOperationException) { return false; }
        });

        Assert.Single(workspace.RedriveStarts);
        Assert.Contains(vm.Activity, item => item.Action == "DLQ redrive accepted by AWS");
        Assert.DoesNotContain("Nothing was moved", vm.ErrorText ?? string.Empty, StringComparison.Ordinal);
    }

    // AWS accepted the move and the answer was lost: the window and Activity keep the guidance, not the transport error alone.
    [Fact]
    public async Task Redrive_ALostAnswerAfterSubmissionShowsTheUnknownOutcomeAndWhatToCheck()
    {
        var workspace = RedriveWorkspace();
        workspace.AfterRedriveSubmitted = () =>
            throw QueueLoom.Infrastructure.Aws.AwsSqsSnsWorkspace.UnknownRedriveOutcome("orders-dlq", new HttpRequestException("connection reset"));
        await using var vm = await ConnectedForRedrive(workspace, new FakeDialogService { ConfirmResult = true });

        await vm.RedriveSelectedDeadLettersCommand.ExecuteAsync();

        Assert.Single(workspace.RedriveStarts);
        Assert.Contains("AWS may be moving the messages of 'orders-dlq' now", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("before starting it again", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("connection reset", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains(vm.Activity, item => item.Details.Contains("AWS may be moving the messages", StringComparison.Ordinal));
        Assert.DoesNotContain("Nothing was moved", vm.ErrorText, StringComparison.Ordinal);
    }

    private sealed class RedriveClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        private readonly List<(DateTimeOffset At, TimerCallback Callback, object? State, Timer Handle)> _timers = [];
        public override DateTimeOffset GetUtcNow() => _now;
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new Timer();
            if (dueTime != Timeout.InfiniteTimeSpan) lock (_timers) _timers.Add((_now + dueTime, callback, state, timer));
            return timer;
        }
        public void Advance(TimeSpan elapsed)
        {
            _now += elapsed;
            (DateTimeOffset At, TimerCallback Callback, object? State, Timer Handle)[] due;
            lock (_timers) due = _timers.Where(timer => timer.At <= _now && !timer.Handle.Disposed).ToArray();
            foreach (var timer in due) timer.Callback(timer.State);
        }
        private sealed class Timer : ITimer
        {
            public bool Disposed { get; private set; }
            public bool Change(TimeSpan dueTime, TimeSpan period) => !Disposed;
            public void Dispose() => Disposed = true;
            public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
        }
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
