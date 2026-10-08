using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>Connection lifecycle, environment switching and damaged local files.</summary>
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AnUnreadableActivityJournalDoesNotHideTheSavedEnvironments()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var journal = new UnreadableActivityJournal();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, test], test.Id), new FakeWorkspace(),
            activityJournal: journal);

        await viewModel.InitializeAsync();

        Assert.Equal(2, viewModel.Profiles.Count);
        Assert.Equal(test.Id, viewModel.SelectedProfile?.Id);
        Assert.True(viewModel.ConnectCommand.CanExecute(null));
    }

    [Fact]
    public void ActivityJournalSkipsARecordWhoseSourceIsDamaged()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var kept = new ActivityRecord(Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(-2), "Warning", "Purge started", "evidence",
            Guid.NewGuid(), "Production", ServiceBusEntityReference.Queue("orders"));
        journal.Append(kept);
        // Valid JSON whose entity has lost its name, as a hand edit or a bad sector leaves it.
        var damaged = kept with { OperationId = Guid.NewGuid(), Timestamp = DateTimeOffset.UtcNow.AddMinutes(-1), Action = "Damaged" };
        var json = JsonSerializer.Serialize(damaged).Replace("\"Name\":\"orders\"", "\"Name\":\"\"", StringComparison.Ordinal);
        Assert.Contains("\"Name\":\"\"", json, StringComparison.Ordinal);
        var day = Directory.GetDirectories(directory.Path).Single();
        File.WriteAllText(Path.Combine(day, $"{damaged.Timestamp.UtcTicks}-{Guid.NewGuid():N}.json"), json);

        var records = journal.ReadRecent();

        Assert.Equal(kept, Assert.Single(records));
        Assert.Equal(2, Directory.GetFiles(day, "*.json").Length);
    }

    [Fact]
    public async Task OneDamagedOperationPlanDoesNotHideTheOtherOperations()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var uncertain = await PrepareOperation(store, profile);
        var damaged = await PrepareOperation(store, profile);
        var planPath = Path.Combine(directory.Path, damaged.Id.ToString("N"), "plan.json");
        var text = await File.ReadAllTextAsync(planPath);
        await File.WriteAllTextAsync(planPath, text.Replace("\"Name\":\"target\"", "\"Name\":\" \"", StringComparison.Ordinal));
        Assert.NotEqual(text, await File.ReadAllTextAsync(planPath));

        var plans = new BatchReplayStore(directory.Path).List();

        Assert.Equal(uncertain.Id, Assert.Single(plans).Id);
        Assert.True(File.Exists(planPath));

        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            replayStore: new BatchReplayStore(directory.Path));
        await viewModel.OperationHistoryRefresh;
        Assert.Equal(uncertain.Id, Assert.Single(viewModel.OperationHistory).Plan.Id);
    }

    [Fact]
    public async Task ClosingDuringAMonitorCheckOfAnotherEnvironmentDoesNotReconnectTheOriginal()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [development.Id] = Snapshot(development.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 1))
            }
        };
        var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();
        viewModel.SelectedDeadLetterEnvironmentFilter = viewModel.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single();
        viewModel.MonitorScope = viewModel.MonitorScopes[1];
        viewModel.MonitorTargetChoice = viewModel.MonitorTargetChoices[1];
        workspace.WaitForSnapshotCancellation = true;
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await workspace.SnapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(test.Id, workspace.ConnectedProfileId);
        var connectionsBeforeClose = workspace.ConnectCalls;

        // The window closes while the monitor is reading the other environment.
        await viewModel.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        // Reconnecting the original environment on the way out only delays the exit (up to 30 s) and can prompt a sign-in.
        Assert.Equal(connectionsBeforeClose, workspace.ConnectCalls);
        Assert.Equal(1, workspace.DisposeCalls);
    }

    [Fact]
    public async Task MonitorAlertsOfAnotherEnvironmentAreJournaledUnderThatEnvironment()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [development.Id] = Snapshot(development.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 4))
            }
        };
        var journal = new RecordingActivityJournal();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace,
            activityJournal: journal);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();
        viewModel.SelectedDeadLetterEnvironmentFilter = viewModel.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
        viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single();
        viewModel.MonitorScope = viewModel.MonitorScopes[1];
        viewModel.MonitorTargetChoice = viewModel.MonitorTargetChoices[1];

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => journal.Snapshot().Any(record => record.Action == "DLQ detected"));
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        var alert = journal.Snapshot().First(record => record.Action == "DLQ detected");
        Assert.StartsWith("Test · orders", alert.Details, StringComparison.Ordinal);
        // The durable audit record must not pair one environment's ID with another environment's name.
        var named = new[] { development, test }.Single(profile => profile.Id == alert.ProfileId);
        Assert.Equal(named.Name, alert.ProfileName);
        // And it is the monitored environment, not the operator's.
        Assert.Equal(test.Id, alert.ProfileId);
        Assert.Equal(development.Id, workspace.ConnectedProfileId);
    }

    // The window shows the operator's environment during a monitor check, but the activity journal records what
    // actually happened: an event of the monitored environment is attributed to that environment, by ID and name, both
    // when the operator is connected to another one and when no environment is connected. Persisted and read back, the
    // history still names the monitored environment.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MonitorEventsOfAnotherEnvironmentArePersistedUnderThatEnvironment(bool operatorConnected)
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            Snapshots =
            {
                [development.Id] = Snapshot(development.Id, new DeadLetterEntitySnapshot(queue.Reference, 1)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 4))
            }
        };
        var journal = new QueueLoom.Infrastructure.Persistence.FileActivityJournal(directory.Path);
        // A slow disk: the journal's background writer is held until the window has closed, so the event is still only
        // queued when the window goes. Closing the journal (as the app does on shutdown) is what puts it on disk.
        var slowDisk = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.BeforeEntryWrite = () => slowDisk.Task;
        await using (var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace,
                         activityJournal: journal))
        {
            await viewModel.InitializeAsync();
            if (operatorConnected)
            {
                await viewModel.ConnectCommand.ExecuteAsync();
                await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();
                viewModel.SelectedDeadLetterEnvironmentFilter = viewModel.DeadLetterEnvironmentFilters.Single(filter => filter.ProfileId == test.Id);
                viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single();
                viewModel.MonitorScope = viewModel.MonitorScopes[1];
                viewModel.MonitorTargetChoice = viewModel.MonitorTargetChoices[1];
            }
            else
            {
                // Nothing connected: the current-environment monitor checks the selected environment, B.
                viewModel.SelectedProfile = viewModel.Profiles.Single(profile => profile.Id == test.Id);
                viewModel.MonitorScope = viewModel.MonitorScopes[0];
            }

            await viewModel.ToggleMonitorCommand.ExecuteAsync();
            await WaitUntilAsync(() => journal.ReadRecent().Any(record => record.Action == "DLQ detected"));
            await viewModel.ToggleMonitorCommand.ExecuteAsync();
            Assert.Equal(operatorConnected ? development.Id : null, viewModel.ConnectedProfileId);
        }
        // Still queued: nothing of it on disk yet.
        Assert.DoesNotContain(new QueueLoom.Infrastructure.Persistence.FileActivityJournal(directory.Path).ReadRecent(),
            record => record.Action == "DLQ detected");
        slowDisk.TrySetResult();
        await journal.DisposeAsync();

        // Read back from disk, as a reopened window does.
        var alert = new QueueLoom.Infrastructure.Persistence.FileActivityJournal(directory.Path).ReadRecent()
            .First(record => record.Action == "DLQ detected");
        Assert.Equal(test.Id, alert.ProfileId);
        Assert.Equal("Test", alert.ProfileName);
        Assert.StartsWith("Test · orders", alert.Details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GlobalScanKeepsThePeekedMessagesTicksAndDraftDestinationOfTheEnvironmentItReturnsTo()
    {
        var development = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var test = CreateProfile("Test", EnvironmentKind.Test);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 2)));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            BrowseMessages =
            [
                SearchMessage(queue.Reference, 1, "2026-10-01T10:00:00Z"),
                SearchMessage(queue.Reference, 2, "2026-10-01T10:01:00Z")
            ],
            Snapshots =
            {
                [development.Id] = Snapshot(development.Id, new DeadLetterEntitySnapshot(queue.Reference, 2)),
                [test.Id] = Snapshot(test.Id, new DeadLetterEntitySnapshot(queue.Reference, 3))
            }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([development, test], development.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == queue.Reference);
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Equal(2, viewModel.Messages.Count);
        viewModel.Messages[1].IsMarked = true;
        viewModel.SelectedMessage = viewModel.Messages[1];
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        Assert.Equal(queue.Reference, viewModel.SelectedDestination?.Reference);
        var browses = workspace.BrowseRequests.Count;

        // A read-only sweep of every environment that ends where it started.
        await viewModel.ScanAllEnvironmentsCommand.ExecuteAsync();

        Assert.False(viewModel.HasError, viewModel.ErrorText);
        Assert.Equal(development.Id, workspace.ConnectedProfileId);
        // Re-reading them is not free: on SQS, Pub/Sub and RabbitMQ every read is another delivery.
        Assert.Equal(2, viewModel.Messages.Count);
        Assert.True(viewModel.Messages[1].IsMarked);
        Assert.Same(viewModel.Messages[1], viewModel.SelectedMessage);
        Assert.Equal(queue.Reference, viewModel.SelectedDestination?.Reference);
        Assert.Equal(browses, workspace.BrowseRequests.Count);
    }

    private sealed class RecordingActivityJournal : IActivityJournal
    {
        private readonly List<ActivityRecord> _records = [];

        public void Append(ActivityRecord record)
        {
            lock (_records) _records.Add(record);
        }

        public ActivityRecord[] Snapshot()
        {
            lock (_records) return [.. _records];
        }

        public IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500) => [];
    }

    private sealed class UnreadableActivityJournal : IActivityJournal
    {
        public List<ActivityRecord> Appended { get; } = [];

        public void Append(ActivityRecord record) => Appended.Add(record);

        public IReadOnlyList<ActivityRecord> ReadRecent(int maximum = 500) =>
            throw new UnauthorizedAccessException("Access to the path 'activity/2026-10-01/638.json' is denied.");
    }
}
