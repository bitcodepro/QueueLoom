using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// When and through what a count was taken travels with it: from the workspace to the scan, history and MCP. Counts of
// another target are never compared, an older point never overrules a newer one, and an approximate-zero confirmation
// needs two fresh, consecutive zeros.
public sealed partial class ViewModelStateTests
{
    // Reader A says ≈10, then reader B says ≈500: no change of ≈+490 anywhere.
    [Fact]
    public async Task ACountThroughAnotherReaderIsComparedWithNothing()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new ReportedDeadLetterWorkspace(QueueLoomPaths.ForRoot(directory.Path)) { Count = 10, Reader = "reader-a" };
        await workspace.ConnectAsync(ReportedProfile());
        var before = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);
        workspace.Count = 500;
        workspace.Reader = "reader-b";
        var after = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        var entity = Assert.Single(after.Entities);
        Assert.Equal("reader-b", entity.MeasuredFrom);
        Assert.Equal("reader-a", entity.PreviousMeasuredFrom);
        Assert.Null(entity.Change);
        Assert.Equal("unknown", new DlqSourceItemViewModel(Guid.NewGuid(), "Test", "TEST", default, entity).Delta);
        Assert.Equal("reader-b", McpMapping.ToInfo(entity).MeasuredFrom);

        using var store = new TemporaryDirectory();
        var history = new JsonLinesDeadLetterHistoryStore(Path.Combine(store.Path, "history.jsonl"));
        await history.AppendAsync(DeadLetterHistorySample.FromSnapshot(before, "Test"));
        await history.AppendAsync(DeadLetterHistorySample.FromSnapshot(after, "Test") with { At = after.CapturedAt.AddMinutes(2) });
        var summary = DeadLetterHistory.Summarize(await history.ReadAsync(before.ProfileId, before.CapturedAt.AddMinutes(-1)),
            before.CapturedAt.AddMinutes(-1), after.CapturedAt.AddMinutes(5))!;
        Assert.True(summary.TargetsChanged);
        Assert.Null(summary.Change);
        Assert.Null(Assert.Single(summary.Sources).Change);
    }

    // ≈100 measured at 10:00, then ≈0 measured at 09:55 (read later): the newer observation stands.
    [Fact]
    public async Task AnOlderPointNeverOverrulesANewerObservation()
    {
        using var directory = new TemporaryDirectory();
        var tenOClock = DateTimeOffset.Parse("2026-10-10T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        await using var workspace = new ReportedDeadLetterWorkspace(QueueLoomPaths.ForRoot(directory.Path)) { Count = 100, Reader = "reader", At = tenOClock };
        await workspace.ConnectAsync(ReportedProfile());
        await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);
        workspace.Count = 0;
        workspace.At = tenOClock.AddMinutes(-5);

        var entity = Assert.Single((await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All)).Entities);

        Assert.Equal(100, entity.Count);
        Assert.Equal(tenOClock, entity.MeasuredAt);
        Assert.Equal(DeadLetterCountQuality.Estimated, entity.CountQuality);
    }

    // ≈40, ≈0, then an unreadable source (or a sampled zero), then ≈0: not two consecutive zeros, so still open. The
    // same zero point read twice is not a second zero either.
    [Theory]
    [InlineData("failed")]
    [InlineData("sampled")]
    [InlineData("same point")]
    public async Task AnInterruptedOrRepeatedZeroDoesNotResolve(string interruption)
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var at = DateTimeOffset.UtcNow;
        DeadLetterSnapshot Of(DeadLetterEntitySnapshot entity) => Snapshot(dev.Id, entity);
        DeadLetterEntitySnapshot Estimated(long count, DateTimeOffset when) =>
            new(Orders, count) { CountQuality = DeadLetterCountQuality.Estimated, MeasuredAt = when, MeasuredFrom = "reader" };
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Of(Estimated(40, at)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        async Task Check() => await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        await Check();

        workspace.Snapshots[dev.Id] = Of(Estimated(0, at.AddMinutes(1)));
        await Check();
        workspace.Snapshots[dev.Id] = interruption switch
        {
            "failed" => Of(new DeadLetterEntitySnapshot(Orders, null, null, "timed out")),
            "sampled" => Of(new DeadLetterEntitySnapshot(Orders, 0) { CountIsLowerBound = true, MeasuredAt = at.AddMinutes(2), MeasuredFrom = "reader" }),
            _ => Of(Estimated(0, at.AddMinutes(1)))
        };
        await Check();
        if (interruption != "same point")
        {
            workspace.Snapshots[dev.Id] = Of(Estimated(0, at.AddMinutes(3)));
            await Check();
        }

        Assert.Single(vm.MonitorNotifications);
    }

    // A sweep in which an environment could not be scanned (or after an earlier exact one) never shows an exact 0.
    [Fact]
    public async Task AFailedEnvironmentInAGlobalScanIsUnknownNotZero()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(Orders, 0)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        Assert.Equal("0", vm.GlobalDlqDisplay);

        workspace.FailNextConnection = true;
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();

        Assert.Equal("unknown", vm.GlobalDlqDisplay);
    }

    private static ServiceBusProfile ReportedProfile() =>
        ServiceBusProfile.CreateNew("Reported", EnvironmentKind.Test, new(AuthenticationKind.KafkaNone)) with { Provider = MessagingProvider.Kafka, Kafka = new("broker.invalid:9092") };

    // Reports an estimated dead-letter count through a reader at a point in time, as Pub/Sub's Cloud Monitoring does.
    private sealed class ReportedDeadLetterWorkspace(QueueLoomPaths paths) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public long Count { get; set; }
        public string? Reader { get; set; }
        public DateTimeOffset? At { get; set; }
        public override MessagingProvider Provider => MessagingProvider.Kafka;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow,
        [
            new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: Count))
                { CountsAreEstimates = true, DeadLetterCountSource = Reader, DeadLetterCountMeasuredAt = At })
        ]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) =>
            throw new InvalidOperationException("A reported count is never sampled.");
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference destination, MessageDraft message, CancellationToken token) =>
            throw new InvalidOperationException("This read-only fixture cannot send.");
    }
}
