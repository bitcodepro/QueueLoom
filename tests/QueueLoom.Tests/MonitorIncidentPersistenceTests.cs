using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The monitor remembers what it already alerted across restarts: an update restart or a reboot does not alert every
// non-empty dead-letter queue again.
public sealed partial class ViewModelStateTests
{
    private static MainWindowViewModel CreateMonitoringViewModel(
        ServiceBusProfile profile, FakeWorkspace workspace, RecordingAlerts alerts, IMonitorIncidentStore incidents) =>
        new(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), workspace, new FakeDialogService(),
            alerts: alerts, monitorIncidents: incidents);

    private static Task<bool> CheckOnceAsync(MainWindowViewModel vm) =>
        (Task<bool>)typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(vm, [CancellationToken.None])!;

    private static async Task<MainWindowViewModel> StartMonitoringAsync(
        ServiceBusProfile profile, FakeWorkspace workspace, RecordingAlerts alerts, IMonitorIncidentStore incidents)
    {
        var vm = CreateMonitoringViewModel(profile, workspace, alerts, incidents);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        return vm;
    }

    [Fact]
    public async Task Monitor_DoesNotAlertAgainAfterARestartForWhatItAlreadyAlerted()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonMonitorIncidentStore(QueueLoomPaths.ForRoot(directory.Path));
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3)) }
        };
        var alerts = new RecordingAlerts();
        await using (var first = await StartMonitoringAsync(dev, workspace, alerts, store))
        {
            await CheckOnceAsync(first);
            await WaitUntilAsync(() => alerts.System.Count == 1);
            await first.MonitorIncidentsSaved;
        }

        // The restarted app: same data folder, same counts.
        await using var second = await StartMonitoringAsync(dev, workspace, alerts, new JsonMonitorIncidentStore(QueueLoomPaths.ForRoot(directory.Path)));
        await CheckOnceAsync(second);
        await Task.Delay(200);
        Assert.Single(alerts.System);
        // Already alerted before the restart: kept, but not listed until it alerts again.
        Assert.Empty(second.MonitorNotifications);

        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 5));
        await CheckOnceAsync(second);

        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Equal(5, alerts.System[1].Count);
        Assert.Equal(3, alerts.System[1].PreviousCount);
        Assert.Single(second.MonitorNotifications);
    }

    [Fact]
    public async Task Monitor_StartsWithoutIncidentsFromADamagedFile()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await File.WriteAllTextAsync(Path.Combine(paths.RootDirectory, "monitor-incidents.json"), "{ not json");
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3)) }
        };
        var alerts = new RecordingAlerts();
        await using var vm = await StartMonitoringAsync(dev, workspace, alerts, new JsonMonitorIncidentStore(paths));

        await CheckOnceAsync(vm);

        await WaitUntilAsync(() => alerts.System.Count == 1);
        Assert.Null(alerts.System[0].PreviousCount);
        await vm.MonitorIncidentsSaved;
        // The damaged file is replaced by the incidents of now.
        Assert.Equal(3, Assert.Single(await new JsonMonitorIncidentStore(paths).LoadAsync()).Count);
    }

    [Fact]
    public async Task Monitor_ForgetsAResolvedIncidentAcrossARestart()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3)) }
        };
        var alerts = new RecordingAlerts();
        await using (var first = await StartMonitoringAsync(dev, workspace, alerts, new JsonMonitorIncidentStore(paths)))
        {
            await CheckOnceAsync(first);
            await WaitUntilAsync(() => alerts.System.Count == 1);
            workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 0));
            await CheckOnceAsync(first);
            await first.MonitorIncidentsSaved;
        }
        Assert.Empty(await new JsonMonitorIncidentStore(paths).LoadAsync());

        // Back after the restart: a new incident, alerted as found.
        workspace.Snapshots[dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3));
        await using var second = await StartMonitoringAsync(dev, workspace, alerts, new JsonMonitorIncidentStore(paths));
        await CheckOnceAsync(second);
        await WaitUntilAsync(() => alerts.System.Count == 2);
        Assert.Null(alerts.System[1].PreviousCount);
    }

    [Fact]
    public async Task IncidentStore_IgnoresUnknownEnvironmentsAndOldFiles()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "monitor-incidents.json");
        var now = DateTimeOffset.Parse("2026-10-20T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var clock = new MutableTimeProvider(now);
        var store = new JsonMonitorIncidentStore(path, clock);
        var incident = new MonitorIncident($"{Guid.NewGuid():N}|orders", ServiceBusEntityReference.Queue("orders"), "DLQ", 3,
            DeadLetterCountQuality.Exact, now, now);
        await store.SaveAsync([incident, incident with { Key = "not a profile" }]);

        Assert.Equal(incident.Key, Assert.Single(await store.LoadAsync()).Key);
        clock.Now = now + LocalHistoryRetention.Period + TimeSpan.FromMinutes(1);
        Assert.Empty(await store.LoadAsync());
    }

    private sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;
        public override DateTimeOffset GetUtcNow() => Now;
    }
}
