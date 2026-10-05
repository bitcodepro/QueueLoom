using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Bug 1. Window B opens with no scheduled resends. Window A (same environment, write access on) then has job J.
    // The environment is deleted through B, which never knew J. A must not send J, and J must not stay pending.
    [Fact]
    public async Task AScheduledResendDoesNotRunAfterItsEnvironmentWasDeletedInAnotherWindow()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var profile = CreateProfile("Shared", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        using (var seed = new JsonProfileRepository(paths))
        {
            await seed.UpsertAsync(profile);
            await seed.SetSelectedProfileIdAsync(profile.Id);
        }
        using var profilesA = new JsonProfileRepository(paths);
        using var profilesB = new JsonProfileRepository(paths);
        var storeA = new JsonScheduledResendStore(paths);
        var storeB = new JsonScheduledResendStore(paths);
        var brokerA = new FakeWorkspace();
        var brokerB = new FakeWorkspace();

        await using var windowB = new MainWindowViewModel(profilesB, new FakeSecretVault(), brokerB,
            new FakeDialogService { ConfirmResult = true }, scheduledResends: storeB);
        await windowB.InitializeAsync();
        Assert.Empty(windowB.ScheduledResends);

        var job = DueJob(profile);
        storeA.Add(job);
        await using var windowA = new MainWindowViewModel(profilesA, new FakeSecretVault(), brokerA,
            new FakeDialogService(), scheduledResends: storeA);
        await windowA.InitializeAsync();
        await windowA.ConnectCommand.ExecuteAsync();
        Assert.Single(windowA.ScheduledResends);

        windowB.SelectedProfile = Assert.Single(windowB.Profiles);
        await windowB.DeleteEnvironmentCommand.ExecuteAsync();
        Assert.Empty(await profilesB.ListAsync());

        await windowA.RunDueScheduledResendsAsync();
        await windowA.RunScheduledNowAsync(windowA.ScheduledResends.FirstOrDefault() ?? CreateOrphanItem(windowA, job));

        Assert.Empty(brokerA.SentMessages);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());
    }

    // Review: the schedule store starts failing after the window loaded (also with no cached jobs). Deleting the
    // environment still removes it from the list, and the warning says its scheduled resends could not all be checked.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeletingAnEnvironmentRefreshesTheListWhenSchedulesCannotBeRead(bool cachedJob)
    {
        var profile = CreateProfile("Doomed", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var repository = new FakeProfileRepository([profile], profile.Id);
        var store = new FailingScheduleStore(cachedJob ? [DueJob(profile)] : []);
        await using var vm = new MainWindowViewModel(repository, new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, scheduledResends: store);
        await vm.InitializeAsync();
        store.Fail = true;

        vm.SelectedProfile = Assert.Single(vm.Profiles);
        await vm.DeleteEnvironmentCommand.ExecuteAsync();

        Assert.Empty(vm.Profiles);
        Assert.Empty(await repository.ListAsync());
        var removed = vm.Activity.First(item => item.Action == "Environment removed");
        Assert.Contains("could not all be checked or cancelled", removed.Details, StringComparison.Ordinal);
    }

    private sealed class FailingScheduleStore(IReadOnlyList<ScheduledResend> jobs) : IScheduledResendStore
    {
        private readonly List<ScheduledResend> _jobs = [.. jobs];
        public bool Fail { get; set; }
        public IReadOnlyList<ScheduledResend> Load() => Fail ? throw new IOException("The schedules file is locked.") : _jobs.ToArray();
        public void Save(IReadOnlyList<ScheduledResend> resends) { if (Fail) throw new IOException("The schedules file is locked."); _jobs.Clear(); _jobs.AddRange(resends); }
        public void Add(ScheduledResend resend) { if (Fail) throw new IOException("The schedules file is locked."); _jobs.Add(resend); }
        public bool TryRemove(ScheduledResend expected) => Fail ? throw new IOException("The schedules file is locked.") : _jobs.RemoveAll(job => job.Id == expected.Id) > 0;
    }

    // Review: window B cached job J; window A cancels it; then B deletes the environment. The saved list no longer has
    // J, so B drops its stale row too, along with the environment.
    [Fact]
    public async Task DeletingAnEnvironmentDropsScheduleRowsAnotherWindowAlreadyCancelled()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var profile = CreateProfile("Shared", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        using (var seed = new JsonProfileRepository(paths))
        {
            await seed.UpsertAsync(profile);
            await seed.SetSelectedProfileIdAsync(profile.Id);
        }
        var store = new JsonScheduledResendStore(paths);
        store.Add(DueJob(profile));
        using var profilesA = new JsonProfileRepository(paths);
        using var profilesB = new JsonProfileRepository(paths);
        await using var windowA = new MainWindowViewModel(profilesA, new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService(), scheduledResends: new JsonScheduledResendStore(paths));
        await using var windowB = new MainWindowViewModel(profilesB, new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, scheduledResends: new JsonScheduledResendStore(paths));
        await windowA.InitializeAsync();
        await windowB.InitializeAsync();
        Assert.Single(windowB.ScheduledResends);

        windowA.CancelScheduledResendCommand.Execute(Assert.Single(windowA.ScheduledResends));
        windowB.SelectedProfile = Assert.Single(windowB.Profiles);
        await windowB.DeleteEnvironmentCommand.ExecuteAsync();

        Assert.Empty(windowB.Profiles);
        Assert.Empty(windowB.ScheduledResends);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());
    }

    private static ScheduledResendItemViewModel CreateOrphanItem(MainWindowViewModel vm, ScheduledResend job) =>
        new(job, vm.RunScheduledResendCommand, vm.CancelScheduledResendCommand);
}
